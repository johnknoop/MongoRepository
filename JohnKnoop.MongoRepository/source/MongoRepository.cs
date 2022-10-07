using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using JohnKnoop.MongoRepository.Extensions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace JohnKnoop.MongoRepository
{
	internal class DeletedObject<TEntity> : SoftDeletedEntity<TEntity>
	{
		public DeletedObject(TEntity entity, string sourceCollectionName, DateTime timestampDeletedUtc) : base(entity, timestampDeletedUtc)
		{
			SourceCollectionName = sourceCollectionName;
			TypeName = entity.GetType().Name;
		}

		public string TypeName { get; private set; }
		public string SourceCollectionName { get; private set; }
	}

	public enum ReturnedDocumentState
	{
		BeforeUpdate = 0,
		AfterUpdate = 1
	}

	public static class MongoRepository
	{
		public static MongoConfigurationBuilder Configure()
		{
			return new MongoConfigurationBuilder();
		}

		/// <summary>
		/// WHen using transactions,
		/// collections cannot be created implicitly at first write operation.
		/// </summary>
		/// <param name="tenantKey"></param>
		/// <returns></returns>
		internal static void EnsureCollectionsCreated(IMongoClient mongoClient, string tenantKey = null)
		{
			MongoConfiguration.EnsureCollectionsCreated(mongoClient, tenantKey);
		}

		public static IList<string> GetDatabaseNames(string tenantKey = null) =>
			MongoConfiguration.GetDatabaseNames(tenantKey);

		public static IList<Type> GetMappedTypes() => MongoConfiguration.GetMappedTypes();

		public static string GetDatabaseName(Type entityType, string tenantKey = null) =>
			MongoConfiguration.GetDatabaseName(entityType, tenantKey);
	}

	public class MongoRepository<TEntity> : IRepository<TEntity>
	{
		protected IMongoCollection<TEntity> MongoCollection;
		private readonly IMongoCollection<SoftDeletedEntity<TEntity>> _trash;

		private readonly bool _autoEnlistWithCurrentTransactionScope;

		/// <summary>
		/// Only exists to be able to create collections eagerly
		/// </summary>
		private readonly string _tenantKey;

		internal IMongoCollection<TEntity> Collection => this.MongoCollection;

		internal MongoRepository(IMongoCollection<TEntity> mongoCollection, IMongoCollection<SoftDeletedEntity<TEntity>> trash, string tenantKey, bool autoEnlistWithCurrentTransactionScope = false)
		{
			this.MongoCollection = mongoCollection;
			this._trash = trash;
			_tenantKey = tenantKey;
			_autoEnlistWithCurrentTransactionScope = autoEnlistWithCurrentTransactionScope;
		}

		public IRepository<TEntity> WithReadPreference(ReadPreference readPreference)
		{
			return new MongoRepository<TEntity>(
				mongoCollection: this.MongoCollection.WithReadPreference(readPreference),
				trash: _trash.WithReadPreference(readPreference),
				tenantKey: _tenantKey,
				autoEnlistWithCurrentTransactionScope: _autoEnlistWithCurrentTransactionScope);
		}

		public IRepository<TEntity> WithReadConcern(ReadConcern readConcern)
		{
			return new MongoRepository<TEntity>(
				mongoCollection: this.MongoCollection.WithReadConcern(readConcern),
				trash: _trash.WithReadConcern(readConcern),
				tenantKey: _tenantKey,
				autoEnlistWithCurrentTransactionScope: _autoEnlistWithCurrentTransactionScope);
		}

		public IRepository<TEntity> WithWriteConcern(WriteConcern writeConcern)
		{
			return new MongoRepository<TEntity>(
				mongoCollection: this.MongoCollection.WithWriteConcern(writeConcern),
				trash: _trash.WithWriteConcern(writeConcern),
				tenantKey: _tenantKey,
				autoEnlistWithCurrentTransactionScope: _autoEnlistWithCurrentTransactionScope);
		}

		public async Task DeletePropertyAsync(Expression<Func<TEntity, bool>> filterExpression, Expression<Func<TEntity, object>> propertyExpression)
		{
			var updateDefinition = Builders<TEntity>.Update.Unset(propertyExpression);

			await (SessionContainer.AmbientSession != null
				? MongoCollection.FindOneAndUpdateAsync(SessionContainer.AmbientSession, filterExpression, updateDefinition).ConfigureAwait(false)
				: MongoCollection.FindOneAndUpdateAsync(filterExpression, updateDefinition).ConfigureAwait(false));
		}

		public async Task<TEntity> GetFromTrashAsync(Expression<Func<SoftDeletedEntity<TEntity>, bool>> filter)
		{
			if (filter == null) throw new ArgumentNullException(nameof(filter));

			var deletedObject = await _trash.OfType<SoftDeletedEntity<TEntity>>().Find(filter).SingleOrDefaultAsync();

			if (deletedObject == null)
			{
				throw new Exception($"No object in the trash matches filter");
			}

			await this.InsertAsync(deletedObject.Entity);

			return deletedObject.Entity;
		}

		public async Task InsertAsync(TEntity entity)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			await (SessionContainer.AmbientSession != null
				? this.MongoCollection.InsertOneAsync(SessionContainer.AmbientSession, entity).ConfigureAwait(false)
				: this.MongoCollection.InsertOneAsync(entity).ConfigureAwait(false));
		}

		public IAggregateFluent<TEntity> Aggregate(AggregateOptions options = null)
		{
			return this.MongoCollection.Aggregate(options);
		}

		public async Task<IList<SoftDeletedEntity>> ListTrashAsync(int? offset = null, int? limit = null)
		{
			var result = await this._trash.Find(x => true)
				.Skip(offset)
				.Limit(limit)
				.As<DeletedObject<TEntity>>()
				.ToListAsync()
				.ConfigureAwait(false);

			return result.Select(x => new SoftDeletedEntity(x.TypeName, x.SourceCollectionName, x.TimestampDeletedUtc))
				.ToList();
		}

		public async Task<ReplaceOneResult> ReplaceOneAsync(string id, TEntity entity, bool upsert = false)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			if (id == null) throw new ArgumentNullException(nameof(id));

			var filter = Builders<TEntity>.Filter.Eq("_id", ObjectId.Parse(id));

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.ReplaceOneAsync(SessionContainer.AmbientSession, filter, entity, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false)
				: await this.MongoCollection.ReplaceOneAsync(filter, entity, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
		}

		public async Task<ReplaceOneResult> ReplaceOneAsync(Expression<Func<TEntity, bool>> filter, TEntity entity, bool upsert = false)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.ReplaceOneAsync(SessionContainer.AmbientSession, filter, entity, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false)
				: await this.MongoCollection.ReplaceOneAsync(filter, entity, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
		}

		public Task<TReturnProjection> FindOneAndUpdateAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, Expression<Func<TEntity, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.BeforeUpdate, bool upsert = false)
		{
			return FindOneAndUpdateAsyncImpl<TReturnProjection>(filter, update(Builders<TEntity>.Update), Builders<TEntity>.Projection.Expression(returnProjection), returnedDocumentState, upsert);
		}

		public Task<TReturnProjection> FindOneAndUpdateAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, UpdateDefinition<TEntity>update, Expression<Func<TEntity, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false)
		{
			return FindOneAndUpdateAsyncImpl(filter, update, Builders<TEntity>.Projection.Expression(returnProjection), returnedDocumentState, upsert);
		}

		public Task<TReturnProjection> FindOneAndUpdateAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, ProjectionDefinition<TEntity, TReturnProjection> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false)
		{
			return FindOneAndUpdateAsyncImpl(filter, update(Builders<TEntity>.Update), returnProjection, returnedDocumentState, upsert);
		}

		public Task<TReturnProjection> FindOneAndUpdateAsync<TDerived, TReturnProjection>(Expression<Func<TDerived, bool>> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, Expression<Func<TDerived, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false) where TDerived : TEntity
		{
			return FindOneAndUpdateDerivedAsyncImpl(filter, update(Builders<TDerived>.Update), Builders<TDerived>.Projection.Expression(returnProjection), returnedDocumentState, upsert);
		}

		public Task<TReturnProjection> FindOneAndUpdateAsync<TDerived, TReturnProjection>(Expression<Func<TDerived, bool>> filter, UpdateDefinition<TDerived> update, Expression<Func<TDerived, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false) where TDerived : TEntity
		{
			return FindOneAndUpdateDerivedAsyncImpl(filter, update, Builders<TDerived>.Projection.Expression(returnProjection), returnedDocumentState, upsert);
		}

		private async Task<TReturnProjection> FindOneAndUpdateAsyncImpl<TReturnProjection>(Expression<Func<TEntity, bool>> filter, UpdateDefinition<TEntity> update, ProjectionDefinition<TEntity, TReturnProjection> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var returnDocument = returnedDocumentState == ReturnedDocumentState.BeforeUpdate
				? ReturnDocument.Before
				: ReturnDocument.After;

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.FindOneAndUpdateAsync(
					SessionContainer.AmbientSession,
					filter,
					update,
					new FindOneAndUpdateOptions<TEntity, TReturnProjection>
					{
						Projection = returnProjection,
						ReturnDocument = returnDocument,
						IsUpsert = upsert
					}).ConfigureAwait(false)
			: await this.MongoCollection.FindOneAndUpdateAsync(
					filter,
					update,
					new FindOneAndUpdateOptions<TEntity, TReturnProjection>
					{
						Projection = returnProjection,
						ReturnDocument = returnDocument,
						IsUpsert = upsert
					}).ConfigureAwait(false);
		}

		public Task<TReturnProjection> FindOneAndUpdateAsync<TDerived, TReturnProjection>(Expression<Func<TDerived, bool>> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, ProjectionDefinition<TDerived, TReturnProjection> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false) where TDerived : TEntity
		{
			return FindOneAndUpdateDerivedAsyncImpl(filter, update(Builders<TDerived>.Update), returnProjection, returnedDocumentState, upsert);
		}

		private async Task<TReturnProjection> FindOneAndUpdateDerivedAsyncImpl<TDerived, TReturnProjection>(Expression<Func<TDerived, bool>> filter, UpdateDefinition<TDerived> update, ProjectionDefinition<TDerived, TReturnProjection> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false) where TDerived : TEntity
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var returnDocument = returnedDocumentState == ReturnedDocumentState.BeforeUpdate
				? ReturnDocument.Before
				: ReturnDocument.After;

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.OfType<TDerived>().FindOneAndUpdateAsync(
					SessionContainer.AmbientSession,
					filter,
					update,
					new FindOneAndUpdateOptions<TDerived, TReturnProjection>
					{
						Projection = returnProjection,
						ReturnDocument = returnDocument,
						IsUpsert = upsert
					}).ConfigureAwait(false)
				: await this.MongoCollection.OfType<TDerived>().FindOneAndUpdateAsync(
					filter,
					update,
					new FindOneAndUpdateOptions<TDerived, TReturnProjection>
					{
						Projection = returnProjection,
						ReturnDocument = returnDocument,
						IsUpsert = upsert
					}).ConfigureAwait(false);
		}

		public Task<TReturnProjection> FindOneAndReplaceAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, TEntity replacement, Expression<Func<TEntity, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false)
		{
			return FindOneAndReplaceAsync(filter, replacement, Builders<TEntity>.Projection.Expression(returnProjection), returnedDocumentState, upsert);
		}

		public async Task<TReturnProjection> FindOneAndReplaceAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, TEntity replacement, ProjectionDefinition<TEntity, TReturnProjection> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var returnDocument = returnedDocumentState == ReturnedDocumentState.BeforeUpdate
				? ReturnDocument.Before
				: ReturnDocument.After;

			return await this.MongoCollection.FindOneAndReplaceAsync(
				filter,
				replacement,
				new FindOneAndReplaceOptions<TEntity, TReturnProjection>
				{
					Projection = returnProjection,
					ReturnDocument = returnDocument,
					IsUpsert = upsert
				})
				.ConfigureAwait(false);
		}

		public async Task<TReturnProjection> FindOneOrInsertAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, TEntity entity, Expression<Func<TEntity, TReturnProjection>> returnProjection,
			ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var returnDocument = returnedDocumentState == ReturnedDocumentState.BeforeUpdate
				? ReturnDocument.Before
				: ReturnDocument.After;

			return await MongoCollection
				.FindOneAndUpdateAsync<TEntity, TReturnProjection>(
					filter: filter,
					update: new BsonDocumentUpdateDefinition<TEntity>(new BsonDocument("$setOnInsert",
						entity.ToBsonDocument(MongoCollection.DocumentSerializer))),
					options: new FindOneAndUpdateOptions<TEntity, TReturnProjection>
					{
						ReturnDocument = ReturnDocument.After,
						IsUpsert = true,
						Projection = Builders<TEntity>.Projection.Expression(returnProjection)
					}
				);
		}

		public Task<UpdateResult> UpdateOneAsync<TDerived>(string id,
			Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false)
			where TDerived : TEntity
				=> UpdateOneAsync<TDerived>(id, update, new UpdateOptions { IsUpsert = upsert });

		public async Task<UpdateResult> UpdateOneAsync<TDerived>(string id, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, UpdateOptions options) where TDerived : TEntity
		{
			if (id == null) throw new ArgumentNullException(nameof(id));

			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var filter = Builders<TDerived>.Filter.Eq("_id", ObjectId.Parse(id));

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.OfType<TDerived>().UpdateOneAsync(SessionContainer.AmbientSession, filter, update(Builders<TDerived>.Update), options).ConfigureAwait(false)
				: await this.MongoCollection.OfType<TDerived>().UpdateOneAsync(filter, update(Builders<TDerived>.Update), options).ConfigureAwait(false);
		}

		public Task<UpdateResult> UpdateOneAsync<TDerived>(Expression<Func<TDerived, bool>> filter,
			Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false)
			where TDerived : TEntity
				=> UpdateOneAsync((FilterDefinition<TDerived>)filter, update, upsert);

		public Task<UpdateResult> UpdateOneAsync<TDerived>(FilterDefinition<TDerived> filter,
			Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false)
			where TDerived : TEntity
				=> UpdateOneAsync<TDerived>(filter, update, new UpdateOptions { IsUpsert = upsert });

		public Task<UpdateResult> UpdateOneAsync<TDerived>(Expression<Func<TDerived, bool>> filter,
			Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, UpdateOptions options)
			where TDerived : TEntity =>
				UpdateOneAsync((FilterDefinition<TDerived>)filter, update, options);

		public async Task<UpdateResult> UpdateOneAsync<TDerived>(FilterDefinition<TDerived> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, UpdateOptions options) where TDerived : TEntity
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.OfType<TDerived>().UpdateOneAsync(SessionContainer.AmbientSession, filter, update(Builders<TDerived>.Update), options).ConfigureAwait(false)
				: await this.MongoCollection.OfType<TDerived>().UpdateOneAsync(filter, update(Builders<TDerived>.Update), options).ConfigureAwait(false);
		}

		public Task<UpdateResult> UpdateOneAsync(string id,
			Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false) =>
				UpdateOneAsync(id, update, new UpdateOptions { IsUpsert = upsert });

		public async Task<UpdateResult> UpdateOneAsync(string id, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, UpdateOptions options)
		{
			if (id == null) throw new ArgumentNullException(nameof(id));

			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var filter = Builders<TEntity>.Filter.Eq("_id", ObjectId.Parse(id));

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.UpdateOneAsync(SessionContainer.AmbientSession, filter, update(Builders<TEntity>.Update), options).ConfigureAwait(false)
				: await this.MongoCollection.UpdateOneAsync(filter, update(Builders<TEntity>.Update), options).ConfigureAwait(false);
		}

		public Task<UpdateResult> UpdateOneAsync(Expression<Func<TEntity, bool>> filter,
			Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false) =>
				UpdateOneAsync((FilterDefinition<TEntity>)filter, update, upsert);

		public Task<UpdateResult> UpdateOneAsync(FilterDefinition<TEntity> filter,
			Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false) =>
				UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = upsert });

		public Task<UpdateResult> UpdateOneAsync(Expression<Func<TEntity, bool>> filter,
			Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, UpdateOptions options) =>
				UpdateOneAsync((FilterDefinition<TEntity>)filter, update, options);

		public async Task<UpdateResult> UpdateOneAsync(FilterDefinition<TEntity> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, UpdateOptions options)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.UpdateOneAsync(SessionContainer.AmbientSession, filter, update(Builders<TEntity>.Update), options).ConfigureAwait(false)
				: await this.MongoCollection.UpdateOneAsync(filter, update(Builders<TEntity>.Update), options).ConfigureAwait(false);
		}

		public async Task<UpdateResult> UpdateOneAsync(string filter, string update, bool upsert = false)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.UpdateOneAsync(SessionContainer.AmbientSession, filter, update, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false)
				: await this.MongoCollection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes multiple update operations in one batch
		/// </summary>
		public async Task<BulkWriteResult<TEntity>> UpdateOneBulkAsync(IEnumerable<UpdateOneCommand<TEntity>> commands)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var cmds = commands.Select(cmd =>
				new UpdateOneModel<TEntity>(cmd.Filter(Builders<TEntity>.Filter), cmd.UpdateJson ?? cmd.Update(Builders<TEntity>.Update))
				{
					IsUpsert = cmd.IsUpsert
				}).ToList();

			if (!cmds.Any())
			{
				return null;
			}

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.BulkWriteAsync(SessionContainer.AmbientSession, cmds).ConfigureAwait(false)
				: await this.MongoCollection.BulkWriteAsync(cmds).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes multiple update operations in one batch
		/// </summary>
		public async Task<BulkWriteResult<TDerived>> UpdateOneBulkAsync<TDerived>(IEnumerable<UpdateOneCommand<TDerived>> commands) where TDerived : TEntity
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var cmds = commands.Select(cmd =>
				new UpdateOneModel<TDerived>(
					filter: cmd.Filter(Builders<TDerived>.Filter),
					update: cmd.UpdateJson ?? cmd.Update(Builders<TDerived>.Update)
				)
				{
					IsUpsert = cmd.IsUpsert
				}
			).ToList();

			if (cmds.Any())
			{
				return SessionContainer.AmbientSession != null
					? await this.MongoCollection.OfType<TDerived>().BulkWriteAsync(SessionContainer.AmbientSession, cmds).ConfigureAwait(false)
					: await this.MongoCollection.OfType<TDerived>().BulkWriteAsync(cmds).ConfigureAwait(false);
			}

			return new NoBulkWriteResult<TDerived>();
		}

		/// <summary>
		/// Applies the same update to multiple entities
		/// </summary>
		public async Task UpdateManyAsync(Expression<Func<TEntity, bool>> filter,
			Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update,
			UpdateOptions options = null)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			await (SessionContainer.AmbientSession != null
				? this.MongoCollection.UpdateManyAsync(SessionContainer.AmbientSession, filter, update(Builders<TEntity>.Update), options).ConfigureAwait(false)
				: this.MongoCollection.UpdateManyAsync(filter, update(Builders<TEntity>.Update), options).ConfigureAwait(false));
		}

		/// <summary>
		/// Applies the same update to multiple entities
		/// </summary>
		public async Task UpdateManyAsync<TDerived>(Expression<Func<TDerived, bool>> filter,
			Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update,
			UpdateOptions options = null)  where TDerived : TEntity
		{
			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			await (SessionContainer.AmbientSession != null
				? this.MongoCollection.OfType<TDerived>().UpdateManyAsync(SessionContainer.AmbientSession, filter, update(Builders<TDerived>.Update), options).ConfigureAwait(false)
				: this.MongoCollection.OfType<TDerived>().UpdateManyAsync(filter, update(Builders<TDerived>.Update), options).ConfigureAwait(false));
		}

		public async Task<long?> GetCounterValueAsync(string name = null)
		{
			var counterCollectionName = "_counters";

			var fieldName = MongoCollection.CollectionNamespace.CollectionName;

			if (name != null)
			{
				fieldName += $"_{name}";
			}

			var collection = MongoCollection.Database.GetCollection<BsonDocument>(counterCollectionName);
			var fieldDefinition = new StringFieldDefinition<BsonDocument, long>(fieldName);

			var result = await collection.Find(x => true)
				.Project(Builders<BsonDocument>.Projection.Include(fieldDefinition))
				.SingleOrDefaultAsync()
				.ConfigureAwait(false);

			return result?.TryGetElement(fieldName, out var element) ?? false
				? element.Value?.ToInt64()
				: null;
		}

		public async Task<long> SetCounterValueIfGreaterAsync(long newValue, string name = null)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			var counterCollectionName = "_counters";

			var fieldName = MongoCollection.CollectionNamespace.CollectionName;

			if (name != null)
			{
				fieldName += $"_{name}";
			}

			var collection = MongoCollection.Database.GetCollection<BsonDocument>(counterCollectionName);
			var fieldDefinition = new StringFieldDefinition<BsonDocument, long>(fieldName);

			if (SessionContainer.AmbientSession != null)
			{
				var newCounterState = await collection.FindOneAndUpdateAsync<BsonDocument>(
					session: SessionContainer.AmbientSession,
					filter: x => true,
					update: Builders<BsonDocument>.Update.Max(fieldDefinition, newValue),
					options: new FindOneAndUpdateOptions<BsonDocument, BsonDocument>
					{
						Projection = Builders<BsonDocument>.Projection.Include(fieldDefinition),
						ReturnDocument = ReturnDocument.After,
						IsUpsert = true
					}
				).ConfigureAwait(false);

				return newCounterState.GetElement(fieldName).Value.AsInt64;
			}
			else
			{
				var newCounterState = await collection.FindOneAndUpdateAsync<BsonDocument>(
					filter: x => true,
					update: Builders<BsonDocument>.Update.Max(fieldDefinition, newValue),
					options: new FindOneAndUpdateOptions<BsonDocument, BsonDocument>
					{
						Projection = Builders<BsonDocument>.Projection.Include(fieldDefinition),
						ReturnDocument = ReturnDocument.After,
						IsUpsert = true
					}
				).ConfigureAwait(false);

				return newCounterState.GetElement(fieldName).Value.AsInt64;
			}
		}

		public async Task ResetCounterAsync(string name = null, long newValue = 0)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			var counterCollectionName = "_counters";

			var fieldName = MongoCollection.CollectionNamespace.CollectionName;

			if (name != null)
			{
				fieldName += $"_{name}";
			}

			var collection = MongoCollection.Database.GetCollection<BsonDocument>(counterCollectionName);
			var fieldDefinition = new StringFieldDefinition<BsonDocument, long>(fieldName);

			if (SessionContainer.AmbientSession != null)
			{
				await collection.UpdateOneAsync(
					session: SessionContainer.AmbientSession,
					filter: x => true,
					update: Builders<BsonDocument>.Update.Set(fieldDefinition, newValue),
					options: new UpdateOptions
					{
						IsUpsert = true
					}
				).ConfigureAwait(false);
			}
			else
			{
				await collection.UpdateOneAsync(
					filter: x => true,
					update: Builders<BsonDocument>.Update.Set(fieldDefinition, newValue),
					options: new UpdateOptions
					{
						IsUpsert = true
					}
				).ConfigureAwait(false);
			}
		}

		public async Task<long> IncrementCounterAsync(string name = null, int incrementBy = 1)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			var counterCollectionName = "_counters";

			var fieldName = MongoCollection.CollectionNamespace.CollectionName;

			if (name != null)
			{
				fieldName += $"_{name}";
			}

			var collection = MongoCollection.Database.GetCollection<BsonDocument>(counterCollectionName);
			var fieldDefinition = new StringFieldDefinition<BsonDocument, long>(fieldName);

			if (SessionContainer.AmbientSession != null)
			{
				var result = await collection.FindOneAndUpdateAsync<BsonDocument>(
					session: SessionContainer.AmbientSession,
					filter: x => true,
					update: Builders<BsonDocument>.Update.Inc(fieldDefinition, incrementBy),
					options: new FindOneAndUpdateOptions<BsonDocument>
					{
						ReturnDocument = ReturnDocument.After,
						IsUpsert = true,
						Projection = Builders<BsonDocument>.Projection.Include(fieldDefinition)
					}
				).ConfigureAwait(false);

				return result.GetElement(fieldName).Value.ToInt64();
			}
			else
			{
				var result = await collection.FindOneAndUpdateAsync<BsonDocument>(
					filter: x => true,
					update: Builders<BsonDocument>.Update.Inc(fieldDefinition, incrementBy),
					options: new FindOneAndUpdateOptions<BsonDocument>
					{
						ReturnDocument = ReturnDocument.After,
						IsUpsert = true,
						Projection = Builders<BsonDocument>.Projection.Include(fieldDefinition)
					}
				).ConfigureAwait(false);

				return result.GetElement(fieldName).Value.ToInt64();
			}
		}

		public async Task<BulkWriteResult<TEntity>> ReplaceManyAsync(IList<ReplaceManyCommand<TEntity>> commands, bool upsert = false)
		{
			if (!commands.Any())
			{
				throw new ArgumentException("At least one command must be provided");
			}

			TryAutoEnlistWithCurrentTransactionScope();

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.BulkWriteAsync(SessionContainer.AmbientSession, commands.Select(cmd => new ReplaceOneModel<TEntity>(cmd.Filter(Builders<TEntity>.Filter), cmd.Replacement)
				{
					IsUpsert = upsert
				})).ConfigureAwait(false)
				: await this.MongoCollection.BulkWriteAsync(commands.Select(cmd => new ReplaceOneModel<TEntity>(cmd.Filter(Builders<TEntity>.Filter), cmd.Replacement)
				{
					IsUpsert = upsert
				})).ConfigureAwait(false);
		}

		public async Task InsertManyAsync(ICollection<TEntity> entities)
		{
			if (entities.Any())
			{
				TryAutoEnlistWithCurrentTransactionScope();

				await MongoConfiguration.EnsureIndexesAndCap(MongoCollection).ConfigureAwait(false);

				await (SessionContainer.AmbientSession != null
					? this.MongoCollection.InsertManyAsync(SessionContainer.AmbientSession, entities).ConfigureAwait(false)
					: this.MongoCollection.InsertManyAsync(entities).ConfigureAwait(false));
			}
		}

		public async Task InsertManyAsync<TDerivedEntity>(ICollection<TDerivedEntity> entities) where TDerivedEntity : TEntity
		{
			if (entities.Any())
			{
				TryAutoEnlistWithCurrentTransactionScope();

				await MongoConfiguration.EnsureIndexesAndCap(MongoCollection).ConfigureAwait(false);

				await (SessionContainer.AmbientSession != null
					? this.MongoCollection.OfType<TDerivedEntity>().InsertManyAsync(SessionContainer.AmbientSession, entities).ConfigureAwait(false)
					: this.MongoCollection.OfType<TDerivedEntity>().InsertManyAsync(entities).ConfigureAwait(false));
			}
		}

		public IFindFluent<TEntity, TEntity> GetAll(FindOptions options = null)
		{
			return this.MongoCollection.Find(FilterDefinition<TEntity>.Empty, options);
		}

		public async Task<TEntity> GetFromTrashAsync(string objectId)
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			var filter = new BsonDocument("_id", ObjectId.Parse(objectId));

			var deletedObject = await _trash.Find(filter).SingleOrDefaultAsync().ConfigureAwait(false);

			if (deletedObject == null)
			{
				throw new Exception($"No document of type {typeof(TEntity).Name} with id {objectId} was found in the trash");
			}

			return deletedObject.Entity;
		}

		public async Task<TEntity> RestoreSoftDeletedAsync(string objectId)
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			TryAutoEnlistWithCurrentTransactionScope();

			return await WithTransaction(async session => { 
				var filter = new BsonDocument("Entity._id", ObjectId.Parse(objectId));
				var deletedObject = await _trash.FindOneAndDeleteAsync(session, filter);

				if (deletedObject == null)
				{
					throw new Exception($"No document with id {objectId} was found in the trash");
				}

				await this.MongoCollection.InsertOneAsync(session, deletedObject.Entity).ConfigureAwait(false);
				return deletedObject.Entity;
			});
		}

		public async Task<IList<TEntity>> RestoreSoftDeletedAsync(Expression<Func<SoftDeletedEntity<TEntity>, bool>> filter)
		{
			if (filter == null) throw new ArgumentNullException(nameof(filter));
			
			TryAutoEnlistWithCurrentTransactionScope();

			var deletedObjects = await _trash.Find(filter).ToListAsync();

			if (deletedObjects.Count == 0)
			{
				return new List<TEntity>(0);
			}

			var deletedEntities = deletedObjects.Select(x => x.Entity).ToList();

			await WithTransaction(async session =>
			{
				await MongoCollection.InsertManyAsync(session, deletedEntities);
				await this._trash.DeleteManyAsync(session, filter);
			});

			return deletedEntities;
		}

		public async Task<IList<TDerived>> RestoreSoftDeletedAsync<TDerived>(Expression<Func<SoftDeletedEntity<TDerived>, bool>> filter) where TDerived : TEntity
		{
			if (filter == null) throw new ArgumentNullException(nameof(filter));
			
			TryAutoEnlistWithCurrentTransactionScope();

			var serializerRegistry = BsonSerializer.SerializerRegistry;
			var documentSerializer = serializerRegistry.GetSerializer<SoftDeletedEntity<TDerived>>();

			var filterBson = Builders<SoftDeletedEntity<TDerived>>.Filter.Where(filter).Render(documentSerializer, serializerRegistry);

			var deletedObjects = await _trash.Find(filterBson).ToListAsync();

			if (deletedObjects.Count == 0)
			{
				return new List<TDerived>(0);
			}

			var deletedEntities = deletedObjects.Select(x => x.Entity).OfType<TDerived>().ToList();

			await WithTransaction(async (session) =>
			{
				foreach (var deletedEntity in deletedEntities)
				{
					await MongoCollection.InsertOneAsync(session, deletedEntity);
				}

				await this._trash.DeleteManyAsync(session, filterBson);
			});

			return deletedEntities;
		}

		private async Task<TOperationReturnType> WithTransaction<TOperationReturnType>(Func<IClientSessionHandle, Task<TOperationReturnType>> operation)
		{
			if (SessionContainer.AmbientSession != null)
			{
				return await operation(SessionContainer.AmbientSession);
			}
			else
			{
				using (var trans = StartTransaction())
				{
					var result = await operation(trans.Session);
					await trans.CommitAsync();

					return result;
				}
			}
		}

		private async Task WithTransaction(Func<IClientSessionHandle, Task> operation)
		{
			if (SessionContainer.AmbientSession != null)
			{
				await operation(SessionContainer.AmbientSession);
			}
			else
			{
				using (var trans = StartTransaction())
				{
					await operation(trans.Session);
					await trans.CommitAsync();
				}
			}
		}

		public async Task<DeleteResult> DeleteManyAsync(Expression<Func<TEntity, bool>> filter, bool softDelete = false)
		{
			TryAutoEnlistWithCurrentTransactionScope();

			if (softDelete)
			{
				var objects = await this.MongoCollection.Find(filter).ToListAsync();

				if (objects.Any())
				{
					var deletedObjects = objects.Select(x => new DeletedObject<TEntity>(x, this.MongoCollection.CollectionNamespace.CollectionName, DateTime.UtcNow)).ToList();
					
					await (SessionContainer.AmbientSession != null
						? this._trash.InsertManyAsync(SessionContainer.AmbientSession, deletedObjects).ConfigureAwait(false)
						: this._trash.InsertManyAsync(deletedObjects).ConfigureAwait(false));
				}
			}

			return SessionContainer.AmbientSession != null
				? await this.MongoCollection.DeleteManyAsync(SessionContainer.AmbientSession, filter).ConfigureAwait(false)
				: await this.MongoCollection.DeleteManyAsync(filter).ConfigureAwait(false);
		}

		public async Task<DeleteResult> DeleteManyAsync<TDerived>(Expression<Func<TDerived, bool>> filter, bool softDelete = false) where TDerived : TEntity
		{
			TryAutoEnlistWithCurrentTransactionScope();

			async Task<DeleteResult> DeleteEntity(IClientSessionHandle session = null) {
				return await (session != null
					? this.MongoCollection.OfType<TDerived>().DeleteManyAsync(session, filter).ConfigureAwait(false)
					: this.MongoCollection.OfType<TDerived>().DeleteManyAsync(filter).ConfigureAwait(false));
			}

			if (softDelete)
			{
				var objects = await this.MongoCollection.OfType<TDerived>().Find(filter).ToListAsync().ConfigureAwait(false);

				if (objects.Any())
				{
					var deletedObjects = objects.Select(x => new DeletedObject<TEntity>(x, this.MongoCollection.CollectionNamespace.CollectionName, DateTime.UtcNow)).ToList();

					return await WithTransaction(async session =>
					{
						await this._trash.InsertManyAsync(session, deletedObjects).ConfigureAwait(false);
						return await DeleteEntity(session);
					});
				}
				else
				{
					return new NoneDeletedResult();
				}
			}
			else
			{
				return await DeleteEntity();
			}
		}

		public async Task<DeleteResult> DeleteByIdAsync(string objectId, bool softDelete = false)
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			TryAutoEnlistWithCurrentTransactionScope();

			var filter = new BsonDocumentFilterDefinition<TEntity>(new BsonDocument("_id", ObjectId.Parse(objectId)));

			if (softDelete)
			{
				return await WithTransaction(async session =>
				{
					var entity = await this.MongoCollection.FindOneAndDeleteAsync(session, filter).ConfigureAwait(false);

					if (entity == null)
					{
						throw new Exception($"No document with id {objectId} was found");
					}

					await this._trash.InsertOneAsync(session, new DeletedObject<TEntity>(entity, this.MongoCollection.CollectionNamespace.CollectionName, DateTime.UtcNow)).ConfigureAwait(false);

					return new SoftDeleteResult(1);
				});
			}
			else
			{
				return (SessionContainer.AmbientSession != null
					? await this.MongoCollection.DeleteOneAsync(SessionContainer.AmbientSession, filter).ConfigureAwait(false)
					: await this.MongoCollection.DeleteOneAsync(filter).ConfigureAwait(false));
			}
		}

		public async Task<IFindFluent<TDerivedEntity, TDerivedEntity>> TextSearch<TDerivedEntity>(string text) where TDerivedEntity : TEntity
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection).ConfigureAwait(false);
			return this.MongoCollection.OfType<TDerivedEntity>().Find(Builders<TDerivedEntity>.Filter.Text(text));
		}

		private void TryAutoEnlistWithCurrentTransactionScope()
		{
			if (_autoEnlistWithCurrentTransactionScope && System.Transactions.Transaction.Current != null)
			{
				EnlistWithCurrentTransactionScope();
			}
		}

		public void EnlistWithCurrentTransactionScope(int maxRetries = 0)
		{
			var ambientTransactionId = System.Transactions.Transaction.Current.TransactionInformation.LocalIdentifier;

			if (SessionContainer.AmbientSession != null)
			{
				// Already enlisted
				return;
			}

			if (SessionContainer.SessionsByTransactionIdentifier.ContainsKey(ambientTransactionId))
			{
				// There is a session started already, but the AsyncLocal doesn't give it to us
				SessionContainer.SetSession(SessionContainer.SessionsByTransactionIdentifier[ambientTransactionId]);
				return;
			}

			if (System.Transactions.Transaction.Current == null)
			{
				throw new InvalidOperationException("There is no ambient transaction present");
			}

			MongoRepository.EnsureCollectionsCreated(MongoCollection.Database.Client, _tenantKey);

			var session = MongoCollection.Database.Client.StartSession();
			session.StartTransaction();

			SessionContainer.SetSession(session);
			SessionContainer.SessionsByTransactionIdentifier.TryAdd(System.Transactions.Transaction.Current.TransactionInformation.LocalIdentifier, session);

			System.Transactions.Transaction.Current.TransactionCompleted += (sender, e) => {
				SessionContainer.SetSession(null);
				SessionContainer.SessionsByTransactionIdentifier.TryRemove(ambientTransactionId, out _);
			};

			var enlistment = new RetryingTransactionEnlistment(session, maxRetries);
			System.Transactions.Transaction.Current.EnlistVolatile(enlistment, System.Transactions.EnlistmentOptions.None);
		}

		public Transaction StartTransaction(ClientSessionOptions sessionOptions = null, MongoDB.Driver.TransactionOptions transactionOptions = null)
		{
			MongoRepository.EnsureCollectionsCreated(MongoCollection.Database.Client, _tenantKey);

			var session = MongoCollection.Database.Client.StartSession(sessionOptions);
			session.StartTransaction(transactionOptions);

			SessionContainer.SetSession(session);

			return new Transaction(session, committed => SessionContainer.SetSession(null));
		}

		#region Find
		public IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(
			FilterDefinition<TDerivedEntity> filter,
			FindOptions options = null) where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>().Find(filter, options);
		}

		public IFindFluent<TEntity, TEntity> Find(FilterDefinition<TEntity> filter, FindOptions options = null)
		{
			return this.MongoCollection.Find(filter, options);
		}

		public IFindFluent<TEntity, TEntity> Find(Expression<Func<TEntity, bool>> filterExpression, FindOptions options = null)
		{
			return this.MongoCollection.Find(filterExpression, options);
		}

		public IFindFluent<TEntity, TEntity> Find(
			FieldDefinition<TEntity> property,
			string regexPattern,
			string regexOptions = "i",
			FindOptions options = null)
		{
			return this.MongoCollection
				.Find(Builders<TEntity>.Filter.Regex(property, new BsonRegularExpression(regexPattern, regexOptions)), options);
		}

		public IFindFluent<TEntity, TEntity> Find(
			Expression<Func<TEntity, object>> property,
			string regexPattern,
			string regexOptions = "i",
			FindOptions options = null)
		{
			return this.MongoCollection
				.Find(Builders<TEntity>.Filter.Regex(property, new BsonRegularExpression(regexPattern, regexOptions)), options);
		}

		public IFindFluent<TEntity, TEntity> Find(
			IEnumerable<FieldDefinition<TEntity>> properties,
			string regexPattern,
			string regexOptions = "i",
			FindOptions options = null)
		{
			var filters = properties.Select(p =>
				Builders<TEntity>.Filter.Regex(p, new BsonRegularExpression(regexPattern, regexOptions)));

			return this.MongoCollection.Find(Builders<TEntity>.Filter.Or(filters), options);
		}

		public IFindFluent<TEntity, TEntity> Find(
			IEnumerable<Expression<Func<TEntity, object>>> properties,
			string regexPattern,
			string regexOptions = "i",
			FindOptions options = null)
		{
			var filters = properties.Select(p =>
				Builders<TEntity>.Filter.Regex(p, new BsonRegularExpression(regexPattern, regexOptions)));

			return this.MongoCollection.Find(Builders<TEntity>.Filter.Or(filters), options);
		}

		public IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(
			FieldDefinition<TDerivedEntity> property,
			string regexPattern,
			string regexOptions = "i",
			FindOptions options = null) where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>()
				.Find(Builders<TDerivedEntity>.Filter.Regex(property, new BsonRegularExpression(regexPattern, regexOptions)), options);
		}

		public IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(
			Expression<Func<TDerivedEntity, object>> property,
			string regexPattern,
			string regexOptions = "i",
			FindOptions options = null) where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>()
				.Find(Builders<TDerivedEntity>.Filter.Regex(property, new BsonRegularExpression(regexPattern, regexOptions)), options);
		}

		public IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(
			IEnumerable<FieldDefinition<TDerivedEntity>> properties,
			string regexPattern,
			string regexOptions = "i",
			FindOptions options = null) where TDerivedEntity : TEntity
		{
			var filters = properties.Select(p =>
				Builders<TDerivedEntity>.Filter.Regex(p, new BsonRegularExpression(regexPattern, regexOptions)));

			return this.MongoCollection.OfType<TDerivedEntity>().Find(Builders<TDerivedEntity>.Filter.Or(filters), options);
		}

		public IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(
			IEnumerable<Expression<Func<TDerivedEntity, object>>> properties,
			string regexPattern,
			string regexOptions = "i",
			FindOptions options = null) where TDerivedEntity : TEntity
		{
			var filters = properties.Select(p =>
				Builders<TDerivedEntity>.Filter.Regex(p, new BsonRegularExpression(regexPattern, regexOptions)));

			return this.MongoCollection.OfType<TDerivedEntity>().Find(Builders<TDerivedEntity>.Filter.Or(filters), options);
		}

		public IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(
			Expression<Func<TDerivedEntity, bool>> filterExpression,
			FindOptions options = null) where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>().Find(filterExpression, options);
		}
		#endregion

		#region FindAsync
		public Task<IAsyncCursor<TEntity>> FindAsync(Expression<Func<TEntity, bool>> filter, FindOptions<TEntity, TEntity> options = null)
		{
			return this.MongoCollection.FindAsync(filter, options);
		}

		public Task<IAsyncCursor<TDerivedEntity>> FindAsync<TDerivedEntity>(
			Expression<Func<TDerivedEntity, bool>> filter,
			FindOptions<TDerivedEntity, TDerivedEntity> options = null) where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>().FindAsync(filter, options);
		}

		public Task<IAsyncCursor<TReturnProjection>> FindAsync<TReturnProjection>(
			Expression<Func<TEntity, bool>> filter,
			Expression<Func<TEntity, TReturnProjection>> returnProjection,
			FindOptions<TEntity, TReturnProjection> options = null)
		{
			var opt = options ?? new FindOptions<TEntity, TReturnProjection>();

			opt.Projection = Builders<TEntity>.Projection.Expression(returnProjection);

			return this.MongoCollection.FindAsync(filter, opt);
		}

		public Task<IAsyncCursor<TReturnProjection>> FindAsync<TDerivedEntity, TReturnProjection>(
			Expression<Func<TDerivedEntity, bool>> filter,
			Expression<Func<TDerivedEntity, TReturnProjection>> returnProjection,
			FindOptions<TDerivedEntity, TReturnProjection> options = null) where TDerivedEntity : TEntity
		{
			var opt = options ?? new FindOptions<TDerivedEntity, TReturnProjection>();

			opt.Projection = Builders<TDerivedEntity>.Projection.Expression(returnProjection);

			return this.MongoCollection.OfType<TDerivedEntity>().FindAsync(filter, opt);
		} 
		#endregion

		#region FindOneAsync
		public async Task<TEntity> FindOneAsync(Expression<Func<TEntity, bool>> filter)
		{
			return await (await this.MongoCollection.FindAsync(filter)).FirstOrDefaultAsync();
		}

		public async Task<TDerivedEntity> FindOneAsync<TDerivedEntity>(Expression<Func<TDerivedEntity, bool>> filter) where TDerivedEntity : TEntity
		{
			return await (await this.MongoCollection.OfType<TDerivedEntity>().FindAsync(filter)).FirstOrDefaultAsync();
		}

		public async Task<TReturnProjection> FindOneAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Expression<Func<TEntity, TReturnProjection>> returnProjection)
		{
			return await (await this.MongoCollection.FindAsync(filter, new FindOptions<TEntity, TReturnProjection>{
				Projection = Builders<TEntity>.Projection.Expression(returnProjection)
			})).FirstOrDefaultAsync();
		}

		public async Task<TReturnProjection> FindOneAsync<TDerivedEntity, TReturnProjection>(Expression<Func<TDerivedEntity, bool>> filter, Expression<Func<TDerivedEntity, TReturnProjection>> returnProjection) where TDerivedEntity : TEntity
		{
			return await (await this.MongoCollection.OfType<TDerivedEntity>().FindAsync(filter, new FindOptions<TDerivedEntity, TReturnProjection>{
				Projection = Builders<TDerivedEntity>.Projection.Expression(returnProjection)
			})).FirstOrDefaultAsync();
		} 
		#endregion

		public IMongoQueryable<TEntity> Query(AggregateOptions options = null)
		{
			return this.MongoCollection.AsQueryable(options);
		}

		public IMongoQueryable<TDerivedEntity> Query<TDerivedEntity>(AggregateOptions options = null) where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>().AsQueryable(options);
		}

		public async Task<TEntity> GetAsync(string objectId)
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			var filter = new BsonDocument("_id", ObjectId.Parse(objectId));

			var cursor = await this.MongoCollection.FindAsync(filter).ConfigureAwait(false);

			return await cursor.FirstOrDefaultAsync().ConfigureAwait(false);
		}

		public async Task<TDerivedEntity> GetAsync<TDerivedEntity>(string objectId) where TDerivedEntity : TEntity
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			var filter = new BsonDocument("_id", ObjectId.Parse(objectId));

			var cursor = await this.MongoCollection.OfType<TDerivedEntity>().FindAsync(filter).ConfigureAwait(false);

			return await cursor.FirstOrDefaultAsync().ConfigureAwait(false);
		}

		public async Task<TReturnProjection> GetAsync<TReturnProjection>(string objectId, Expression<Func<TEntity, TReturnProjection>> returnProjection)
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			var filter = new BsonDocument("_id", ObjectId.Parse(objectId));

			var cursor = await this.MongoCollection.FindAsync(filter, new FindOptions<TEntity, TReturnProjection> {
				Projection = Builders<TEntity>.Projection.Expression(returnProjection)
			}).ConfigureAwait(false);

			return await cursor.FirstOrDefaultAsync().ConfigureAwait(false);
		}

		public async Task<TReturnProjection> GetAsync<TDerivedEntity, TReturnProjection>(string objectId, Expression<Func<TDerivedEntity, TReturnProjection>> returnProjection) where TDerivedEntity : TEntity
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			var filter = new BsonDocument("_id", ObjectId.Parse(objectId));

			var cursor = await this.MongoCollection.OfType<TDerivedEntity>().FindAsync(filter, new FindOptions<TDerivedEntity, TReturnProjection> {
				Projection = Builders<TDerivedEntity>.Projection.Expression(returnProjection)
			}).ConfigureAwait(false);

			return await cursor.FirstOrDefaultAsync().ConfigureAwait(false);
		}

		public async Task<IFindFluent<TEntity, TEntity>> TextSearch(string text)
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection).ConfigureAwait(false);
			return this.MongoCollection.Find(Builders<TEntity>.Filter.Text(text));
		}

		public async Task<long> PermamentlyDeleteSoftDeletedAsync(Expression<Func<SoftDeletedEntity<TEntity>, bool>> filter)
		{
			var result = await _trash.DeleteManyAsync(filter);
			return result.DeletedCount;
		}

		public async Task<TReturn> WithTransactionAsync<TReturn>(Func<Task<TReturn>> transactionBody, TransactionType type = TransactionType.MongoDB, int maxRetries = 0)
		{
			if (type == TransactionType.TransactionScope)
			{
				return await Retryer.RetryAsync(async () => {
					using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
					{
						EnlistWithCurrentTransactionScope(maxRetries);
						var result = await transactionBody().ConfigureAwait(false);
						trans.Complete();

						return result;
					}
				}, maxRetries);
			}
			else
			{
				using (var session = await MongoCollection.Database.Client.StartSessionAsync())
				{
					SessionContainer.SetSession(session);
					return await session.WithTransactionAsync(async (session, cancel) => await transactionBody());
				}
			}
		}

		public async Task WithTransactionAsync(Func<Task> transactionBody, TransactionType type = TransactionType.MongoDB, int maxRetries = 0)
		{
			if (type == TransactionType.TransactionScope)
			{
				await Retryer.RetryAsync(async () => {
					using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
					{
						EnlistWithCurrentTransactionScope(maxRetries);
						await transactionBody().ConfigureAwait(false);
						trans.Complete();
					}
				}, maxRetries);
			}
			else
			{
				using (var session = await MongoCollection.Database.Client.StartSessionAsync())
				{
					SessionContainer.SetSession(session);
					await session.WithTransactionAsync(async (session, cancel) => {
						await transactionBody().ConfigureAwait(false);
						return 0;
					});
				}
			}
		}

		#region FindOneAndDelete

		public Task<TEntity> FindOneAndDeleteAsync(string id, bool softDelete = false)
		{
			if (id is null)
			{
				throw new ArgumentNullException(nameof(id));
			}

			var filter = new BsonDocumentFilterDefinition<TEntity>(new BsonDocument("_id", ObjectId.Parse(id)));

			return FindOneAndDeleteImplAsync(MongoCollection, filter, softDelete);
		}

		public Task<TEntity> FindOneAndDeleteAsync(Expression<Func<TEntity, bool>> filter, bool softDelete = false)
		{
			return FindOneAndDeleteImplAsync(MongoCollection, Builders<TEntity>.Filter.Where(filter), softDelete);
		}

		private async Task<TDerived> FindOneAndDeleteImplAsync<TDerived>(IMongoCollection<TDerived> collection, FilterDefinition<TDerived> filter, bool softDelete = false) where TDerived : TEntity
		{
			TryAutoEnlistWithCurrentTransactionScope();

			if (softDelete)
			{
				return await WithTransaction(async session =>
				{
					var entity = await collection.FindOneAndDeleteAsync(session, filter).ConfigureAwait(false);

					if (entity == null)
					{
						return default;
					}

					await this._trash.InsertOneAsync(session, new DeletedObject<TEntity>(entity, this.MongoCollection.CollectionNamespace.CollectionName, DateTime.UtcNow)).ConfigureAwait(false);

					return entity;
				});
			}
			else
			{
				return (SessionContainer.AmbientSession != null
					? await collection.FindOneAndDeleteAsync(SessionContainer.AmbientSession, filter).ConfigureAwait(false)
					: await collection.FindOneAndDeleteAsync(filter).ConfigureAwait(false));
			}
		}

		public Task<TDerivedEntity> FindOneAndDeleteAsync<TDerivedEntity>(string id, bool softDelete = false) where TDerivedEntity : TEntity
		{
			if (id is null)
			{
				throw new ArgumentNullException(nameof(id));
			}

			var filter = new BsonDocumentFilterDefinition<TDerivedEntity>(new BsonDocument("_id", ObjectId.Parse(id)));

			return FindOneAndDeleteImplAsync(MongoCollection.OfType<TDerivedEntity>(), filter, softDelete);
		}

		public Task<TDerivedEntity> FindOneAndDeleteAsync<TDerivedEntity>(Expression<Func<TDerivedEntity, bool>> filter, bool softDelete = false) where TDerivedEntity : TEntity
		{
			return FindOneAndDeleteImplAsync(MongoCollection.OfType<TDerivedEntity>(), Builders<TDerivedEntity>.Filter.Where(filter), softDelete);
		}

		#endregion

		public Task<TReturnProjection> UpdateOrInsertOneAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> updateExpression, TEntity entityToInsertIfNoMatch, Expression<Func<TEntity, TReturnProjection>> returnProjection)
		{
			return UpdateOrInsertOneAsyncImpl<TReturnProjection>(filter, updateExpression, entityToInsertIfNoMatch, returnProjection);
		}

		public Task<TReturnProjection> UpdateOrInsertOneAsync<TDerived, TReturnProjection>(Expression<Func<TDerived, bool>> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> updateExpression, TDerived entityToInsertIfNoMatch, Expression<Func<TDerived, TReturnProjection>> returnProjection) where TDerived : TEntity
		{
			return UpdateOrInsertDerivedOneAsyncImpl<TDerived, TReturnProjection>(filter, updateExpression, entityToInsertIfNoMatch, returnProjection);
		}

		public Task<TEntity> UpdateOrInsertOneAsync(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> updateExpression, TEntity entityToInsertIfNoMatch)
		{
			return UpdateOrInsertOneAsyncImpl<TEntity>(filter, updateExpression, entityToInsertIfNoMatch, x => x);
		}

		public Task<TDerived> UpdateOrInsertOneAsync<TDerived>(Expression<Func<TDerived, bool>> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> updateExpression, TDerived entityToInsertIfNoMatch) where TDerived : TEntity
		{
			return UpdateOrInsertDerivedOneAsyncImpl<TDerived, TDerived>(filter, updateExpression, entityToInsertIfNoMatch, x => x);
		}

		private async Task<TReturnProjection> UpdateOrInsertOneAsyncImpl<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> updateExpression, TEntity entityToInsertIfNoMatch, Expression<Func<TEntity, TReturnProjection>> returnProjection)
		{
			BsonDocument doc = entityToInsertIfNoMatch.ToBsonDocument();

			var patch = updateExpression(Builders<TEntity>.Update);

			var patchedPropertyNames = patch.Render(
				   BsonSerializer.SerializerRegistry.GetSerializer<TEntity>(),
				   BsonSerializer.SerializerRegistry
				)
				.AsBsonDocument.Select(x => x.Value.AsBsonDocument).SelectMany(prop => prop.Select(p => p.Name)).ToList();

			var update = Builders<TEntity>.Update
					.Combine(doc
						.Where(x => !patchedPropertyNames.Contains(x.Name))
							.Select(x => Builders<TEntity>.Update.SetOnInsert(x.Name, x.Value))
						.Append(patch)
					);

			try
			{
				return await this.FindOneAndUpdateAsync<TReturnProjection>(
					filter,
					update,
					returnProjection,
					ReturnedDocumentState.AfterUpdate,
					true
					);
			}
			catch (MongoDB.Driver.MongoCommandException ex) when (ex.CodeName == "ConflictingUpdateOperators")
			{
				throw new NotSupportedException("Only root-level properties can be patched. See issue #54", ex);
			}
		}

		private async Task<TReturnProjection> UpdateOrInsertDerivedOneAsyncImpl<TDerived, TReturnProjection>(Expression<Func<TDerived, bool>> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> updateExpression, TDerived entityToInsertIfNoMatch, Expression<Func<TDerived, TReturnProjection>> returnProjection) where TDerived : TEntity
		{
			BsonDocument doc = entityToInsertIfNoMatch.ToBsonDocument();

			var patch = updateExpression(Builders<TDerived>.Update);

			var patchedPropertyNames = patch.Render(
				   BsonSerializer.SerializerRegistry.GetSerializer<TDerived>(),
				   BsonSerializer.SerializerRegistry
				)
				.AsBsonDocument.Select(x => x.Value.AsBsonDocument).SelectMany(prop => prop.Select(p => p.Name)).ToList();

			var update = Builders<TDerived>.Update
					.Combine(doc
						.Where(x => !patchedPropertyNames.Contains(x.Name))
							.Select(x => Builders<TDerived>.Update.SetOnInsert(x.Name, x.Value))
						.Append(patch)
					);

			try
			{
				return await this.FindOneAndUpdateAsync<TDerived, TReturnProjection>(
					filter,
					update,
					returnProjection,
					ReturnedDocumentState.AfterUpdate,
					true
					);
			}
			catch (MongoDB.Driver.MongoCommandException ex) when (ex.CodeName == "ConflictingUpdateOperators")
			{
				throw new NotSupportedException("Only root-level properties can be patched. See issue #54", ex);
			}
		}
	}

	public class NoBulkWriteResult<T> : BulkWriteResult<T>
	{
		public NoBulkWriteResult() : base(0, Enumerable.Empty<WriteModel<T>>())
		{
		}

		public override long DeletedCount => 0;
		public override long InsertedCount => 0;
		public override bool IsAcknowledged => false;
		public override bool IsModifiedCountAvailable => false;
		public override long MatchedCount => 0;
		public override long ModifiedCount => 0;
		public override IReadOnlyList<BulkWriteUpsert> Upserts => new List<BulkWriteUpsert>(0);
	}

	public class NoneDeletedResult : DeleteResult
	{
		public override long DeletedCount { get; } = 0;
		public override bool IsAcknowledged { get; } = false;
	}

	public class SoftDeleteResult : DeleteResult
	{
		public SoftDeleteResult(long deletedCount)
		{
			DeletedCount = deletedCount;
		}

		public override long DeletedCount { get; }
		public override bool IsAcknowledged { get; } = true;
	}
}