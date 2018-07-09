using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace JohnKnoop.MongoRepository
{
	public class DeletedObject
	{
		public DeletedObject(object content)
		{
			Content = content;
		    Type = content.GetType();
            TypeName = Type.Name;
		}

		public string TypeName { get; private set; }
		public dynamic Content { get; private set; }
        internal Type Type { get; private set; }
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

		public static IList<Type> GetMappedTypes() => MongoConfiguration.GetMappedTypes();
	}

	public class MongoRepository<TEntity> : IRepository<TEntity>
	{
		protected readonly IMongoCollection<TEntity> MongoCollection;
		private readonly IMongoCollection<DeletedObject> _trash;

		internal MongoRepository(IMongoCollection<TEntity> mongoCollection, IMongoCollection<DeletedObject> trash)
		{
			this.MongoCollection = mongoCollection;
			this._trash = trash;
		}

		public async Task DeletePropertyAsync(Expression<Func<TEntity, bool>> filterExpression, Expression<Func<TEntity, object>> propertyExpression)
		{
			var updateDefinition = Builders<TEntity>.Update.Unset(propertyExpression);
			await this.MongoCollection.FindOneAndUpdateAsync(filterExpression, updateDefinition).ConfigureAwait(false);
		}

		public async Task AddToTrash<TObject>(TObject objectToTrash)
		{
			await this._trash.InsertOneAsync(new DeletedObject(objectToTrash)).ConfigureAwait(false);
		}

		public async Task InsertAsync(TEntity entity)
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);
			await this.MongoCollection.InsertOneAsync(entity).ConfigureAwait(false);
		}

		public IAggregateFluent<TEntity> Aggregate(AggregateOptions options = null)
		{
			return this.MongoCollection.Aggregate(options);
		}

		public async Task<ReplaceOneResult> ReplaceOneAsync(string id, TEntity entity, bool upsert = false)
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			if (id == null) throw new ArgumentNullException(nameof(id));
			
			var filter = Builders<TEntity>.Filter.Eq("_id", ObjectId.Parse(id));

			return await this.MongoCollection.ReplaceOneAsync(filter, entity, new UpdateOptions { IsUpsert = upsert}).ConfigureAwait(false);
		}

		public async Task<ReplaceOneResult> ReplaceOneAsync(Expression<Func<TEntity, bool>> filter, TEntity entity, bool upsert = false)
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			return await this.MongoCollection.ReplaceOneAsync(filter, entity, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
		}

		public async Task<TReturnProjection> FindOneAndUpdateAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, Expression<Func<TEntity, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.BeforeUpdate, bool upsert = false)
		{
			return await FindOneAndUpdateAsync(filter, update, Builders<TEntity>.Projection.Expression(returnProjection), returnedDocumentState, upsert);
		}

		public async Task<TReturnProjection> FindOneAndUpdateAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, ProjectionDefinition<TEntity, TReturnProjection> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false)
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var returnDocument = returnedDocumentState == ReturnedDocumentState.BeforeUpdate
				? ReturnDocument.Before
				: ReturnDocument.After;

			return await this.MongoCollection.FindOneAndUpdateAsync(
				filter,
				update(Builders<TEntity>.Update),
				new FindOneAndUpdateOptions<TEntity, TReturnProjection>
				{
					Projection = returnProjection,
					ReturnDocument = returnDocument,
					IsUpsert = upsert
				}).ConfigureAwait(false);
		}

		public async Task<bool> UpdateOneAsync<TDerived>(string id, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false) where TDerived : TEntity
		{
			if (id == null) throw new ArgumentNullException(nameof(id));

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var filter = Builders<TDerived>.Filter.Eq("_id", ObjectId.Parse(id));

			var result = await this.MongoCollection.OfType<TDerived>().UpdateOneAsync(filter, update(Builders<TDerived>.Update), new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
			return result.MatchedCount == 1;
		}

		public async Task<bool> UpdateOneAsync<TDerived>(Expression<Func<TDerived, bool>> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false) where TDerived : TEntity
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var result = await this.MongoCollection.OfType<TDerived>().UpdateOneAsync(filter, update(Builders<TDerived>.Update), new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
			return result.MatchedCount == 1;
		}

		public async Task<bool> UpdateOneAsync(string id, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false)
		{
			if (id == null) throw new ArgumentNullException(nameof(id));

			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var filter = Builders<TEntity>.Filter.Eq("_id", ObjectId.Parse(id));

			var result = await this.MongoCollection.UpdateOneAsync(filter, update(Builders<TEntity>.Update), new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
			return result.MatchedCount == 1;
		}

		public async Task<bool> UpdateOneAsync(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false)
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var result = await this.MongoCollection.UpdateOneAsync(filter, update(Builders<TEntity>.Update), new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
			return result.MatchedCount == 1;
		}

		public async Task<bool> UpdateOneAsync(string filter, string update, bool upsert = false)
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var result = await this.MongoCollection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
			return result.MatchedCount == 1;
		}

		/// <summary>
		/// Executes multiple update operations in one batch
		/// </summary>
		public async Task<BulkWriteResult<TEntity>> UpdateOneBulkAsync(IEnumerable<UpdateOneCommand<TEntity>> commands)
		{
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

			var result = await this.MongoCollection.BulkWriteAsync(cmds).ConfigureAwait(false);
			return result;
		}

		/// <summary>
		/// Executes multiple update operations in one batch
		/// </summary>
		public async Task<BulkWriteResult<TDerived>> UpdateOneBulkAsync<TDerived>(IEnumerable<UpdateOneCommand<TDerived>> commands) where TDerived : TEntity
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			var cmds = commands.Select(cmd => 
			new UpdateOneModel<TDerived>(cmd.Filter(Builders<TDerived>.Filter), cmd.UpdateJson ?? cmd.Update(Builders<TDerived>.Update))
				{
					IsUpsert = cmd.IsUpsert
				}).ToList();

			if (cmds.Any())
			{
				var result = await this.MongoCollection.OfType<TDerived>().BulkWriteAsync(cmds).ConfigureAwait(false);
				return result;
			}

			return null;
		}

		/// <summary>
		/// Applies the same update to multiple entities
		/// </summary>
		public async Task UpdateManyAsync(Expression<Func<TEntity, bool>> filter,
			Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update,
			UpdateOptions options = null)
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			await this.MongoCollection.UpdateManyAsync(filter, update(Builders<TEntity>.Update), options).ConfigureAwait(false);
		}

		/// <summary>
		/// Applies the same update to multiple entities
		/// </summary>
		public async Task UpdateManyAsync(Expression<Func<TEntity, bool>> filter,
			string update,
			UpdateOptions options = null)
		{
			await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

			await this.MongoCollection.UpdateManyAsync(filter, update, options).ConfigureAwait(false);
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

		public async Task ResetCounterAsync(string name = null, long newValue = 0)
		{
			var counterCollectionName = "_counters";

			var fieldName = MongoCollection.CollectionNamespace.CollectionName;

			if (name != null)
			{
				fieldName += $"_{name}";
			}

			var collection = MongoCollection.Database.GetCollection<BsonDocument>(counterCollectionName);
			var fieldDefinition = new StringFieldDefinition<BsonDocument, long>(fieldName);

			await collection.UpdateOneAsync(
				filter: x => true,
				update: Builders<BsonDocument>.Update.Set(fieldDefinition, newValue),
				options: new UpdateOptions
				{
					IsUpsert = true
				}
			).ConfigureAwait(false);
		}

		public async Task<long> IncrementCounterAsync(string name = null, int incrementBy = 1)
		{
			var counterCollectionName = "_counters";

			var fieldName = MongoCollection.CollectionNamespace.CollectionName;

			if (name != null)
			{
				fieldName += $"_{name}";
			}

			var collection = MongoCollection.Database.GetCollection<BsonDocument>(counterCollectionName);
			var fieldDefinition = new StringFieldDefinition<BsonDocument, long>(fieldName);

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

		public async Task ReplaceManyAsync(IEnumerable<ReplaceManyCommand<TEntity>> commands, bool upsert = false)
		{
			if (commands.Any())
			{
				await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

				await this.MongoCollection.BulkWriteAsync(commands.Select(cmd => new ReplaceOneModel<TEntity>(cmd.Filter(Builders<TEntity>.Filter), cmd.Replacement)
				{
					IsUpsert = upsert
				})).ConfigureAwait(false);
			}
		}

		public async Task InsertManyAsync(IEnumerable<TEntity> entities)
		{
			if (entities.Any())
			{
				await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

				await this.MongoCollection.InsertManyAsync(entities).ConfigureAwait(false);
			}
		}

		public async Task InsertManyAsync(params TEntity[] entities) =>
			await InsertManyAsync((IEnumerable<TEntity>)entities);

		public IFindFluent<TEntity, TEntity> GetAll()
		{
			return this.MongoCollection.Find(FilterDefinition<TEntity>.Empty);
		}

		public async Task DeleteManyAsync<TDerived>(Expression<Func<TDerived, bool>> filter, bool softDelete = false) where TDerived : TEntity
		{
			if (softDelete)
			{
				var objects = await this.MongoCollection.OfType<TDerived>().Find(filter).ToListAsync().ConfigureAwait(false);

				var deletedObjects = objects.Select(x => new DeletedObject(x)).ToList();

				if (deletedObjects.Any())
				{
					await this._trash.InsertManyAsync(deletedObjects).ConfigureAwait(false);
				}
			}

			await this.MongoCollection.OfType<TDerived>().DeleteManyAsync(filter).ConfigureAwait(false);
		}

		public async Task<TEntity> GetFromTrashAsync(string objectId)
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			var filter = new BsonDocument("_id", ObjectId.Parse(objectId));

			var deletedObject = await _trash.Find(filter).SingleOrDefaultAsync().ConfigureAwait(false);

			if (deletedObject == null)
			{
				throw new Exception($"No object of type {typeof(TEntity).Name} with id {objectId} was found in the trash");
			}

			return (TEntity) deletedObject.Content;
		}

		//public Task<TEntity> RestoreSoftDeleted(string objectId)
		//{
		//	if (objectId == null) throw new ArgumentNullException(nameof(objectId));

		//	var filter = new BsonDocument("_id", ObjectId.Parse(objectId));

		//	var deletedObject = _trash.Find(filter).SingleOrDefaultAsync();

		//	if (deletedObject == null)
		//	{
		//		throw new Exception($"No object of type {typeof(TEntity).Name} with id {objectId} was found in the trash");
		//	}

		// Vill man återställa eller bara läsa upp borttagna objekt?
		//}

		public async Task DeleteManyAsync(Expression<Func<TEntity, bool>> filter, bool softDelete = false)
		{
			if (softDelete)
			{
				var objects = await this.MongoCollection.Find(filter).ToListAsync();

				var deletedObjects = objects.Select(x => new DeletedObject(x)).ToList();

				if (deletedObjects.Any())
				{
					await this._trash.InsertManyAsync(deletedObjects).ConfigureAwait(false);
				}
			}

			await this.MongoCollection.DeleteManyAsync(filter).ConfigureAwait(false);
		}

		public IFindFluent<TEntity, TEntity> TextSearch(string text)
		{
			return this.MongoCollection.Find(Builders<TEntity>.Filter.Text(text));
		}

		public IFindFluent<TDerivedEntity, TDerivedEntity> TextSearch<TDerivedEntity>(string text) where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>().Find(Builders<TDerivedEntity>.Filter.Text(text));
		}

		public IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(FilterDefinition<TDerivedEntity> filter) where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>().Find(filter);
		}

		public IFindFluent<TEntity, TEntity> Find(FilterDefinition<TEntity> filter)
		{
			return this.MongoCollection.Find(filter);
		}

		public IFindFluent<TEntity, TEntity> Find(Expression<Func<TEntity, bool>> filterExpression)
		{
			return this.MongoCollection.Find(filterExpression);
		}

		public IFindFluent<TEntity, TEntity> Find(FieldDefinition<TEntity> property, string regexPattern, string regexOptions = "i")
		{
			return this.MongoCollection.Find(Builders<TEntity>.Filter.Regex(property, new BsonRegularExpression(regexPattern, regexOptions)));
		}

		public IFindFluent<TEntity, TEntity> Find(Expression<Func<TEntity, object>> property, string regexPattern, string regexOptions = "i")
		{
			return this.MongoCollection.Find(Builders<TEntity>.Filter.Regex(property, new BsonRegularExpression(regexPattern, regexOptions)));
		}

		public IFindFluent<TEntity, TEntity> Find(IEnumerable<FieldDefinition<TEntity>> properties, string regexPattern, string regexOptions = "i")
		{
			var filters = properties.Select(p =>
				Builders<TEntity>.Filter.Regex(p, new BsonRegularExpression(regexPattern, regexOptions)));

			return this.MongoCollection.Find(Builders<TEntity>.Filter.Or(filters));
		}

		public IFindFluent<TEntity, TEntity> Find(IEnumerable<Expression<Func<TEntity, object>>> properties, string regexPattern, string regexOptions = "i")
		{
			var filters = properties.Select(p =>
				Builders<TEntity>.Filter.Regex(p, new BsonRegularExpression(regexPattern, regexOptions)));

			return this.MongoCollection.Find(Builders<TEntity>.Filter.Or(filters));
		}

		public IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(Expression<Func<TDerivedEntity, bool>> filterExpression) where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>().Find(filterExpression);
		}

		public IMongoQueryable<TEntity> Query()
		{
			return this.MongoCollection.AsQueryable();
		}

		public IMongoQueryable<TDerivedEntity> Query<TDerivedEntity>() where TDerivedEntity : TEntity
		{
			return this.MongoCollection.OfType<TDerivedEntity>().AsQueryable();
		}

		public async Task<TEntity> GetAsync(string objectId) => await GetAsync<TEntity>(objectId).ConfigureAwait(false);

		public async Task<TReturnProjection> GetAsync<TReturnProjection>(string objectId, Expression<Func<TEntity, TReturnProjection>> returnProjection) => 
			await GetAsync<TEntity, TReturnProjection>(objectId, returnProjection).ConfigureAwait(false);

		public async Task<T> GetAsync<T>(string objectId) where T : TEntity
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			var filter = new BsonDocument("_id", ObjectId.Parse(objectId));

			return await this.MongoCollection.Find(filter).As<T>().FirstOrDefaultAsync().ConfigureAwait(false);
		}

		public async Task<TReturnProjection> GetAsync<T, TReturnProjection>(string objectId, Expression<Func<TEntity, TReturnProjection>> returnProjection) where T : TEntity
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			var filter = new BsonDocument("_id", ObjectId.Parse(objectId));

			return await this.MongoCollection.Find(filter).As<T>().Project(returnProjection).FirstOrDefaultAsync().ConfigureAwait(false);
		}

		public async Task DeleteByIdAsync(string objectId, bool softDelete = false)
		{
			if (objectId == null) throw new ArgumentNullException(nameof(objectId));

			if (softDelete)
			{
				TEntity obj = await this.GetAsync(objectId).ConfigureAwait(false);
				await this._trash.InsertOneAsync(new DeletedObject(obj)).ConfigureAwait(false);
			}

			var filter = new BsonDocumentFilterDefinition<TEntity>(new BsonDocument("_id", ObjectId.Parse(objectId)));
			await this.MongoCollection.DeleteOneAsync(filter).ConfigureAwait(false);
		}
	}
}