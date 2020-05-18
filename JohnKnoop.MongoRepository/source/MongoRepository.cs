using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
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

        public static IList<string> GetDatabaseNames(string tenantKey = null) =>
            MongoConfiguration.GetDatabaseNames(tenantKey);

        public static IList<Type> GetMappedTypes() => MongoConfiguration.GetMappedTypes();

        public static string GetDatabaseName(Type entityType, string tenantKey = null) =>
            MongoConfiguration.GetDatabaseName(entityType, tenantKey);
    }

    public class MongoRepository<TEntity> : IRepository<TEntity>
    {
        protected readonly IMongoCollection<TEntity> MongoCollection;
        private readonly IMongoCollection<SoftDeletedEntity<TEntity>> _trash;

        internal MongoRepository(IMongoCollection<TEntity> mongoCollection, IMongoCollection<SoftDeletedEntity<TEntity>> trash)
        {
            this.MongoCollection = mongoCollection;
            this._trash = trash;
        }

        public async Task DeletePropertyAsync(Expression<Func<TEntity, bool>> filterExpression, Expression<Func<TEntity, object>> propertyExpression)
        {
            var updateDefinition = Builders<TEntity>.Update.Unset(propertyExpression);
            await this.MongoCollection.FindOneAndUpdateAsync(filterExpression, updateDefinition).ConfigureAwait(false);
        }

        private async Task AddToTrashAsync(TEntity objectToTrash)
        {
            await this._trash.InsertOneAsync(new DeletedObject<TEntity>(objectToTrash, this.MongoCollection.CollectionNamespace.CollectionName, DateTime.UtcNow)).ConfigureAwait(false);
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
            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);
            await this.MongoCollection.InsertOneAsync(entity).ConfigureAwait(false);
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
            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

            if (id == null) throw new ArgumentNullException(nameof(id));

            var filter = Builders<TEntity>.Filter.Eq("_id", ObjectId.Parse(id));

            return await this.MongoCollection.ReplaceOneAsync(filter, entity, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
        }

        public async Task<ReplaceOneResult> ReplaceOneAsync(Expression<Func<TEntity, bool>> filter, TEntity entity, bool upsert = false)
        {
            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

            return await this.MongoCollection.ReplaceOneAsync(filter, entity, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
        }

        public Task<TReturnProjection> FindOneAndUpdateAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, Expression<Func<TEntity, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.BeforeUpdate, bool upsert = false)
        {
            return FindOneAndUpdateAsync(filter, update, Builders<TEntity>.Projection.Expression(returnProjection), returnedDocumentState, upsert);
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

        public Task<TReturnProjection> FindOneAndReplaceAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, TEntity replacement, Expression<Func<TEntity, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false)
        {
            return FindOneAndReplaceAsync(filter, replacement, Builders<TEntity>.Projection.Expression(returnProjection), returnedDocumentState, upsert);
        }

        public async Task<TReturnProjection> FindOneAndReplaceAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, TEntity replacement, ProjectionDefinition<TEntity, TReturnProjection> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false)
        {
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

            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

            var filter = Builders<TDerived>.Filter.Eq("_id", ObjectId.Parse(id));

            return await this.MongoCollection.OfType<TDerived>().UpdateOneAsync(filter, update(Builders<TDerived>.Update), options).ConfigureAwait(false);
        }

        public Task<UpdateResult> UpdateOneAsync<TDerived>(Expression<Func<TDerived, bool>> filter,
            Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false)
            where TDerived : TEntity
                => UpdateOneAsync((FilterDefinition<TDerived>) filter, update, upsert);

        public Task<UpdateResult> UpdateOneAsync<TDerived>(FilterDefinition<TDerived> filter,
            Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false)
            where TDerived : TEntity
                => UpdateOneAsync<TDerived>(filter, update, new UpdateOptions { IsUpsert = upsert });

        public Task<UpdateResult> UpdateOneAsync<TDerived>(Expression<Func<TDerived, bool>> filter,
            Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, UpdateOptions options)
            where TDerived : TEntity =>
                UpdateOneAsync((FilterDefinition<TDerived>) filter, update, options);

        public async Task<UpdateResult> UpdateOneAsync<TDerived>(FilterDefinition<TDerived> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, UpdateOptions options) where TDerived : TEntity
        {
            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

            return await this.MongoCollection.OfType<TDerived>().UpdateOneAsync(filter, update(Builders<TDerived>.Update), options).ConfigureAwait(false);
        }

        public Task<UpdateResult> UpdateOneAsync(string id,
            Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false) =>
                UpdateOneAsync(id, update, new UpdateOptions { IsUpsert = upsert });

        public async Task<UpdateResult> UpdateOneAsync(string id, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, UpdateOptions options)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));

            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

            var filter = Builders<TEntity>.Filter.Eq("_id", ObjectId.Parse(id));

            return await this.MongoCollection.UpdateOneAsync(filter, update(Builders<TEntity>.Update), options).ConfigureAwait(false);
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
            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

            return await this.MongoCollection.UpdateOneAsync(filter, update(Builders<TEntity>.Update), options).ConfigureAwait(false);
        }

        public async Task<UpdateResult> UpdateOneAsync(string filter, string update, bool upsert = false)
        {
            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

            return await this.MongoCollection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = upsert }).ConfigureAwait(false);
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
                var result = await this.MongoCollection.OfType<TDerived>().BulkWriteAsync(cmds).ConfigureAwait(false);
                return result;
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

        public async Task<BulkWriteResult<TEntity>> ReplaceManyAsync(IList<ReplaceManyCommand<TEntity>> commands, bool upsert = false)
        {
            if (!commands.Any())
            {
                throw new ArgumentException("At least one command must be provided");
            }

            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection);

            return await this.MongoCollection.BulkWriteAsync(commands.Select(cmd => new ReplaceOneModel<TEntity>(cmd.Filter(Builders<TEntity>.Filter), cmd.Replacement)
            {
                IsUpsert = upsert
            })).ConfigureAwait(false);
        }

		public async Task InsertManyAsync(ICollection<TEntity> entities)
		{
			if (entities.Any())
			{
				await MongoConfiguration.EnsureIndexesAndCap(MongoCollection).ConfigureAwait(false);

                await this.MongoCollection.InsertManyAsync(entities).ConfigureAwait(false);
            }
        }

		public async Task InsertManyAsync<TDerivedEntity>(ICollection<TDerivedEntity> entities) where TDerivedEntity : TEntity
		{
			if (entities.Any())
			{
				await MongoConfiguration.EnsureIndexesAndCap(MongoCollection).ConfigureAwait(false);

				await this.MongoCollection.OfType<TDerivedEntity>().InsertManyAsync(entities).ConfigureAwait(false);
			}
		}

		public IFindFluent<TEntity, TEntity> GetAll()
		{
			return this.MongoCollection.Find(FilterDefinition<TEntity>.Empty);
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

            var filter = new BsonDocument("Entity._id", ObjectId.Parse(objectId));

            using (await StartTransactionAsync())
            {
                var deletedObject = await _trash.FindOneAndDeleteAsync(filter);

                if (deletedObject == null)
                {
                    throw new Exception($"No document with id {objectId} was found in the trash");
                }

                await this.InsertAsync(deletedObject.Entity);
                return deletedObject.Entity;
            }
        }

        public async Task<IList<TEntity>> RestoreSoftDeletedAsync(Expression<Func<SoftDeletedEntity<TEntity>, bool>> filter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));

            var deletedObjects = await _trash.Find(filter).ToListAsync();

            if (deletedObjects.Count == 0)
            {
                return new List<TEntity>(0);
            }

            var deletedEntities = deletedObjects.Select(x => x.Entity).ToList();

            using (await StartTransactionAsync())
            {
                await this.InsertManyAsync(deletedEntities);
                await this._trash.DeleteManyAsync(filter);
            }

            return deletedEntities;
        }

        public async Task<IList<TDerived>> RestoreSoftDeletedAsync<TDerived>(Expression<Func<SoftDeletedEntity<TDerived>, bool>> filter) where TDerived : TEntity
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));

            var serializerRegistry = BsonSerializer.SerializerRegistry;
            var documentSerializer = serializerRegistry.GetSerializer<SoftDeletedEntity<TDerived>>();

            var filterBson = Builders<SoftDeletedEntity<TDerived>>.Filter.Where(filter).Render(documentSerializer, serializerRegistry);

            var deletedObjects = await _trash.Find(filterBson).ToListAsync();

            if (deletedObjects.Count == 0)
            {
                return new List<TDerived>(0);
            }

            var deletedEntities = deletedObjects.Select(x => x.Entity).OfType<TDerived>().ToList();

            using (await StartTransactionAsync())
            {
                foreach (var deletedEntity in deletedEntities)
                {
                    await InsertAsync(deletedEntity);
                }

                await this._trash.DeleteManyAsync(filterBson);
            }

            return deletedEntities;
        }

        public async Task<DeleteResult> DeleteManyAsync(Expression<Func<TEntity, bool>> filter, bool softDelete = false)
        {
            if (softDelete)
            {
                var objects = await this.MongoCollection.Find(filter).ToListAsync();

                if (objects.Any())
                {
                    var deletedObjects = objects.Select(x => new DeletedObject<TEntity>(x, this.MongoCollection.CollectionNamespace.CollectionName, DateTime.UtcNow)).ToList();
                    await this._trash.InsertManyAsync(deletedObjects).ConfigureAwait(false);
                }
            }

            return await this.MongoCollection.DeleteManyAsync(filter).ConfigureAwait(false);
        }

        public async Task<DeleteResult> DeleteManyAsync<TDerived>(Expression<Func<TDerived, bool>> filter, bool softDelete = false) where TDerived : TEntity
        {
            async Task<DeleteResult> DeleteEntity() => await this.MongoCollection.OfType<TDerived>().DeleteManyAsync(filter).ConfigureAwait(false);

            if (softDelete)
            {
                var objects = await this.MongoCollection.OfType<TDerived>().Find(filter).ToListAsync().ConfigureAwait(false);

                if (objects.Any())
                {
                    var deletedObjects = objects.Select(x => new DeletedObject<TEntity>(x, this.MongoCollection.CollectionNamespace.CollectionName, DateTime.UtcNow)).ToList();

                    using (await StartTransactionAsync())
                    {
                        await this._trash.InsertManyAsync(deletedObjects).ConfigureAwait(false);
                        return await DeleteEntity();
                    }
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

            var filter = new BsonDocumentFilterDefinition<TEntity>(new BsonDocument("_id", ObjectId.Parse(objectId)));

            if (softDelete)
            {
                using (await StartTransactionAsync())
                {
                    var entity = await this.MongoCollection.FindOneAndDeleteAsync(filter).ConfigureAwait(false);

                    if (entity == null)
                    {
                        throw new Exception($"No document with id {objectId} was found");
                    }

                    await this.AddToTrashAsync(entity).ConfigureAwait(false);

                    return new SoftDeleteResult(1);
                }
            }
            else
            {
                return await this.MongoCollection.DeleteOneAsync(filter).ConfigureAwait(false);
            }
        }

        public async Task<IFindFluent<TDerivedEntity, TDerivedEntity>> TextSearch<TDerivedEntity>(string text) where TDerivedEntity : TEntity
        {
            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection).ConfigureAwait(false);
            return this.MongoCollection.OfType<TDerivedEntity>().Find(Builders<TDerivedEntity>.Filter.Text(text));
        }

        public Task<IClientSessionHandle> StartSessionAsync(ClientSessionOptions options = null)
        {
            return MongoCollection.Database.Client.StartSessionAsync(options);
        }

        public async Task<Transaction> StartTransactionAsync(ClientSessionOptions sessionOptions = null, TransactionOptions transactionOptions = null)
        {
            var session = await MongoCollection.Database.Client.StartSessionAsync(sessionOptions);
            session.StartTransaction(transactionOptions);
            return new Transaction(session);
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

        public async Task<IFindFluent<TEntity, TEntity>> TextSearch(string text)
        {
            await MongoConfiguration.EnsureIndexesAndCap(MongoCollection).ConfigureAwait(false);
            return this.MongoCollection.Find(Builders<TEntity>.Filter.Text(text));
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