using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace JohnKnoop.MongoRepository
{
	public interface IRepository<TEntity>
	{
		Task InsertAsync(TEntity entity);
		Task InsertManyAsync(IEnumerable<TEntity> entities);
		Task InsertManyAsync(params TEntity[] entities);

		IMongoQueryable<TEntity> Query();
	    IMongoQueryable<TDerivedEntity> Query<TDerivedEntity>() where TDerivedEntity : TEntity;
		
		/// <returns>An instance of <c>TEntity</c> or null</returns>
		Task<TEntity> GetAsync(string id);
		Task<TReturnProjection> GetAsync<TReturnProjection>(string id, Expression<Func<TEntity, TReturnProjection>> returnProjection);
		/// <returns>An instance of <c>T</c> or null</returns>
		Task<T> GetAsync<T>(string id) where T : TEntity;
		Task<TReturnProjection> GetAsync<T, TReturnProjection>(string id, Expression<Func<TEntity, TReturnProjection>> returnProjection) where T : TEntity;

		Task DeleteByIdAsync(string id, bool softDelete = false);

		IFindFluent<TEntity, TEntity> GetAll();
		IFindFluent<TEntity, TEntity> Find(Expression<Func<TEntity, bool>> filter);
		IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(Expression<Func<TDerivedEntity, bool>> filter) where TDerivedEntity : TEntity;
		
		IFindFluent<TEntity, TEntity> Find(Expression<Func<TEntity, object>> property, string regexPattern, string regexOptions = "i");
		IFindFluent<TEntity, TEntity> Find(IEnumerable<Expression<Func<TEntity, object>>> properties, string regexPattern, string regexOptions = "i");

		IFindFluent<TEntity, TEntity> Find(FieldDefinition<TEntity> property, string regexPattern, string regexOptions = "i");
		IFindFluent<TEntity, TEntity> Find(IEnumerable<FieldDefinition<TEntity>> properties, string regexPattern, string regexOptions = "i");

		IFindFluent<TEntity, TEntity> Find(FilterDefinition<TEntity> filter);

		IFindFluent<TDerivedEntity, TDerivedEntity> Find<TDerivedEntity>(FilterDefinition<TDerivedEntity> filter)
			where TDerivedEntity : TEntity;

		IAggregateFluent<TEntity> Aggregate(AggregateOptions options = null);
	    Task DeletePropertyAsync(Expression<Func<TEntity, bool>> filter, Expression<Func<TEntity, object>> propertyExpression);

		/// <summary>
		/// Stores an arbitrary object in the trash
		/// </summary>
		Task AddToTrash<TObject>(TObject objectToTrash);

		Task DeleteManyAsync(Expression<Func<TEntity, bool>> filter, bool softDelete = false);
		Task DeleteManyAsync<TDerived>(Expression<Func<TDerived, bool>> filter, bool softDelete = false) where TDerived : TEntity;

		//Task<TEntity> RestoreSoftDeleted(string id);
		//Task<TEntity> RestoreSoftDeleted(Expression<Func<TEntity, bool>> filter);
		Task<TEntity> GetFromTrashAsync(string id);

		Task<ReplaceOneResult> ReplaceOneAsync(string id, TEntity entity, bool upsert = false);
		Task<ReplaceOneResult> ReplaceOneAsync(Expression<Func<TEntity, bool>> filter, TEntity entity, bool upsert = false);
		Task ReplaceManyAsync(IEnumerable<ReplaceManyCommand<TEntity>> commands, bool upsert = false);

		Task<bool> UpdateOneAsync(string id, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false);
		Task<bool> UpdateOneAsync(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false);
		Task<bool> UpdateOneAsync<TDerived>(string id, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false) where TDerived : TEntity;
		Task<bool> UpdateOneAsync<TDerived>(Expression<Func<TDerived, bool>> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false) where TDerived : TEntity;
		Task<TReturnProjection> FindOneAndUpdateAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, Expression<Func<TEntity, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false);
		Task<TReturnProjection> FindOneAndUpdateAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, ProjectionDefinition<TEntity, TReturnProjection> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false);

		/// <summary>
		/// Executes multiple update operations in one batch
		/// </summary>
		Task<BulkWriteResult<TEntity>> UpdateOneBulkAsync(IEnumerable<UpdateOneCommand<TEntity>> commands);

		/// <summary>
		/// Executes multiple update operations in one batch
		/// </summary>
		Task<BulkWriteResult<TDerived>> UpdateOneBulkAsync<TDerived>(IEnumerable<UpdateOneCommand<TDerived>> commands) where TDerived : TEntity;

		/// <summary>
		/// Applies the same update to multiple entities
		/// </summary>
		Task UpdateManyAsync(Expression<Func<TEntity, bool>> filter,
			Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update,
			UpdateOptions options = null);

		Task UpdateManyAsync(Expression<Func<TEntity, bool>> filter,
			string update,
			UpdateOptions options = null);

		Task<long> IncrementCounterAsync(string name = null, int incrementBy = 1);
		Task<long?> GetCounterValueAsync(string name = null);
		Task ResetCounterAsync(string name = null, long newValue = 1);
		Task<bool> UpdateOneAsync(string filter, string update, bool upsert = false);
		IFindFluent<TEntity, TEntity> TextSearch(string text);
		IFindFluent<TDerivedEntity, TDerivedEntity> TextSearch<TDerivedEntity>(string text) where TDerivedEntity : TEntity;
	}

	public class UpdateOneCommand<TEntity>
	{
		public UpdateOneCommand() { }

		public UpdateOneCommand(Func<FilterDefinitionBuilder<TEntity>, FilterDefinition<TEntity>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update)
		{
			Filter = filter;
			Update = update;
		}

		public UpdateOneCommand(Func<FilterDefinitionBuilder<TEntity>, FilterDefinition<TEntity>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert)
			: this(filter, update)
		{
			IsUpsert = upsert;
		}

		public Func<FilterDefinitionBuilder<TEntity>, FilterDefinition<TEntity>> Filter { get; set; }
		public Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> Update { get; set; }
		public string UpdateJson { get; set; }
		public bool IsUpsert { get; set; }
	}

	public class ReplaceManyCommand<TEntity>
	{
		public ReplaceManyCommand() { }

		public ReplaceManyCommand(Func<FilterDefinitionBuilder<TEntity>, FilterDefinition<TEntity>> filter, TEntity replacement)
		{
			Filter = filter;
			Replacement = replacement;
		}

		public Func<FilterDefinitionBuilder<TEntity>, FilterDefinition<TEntity>> Filter { get; set; }
		public TEntity Replacement { get; set; }
	}
}