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
		IFindFluent<TEntity, TEntity> Find(Expression<Func<TEntity, object>> property, string regexPattern);

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

		Task<bool> ModifyOneAsync(string id, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false);
		Task<bool> ModifyOneAsync(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, bool upsert = false);
		Task<bool> ModifyOneAsync<TDerived>(string id, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false) where TDerived : TEntity;
		Task<bool> ModifyOneAsync<TDerived>(Expression<Func<TDerived, bool>> filter, Func<UpdateDefinitionBuilder<TDerived>, UpdateDefinition<TDerived>> update, bool upsert = false) where TDerived : TEntity;
		Task<TReturnProjection> FindOneAndModifyAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, Expression<Func<TEntity, TReturnProjection>> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false);
		Task<TReturnProjection> FindOneAndModifyAsync<TReturnProjection>(Expression<Func<TEntity, bool>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update, ProjectionDefinition<TEntity, TReturnProjection> returnProjection, ReturnedDocumentState returnedDocumentState = ReturnedDocumentState.AfterUpdate, bool upsert = false);

		/// <summary>
		/// Executes multiple update operations in one batch
		/// </summary>
		Task ModifyOneBulkAsync(IEnumerable<ModifyOneCommand<TEntity>> commands);

		/// <summary>
		/// Executes multiple update operations in one batch
		/// </summary>
		Task ModifyOneBulkAsync<TDerived>(IEnumerable<ModifyOneCommand<TDerived>> commands) where TDerived : TEntity;

		/// <summary>
		/// Applies the same update to multiple entities
		/// </summary>
		Task ModifyManyAsync(Expression<Func<TEntity, bool>> filter,
			Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update,
			UpdateOptions options = null);

		Task ModifyManyAsync(Expression<Func<TEntity, bool>> filter,
			string update,
			UpdateOptions options = null);

		Task<long> IncrementCounterAsync(string name = null, int incrementBy = 1);
		Task<long?> GetCounterValueAsync(string name = null);
		Task ResetCounterAsync(string name = null, long newValue = 1);
	}

	public class ModifyOneCommand<TEntity>
	{
		public ModifyOneCommand() { }

		public ModifyOneCommand(Func<FilterDefinitionBuilder<TEntity>, FilterDefinition<TEntity>> filter, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> update)
		{
			Filter = filter;
			Update = update;
		}

		public Func<FilterDefinitionBuilder<TEntity>, FilterDefinition<TEntity>> Filter { get; set; }
		public Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> Update { get; set; }
		public string UpdateJson { get; set; }
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