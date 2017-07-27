using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using JohnKnoop.MongoRepository.DatabaseNameProviders;
using MongoDb.Bson.NodaTime;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;

namespace JohnKnoop.MongoRepository
{
	public class TypeMapping
	{
		public TypeMapping(string collectionName, string idMember)
		{
			CollectionName = collectionName;
			IdMember = idMember;
		}

		public TypeMapping(string collectionName)
		{
			CollectionName = collectionName;
		}

		public string CollectionName { get; private set; }
		public string IdMember { get; private set; }
	}

	public class TypeMappingConfiguration
	{
		internal Dictionary<Type, TypeMapping> SingleClasses { get; private set; } = new Dictionary<Type, TypeMapping>();
		internal Dictionary<Type, TypeMapping> PolymorpicClasses { get; private set; } = new Dictionary<Type, TypeMapping>();

		private string GetPropertyName<TSource>(Expression<Func<TSource, string>> memberExpression)
		{
			var expression = memberExpression.Body as UnaryExpression;
			string propertyName;

			if (expression == null)
			{
				var memExpr = memberExpression.Body as MemberExpression;
				propertyName = memExpr.Member.Name;
			}
			else
			{
				var property = expression.Operand as MemberExpression;
				propertyName = property.Member.Name;
			}

			return propertyName;
		}

		public TypeMappingConfiguration Map<T>(string collectionName, Expression<Func<T, string>> idProperty = null)
		{
			var idPropertyName = idProperty != null
				? GetPropertyName(idProperty)
				: null;

			SingleClasses[typeof(T)] = new TypeMapping(collectionName, idPropertyName);
			return this;
		}

		public TypeMappingConfiguration Map<T>() => Map<T>(nameof(T));

		public TypeMappingConfiguration MapAlongWithSubclassesInSameAssebmly<T>(string collectionName, Expression<Func<T, string>> idProperty = null)
		{
			var idPropertyName = idProperty != null
				? GetPropertyName(idProperty)
				: null;

			PolymorpicClasses[typeof(T)] = new TypeMapping(collectionName, idPropertyName);
			return this;
		}

		public TypeMappingConfiguration MapAlongWithSubclassesInSameAssebmly<T>() => MapAlongWithSubclassesInSameAssebmly<T>(nameof(T));
	}

	public class MongoConfigurationBuilder
	{
		internal TypeMappingConfiguration GlobalTypes { get; private set; }
		internal TypeMappingConfiguration TenantTypes { get; private set; }
		internal IList<DatabaseIndexDefinition> Indices { get; } = new List<DatabaseIndexDefinition>();
		internal IDatabaseNameProvider DatabaseNameProvider { get; private set; }

		private static string GetPropertyName<TSource>(Expression<Func<TSource, object>> memberExpression)
		{
			var expression = memberExpression.Body as UnaryExpression;
			string propertyName;

			if (expression == null)
			{
				var memExpr = memberExpression.Body as MemberExpression;
				propertyName = memExpr.Member.Name;
			}
			else
			{
				var property = expression.Operand as MemberExpression;
				propertyName = property.Member.Name;
			}

			return propertyName;
		}

		/// <summary>
		/// These types will be persisted in database-per-assembly-and-tenant style
		/// </summary>
		public MongoConfigurationBuilder WithTenantMappings(Func<TypeMappingConfiguration, TypeMappingConfiguration> collectionNameMapping)
		{
			TenantTypes = collectionNameMapping(new TypeMappingConfiguration());
			return this;
		}

		/// <summary>
		/// These types will be persisted in a database-per-assembly style
		/// </summary>
		public MongoConfigurationBuilder WithMappings(Func<TypeMappingConfiguration, TypeMappingConfiguration> collectionNameMapping)
		{
			GlobalTypes = collectionNameMapping(new TypeMappingConfiguration());
			return this;
		}

		public MongoConfigurationBuilder WithIndex<T>(Expression<Func<T, object>> memberExpression, bool unique = false, bool sparse = false)
		{
			var names = new Dictionary<string, int>
			{
				{GetPropertyName(memberExpression), 1}
			};

			var indexKeys = Newtonsoft.Json.JsonConvert.SerializeObject(names);

			this.Indices.Add(new DatabaseIndexDefinition
			{
				Type = typeof(T),
				Keys = indexKeys,
				Sparse = sparse,
				Unique = unique
			});

			return this;
		}

		public MongoConfigurationBuilder WithIndex<T>(IEnumerable<Expression<Func<T, object>>> memberExpressions, bool unique = false, bool sparse = false)
		{
			var names =
				memberExpressions.Select(GetPropertyName).ToDictionary(x => x, x => 1);

			var indexKeys = Newtonsoft.Json.JsonConvert.SerializeObject(names);

			this.Indices.Add(new DatabaseIndexDefinition
			{
				Type = typeof(T),
				Keys = indexKeys,
				Sparse = sparse,
				Unique = unique
			});

			return this;
		}

		public MongoConfigurationBuilder WithDatabaseNameProvider<T>(T databaseNameProvider) where T : IDatabaseNameProvider
		{
			this.DatabaseNameProvider = databaseNameProvider;

			return this;
		}

		public void Build()
		{
			MongoConfiguration.Build(this);
		}
	}

	public static class MongoConfiguration
    {
	    private static bool _isConfigured = false;
        private static Dictionary<Type, string> _collectionNames;
        private static ILookup<Type, DatabaseIndexDefinition> _indices;
	    private static HashSet<Type> _globalTypes;
	    private static IDatabaseNameProvider _databaseNameProvider;
		private static readonly ConcurrentDictionary<Type, bool> _indexesEnsured = new ConcurrentDictionary<Type, bool>();

	    internal static void Build(MongoConfigurationBuilder builder)
	    {
			if (_isConfigured)
			{
				return;
			}

		    _databaseNameProvider = builder.DatabaseNameProvider ?? new ContainingAssemblyNameDatabaseNameProvider();

			var globalTypesSingleClasses     = builder.GlobalTypes?.SingleClasses ?? new Dictionary<Type, TypeMapping>();
		    var globalTypesPolymorpicClasses = builder.GlobalTypes?.PolymorpicClasses ?? new Dictionary<Type, TypeMapping>();
		    var tenantTypesPolymorpicClasses = builder.TenantTypes?.PolymorpicClasses ?? new Dictionary<Type, TypeMapping>();
		    var tenantTypesSingleClasses     = builder.TenantTypes?.SingleClasses ?? new Dictionary<Type, TypeMapping>();

		    _collectionNames = globalTypesSingleClasses
			    .Concat(globalTypesPolymorpicClasses)
			    .Concat(tenantTypesSingleClasses)
			    .Concat(tenantTypesPolymorpicClasses)
			    .ToDictionary(x => x.Key, x => x.Value.CollectionName);

		    if (!_collectionNames.ContainsKey(typeof(DeletedObject)))
		    {
			    _collectionNames[typeof(DeletedObject)] = "DeletedObjects";
		    }

			_indices = builder.Indices.ToLookup(x => x.Type);
		    _globalTypes = new HashSet<Type>(globalTypesSingleClasses.Keys.Concat(globalTypesPolymorpicClasses.Keys));

		    var conventionPack = new ConventionPack
		    {
			    new IgnoreExtraElementsConvention(true),
			    new StringObjectIdConvention()
		    };

		    ConventionRegistry.Register("Conventions", conventionPack, type => true);

		    var typesWithNonConventionalIds = 
				globalTypesSingleClasses.Select(x => new
					{
						IsPolymorphic = false,
						Type = x.Key,
						Mapping = x.Value
					})
				.Concat(tenantTypesSingleClasses.Select(x => new
					{
						IsPolymorphic = false,
						Type = x.Key,
						Mapping = x.Value
					}))
				.Concat(globalTypesPolymorpicClasses.Select(x => new
					{
						IsPolymorphic = true,
						Type = x.Key,
						Mapping = x.Value
					}))
			    
			    .Concat(tenantTypesPolymorpicClasses.Select(x => new
					{
						IsPolymorphic = true,
						Type = x.Key,
						Mapping = x.Value
					}));

		    foreach (var t in typesWithNonConventionalIds)
		    {
			    var bsonClassMap = new BsonClassMap(t.Type);

			    bsonClassMap.AutoMap();

			    if (t.Mapping.IdMember != null)
			    {
				    bsonClassMap.MapIdProperty(t.Mapping.IdMember).SetSerializer(new StringSerializer(BsonType.ObjectId)).SetIdGenerator(new StringObjectIdGenerator());
			    }

			    BsonClassMap.RegisterClassMap(bsonClassMap);

			    if (t.IsPolymorphic)
			    {
				    var subtypes = t.Type.GetTypeInfo().Assembly.GetTypes().Where(st => st.GetTypeInfo().IsSubclassOf(t.Type));

				    foreach (var st in subtypes)
				    {
					    var bsonSubClassMap = new BsonClassMap(st);
					    bsonSubClassMap.AutoMap();

					    if (!BsonClassMap.IsClassMapRegistered(st))
					    {
						    BsonClassMap.RegisterClassMap(bsonSubClassMap);
					    }
				    }
			    }
			}

			BsonSerializer.RegisterSerializer(typeof(decimal), new DecimalToWholeCentsSerializer());
		    NodaTimeSerializers.Register();

		    MongoDefaults.MaxConnectionIdleTime = TimeSpan.FromMinutes(1);

		    _isConfigured = true;
		}

        private static IEnumerable<DatabaseIndexDefinition> GetIndicesFor<TEntity>()
        {
            var type = typeof(TEntity);
            return _indices[type];
        }

        internal static IMongoCollection<TEntity> GetMongoCollection<TEntity>(IMongoClient mongoClient, string tenantKey)
        {
            var entityType = typeof(TEntity);
            var databaseName = GetDatabaseName(entityType, tenantKey);

            var database = mongoClient.GetDatabase(databaseName);

            if (!_collectionNames.ContainsKey(entityType))
            {
                throw new NotImplementedException($"{entityType.Name} is not mapped");
            }

            var mongoCollection = database.GetCollection<TEntity>(_collectionNames[entityType]);

            return mongoCollection;
        }

	    public static IRepository<TEntity> GetRepository<TEntity>(IMongoClient mongoClient, string tenantKey = null)
	    {
		    var mongoCollection = GetMongoCollection<TEntity>(mongoClient, tenantKey);
		    var trashCollection = GetMongoCollection<DeletedObject>(mongoClient, tenantKey);

		    return new DatabaseRepository<TEntity>(mongoCollection, trashCollection);
		}

        private static string GetDatabaseName(Type entityType, string tenantKey = null)
        {
	        if (entityType == null) throw new ArgumentNullException(nameof(entityType));

	        return _databaseNameProvider.GetDatabaseName(
		        entityType: entityType,
		        tenantKey: _globalTypes.Contains(entityType)
			        ? null
			        : tenantKey
				);
        }

        private class StringObjectIdConvention : ConventionBase, IPostProcessingConvention
        {
            public void PostProcess(BsonClassMap classMap)
            {
                var idMap = classMap.IdMemberMap;
                if (idMap != null && idMap.MemberName == "Id" && idMap.MemberType == typeof(string))
                {
                    idMap.SetSerializer(new StringSerializer(BsonType.ObjectId));
                    idMap.SetIdGenerator(new StringObjectIdGenerator());
                }
            }
        }

	    internal static async Task EnsureIndexes<TEntity>(IMongoCollection<TEntity> mongoCollection)
	    {
		    if (_indexesEnsured.ContainsKey(typeof(TEntity)))
		    {
			    return;
		    }

		    var createIndexOptionses = GetIndicesFor<TEntity>().Select(ix => new CreateIndexModel<TEntity>(
				ix.Keys,
			    new CreateIndexOptions
			    {
				    Unique = ix.Unique,
				    Sparse = ix.Sparse
			    })
			).ToList();

		    if (createIndexOptionses.Any())
		    {
			    await mongoCollection.Indexes.CreateManyAsync(createIndexOptionses).ConfigureAwait(false);
		    }

		    _indexesEnsured.TryAdd(typeof(TEntity), true);
	    }
    }

    public class DecimalToWholeCentsSerializer : IBsonSerializer<decimal>
    {
        public decimal Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var dbData = context.Reader.ReadInt32();
            return (decimal)dbData / (decimal)100;
        }

        public void Serialize(BsonSerializationContext context, BsonSerializationArgs args, decimal value)
        {
            var realValue = (decimal)value;
            context.Writer.WriteInt32(Convert.ToInt32(realValue * 100));
        }

        object IBsonSerializer.Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var dbData = context.Reader.ReadInt32();
            return (decimal)dbData / (decimal)100;
        }

        public void Serialize(BsonSerializationContext context, BsonSerializationArgs args, object value)
        {
            var realValue = (decimal)value;
            context.Writer.WriteInt32(Convert.ToInt32(realValue * 100));
        }

        public Type ValueType => typeof(decimal);
    }

    public class DatabaseIndexDefinition
    {
	    public Type Type { get; set; }
        public string Keys { get; set; }
        public bool Unique { get; set; }
        public bool Sparse { get; set; }
    }

	public static class MongoClientExtensions
	{
		public static IRepository<T> GetRepository<T>(this IMongoClient client) =>
			MongoConfiguration.GetRepository<T>(client);

		public static IRepository<T> GetRepository<T>(this IMongoClient client, string tenantKey) =>
			MongoConfiguration.GetRepository<T>(client, tenantKey);
	}
}