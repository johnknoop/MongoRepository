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
    public class CappedCollectionConfig
    {
        public int? MaxSize { get; internal set; }
        public int? MaxDocuments { get; internal set; }
    }

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

	    public string CollectionName { get; internal set; }
		public string IdMember { get; internal set; }
	    public bool Capped { get; internal set; }
	    public CappedCollectionConfig CappedCollectionConfig { get; internal set; }
	    public IList<DatabaseIndexDefinition> Indices { get; internal set; } = new List<DatabaseIndexDefinition>();
	}

    public class TypeMappingBuilder<TEnity>
    {
        internal TypeMapping Mapping { get; }

        public TypeMappingBuilder(TypeMapping mapping)
        {
            Mapping = mapping;
        }

		/// <summary>
		/// If the entity has an id property with a different name than <c>Id</c>
		/// </summary>
        public TypeMappingBuilder<TEnity> WithIdProperty(Expression<Func<TEnity, string>> idProperty)
        {
            Mapping.IdMember = GetPropertyName(idProperty);
            return this;
        }

        public TypeMappingBuilder<TEnity> InCappedCollection(int? maxDocuments = null, int? maxSize = null)
        {
            Mapping.Capped = true;
            Mapping.CappedCollectionConfig = new CappedCollectionConfig
            {
                MaxDocuments = maxDocuments,
                MaxSize = maxSize
            };

            return this;
        }

        public TypeMappingBuilder<TEnity> WithIndex(Expression<Func<TEnity, object>> memberExpression, bool unique = false, bool sparse = false) {
            var names = new Dictionary<string, int>
            {
                {GetPropertyName(memberExpression), 1}
            };

            var indexKeys = Newtonsoft.Json.JsonConvert.SerializeObject(names);

            Mapping.Indices.Add(new DatabaseIndexDefinition {
                Type = typeof(TEnity),
                Keys = indexKeys,
                Sparse = sparse,
                Unique = unique
            });

            return this;
        }

        public TypeMappingBuilder<TEnity> WithIndex(IEnumerable<Expression<Func<TEnity, object>>> memberExpressions, bool unique = false, bool sparse = false) {
            var names =
                memberExpressions.Select(GetPropertyName).ToDictionary(x => x, x => 1);

            var indexKeys = Newtonsoft.Json.JsonConvert.SerializeObject(names);

            Mapping.Indices.Add(new DatabaseIndexDefinition {
                Type = typeof(TEnity),
                Keys = indexKeys,
                Sparse = sparse,
                Unique = unique
            });

            return this;
        }

        private string GetPropertyName<TSource>(Expression<Func<TSource, object>> memberExpression) {
            var expression = memberExpression.Body as UnaryExpression;
            string propertyName;

            if (expression == null) {
                var memExpr = memberExpression.Body as MemberExpression;
                propertyName = memExpr.Member.Name;
            } else {
                var property = expression.Operand as MemberExpression;
                propertyName = property.Member.Name;
            }

            return propertyName;
        }

        private string GetPropertyName<TSource>(Expression<Func<TSource, string>> memberExpression) {
            var expression = memberExpression.Body as UnaryExpression;
            string propertyName;

            if (expression == null) {
                var memExpr = memberExpression.Body as MemberExpression;
                propertyName = memExpr.Member.Name;
            } else {
                var property = expression.Operand as MemberExpression;
                propertyName = property.Member.Name;
            }

            return propertyName;
        }
    }

	public class DatabaseConfiguration
	{
		internal Dictionary<Type, TypeMapping> SingleClasses { get; } = new Dictionary<Type, TypeMapping>();
		internal Dictionary<Type, TypeMapping> PolymorpicClasses { get; } = new Dictionary<Type, TypeMapping>();

		public DatabaseConfiguration Map<T>(string collectionName, Action<TypeMappingBuilder<T>> builderFactory = null)
		{
            var builder = new TypeMappingBuilder<T>(new TypeMapping(collectionName));

		    builderFactory?.Invoke(builder);
		    SingleClasses[typeof(T)] = builder.Mapping;

			return this;
		}

		public DatabaseConfiguration Map<T>() => Map<T>(nameof(T));

		public DatabaseConfiguration MapAlongWithSubclassesInSameAssebmly<T>(string collectionName, Action<TypeMappingBuilder<T>> builderFactory = null)
		{
		    var builder = new TypeMappingBuilder<T>(new TypeMapping(collectionName));

		    builderFactory?.Invoke(builder);
		    PolymorpicClasses[typeof(T)] = builder.Mapping;

		    return this;
        }

		public DatabaseConfiguration MapAlongWithSubclassesInSameAssebmly<T>() => MapAlongWithSubclassesInSameAssebmly<T>(nameof(T));
	}

	public class MongoConfigurationBuilder
	{
		internal Dictionary<string, DatabaseConfiguration> GlobalDatabases { get; private set; } = new Dictionary<string, DatabaseConfiguration>();
		internal Dictionary<string, DatabaseConfiguration> TenantDatabases { get; private set; } = new Dictionary<string, DatabaseConfiguration>();
		internal IDatabaseNameProvider DatabaseNameProvider { get; private set; }

		/// <summary>
		/// These types will be persisted in database-per-assembly-and-tenant style
		/// </summary>
		public MongoConfigurationBuilder DatabasePerTenant(string databaseName, Func<DatabaseConfiguration, DatabaseConfiguration> collectionNameMapping)
		{
			TenantDatabases.Add(databaseName, collectionNameMapping(new DatabaseConfiguration()));
			return this;
		}

		/// <summary>
		/// These types will be persisted in a database-per-assembly style
		/// </summary>
		public MongoConfigurationBuilder Database(string databaseName, Func<DatabaseConfiguration, DatabaseConfiguration> collectionNameMapping)
		{
			GlobalDatabases.Add(databaseName, collectionNameMapping(new DatabaseConfiguration()));
			return this;
		}

		public void Build()
		{
			MongoConfiguration.Build(this);
		}
	}

	public static class MongoConfiguration
    {
        private class DatabaseCollectionNamePair
        {
            public string DatabaseName { get; set; }
            public string CollectionName { get; set; }
        }

	    private static bool _isConfigured = false;
        private static Dictionary<Type, DatabaseCollectionNamePair> _collections = new Dictionary<Type, DatabaseCollectionNamePair>();
        private static ILookup<Type, DatabaseIndexDefinition> _indices;
	    private static HashSet<Type> _globalTypes;
		private static readonly ConcurrentDictionary<Type, bool> _indexesAndCapEnsured = new ConcurrentDictionary<Type, bool>();
		private static Dictionary<Type, CappedCollectionConfig> _cappedCollections;

	    internal static void Build(MongoConfigurationBuilder builder)
	    {
			if (_isConfigured)
			{
				return;
			}

	        var allMappedClasses = builder.GlobalDatabases.SelectMany(x => x.Value.PolymorpicClasses.Select(y => new
	        {
	            DatabaseName = WashDatabaseName(x.Key),
	            IsPerTenantDatabase = false,
	            IsPolymorphic = true,
	            Type = y.Key,
	            Mapping = y.Value
	        }).Concat(x.Value.SingleClasses.Select(y => new
	        {
	            DatabaseName = WashDatabaseName(x.Key),
	            IsPerTenantDatabase = false,
	            IsPolymorphic = false,
	            Type = y.Key,
	            Mapping = y.Value
	        }))).Concat(builder.TenantDatabases.SelectMany(x => x.Value.PolymorpicClasses.Select(y => new
	        {
	            DatabaseName = WashDatabaseName(x.Key),
	            IsPerTenantDatabase = true,
	            IsPolymorphic = true,
	            Type = y.Key,
	            Mapping = y.Value
	        }).Concat(x.Value.SingleClasses.Select(y => new
	        {
	            DatabaseName = WashDatabaseName(x.Key),
	            IsPerTenantDatabase = true,
	            IsPolymorphic = false,
	            Type = y.Key,
	            Mapping = y.Value
	        })))).ToList();

	        _cappedCollections = allMappedClasses.Where(x => x.Mapping.Capped).ToDictionary(x => x.Type, x => x.Mapping.CappedCollectionConfig);
            _collections = allMappedClasses.ToDictionary(x => x.Type, x => new DatabaseCollectionNamePair
            {
                CollectionName = x.Mapping.CollectionName,
                DatabaseName = x.DatabaseName
            });

		    if (!_collections.ContainsKey(typeof(DeletedObject)))
		    {
			    _collections[typeof(DeletedObject)] = new DatabaseCollectionNamePair {
			        CollectionName = "DeletedObjects",
			        DatabaseName = null // One per database
			    };
		    }

			_indices = allMappedClasses
                .SelectMany(x => x.Mapping.Indices.Select(y => new
			        {
			            x.Type,
                        Index = y
			        })
			    )
                .ToLookup(x => x.Type, x => x.Index);

		    _globalTypes = new HashSet<Type>(allMappedClasses.Where(x => !x.IsPerTenantDatabase).Select(x => x.Type));

		    var conventionPack = new ConventionPack
		    {
			    new IgnoreExtraElementsConvention(true),
			    new StringObjectIdConvention()
		    };

		    ConventionRegistry.Register("Conventions", conventionPack, type => true);


		    foreach (var t in allMappedClasses)
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

        private static IMongoCollection<TEntity> GetMongoCollection<TEntity>(IMongoClient mongoClient, string tenantKey)
        {
            var entityType = typeof(TEntity);

            if (!_collections.ContainsKey(entityType)) {
                throw new NotImplementedException($"{entityType.Name} is not mapped");
            }

            var databaseName = GetDatabaseName(entityType, tenantKey);

            return GetMongoCollectionInDatabase<TEntity>(mongoClient, databaseName);
        }

        private static IMongoCollection<TEntity> GetMongoCollectionInDatabase<TEntity>(IMongoClient mongoClient, string databaseName) {
            var entityType = typeof(TEntity);

            if (!_collections.ContainsKey(entityType)) {
                throw new NotImplementedException($"{entityType.Name} is not mapped");
            }

            var database = mongoClient.GetDatabase(databaseName);

            var mongoCollection = database.GetCollection<TEntity>(_collections[entityType].CollectionName);

            return mongoCollection;
        }

        public static IRepository<TEntity> GetRepository<TEntity>(IMongoClient mongoClient, string tenantKey = null)
	    {
		    var mongoCollection = GetMongoCollection<TEntity>(mongoClient, tenantKey);
		    var trashCollection = GetMongoCollectionInDatabase<DeletedObject>(mongoClient, GetDatabaseName(typeof(TEntity), tenantKey));

		    return new DatabaseRepository<TEntity>(mongoCollection, trashCollection);
		}

        private static string WashDatabaseName(string name) {
            return new string(name.Where(letter => letter >= 97 && letter <= 122 || letter >= 65 && letter <= 90).ToArray());
        }

        private static string GetDatabaseName(Type entityType, string tenantKey = null)
        {
	        if (entityType == null) throw new ArgumentNullException(nameof(entityType));

            var databaseName = _collections[entityType].DatabaseName;

            return _globalTypes.Contains(entityType)
                ? databaseName
                : $"{tenantKey}_{databaseName}";
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

	    internal static async Task EnsureIndexesAndCap<TEntity>(IMongoCollection<TEntity> mongoCollection)
	    {
		    if (_indexesAndCapEnsured.ContainsKey(typeof(TEntity)))
		    {
			    return;
		    }

            // Create capped collection

	        if (_cappedCollections.ContainsKey(typeof(TEntity)) &&
				!(await mongoCollection .Database.ListCollectionsAsync(new ListCollectionsOptions
				{
					Filter = new BsonDocument("name", mongoCollection.CollectionNamespace.CollectionName)
				})).Any()
			)
	        {
	            var capConfig = _cappedCollections[typeof(TEntity)];

                await mongoCollection.Database.CreateCollectionAsync(_collections[typeof(TEntity)].CollectionName, new CreateCollectionOptions
	            {
	                Capped = true,
                    MaxDocuments = capConfig.MaxDocuments,
                    MaxSize = capConfig.MaxSize ?? 1000000000000 // One terabyte
				});
	        }

            // Create index

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

		    _indexesAndCapEnsured.TryAdd(typeof(TEntity), true);
	    }

	    internal static IList<Type> GetMappedTypes() => _collections.Keys.ToList();
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