using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
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
            Mapping.IdMember = PropertyNameExtractor.GetPropertyName(idProperty);
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

		public TypeMappingBuilder<TEnity> WithTextIndex(Expression<Func<TEnity, object>> memberExpression, bool sparse = false, double weight = 1, string language = "english") {
            var names = new Dictionary<string, string>
            {
                {PropertyNameExtractor.GetPropertyName(memberExpression), "text"}
            };

			var weights = new Dictionary<string, double>
            {
                {PropertyNameExtractor.GetPropertyName(memberExpression), weight}
            };

            var indexKeys = Newtonsoft.Json.JsonConvert.SerializeObject(names);
            var weightsDocument = BsonSerializer.Deserialize<BsonDocument>(Newtonsoft.Json.JsonConvert.SerializeObject(weights));

            Mapping.Indices.Add(new DatabaseIndexDefinition {
                Type = typeof(TEnity),
                Keys = indexKeys,
                Sparse = sparse,
                Weights = weightsDocument,
				Language = language
            });

            return this;
        }

		public TypeMappingBuilder<TEnity> WithTextIndex(Dictionary<Expression<Func<TEnity, object>>, double> memberExpressions, bool sparse = false, string language = "english") {
            var names =
                memberExpressions.Keys.Select(PropertyNameExtractor.GetPropertyName).ToDictionary(x => x, x => "text");

			var weights = memberExpressions.ToDictionary(x => PropertyNameExtractor.GetPropertyName(x.Key), x => x.Value);

            var indexKeys = Newtonsoft.Json.JsonConvert.SerializeObject(names);
			var weightsDocument = BsonSerializer.Deserialize<BsonDocument>(Newtonsoft.Json.JsonConvert.SerializeObject(weights));

            Mapping.Indices.Add(new DatabaseIndexDefinition {
                Type = typeof(TEnity),
                Keys = indexKeys,
                Sparse = sparse,
                Weights = weightsDocument,
				Language = language
            });

            return this;
        }

        public TypeMappingBuilder<TEnity> WithIndex(Expression<Func<TEnity, object>> memberExpression, bool unique = false, bool sparse = false) {
            var names = new Dictionary<string, int>
            {
                {PropertyNameExtractor.GetPropertyName(memberExpression), 1}
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
                memberExpressions.Select(PropertyNameExtractor.GetPropertyName).ToDictionary(x => x, x => 1);

            var indexKeys = Newtonsoft.Json.JsonConvert.SerializeObject(names);

            Mapping.Indices.Add(new DatabaseIndexDefinition {
                Type = typeof(TEnity),
                Keys = indexKeys,
                Sparse = sparse,
                Unique = unique
            });

            return this;
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

		public DatabaseConfiguration Map<T>() => Map<T>(typeof(T).Name);

		public DatabaseConfiguration MapAlongWithSubclassesInSameAssebmly<T>(string collectionName, Action<TypeMappingBuilder<T>> builderFactory = null)
		{
		    var builder = new TypeMappingBuilder<T>(new TypeMapping(collectionName));

		    builderFactory?.Invoke(builder);
		    PolymorpicClasses[typeof(T)] = builder.Mapping;

		    return this;
        }

		public DatabaseConfiguration MapAlongWithSubclassesInSameAssebmly<T>() => MapAlongWithSubclassesInSameAssebmly<T>(typeof(T).Name);
	}

	public class MongoConfigurationBuilder
	{
		internal Dictionary<string, DatabaseConfiguration> GlobalDatabases { get; private set; } = new Dictionary<string, DatabaseConfiguration>();
		internal Dictionary<string, DatabaseConfiguration> TenantDatabases { get; private set; } = new Dictionary<string, DatabaseConfiguration>();
        private IList<Action<MongoConfigurationBuilder>> _plugins = new List<Action<MongoConfigurationBuilder>>();
		internal bool ShouldAutoEnlistWithTransactionScopes { get; private set; }

		public void AddPlugin(Action<MongoConfigurationBuilder> plugin)
	    {
	        _plugins.Add(plugin);
	    }

		public MongoConfigurationBuilder AutoEnlistWithTransactionScopes()
		{
			ShouldAutoEnlistWithTransactionScopes = true;
			return this;
		}

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
			MongoConfiguration.Build(this, _plugins);
		}

		public MongoConfigurationBuilder StoreDecimalsAsWholeCents()
		{
			_plugins.Add(_ =>
			{
				BsonSerializer.RegisterSerializer(typeof(decimal), new DecimalToWholeCentsSerializer());
			});

			return this;
		}

		public MongoConfigurationBuilder StoreDecimalsAs128BitNumberDecimal()
		{
			_plugins.Add(_ =>
			{
				BsonSerializer.RegisterSerializer(typeof(decimal), new DecimalSerializer(BsonType.Decimal128));
				BsonSerializer.RegisterSerializer(typeof(decimal?), new NullableSerializer<decimal>(new DecimalSerializer(BsonType.Decimal128)));
			});

			return this;
		}
	}

	public static class MongoConfiguration
    {
        private class DatabaseCollectionDefinition
        {
            public string DatabaseName { get; set; }
            public string CollectionName { get; set; }
			public bool IsPolymorphic { get; set; }
        }

	    private static bool _isConfigured = false;
        private static Dictionary<Type, DatabaseCollectionDefinition> _collections = new Dictionary<Type, DatabaseCollectionDefinition>();
        private static ILookup<Type, DatabaseIndexDefinition> _indices;
	    private static HashSet<Type> _globalTypes;
		private static bool _shouldAutoEnlistWithTransactionScopes;

		/// <summary>
		/// Key: {database}.{collection}
		/// </summary>
		private static readonly ConcurrentDictionary<string, bool> IndexesAndCapEnsured = new ConcurrentDictionary<string, bool>();
		private static Dictionary<Type, CappedCollectionConfig> _cappedCollections;

	    internal static void Build(MongoConfigurationBuilder builder, IList<Action<MongoConfigurationBuilder>> plugins)
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

            _collections = allMappedClasses.ToDictionary(x => x.Type, x => new DatabaseCollectionDefinition
            {
                CollectionName = x.Mapping.CollectionName,
                DatabaseName = x.DatabaseName,
				IsPolymorphic = x.IsPolymorphic
            });

		    _collections[typeof(SoftDeletedEntity<>)] = new DatabaseCollectionDefinition {
				IsPolymorphic = true,
		        CollectionName = "DeletedObjects",
		        DatabaseName = null // One per database
		    };

			var baseClasses = allMappedClasses.Where(x => x.IsPolymorphic);

			foreach (var baseClass in baseClasses)
			{
				var subtypes = baseClass.Type.GetTypeInfo().Assembly.GetTypes().Where(st => st.GetTypeInfo().IsSubclassOf(baseClass.Type));

				foreach (var subtype in subtypes)
				{
					_collections[subtype] = new DatabaseCollectionDefinition
					{
						CollectionName = baseClass.Mapping.CollectionName,
						IsPolymorphic = false,
						DatabaseName = baseClass.DatabaseName
					};
				}
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

			_shouldAutoEnlistWithTransactionScopes = builder.ShouldAutoEnlistWithTransactionScopes;

		    var conventionPack = new ConventionPack
		    {
			    new IgnoreExtraElementsConvention(true),
			    // Todo: behövs både denna och rad 356
				new StringObjectIdConvention()
		    };

		    ConventionRegistry.Register("Conventions", conventionPack, type => true);

		    foreach (var t in allMappedClasses)
		    {
			    var bsonClassMap = new BsonClassMap(t.Type);

			    bsonClassMap.AutoMap();
				
				if (t.Mapping.IdMember != null)
			    {
				    bsonClassMap
						.MapIdProperty(t.Mapping.IdMember)
						.SetSerializer(new StringSerializer(BsonType.ObjectId))
						.SetIdGenerator(new StringObjectIdGenerator());
			    }

				if (!BsonClassMap.IsClassMapRegistered(t.Type))
				{
					BsonClassMap.RegisterClassMap(bsonClassMap);
				}

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
		    
            foreach (var plugin in plugins)
            {
                plugin(builder);
            }

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

			if (entityType.IsGenericType && entityType.GetGenericTypeDefinition() == typeof(SoftDeletedEntity<>))
			{
				entityType = typeof(SoftDeletedEntity<>);
			}
            
			if (!_collections.ContainsKey(entityType))
			{
                throw new NotImplementedException($"{entityType.Name} is not mapped");
            }

            var database = mongoClient.GetDatabase(databaseName);

            return database.GetCollection<TEntity>(_collections[entityType].CollectionName);
        }

        public static IRepository<TEntity> GetRepository<TEntity>(IMongoClient mongoClient, string tenantKey = null)
	    {
		    var mongoCollection = GetMongoCollection<TEntity>(mongoClient, tenantKey);
		    var trashCollection = GetMongoCollectionInDatabase<SoftDeletedEntity<TEntity>>(mongoClient, GetDatabaseName(typeof(TEntity), tenantKey));

		    return new MongoRepository<TEntity>(mongoCollection, trashCollection, tenantKey, _shouldAutoEnlistWithTransactionScopes);
		}

        private static string WashDatabaseName(string name) {
            return new string(name.Where(letter => letter >= 97 && letter <= 122 || letter >= 65 && letter <= 90).ToArray());
        }

		private static readonly ConcurrentDictionary<string, IList<string>> DatabaseNamesPerTenant = new ConcurrentDictionary<string, IList<string>>();
		internal static IList<string> GetDatabaseNames(string tenantKey = null)
		{
			if (!_isConfigured)
			{
				throw new InvalidOperationException("Configuration not built");
			}

			if (DatabaseNamesPerTenant.ContainsKey(tenantKey ?? "global"))
			{
				return DatabaseNamesPerTenant[tenantKey ?? "global"];
			}

			var databaseNames = _collections
				.Where(x => x.Key != typeof(SoftDeletedEntity<>))
				.Select(x => new
				{
					IsGlobal = _globalTypes.Contains(x.Key),
					DatabaseName = x.Value.DatabaseName
				})
				.Where(x => tenantKey == null ? x.IsGlobal : !x.IsGlobal)
				.GroupBy(x => new {x.DatabaseName, x.IsGlobal})
				.Select(x => x.Key.IsGlobal ? x.Key.DatabaseName : $"{tenantKey}_{x.Key.DatabaseName}")
				.ToList();

			DatabaseNamesPerTenant.TryAdd(tenantKey ?? "global", databaseNames);

			return databaseNames;
		}

        internal static string GetDatabaseName(Type entityType, string tenantKey = null)
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
				bool IsEntityOrBaseOfEntity(Type t)
				{
					return _collections.ContainsKey(t) || _collections.Any(x => t.IsAssignableFrom(x.Key));
				}

				bool IsTrash(Type t)
				{
					if (!t.IsGenericType)
					{
						return false;
					}

					return t.GetGenericTypeDefinition().Equals(typeof(SoftDeletedEntity<>));
				}

				if (!IsEntityOrBaseOfEntity(classMap.ClassType) && !IsTrash(classMap.ClassType))
				{
					return;
				}

                var idMap = classMap.IdMemberMap;
                if (idMap != null && idMap.MemberName == "Id" && idMap.MemberType == typeof(string))
                {
                    idMap.SetSerializer(new StringSerializer(BsonType.ObjectId));
                    idMap.SetIdGenerator(new StringObjectIdGenerator());
                }
            }
        }

	    internal static async Task EnsureIndexesAndCap<TEntity>(IMongoCollection<TEntity> mongoCollection, bool forceCreateCollection = false)
	    {
			var entityType = typeof(TEntity);

			if (IndexesAndCapEnsured.ContainsKey($"{mongoCollection.Database.DatabaseNamespace.DatabaseName}.{mongoCollection.CollectionNamespace.CollectionName}"))
		    {
			    return;
		    }

			var collectionDefinition = _collections[entityType];
			var collectionCreated = false;

			var collectionExists = new Lazy<Task<bool>>(async () => 
				await(
					await mongoCollection.Database.ListCollectionNamesAsync(
						new ListCollectionNamesOptions { Filter = new BsonDocument("name", collectionDefinition.CollectionName) } ).ConfigureAwait(false)
				).AnyAsync().ConfigureAwait(false)
			);

			// Create capped collection

			if (_cappedCollections.ContainsKey(entityType) && !await collectionExists.Value)
	        {
	            var capConfig = _cappedCollections[entityType];

				await mongoCollection.Database.CreateCollectionAsync(collectionDefinition.CollectionName, new CreateCollectionOptions
	            {
	                Capped = true,
                    MaxDocuments = capConfig.MaxDocuments,
                    MaxSize = capConfig.MaxSize ?? 1_000_000_000_000 // One terabyte
				}).ConfigureAwait(false);

				collectionCreated = true;
	        }

			// Create index

			var createIndexOptions = GetIndicesFor<TEntity>().Select(ix => new CreateIndexModel<TEntity>(
				ix.Keys,
			    new CreateIndexOptions
			    {
				    Unique = ix.Unique,
				    Sparse = ix.Sparse,
					Weights = ix.Weights,
					DefaultLanguage = ix.Language
			    })
			).ToList();

			if (collectionDefinition.IsPolymorphic)
			{
				createIndexOptions.Add(new CreateIndexModel<TEntity>(Builders<TEntity>.IndexKeys.Ascending("_t")));
			}

		    if (createIndexOptions.Any())
		    {
				// Collection will be created if it doesn't already exist
			    await mongoCollection.Indexes.CreateManyAsync(createIndexOptions).ConfigureAwait(false);
				collectionCreated = true;
		    }

			if (!collectionCreated && forceCreateCollection && !await collectionExists.Value) {
				await mongoCollection.Database.CreateCollectionAsync(collectionDefinition.CollectionName).ConfigureAwait(false);
			}

		    IndexesAndCapEnsured.TryAdd($"{mongoCollection.Database.DatabaseNamespace.DatabaseName}.{mongoCollection.CollectionNamespace.CollectionName}", true);
	    }

	    internal static IList<Type> GetMappedTypes() => _collections.Keys.ToList();

		private static ConcurrentDictionary<string, bool> InitializedTenants = new ConcurrentDictionary<string, bool>();
		internal static void EnsureCollectionsCreated(IMongoClient client, string tenantKey = null)
		{
			if (InitializedTenants.ContainsKey(tenantKey ?? ""))
			{
				return;
			}

			foreach (var dbName in GetDatabaseNames(tenantKey))
			{
				var db = client.GetDatabase(dbName);

				var GetCollectionMethod = typeof(IMongoDatabase).GetMethod(nameof(IMongoDatabase.GetCollection));

				// Create collectinos for all mapped types

				foreach (var entityType in GetMappedTypes())
				{
					if (entityType == (typeof(SoftDeletedEntity<>)))
					{
						continue;
					}
					
					var genenricMethod = GetCollectionMethod.MakeGenericMethod(entityType);

					var collection = genenricMethod
						.Invoke(db, new object[] { _collections[entityType].CollectionName, null });

					var ensureIndexesMethod = typeof(MongoConfiguration).GetMethod(nameof(MongoConfiguration.EnsureIndexesAndCap), BindingFlags.NonPublic | BindingFlags.Static);
					var genericEnsureIndexesMethod = ensureIndexesMethod.MakeGenericMethod(entityType);

					var task = (Task)genericEnsureIndexesMethod.Invoke(null, new object[] { collection, true });
					task.GetAwaiter().GetResult();
				}

				// Create the trash collection

				if (!db.ListCollectionNames(new ListCollectionNamesOptions { Filter = new BsonDocument("name", "DeletedObjects") }).Any())
				{
					db.GetCollection<dynamic>("DeletedObjects").Indexes.CreateOne(new CreateIndexModel<dynamic>(Builders<dynamic>.IndexKeys.Ascending("_t")));
				}
			}

			InitializedTenants.TryAdd(tenantKey ?? "", true);
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
		public BsonDocument Weights { get; set; }
		public string Language { get; set; }
	}

	public static class MongoClientExtensions
	{
		public static IRepository<T> GetRepository<T>(this IMongoClient client) =>
			MongoConfiguration.GetRepository<T>(client);

		public static IRepository<T> GetRepository<T>(this IMongoClient client, string tenantKey) =>
			MongoConfiguration.GetRepository<T>(client, tenantKey);
	}
}