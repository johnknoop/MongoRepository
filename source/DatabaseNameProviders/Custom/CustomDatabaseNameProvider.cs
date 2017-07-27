using System;
using System.Collections.Generic;
using System.Linq;

namespace JohnKnoop.MongoRepository.DatabaseNameProviders.Custom
{
	public class DatabaseNameScopeInitializer
	{
		private readonly string _databaseName;
		private readonly Func<string, DatabaseNameScopeInitializer> _blankScopeFactory;
		private readonly Action<Type, string> _associateTypeWithDatabaseName;
		private Func<CustomDatabaseNameProvider> _buildDelegate;

		internal DatabaseNameScopeInitializer(string databaseName, Func<string, DatabaseNameScopeInitializer> blankScopeFactory, Action<Type, string> associateTypeWithDatabaseName, Func<CustomDatabaseNameProvider> buildDelegate)
		{
			_databaseName = databaseName;
			_blankScopeFactory = blankScopeFactory;
			_associateTypeWithDatabaseName = associateTypeWithDatabaseName;
			_buildDelegate = buildDelegate;
		}

		public DatabaseNameScope For<T>()
		{
			_associateTypeWithDatabaseName(typeof(T), _databaseName);
			return new DatabaseNameScope(_databaseName, _blankScopeFactory, _associateTypeWithDatabaseName, _buildDelegate);
		}
	}

	public class DatabaseNameScope
	{
		private readonly string _databaseName;
		private readonly Func<string, DatabaseNameScopeInitializer> _blankScopeFactory;
		private readonly Action<Type, string> _associateTypeWithDatabaseName;
		private Func<CustomDatabaseNameProvider> _buildDelegate;

		internal DatabaseNameScope(string databaseName, Func<string, DatabaseNameScopeInitializer> blankScopeFactory, Action<Type, string> associateTypeWithDatabaseName, Func<CustomDatabaseNameProvider> buildDelegate)
		{
			_databaseName = databaseName;
			_blankScopeFactory = blankScopeFactory;
			_associateTypeWithDatabaseName = associateTypeWithDatabaseName;
			_buildDelegate = buildDelegate;
		}

		public DatabaseNameScope For<T>()
		{
			_associateTypeWithDatabaseName(typeof(T), _databaseName);
			return this;
		}

		public DatabaseNameScope And<T>()
		{
			_associateTypeWithDatabaseName(typeof(T), _databaseName);
			return this;
		}

		public CustomDatabaseNameProvider Build()
		{
			return _buildDelegate();
		}

		public DatabaseNameScopeInitializer UseDatabaseName(string name)
		{
			return _blankScopeFactory(name);
		}
	}

	public class CustomDatabaseNameProviderBuilder
	{ 
		private readonly Dictionary<string, IList<Type>> _mappings = new Dictionary<string, IList<Type>>();

		public DatabaseNameScopeInitializer UseDatabaseName(string name)
		{
			if (name == null) throw new ArgumentNullException(nameof(name));

			return new DatabaseNameScopeInitializer(name, UseDatabaseName, AssociateTypeWithDatabaseName, Build);
		}

		internal void AssociateTypeWithDatabaseName(Type type, string databaseName)
		{
			if (!_mappings.ContainsKey(databaseName))
			{
				_mappings[databaseName] = new List<Type>();
			}

			_mappings[databaseName].Add(type);
		}

		public CustomDatabaseNameProvider Build()
		{
			var reverseLookup = new Dictionary<Type, string>();

			foreach (var mapping in _mappings)
			{
				foreach (var type in mapping.Value)
				{
					reverseLookup[type] = mapping.Key;
				}
			}

			return new CustomDatabaseNameProvider(reverseLookup);
		}
	}

	public class CustomDatabaseNameProvider : IDatabaseNameProvider
	{
		private readonly Dictionary<Type, string> _databaseNameLookup;

		public CustomDatabaseNameProvider(Dictionary<Type, string> databaseNameLookup)
		{
			_databaseNameLookup = databaseNameLookup;
		}

		private static string WashDatabaseName(string name)
		{
			return new string(name.Where(letter => letter >= 97 && letter <= 122 || letter >= 65 && letter <= 90).ToArray());
		}

		public string GetDatabaseName(Type entityType, string tenantKey = null)
		{
			if (entityType == null) throw new ArgumentNullException(nameof(entityType));

			if (!_databaseNameLookup.TryGetValue(entityType, out var databaseName))
			{
				if (entityType == typeof(DeletedObject))
				{
					return "Trash";
				}

				throw new InvalidOperationException($"The type {entityType.Name} has not been mapped to a database name");
			}

			var washedAssemblyName = WashDatabaseName(databaseName);

			return tenantKey == null
				? washedAssemblyName
				: $"{tenantKey}_{washedAssemblyName}";
		}
	}
}