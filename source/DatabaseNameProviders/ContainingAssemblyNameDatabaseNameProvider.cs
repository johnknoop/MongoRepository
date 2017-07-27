using System;
using System.Linq;
using System.Reflection;

namespace JohnKnoop.MongoRepository.DatabaseNameProviders
{
	public class ContainingAssemblyNameDatabaseNameProvider : IDatabaseNameProvider
	{
		private static string WashDatabaseName(string name)
		{
			return new string(name.Where(letter => letter >= 97 && letter <= 122 || letter >= 65 && letter <= 90).ToArray());
		}

		public string GetDatabaseName(Type entityType, string tenantKey = null)
		{
			if (entityType == null) throw new ArgumentNullException(nameof(entityType));

			var assemblyName = entityType.GetTypeInfo().Assembly.GetName().Name;
			var washedAssemblyName = WashDatabaseName(assemblyName);

			return tenantKey == null
				? washedAssemblyName
				: $"{tenantKey}_{washedAssemblyName}";
		}
	}
}