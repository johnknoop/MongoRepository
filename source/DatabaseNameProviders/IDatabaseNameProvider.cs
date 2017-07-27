using System;

namespace JohnKnoop.MongoRepository.DatabaseNameProviders
{
	public interface IDatabaseNameProvider
	{
		string GetDatabaseName(Type entityType, string tenantKey = null);
	}
}