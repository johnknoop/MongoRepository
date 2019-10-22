using MongoDB.Driver;

namespace JohnKnoop.MongoRepository.Extensions
{
	public static class UpdateResultExtensions
	{
		public static bool AnyDocumentsMatched(this UpdateResult result) =>
			result.MatchedCount > 0;

		public static bool AnyDocumentsModified(this UpdateResult result) =>
			result.IsModifiedCountAvailable && result.ModifiedCount > 0;
	}
}