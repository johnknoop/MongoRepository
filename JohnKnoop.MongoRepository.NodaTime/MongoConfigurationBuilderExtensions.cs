using MongoDb.Bson.NodaTime;

namespace JohnKnoop.MongoRepository.NodaTime
{
    public static class MongoConfigurationBuilderExtensions
    {
        public static MongoConfigurationBuilder MapNodaTimeDates(this MongoConfigurationBuilder builder)
        {
            builder.AddPlugin(_ => { NodaTimeSerializers.Register(); });
            return builder;
        }
    }
}