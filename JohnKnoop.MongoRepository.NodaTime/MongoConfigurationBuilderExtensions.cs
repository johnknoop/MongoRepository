using JohnKnoop.MongoRepository;
using MongoDb.Bson.NodaTime;

namespace MongoRepository.NodaTime
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