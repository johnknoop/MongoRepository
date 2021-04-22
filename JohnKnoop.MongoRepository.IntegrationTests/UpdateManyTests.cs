using FluentAssertions;
using JohnKnoop.MongoRepository.IntegrationTests.TestEntities;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace JohnKnoop.MongoRepository.IntegrationTests
{
	[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
    public class UpdateManyTests : IClassFixture<LaunchSettingsFixture>
    {
		private const string DbName = "TestDb";
		private const string CollectionName = "MyEntities";
		private readonly MongoClient _mongoClient;
		private readonly IRepository<Person> _repository;

		public UpdateManyTests(LaunchSettingsFixture launchSettingsFixture)
		{
			_mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			MongoRepository.Configure()
				.Database(DbName, x => x
					.MapAlongWithSubclassesInSameAssebmly<Person>(CollectionName)
				)
				.AutoEnlistWithTransactionScopes()
				.Build();

			// Empty all collections in database
			foreach (var collectionName in _mongoClient.GetDatabase(DbName).ListCollectionNames().ToEnumerable())
			{
				_mongoClient.GetDatabase(DbName).GetCollection<BsonDocument>(collectionName).DeleteMany(x => true);
			}

			_repository = _mongoClient.GetRepository<Person>();
		}

		[Fact]
		public async Task CanUpdateManyDerivedEntities()
		{
			// Arrange

			var athleteJane = new Athlete("Jane", 40, "Soccer");
			var athleteBob = new Athlete("Bob", 33, "Snooker");

			await _repository.InsertManyAsync<Athlete>(new [] { athleteBob, athleteJane });

			// Act

			await _repository.UpdateManyAsync<Athlete>(
				filter: x => new [] { "Jane", "Bob" }.Contains(x.Name),
				update: x => x.Set(y => y.Sport, "Curling")
			);

			// Assert

			var allAthletesInDb = await _repository.GetAll().ToListAsync();

			allAthletesInDb.Should().HaveCount(2).And
				.AllBeOfType<Athlete>().And
				.OnlyContain(x => ((Athlete)x).Sport == "Curling");
		}
    }
}
