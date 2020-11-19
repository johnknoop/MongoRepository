using FluentAssertions;
using JohnKnoop.MongoRepository.IntegrationTests.TestEntities;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace JohnKnoop.MongoRepository.IntegrationTests
{
	[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
    public class FindOneAndUpdateTests : IClassFixture<LaunchSettingsFixture>
    {
		private const string DbName = "TestDb";
		private const string CollectionName = "MyEntities";
		private readonly MongoClient _mongoClient;
		private readonly IRepository<Person> _repository;

		public FindOneAndUpdateTests(LaunchSettingsFixture launchSettingsFixture)
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
		public async Task BaseEntity()
		{
			var maryBefore = new Person("Mary", 45);

			await _repository.InsertAsync(maryBefore);

			var resultMary = await _repository.FindOneAndUpdateAsync(
				filter: x => x.Name == "Mary",
				update: x => x
					.Set(y => y.Age, 46)
				,
				returnProjection: x => new
				{
					Age = x.Age,
					Id = x.Id
				}
			);

			var resultBob = await _repository.FindOneAndUpdateAsync(
				filter: x => x.Name == "Bob",
				update: x => x
					.Set(y => y.Age, 50)
				,
				returnProjection: x => new
				{
					Age = x.Age,
					Id = x.Id
				},
				upsert: true
			);

			resultMary.Age.Should().Be(46);
			resultMary.Id.Should().Be(maryBefore.Id);
			resultBob.Age.Should().Be(50);
		}

		[Fact]
		public async Task DerivedEntity()
		{
			var maryBefore = new Athlete("Mary", 45, "Soccer");

			await _repository.InsertAsync(maryBefore);

			var resultMary = await _repository.FindOneAndUpdateAsync<Athlete, string>(
				filter: x => x.Name == "Mary",
				update: x => x
					.Set(y => y.Sport, "Skating")
				,
				returnProjection: x => x.Sport
			);

			var resultBob = await _repository.FindOneAndUpdateAsync<Athlete, string>(
				filter: x => x.Name == "Bob",
				update: x => x
					.Set(y => y.Age, 50)
					.Set(y => y.Sport, "Hockey")
				,
				returnProjection: x => x.Sport,
				upsert: true
			);

			resultMary.Should().Be("Skating");
			resultBob.Should().Be("Hockey");
		}
    }
}
