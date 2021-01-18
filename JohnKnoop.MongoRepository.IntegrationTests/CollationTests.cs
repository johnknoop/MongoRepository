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
    public class CollationTests : IClassFixture<LaunchSettingsFixture>
    {
		private const string DbName = "TestDb";
		private const string CollectionName = "MyEntities";
		private readonly MongoClient _mongoClient;
		private readonly IRepository<MySimpleEntity> _repository;

		public CollationTests(LaunchSettingsFixture launchSettingsFixture)
		{
			_mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			MongoRepository.Configure()
				.Database(DbName, x => x
					.MapAlongWithSubclassesInSameAssebmly<MySimpleEntity>(CollectionName)
				)
				.AutoEnlistWithTransactionScopes()
				.Build();

			// Empty all collections in database
			foreach (var collectionName in _mongoClient.GetDatabase(DbName).ListCollectionNames().ToEnumerable())
			{
				_mongoClient.GetDatabase(DbName).GetCollection<BsonDocument>(collectionName).DeleteMany(x => true);
			}

			_repository = _mongoClient.GetRepository<MySimpleEntity>();
		}



		[Fact]
		public async Task CanSortNaturally()
		{
			// Arrange

			await _repository.InsertManyAsync(new []
			{
				new MySimpleEntity("A10-20"),
				new MySimpleEntity("A9-1"),
				new MySimpleEntity("A9-2"),
				new MySimpleEntity("A9-11"),
				new MySimpleEntity("A8-1"),
				new MySimpleEntity("A10-3"),
			});

			// Act

			var result = await _repository.GetAll(new FindOptions
			{
				Collation = new Collation("sv", numericOrdering: true)
			}).SortBy(x => x.Name).ToListAsync();

			// Assert

			result.Select(x => x.Name).Should().Equal(new [] {
				"A8-1",
				"A9-1",
				"A9-2",
				"A9-11",
				"A10-3",
				"A10-20",
			});
		}
	}
}
