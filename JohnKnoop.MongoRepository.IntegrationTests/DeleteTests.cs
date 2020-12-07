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
	public class DeleteTests : IClassFixture<LaunchSettingsFixture>
	{
		private const string DbName = "TestDb";
		private const string CollectionName = "MyEntities";
		private readonly MongoClient _mongoClient;
		private readonly IRepository<MySimpleEntity> _repository;

		public DeleteTests(LaunchSettingsFixture launchSettingsFixture)
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

		private async Task AssertNumberOfDocumentsInCollection(int expected)
		{
			var documentsInCollection = await _mongoClient.GetDatabase(DbName).GetCollection<MySimpleEntity>(CollectionName).CountDocumentsAsync(x => true);
			documentsInCollection.Should().Be(expected);
		}

		[Fact]
		public async Task DeleteMany_WithoutFilter_ShouldDeleteAllDocumentsInCollection()
		{
			await _repository.InsertManyAsync(new []
			{
				new MySimpleEntity("Bob"),
				new MySimpleEntity("Mary"),
			});

			await AssertNumberOfDocumentsInCollection(2);

			await _repository.DeleteManyAsync(x => true);

			await AssertNumberOfDocumentsInCollection(0);
		}
	}
}
