using FluentAssertions;
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
	public class MyBaseEntity
	{
		public MyBaseEntity(string name)
		{
			Id = ObjectId.GenerateNewId().ToString();
			Name = name;
		}

		public string Id { get; private set; }
		public string Name { get; private set; }
	}

	public class MyDerivedEntity : MyBaseEntity {
		public MyDerivedEntity(string name, int age) : base(name)
		{
			Age = age;
		}

		public int Age { get; private set; }
	}

    [CollectionDefinition("IntegrationTests", DisableParallelization = true)]
    public class FindOneAndDeleteTests : IClassFixture<LaunchSettingsFixture>
    {
		private const string DbName = "TestDb";
		private const string CollectionName = "MyEntities";
		private readonly MongoClient _mongoClient;
		private readonly IRepository<MyBaseEntity> _repository;
		private readonly string _baseEntityId;
		private readonly string _derivedEntityId;

		public FindOneAndDeleteTests(LaunchSettingsFixture launchSettingsFixture)
		{
            _mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			MongoRepository.Configure()
				.Database(DbName, x => x
					.MapAlongWithSubclassesInSameAssebmly<MyBaseEntity>(CollectionName)
				)
				.AutoEnlistWithTransactionScopes()
				.Build();

			// Empty all collections in database
			foreach (var collectionName in _mongoClient.GetDatabase(DbName).ListCollectionNames().ToEnumerable())
			{
				_mongoClient.GetDatabase(DbName).GetCollection<BsonDocument>(collectionName).DeleteMany(x => true);
			}

			// Add test documents to db
			var baseEntityDocument = new MyBaseEntity("Mary");
			var derivedEntityDocument = new MyDerivedEntity("Joe", 45);

			_baseEntityId = baseEntityDocument.Id;
			_derivedEntityId = derivedEntityDocument.Id;

			_repository = _mongoClient.GetRepository<MyBaseEntity>();
			_repository.InsertAsync(baseEntityDocument).Wait();
			_repository.InsertAsync(derivedEntityDocument).Wait();

			AssertNumberOfDocumentsInCollection(2).Wait();
			AssertNumberOfDocumentsInTrash(0).Wait();
		}

		private async Task AssertNumberOfDocumentsInCollection(int expected)
		{
			var documentsInCollection = await _mongoClient.GetDatabase(DbName).GetCollection<MyBaseEntity>(CollectionName).CountDocumentsAsync(x => true);
			documentsInCollection.Should().Be(expected);
		}

		private async Task AssertNumberOfDocumentsInTrash(int expected)
		{
			var docsInTrash = await _mongoClient.GetDatabase(DbName).GetCollection<BsonDocument>("DeletedObjects").CountDocumentsAsync(x => true);
			docsInTrash.Should().Be(expected);
		}

		[Fact]
		public async Task BaseEntity_HardDeleted_ShouldBeRemovedFromCollection()
		{
			// By id
			var doc = await _repository.FindOneAndDeleteAsync(_baseEntityId);
			doc.Name.Should().Be("Mary");
			await AssertNumberOfDocumentsInCollection(1);
		}

		[Fact]
		public async Task DerivedEntity_HardDeleted_ShouldBeRemovedFromCollection()
		{
			// By expression
			var doc = await _repository.FindOneAndDeleteAsync<MyDerivedEntity>(x => x.Age == 45);
			doc.Name.Should().Be("Joe");
			await AssertNumberOfDocumentsInCollection(1);
		}

		[Fact]
		public async Task BaseEntity_SoftDeleted_ShouldBeRemovedFromCollectionAndAddedToTrash()
		{
			// By expression
			var doc = await _repository.FindOneAndDeleteAsync(x => x.Name == "Mary", softDelete: true);
			doc.Name.Should().Be("Mary");
			await AssertNumberOfDocumentsInCollection(1);
			await AssertNumberOfDocumentsInTrash(1);
		}

		[Fact]
		public async Task DerivedEntity_SoftDeleted_ShouldBeRemovedFromCollectionAndAddedToTrash()
		{
			// By id
			var doc = await _repository.FindOneAndDeleteAsync(_derivedEntityId, softDelete: true);
			doc.Name.Should().Be("Joe");
			await AssertNumberOfDocumentsInCollection(1);
			await AssertNumberOfDocumentsInTrash(1);
		}

		[Fact]
		public async Task SoftDelete_WithAbortedTransactions_ShouldNotAddAnythingToTrash()
		{
			// By id
			using (var transaction = _repository.StartTransaction())
			{
				var doc = await _repository.FindOneAndDeleteAsync(_baseEntityId, softDelete: true);
				doc.Name.Should().Be("Mary");
				await transaction.AbortAsync();
			}

			await AssertNumberOfDocumentsInCollection(2);
			await AssertNumberOfDocumentsInTrash(0);
		}

		[Fact]
		public async Task HardDeleted_WithAbortedTransactions_ShouldNotRemoveFromCollection()
		{
			// By expression
			using (var transaction = _repository.StartTransaction())
			{
				var doc = await _repository.FindOneAndDeleteAsync(x => x.Id == _derivedEntityId);
				doc.Name.Should().Be("Joe");
			}

			await AssertNumberOfDocumentsInCollection(2);
			await AssertNumberOfDocumentsInTrash(0);
		}

		[Fact]
		public async Task CanRestoreSoftdeletedBaseEntity()
		{
			await _repository.FindOneAndDeleteAsync(_baseEntityId, softDelete: true);

			await AssertNumberOfDocumentsInCollection(1);
			await AssertNumberOfDocumentsInTrash(1);

			await _repository.RestoreSoftDeletedAsync(_baseEntityId);

			await AssertNumberOfDocumentsInCollection(2);
			await AssertNumberOfDocumentsInTrash(0);
		}

		[Fact]
		public async Task CanRestoreSoftdeletedDerivedEntity()
		{
			await _repository.FindOneAndDeleteAsync<MyDerivedEntity>(x => x.Age == 45, softDelete: true);

			await AssertNumberOfDocumentsInCollection(1);
			await AssertNumberOfDocumentsInTrash(1);

			await _repository.RestoreSoftDeletedAsync<MyDerivedEntity>(x => x.Entity.Age == 45);

			await AssertNumberOfDocumentsInCollection(2);
			await AssertNumberOfDocumentsInTrash(0);
		}
    }
}
