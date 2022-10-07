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
	[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
	public class UpdateOrInsertOneTests : IClassFixture<LaunchSettingsFixture>
	{
		private const string DbName = "TestDb";
		private const string CollectionName = "MyEntities";
		private readonly MongoClient _mongoClient;
		private readonly IRepository<MyBaseEntity> _repository;

		public UpdateOrInsertOneTests(LaunchSettingsFixture launchSettingsFixture)
		{
			_mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			MongoRepository.Configure()
				.Database(DbName, x => x
					.MapAlongWithSubclassesInSameAssebmly<MyBaseEntity>(CollectionName)
				)
				.Build();

			// Empty all collections in database
			foreach (var collectionName in _mongoClient.GetDatabase(DbName).ListCollectionNames().ToEnumerable())
			{
				_mongoClient.GetDatabase(DbName).GetCollection<BsonDocument>(collectionName).DeleteMany(x => true);
			}

			_repository = _mongoClient.GetRepository<MyBaseEntity>();
		}

		[Fact]
		public async Task WhenNoMatch_ShouldInsertDocumentWithUpdates()
		{
			var fallback = new MyBaseEntity("Rosie");

			await _repository.UpdateOrInsertOneAsync(
				x => x.Name == "Alice",
				x => x.Set(y => y.Name, "Mary"),
				fallback);

			// Assert

			var allDocsInDb = await _repository.GetAll().ToListAsync();

			allDocsInDb.Should().ContainSingle().Which
				.ShouldHaveProperty(x => x.Id, fallback.Id).And
				.Name.Should().Be("Mary");
		}

		[Fact]
		public async Task ShouldReturnUpsertedDocument()
		{
			var preExisting = new MyBaseEntity("Jane");
			var fallback = new MyBaseEntity("Jenny");

			// Arrange

			await _repository.InsertAsync(preExisting);

			// Act

			var result = await _repository.UpdateOrInsertOneAsync(x => x.Name == "Jane", x => x.Set(y => y.Name, fallback.Name), fallback);

			// Assert

			result.Name.Should().Be(fallback.Name);
		}

		[Fact]
		public async Task ShouldReturnProjection()
		{
			var preExisting = new MyBaseEntity("Jane");
			var fallback = new MyBaseEntity("Jenny");

			// Arrange

			await _repository.InsertAsync(preExisting);

			// Act

			var result = await _repository.UpdateOrInsertOneAsync<string>(x => x.Name == "Jane", x => x.Set(y => y.Name, fallback.Name), fallback, x => x.Name);

			// Assert

			result.Should().Be(fallback.Name);
		}

		[Fact]
		public async Task ShouldNotOverwriteId()
		{
			var preExisting = new MyBaseEntity("Jane");
			var fallback = new MyBaseEntity("Jenny");

			// Arrange

			await _repository.InsertAsync(preExisting);

			// Act

			await _repository.UpdateOrInsertOneAsync(x => x.Name == "Jane", x => x.Set(y => y.Name, fallback.Name), fallback);

			// Assert

			var allDocsInDb = await _repository.GetAll().ToListAsync();

			allDocsInDb.Should().ContainSingle().Which
				.ShouldHaveProperty(x => x.Id, preExisting.Id).And
				.ShouldHaveProperty(x => x.Name, fallback.Name);
		}

		[Fact]
		public async Task ShouldSupportMatchingOnId()
		{
			var preExisting = new MyBaseEntity("Jane");
			var updated = new MyBaseEntity(preExisting.Id, "Jenny");

			// Arrange

			await _repository.InsertAsync(preExisting);

			// Act

			await _repository.UpdateOrInsertOneAsync(x => x.Id == updated.Id, x => x.Set(y => y.Name, updated.Name), updated);

			// Assert

			var allDocsInDb = await _repository.GetAll().ToListAsync();

			allDocsInDb.Should().ContainSingle().Which
				.ShouldHaveProperty(x => x.Id, updated.Id).And
				.ShouldHaveProperty(x => x.Name, updated.Name);
		}

		[Fact]
		public async Task SupportsSettingIdWhenMatchingOnOtherKey()
		{
			var updated = new MyBaseEntity("Jane");

			// Act

			await _repository.UpdateOrInsertOneAsync(x => x.Name == updated.Name, x => x.Combine(), updated);

			// Assert

			var allDocsInDb = await _repository.GetAll().ToListAsync();

			allDocsInDb.Should().ContainSingle().Which
				.ShouldHaveProperty(x => x.Id, updated.Id).And
				.ShouldHaveProperty(x => x.Name, updated.Name);
		}

		[Fact]
		public async Task ShouldSupportDerivedEntity_NoProjection()
		{
			var preExisting = new MyDerivedEntity("Jane", 15);
			var fallback = new MyDerivedEntity("Jenny", 15);

			// Arrange

			await _repository.InsertAsync(preExisting);

			// Act

			await _repository.UpdateOrInsertOneAsync(x => x.Name == "Jane", x => x.Set(y => y.Name, fallback.Name), fallback);

			// Assert

			var allDocsInDb = await _repository.GetAll().ToListAsync();

			allDocsInDb.Should().ContainSingle().Which
				.ShouldHaveProperty(x => x.Id, preExisting.Id).And
				.ShouldHaveProperty(x => x.Name, fallback.Name);
		}

		[Fact]
		public async Task ShouldSupportDerivedEntity_WithProjection()
		{
			var preExisting = new MyDerivedEntity("Jane", 15);
			var fallback = new MyDerivedEntity("Jenny", 15);

			// Arrange

			await _repository.InsertAsync(preExisting);

			// Act

			await _repository.UpdateOrInsertOneAsync(x => x.Name == "Jane", x => x.Set(y => y.Name, fallback.Name), fallback, x => x.Age);

			// Assert

			var allDocsInDb = await _repository.GetAll().ToListAsync();

			allDocsInDb.Should().ContainSingle().Which
				.ShouldHaveProperty(x => x.Id, preExisting.Id).And
				.ShouldHaveProperty(x => x.Name, fallback.Name);
		}
	}
}
