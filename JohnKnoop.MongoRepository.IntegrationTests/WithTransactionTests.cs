using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Xunit;
using JohnKnoop.MongoRepository.Extensions;
using FluentAssertions;
using MongoDB.Bson;
using JohnKnoop.MongoRepository.IntegrationTests.TestEntities;

namespace JohnKnoop.MongoRepository.IntegrationTests
{
	public class ThrowingDummy
	{
		private int _callCount;
		private readonly int _maxThrows;

		public ThrowingDummy(int maxThrows)
		{
			_maxThrows = maxThrows;
		}

		public void TryMe()
		{
			if (++_callCount < _maxThrows)
			{
				var ex = new MongoException("Error");

				ex.AddErrorLabel("TransientTransactionError");

				throw ex;
			}
		}
	}

	public class SharedClass
	{
		public string Name { get; private set; }

		public SharedClass(string name)
		{
			Name = name;
		}
	}

	public class MyStandaloneEntity
	{
		public MyStandaloneEntity(string name, SharedClass myProperty)
		{
			Id = ObjectId.GenerateNewId().ToString();
			Name = name;
			MyProperty = myProperty;
		}

		public string Id { get; private set; }
		public string Name { get; private set; }
		public SharedClass MyProperty { get; private set; }
	}

	[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
	public class WithTransactionTests : IClassFixture<LaunchSettingsFixture>
	{
		private readonly MongoClient _mongoClient;

		public WithTransactionTests(LaunchSettingsFixture launchSettingsFixture)
		{
			MongoRepository.Configure()
				.Database("TestDb", x => x
					.Map<MyStandaloneEntity>("MyStandaloneEntities")
					.Map<ArrayContainer>("ArrayContainers")
				)
				.AutoEnlistWithTransactionScopes()
				.Build();

			this._mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			_mongoClient.GetDatabase("TestDb").GetCollection<BsonDocument>("MyStandaloneEntities").DeleteMany(x => true);
			_mongoClient.GetDatabase("TestDb").GetCollection<BsonDocument>("ArrayContainers").DeleteMany(x => true);
		}

		[Fact]
		public async Task TransactionScope_WithoutMaxRetries_ShouldRetryUntilSuccessful()
		{
			var throwingDummy = new ThrowingDummy(4);
			var repo = _mongoClient.GetRepository<ArrayContainer>();

			var doc = new ArrayContainer(new List<Dummy> { new Dummy("olle"), new Dummy("bengt") }, "roy");
			await repo.InsertAsync(doc);

			await repo.WithTransactionAsync(async () =>
			{
				doc.Dummies.Add(new Dummy("rolle"));

				await repo.ReplaceOneAsync(
					x => x.Id == doc.Id,
					doc
				);

				await repo.UpdateOneAsync(
					x => x.Id == doc.Id,
					x => x.Push(x => x.Dummies, new Dummy("fia"))
				);
			}, TransactionType.TransactionScope, 3);

			var allSaved = await _mongoClient.GetRepository<ArrayContainer>().GetAll().ToListAsync();

			allSaved.Should().ContainSingle()
				.Which.Dummies.Should().HaveCount(4);
		}

		[Fact]
		public async Task AutomaticallyEnlistsToAmbientTransaction()
		{
			var throwingDummy = new ThrowingDummy(4);
			var repo = _mongoClient.GetRepository<ArrayContainer>();

			var doc = new ArrayContainer(new List<Dummy> { new Dummy("olle"), new Dummy("bengt") }, "roy");
			await repo.InsertAsync(doc);

			await repo.WithTransactionAsync(async () =>
			{
				doc.Dummies.Add(new Dummy("rolle"));

				await repo.ReplaceOneAsync(
					x => x.Id == doc.Id,
					doc
				);

				await repo.UpdateOneAsync(
					x => x.Id == doc.Id,
					x => x.Push(x => x.Dummies, new Dummy("fia"))
				);
			}, TransactionType.TransactionScope);

			var allSaved = await _mongoClient.GetRepository<ArrayContainer>().GetAll().ToListAsync();

			allSaved.Should().ContainSingle()
				.Which.Dummies.Should().HaveCount(4);
		}

		[Fact]
		public async Task TransactionScope_WithMaxRetries_ShouldRetryUntilMaxRetriesReached()
		{
			var throwingDummy = new ThrowingDummy(5);
			var repo = _mongoClient.GetRepository<MyStandaloneEntity>();

			await Assert.ThrowsAsync<MongoException>(async () =>
			{
				await repo.WithTransactionAsync(async () =>
				{
					throwingDummy.TryMe();
					await repo.InsertAsync(new MyStandaloneEntity("test", new SharedClass("test")));
				}, TransactionType.TransactionScope, maxRetries: 3);
			});

			var allSaved = await _mongoClient.GetRepository<MyStandaloneEntity>().GetAll().ToListAsync();

			allSaved.Should().BeEmpty();
		}

		[Fact]
		public async Task NativeTransaction_WithoutMaxRetries_ShouldRetryUntilSuccessful()
		{
			var throwingDummy = new ThrowingDummy(4);
			var repo = _mongoClient.GetRepository<MyStandaloneEntity>();

			await repo.WithTransactionAsync(async () =>
			{
				throwingDummy.TryMe();
				await repo.InsertAsync(new MyStandaloneEntity("test", new SharedClass("test")));
			}, maxRetries: 0);

			var allSaved = await _mongoClient.GetRepository<MyStandaloneEntity>().GetAll().ToListAsync();

			allSaved.Should().HaveCount(1);
		}

		[Fact]
		public async Task NativeTransaction_ReturnsValue()
		{
			var repo = _mongoClient.GetRepository<ArrayContainer>();
			var doc = new ArrayContainer(new List<Dummy> { new Dummy("olle"), new Dummy("bengt") }, "roy");
			await repo.InsertAsync(doc);

			var result = await repo.WithTransactionAsync(async () =>
			{
				return await repo.FindOneAndUpdateAsync(
					filter: x => x.Id == doc.Id,
					update: x => x.AddToSet(y => y.Dummies, new Dummy("steve")),
					returnProjection: x => x.Dummies
				);
			}, TransactionType.MongoDB);

			result.Should().HaveCount(3);
		}

		[Fact]
		public async Task MongoDbTransaction_ReturnsValue()
		{
			var repo = _mongoClient.GetRepository<ArrayContainer>();
			var doc = new ArrayContainer(new List<Dummy> { new Dummy("olle"), new Dummy("bengt") }, "roy");
			await repo.InsertAsync(doc);

			var result = await repo.WithTransactionAsync(async () =>
			{
				return await repo.FindOneAndUpdateAsync(
					filter: x => x.Id == doc.Id,
					update: x => x.AddToSet(y => y.Dummies, new Dummy("steve")),
					returnProjection: x => x.Dummies
				);
			}, TransactionType.TransactionScope);

			result.Should().HaveCount(3);
		}
	}
}
