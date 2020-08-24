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

	[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
	public class RetriesTests : IClassFixture<LaunchSettingsFixture>
	{
		private readonly MongoClient _mongoClient;

		public RetriesTests(LaunchSettingsFixture launchSettingsFixture)
		{
			MongoRepository.Configure()
				.Database("TestDb", x => x
					.Map<MyStandaloneEntity>("MyStandaloneEntities")
				)
				.AutoEnlistWithTransactionScopes()
				.Build();

			this._mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			_mongoClient.GetDatabase("TestDb").GetCollection<MyStandaloneEntity>("MyStandaloneEntities").DeleteMany(x => true);
		}

		[Fact]
		public async Task TransactionScope_WithoutMaxRetries_ShouldRetryUntilSuccessful()
		{
			var throwingDummy = new ThrowingDummy(4);
			var repo = _mongoClient.GetRepository<MyStandaloneEntity>();

			await repo.WithTransactionAsync(async () =>
				{
					throwingDummy.TryMe();
					await repo.InsertAsync(new MyStandaloneEntity("test", new SharedClass("test")));
				}, TransactionType.TransactionScope, maxRetries: 0);

			var allSaved = await _mongoClient.GetRepository<MyStandaloneEntity>().GetAll().ToListAsync();

			allSaved.Should().HaveCount(1);
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
		public async Task NativeTransaction_WithMaxRetries_ShouldRetryUntilMaxRetriesReached()
		{
			var throwingDummy = new ThrowingDummy(5);
			var repo = _mongoClient.GetRepository<MyStandaloneEntity>();

			await Assert.ThrowsAsync<MongoException>(async () =>
			{
				await repo.WithTransactionAsync(async () =>
				{
					throwingDummy.TryMe();
					await repo.InsertAsync(new MyStandaloneEntity("test", new SharedClass("test")));
				}, maxRetries: 3);
			});

			var allSaved = await _mongoClient.GetRepository<MyStandaloneEntity>().GetAll().ToListAsync();

			allSaved.Should().BeEmpty();
		}
	}
}
