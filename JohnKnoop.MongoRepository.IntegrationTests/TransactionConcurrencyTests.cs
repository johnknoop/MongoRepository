using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Xunit;

namespace JohnKnoop.MongoRepository.IntegrationTests
{
	[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
	public class TransactionConcurrencyTests : IClassFixture<LaunchSettingsFixture>
	{
		private readonly MongoClient _mongoClient;

		public TransactionConcurrencyTests(LaunchSettingsFixture launchSettingsFixture)
		{
			MongoRepository.Configure()
				.DatabasePerTenant("TestDb", x => x
					.Map<MyStandaloneEntity>("MyStandaloneEntities")
				)
				.AutoEnlistWithTransactionScopes()
				.Build();

			this._mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			_mongoClient.GetDatabase("a_TestDb").GetCollection<BsonDocument>("MyStandaloneEntities").DeleteMany(x => true);
			_mongoClient.GetDatabase("b_TestDb").GetCollection<BsonDocument>("MyStandaloneEntities").DeleteMany(x => true);
			_mongoClient.GetDatabase("c_TestDb").GetCollection<BsonDocument>("MyStandaloneEntities").DeleteMany(x => true);
			_mongoClient.GetDatabase("d_TestDb").GetCollection<BsonDocument>("MyStandaloneEntities").DeleteMany(x => true);
		}

		[Fact]
		public async Task RepositoriesCreatedWithinAsyncContext_TransactionsAreTolerantToConcurrency()
		{
			var tasks = new Func<Task>[]
			{
				async () =>
				{
					var repoA = _mongoClient.GetRepository<MyStandaloneEntity>("a");

					using(var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
					{
						await repoA.InsertAsync(new MyStandaloneEntity("Jane", new SharedClass("Doe")));

						transaction.Complete();
					}
				},
				async () =>
				{
					var repoB = _mongoClient.GetRepository<MyStandaloneEntity>("b");

					using(var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
					{
						await repoB.InsertAsync(new MyStandaloneEntity("John", new SharedClass("Doe")));
					}
				},
				async () =>
				{
					var repoC = _mongoClient.GetRepository<MyStandaloneEntity>("c");

					await repoC.WithTransactionAsync(async () =>
					{
						await repoC.InsertAsync(new MyStandaloneEntity("Janet", new SharedClass("Doe")));
					}, TransactionType.TransactionScope);
				},
				async () =>
				{
					var repoD = _mongoClient.GetRepository<MyStandaloneEntity>("d");

					await repoD.WithTransactionAsync(async () =>
					{
						await repoD.InsertAsync(new MyStandaloneEntity("Jeff", new SharedClass("Doe")));
					}, TransactionType.TransactionScope);
				},
			}
				.AsParallel()
				.WithDegreeOfParallelism(4)
				.WithExecutionMode(ParallelExecutionMode.ForceParallelism)
				.Select(async x => await x());

			await Task.WhenAll(tasks);

			var repoA = _mongoClient.GetRepository<MyStandaloneEntity>("a");
			var repoB = _mongoClient.GetRepository<MyStandaloneEntity>("b");
			var repoC = _mongoClient.GetRepository<MyStandaloneEntity>("c");
			var repoD = _mongoClient.GetRepository<MyStandaloneEntity>("d");

			var jane = await repoA.GetAll().SingleAsync();
			var none = await repoB.GetAll().SingleOrDefaultAsync();
			var janet = await repoC.GetAll().SingleAsync();
			var jeff = await repoD.GetAll().SingleAsync();

			jane.Name.Should().Be("Jane");
			none.Should().BeNull();
			janet.Name.Should().Be("Janet");
			jeff.Name.Should().Be("Jeff");
		}

		[Fact]
		public async Task RepositoriesCreatedOutsideAsyncContext_TransactionsAreTolerantToConcurrency()
		{
			var repoA = _mongoClient.GetRepository<MyStandaloneEntity>("a");
			var repoB = _mongoClient.GetRepository<MyStandaloneEntity>("b");
			var repoC = _mongoClient.GetRepository<MyStandaloneEntity>("c");
			
			await repoA.InsertAsync(new MyStandaloneEntity("Jane", new SharedClass("Doe")));
			await repoB.InsertAsync(new MyStandaloneEntity("John", new SharedClass("Doe")));
			await repoC.InsertAsync(new MyStandaloneEntity("Janet", new SharedClass("Doe")));

			var tasks = new Func<Task>[]
			{
				async () =>
				{
					using(var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
					{
						await repoA.DeleteManyAsync(x => true);

						transaction.Complete();
					}
				},
				async () =>
				{
					using(var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
					{
						await repoB.DeleteManyAsync(x => true);
					}
				},
				async () =>
				{
					await repoC.WithTransactionAsync(async () =>
					{
						await repoC.DeleteManyAsync(x => true);
					}, TransactionType.TransactionScope);
				},
				async () =>
				{
					using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
					{
						var repoD = _mongoClient.GetRepository<MyStandaloneEntity>("d");

						await Task.Delay(1);

						await MyStaticInserter.InsertDocument(new MyStandaloneEntity("Jeff", new SharedClass("Doe")), repoD);
						MyStaticInserter.InsertDocument(new MyStandaloneEntity("Jeff", new SharedClass("Doe")), repoD).Wait();

						// No commit
					}
				},
			}
				.AsParallel()
				.WithDegreeOfParallelism(4)
				.WithExecutionMode(ParallelExecutionMode.ForceParallelism)
				.Select(async x => await x());

			await Task.WhenAll(tasks);

			var jane = await repoA.GetAll().SingleOrDefaultAsync();
			var john = await repoB.GetAll().SingleOrDefaultAsync();
			var janet = await repoC.GetAll().SingleOrDefaultAsync();
			var jeff = await _mongoClient.GetRepository<MyStandaloneEntity>("d").GetAll().SingleOrDefaultAsync();

			jane.Should().BeNull();
			john.Name.Should().Be("John");
			janet.Should().BeNull();
			jeff.Should().BeNull();
		}
	}
}
