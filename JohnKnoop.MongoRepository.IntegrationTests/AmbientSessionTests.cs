using FluentAssertions;
using JohnKnoop.MongoRepository.IntegrationTests.TestEntities;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Xunit;

namespace JohnKnoop.MongoRepository.IntegrationTests
{
	class DummyEntity
	{
		public string MyProperty { get; private set; }
		public string Id { get; private set; }

		public DummyEntity(string myProperty)
		{
			MyProperty = myProperty;
			Id = ObjectId.GenerateNewId().ToString();
		}
	}

	class DerivedEntity : DummyEntity
	{
		public DerivedEntity(string myProperty, bool myBool) : base(myProperty)
		{
			MyBool = myBool;
		}

		public bool MyBool { get; private set; }
	}

	static class MyStaticInserter
	{
		public static async Task InsertDocument<T>(T entity, IRepository<T> repo)
		{
			await repo.InsertAsync(entity);
		}
	}

	[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
	public class AmbientSessionTests : IClassFixture<LaunchSettingsFixture>
	{
		private readonly MongoClient _mongoClient;

		public AmbientSessionTests(LaunchSettingsFixture launchSettingsFixture)
		{
			MongoRepository.Configure()
				.DatabasePerTenant("TestDb", x => x
					.MapAlongWithSubclassesInSameAssebmly<DummyEntity>("DummyEntities")
					.Map<ArrayContainer>("ArrayContainers")
				)
				.AutoEnlistWithTransactionScopes()
				.Build();

			this._mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			_mongoClient.GetRepository<DummyEntity>("tenant_a").DeleteManyAsync(x => true).Wait();
			_mongoClient.GetRepository<DummyEntity>("tenant_b").DeleteManyAsync(x => true).Wait();
			_mongoClient.GetRepository<ArrayContainer>("tenant_c").DeleteManyAsync(x => true).Wait();
			_mongoClient.GetRepository<DummyEntity>().DeleteManyAsync(x => true).Wait();

			_mongoClient.GetRepository<DummyEntity>("tenant_a").PermamentlyDeleteSoftDeletedAsync(x => true).Wait();
			_mongoClient.GetRepository<DummyEntity>("tenant_b").PermamentlyDeleteSoftDeletedAsync(x => true).Wait();
			_mongoClient.GetRepository<ArrayContainer>("tenant_c").PermamentlyDeleteSoftDeletedAsync(x => true).Wait();
			_mongoClient.GetRepository<DummyEntity>().PermamentlyDeleteSoftDeletedAsync(x => true).Wait();
		}

		// DeleteManyAsync
		// DeleteByIdAsync

		[Fact]
		public async Task OperationsOutsideTransactionsAreUnaffected()
		{
			var mary1 = new DummyEntity("Mary1");
			var mary2 = new DummyEntity("Mary2");
			var mary3 = new DummyEntity("Mary3");
			var mary4 = new DummyEntity("Mary4");
			var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");

			using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
			{
				//repo.EnlistWithCurrentTransactionScope();
				await repo.InsertAsync(mary1);
			}

			await repo.InsertAsync(mary2);

			using (var transaction = repo.StartTransaction())
			{
				await repo.InsertAsync(mary3);
			}

			await repo.InsertAsync(mary4);

			var allSaved = await repo.GetAll().ToListAsync();

			allSaved.Should().HaveCount(2).And.OnlyContain(x => x.MyProperty == "Mary2" || x.MyProperty == "Mary4");
		}

		[Fact]
		public async Task CanRestoreManyDerivedSoftDeletedAtOnce()
		{
			var mary = new DummyEntity("Mary");
			var jane = new DerivedEntity("Jane", true);

			await _mongoClient.GetRepository<DummyEntity>("tenant_a").InsertAsync(mary);
			await _mongoClient.GetRepository<DummyEntity>("tenant_a").InsertAsync(jane);

			using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
			{
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");
				//repo.EnlistWithCurrentTransactionScope();
				await repo.DeleteByIdAsync(mary.Id, softDelete: true);
				await repo.DeleteByIdAsync(jane.Id, softDelete: true);
				transaction.Complete();
			}
			
			using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
			{
				await Task.Delay(1);
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");

				//repo.EnlistWithCurrentTransactionScope();

				await _mongoClient.GetRepository<DummyEntity>("tenant_a").RestoreSoftDeletedAsync<DerivedEntity>(x => x.Entity.MyBool == true);

				transaction.Complete();
			}

			var allDocsInDb = await _mongoClient.GetRepository<DummyEntity>("tenant_a").GetAll().ToListAsync();
			allDocsInDb.Should().HaveCount(1).And.OnlyHaveUniqueItems();

			(await _mongoClient.GetRepository<DummyEntity>("tenant_a").ListTrashAsync()).Should().HaveCount(1);
		}

		[Fact]
		public async Task CanRestoreManySoftDeletedAtOnce()
		{
			var mary = new DummyEntity("Mary");
			var jane = new DummyEntity("Jane");

			await _mongoClient.GetRepository<DummyEntity>("tenant_a").InsertAsync(mary);
			await _mongoClient.GetRepository<DummyEntity>("tenant_a").InsertAsync(jane);

			using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
			{
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");
				//repo.EnlistWithCurrentTransactionScope();
				await repo.DeleteByIdAsync(mary.Id, softDelete: true);
				await repo.DeleteByIdAsync(jane.Id, softDelete: true);
				transaction.Complete();
			}
			
			using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
			{
				await Task.Delay(1);
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");

				//repo.EnlistWithCurrentTransactionScope();

				await _mongoClient.GetRepository<DummyEntity>("tenant_a").RestoreSoftDeletedAsync(x => true);

				transaction.Complete();
			}

			var allDocsInDb = await _mongoClient.GetRepository<DummyEntity>("tenant_a").GetAll().ToListAsync();
			allDocsInDb.Should().HaveCount(2).And.OnlyHaveUniqueItems();

			(await _mongoClient.GetRepository<DummyEntity>("tenant_a").ListTrashAsync()).Should().BeEmpty();
		}

		[Fact]
		public async Task CanRestoreSoftDeletedEntities()
		{
			var mary = new DummyEntity("Mary");
			var jane = new DummyEntity("Jane");

			await _mongoClient.GetRepository<DummyEntity>("tenant_a").InsertAsync(mary);

			using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
			{
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");
				//repo.EnlistWithCurrentTransactionScope();
				await repo.DeleteByIdAsync(mary.Id, softDelete: true);
				transaction.Complete();
			}
			
			await Task.WhenAll(
				Task.Run(async () => {
					using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
					{
						var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");
						await Task.Delay(2);
						//repo.EnlistWithCurrentTransactionScope();

						
						await repo.RestoreSoftDeletedAsync(mary.Id);
						

						transaction.Complete();
					}
					
				}),
				Task.Run(async () => {
					using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
					{
						await Task.Delay(1);
						var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");

						//repo.EnlistWithCurrentTransactionScope();

						await _mongoClient.GetRepository<DummyEntity>("tenant_a").InsertAsync(jane);

						transaction.Complete();
					}
					
				})
			);

			var allDocsInDb = await _mongoClient.GetRepository<DummyEntity>("tenant_a").GetAll().ToListAsync();
			allDocsInDb.Should().HaveCount(2).And.OnlyHaveUniqueItems();

			(await _mongoClient.GetRepository<DummyEntity>("tenant_a").ListTrashAsync()).Should().BeEmpty();
		}

		[Fact]
		public async Task ShouldUseTheRightSessionInAMultiThreadedScenarioWithTransactionScope()
		{
			var request1 = Task.Run(async () =>
			{
				using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
				{
					var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");
					await Task.Delay(1);

					await repo.InsertAsync(new DummyEntity("Hello!"));

					transaction.Complete();
				}
			});

			var request2 = Task.Run(async () =>
			{
				using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
				{
					var repo = _mongoClient.GetRepository<DummyEntity>("tenant_b");

					await Task.Delay(1);

					await MyStaticInserter.InsertDocument(new DummyEntity("Hola senor"), repo);
					MyStaticInserter.InsertDocument(new DummyEntity("Hola senor"), repo).Wait();

					// No commit
				}
			});

			var request3 = Task.Run(async () =>
			{
				using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
				{
					var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");
					await Task.Delay(1);

					await Task.Run(async () => await repo.InsertAsync(new DummyEntity("Hello again!")));

					// No commit
				}
			});

			var request4 = Task.Run(async () =>
			{
				using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
				{
					var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");

					await Task.Delay(50);
					await repo.InsertAsync(new DummyEntity("Hello good sir or madam!"));

					transaction.Complete();
				}
			});

			var request5 = Task.Run(async () =>
			{
				using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
				{
					var repo = _mongoClient.GetRepository<DummyEntity>("tenant_b");
					await Task.Delay(1);

					await MyStaticInserter.InsertDocument(new DummyEntity("Hola senor"), repo);
					MyStaticInserter.InsertDocument(new DummyEntity("Hola senor"), repo).Wait();

					transaction.Complete();
				}
			});

			await Task.WhenAll(request1, request2, request3, request4, request5);

			var allDocumentsForTenantA = await _mongoClient.GetRepository<DummyEntity>("tenant_a").GetAll().ToListAsync();
			var allDocumentsForTenantB = await _mongoClient.GetRepository<DummyEntity>("tenant_b").GetAll().ToListAsync();

			allDocumentsForTenantA.Should().HaveCount(2);
			allDocumentsForTenantB.Should().HaveCount(2);

			allDocumentsForTenantA.Should().ContainSingle(x => x.MyProperty == "Hello!");
			allDocumentsForTenantA.Should().ContainSingle(x => x.MyProperty == "Hello good sir or madam!");
			allDocumentsForTenantB.Count(x => x.MyProperty == "Hola senor").Should().Be(2);
		}

		[Fact]
		public async Task ShouldUseTheRightSessionInAMultiThreadedScenarioWithExplicitTransaction()
		{
			var request1 = Task.Run(async () =>
			{
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");

				using (var transaction = repo.StartTransaction())
				{
					await repo.InsertAsync(new DummyEntity("Hello!"));

					await transaction.CommitAsync();
				}
			});

			var request2 = Task.Run(async () =>
			{
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_b");

				using (var transaction = repo.StartTransaction())
				{

					await Task.Run(async () => await repo.InsertAsync(new DummyEntity("Hola!")));

					// No commit 
				}
				
			});

			var request3 = Task.Run(async () =>
			{
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");

				using (var transaction = repo.StartTransaction())
				{
					await Task.Run(async () => await repo.InsertAsync(new DummyEntity("Hello again!")));

					// No commit 
				}
				
			});

			var request4 = Task.Run(async () =>
			{
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_a");
				using (var transaction = repo.StartTransaction())
				{
					await Task.Delay(50);
					await repo.InsertAsync(new DummyEntity("Hello good sir or madam!"));

					await transaction.CommitAsync(); 
				}
				
			});

			var request5 = Task.Run(async () =>
			{
				var repo = _mongoClient.GetRepository<DummyEntity>("tenant_b");
				
				using (var transaction = repo.StartTransaction())
				{
					await MyStaticInserter.InsertDocument(new DummyEntity("Hola senor"), repo);
					MyStaticInserter.InsertDocument(new DummyEntity("Hola senor"), repo).Wait();

					await transaction.CommitAsync(); 
				}
				
			});

			await Task.WhenAll(request1, request2, request3, request4, request5);

			var allDocumentsForTenantA = await _mongoClient.GetRepository<DummyEntity>("tenant_a").GetAll().ToListAsync();
			var allDocumentsForTenantB = await _mongoClient.GetRepository<DummyEntity>("tenant_b").GetAll().ToListAsync();

			allDocumentsForTenantA.Should().HaveCount(2);
			allDocumentsForTenantB.Should().HaveCount(2);

			allDocumentsForTenantA.Should().ContainSingle(x => x.MyProperty == "Hello!");
			allDocumentsForTenantA.Should().ContainSingle(x => x.MyProperty == "Hello good sir or madam!");
			allDocumentsForTenantB.Count(x => x.MyProperty == "Hola senor").Should().Be(2);
		}

		[Fact]
		public async Task AutomaticallyEnlistsWithAmbientTransaction()
		{
			var throwingDummy = new ThrowingDummy(4);
			var repo = _mongoClient.GetRepository<ArrayContainer>("tenant_c");

			var doc = new ArrayContainer(new List<Dummy> { new Dummy("olle"), new Dummy("bengt") }, "roy");
			await repo.InsertAsync(doc);

			using (var trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
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

				trans.Complete();
			}

			var allSaved = await repo.GetAll().ToListAsync();

			allSaved.Should().ContainSingle()
				.Which.Dummies.Should().HaveCount(4);
		}
	}
}
