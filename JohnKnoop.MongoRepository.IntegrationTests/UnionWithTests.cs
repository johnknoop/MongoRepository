using FluentAssertions;
using JohnKnoop.MongoRepository.IntegrationTests.TestEntities;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using JohnKnoop.MongoRepository.Extensions;

namespace JohnKnoop.MongoRepository.IntegrationTests
{
	[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
	public class UnionWithTests : IClassFixture<LaunchSettingsFixture>
	{
		private const string DbName = "TestDb";
		private readonly MongoClient _mongoClient;
		private readonly IRepository<MySimpleEntity> _simpEntityRepo;
		private readonly IRepository<Person> _personRepo;

		public UnionWithTests(LaunchSettingsFixture fixture)
		{
			_mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			MongoRepository.Configure()
				.Database(DbName, x => x
					.MapAlongWithSubclassesInSameAssebmly<MySimpleEntity>()
					.MapAlongWithSubclassesInSameAssebmly<Person>()
				)
				.AutoEnlistWithTransactionScopes()
				.Build();

			// Empty all collections in database
			foreach (var collectionName in _mongoClient.GetDatabase(DbName).ListCollectionNames().ToEnumerable())
			{
				_mongoClient.GetDatabase(DbName).GetCollection<BsonDocument>(collectionName).DeleteMany(x => true);
			}

			_simpEntityRepo = _mongoClient.GetRepository<MySimpleEntity>();
			_personRepo = _mongoClient.GetRepository<Person>();
		}


		[Fact]
		public async Task ShouldUnionTwoCollections()
		{
			await _personRepo.InsertManyAsync(new[]
			{
				new Person("Alice", 25),
				new Person("Carra", 35),
			});

			await _simpEntityRepo.InsertManyAsync(new[]
			{
				new MySimpleEntity("Bob"),
				new MySimpleEntity("Carra"),
			});

			var result = await _personRepo.Aggregate()
				.Project(x => new
				{
					FullName = x.Name
				})
				.UnionWith(
					_simpEntityRepo,
					x => new
					{
						FullName = x.Name,
					}
				)
				.SortBy(x => x.FullName)
				.Limit(3)
				.ToListAsync();

			result.Should().SatisfyRespectively(
				x => x.FullName.Should().Be("Alice"),
				x => x.FullName.Should().Be("Bob"),
				x => x.FullName.Should().Be("Carra")
			);
		}
	}


}
