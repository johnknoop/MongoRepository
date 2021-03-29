using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using NodaTime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace JohnKnoop.MongoRepository.IntegrationTests
{
	[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
	public class UpdateOneBulkTests : IClassFixture<LaunchSettingsFixture>
	{
		private readonly MongoClient _mongoClient;

		public UpdateOneBulkTests(LaunchSettingsFixture fixture)
		{
			MongoRepository.Configure()
				.DatabasePerTenant("TestDb", x => x
					.Map<Item>("Items")
				)
				.AutoEnlistWithTransactionScopes()
				.Build();

			MongoDb.Bson.NodaTime.NodaTimeSerializers.Register();

			_mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

			_mongoClient.GetDatabase("_TestDb").GetCollection<BsonDocument>("Items").DeleteMany(x => true);
		}

		public record RequestVariant(string ItemId, string SuppliersItemNumber);
		public record RequestLine(string ItemId, decimal OrderedPrice, string SuppliersItemNumber, bool HasVariants, IList<RequestVariant> Variants);

		public record PurchasePrice(decimal OrderedPrice, string Currency);
		public record Supply(PurchasePrice? LastPurchasePrice, Instant? LastPurchaseDate, IList<string> Suppliers, Dictionary<string, string> SuppliersItemReferences);

		public class Item
		{
			public Item(Supply supply)
			{
				Id = ObjectId.GenerateNewId().ToString();
				Supply = supply;
			}

			public string Id { get; private set; }
			public Supply Supply { get; private set; }
		}

		[Fact]
		public async Task CanBulkUpdateDocuments()
		{
			var standaloneItem = new Item(new Supply(null, null, new List<string>(), new Dictionary<string, string>()));
			var variableItem = new Item(new Supply(null, null, new List<string>(), new Dictionary<string, string>()));
			var variant1 = new Item(new Supply(null, null, new List<string>(), new Dictionary<string, string>()));
			var variant2 = new Item(new Supply(null, null, new List<string>(), new Dictionary<string, string>()));

			var repository = _mongoClient.GetRepository<Item>();

			await repository.InsertManyAsync(new[] { standaloneItem, variableItem, variant1, variant2 });

			var supplierId = ObjectId.GenerateNewId().ToString();

			var lines = new RequestLine[]
			{
				new RequestLine(standaloneItem.Id, 10, "ABC", false, Array.Empty<RequestVariant>()),
				new RequestLine(variableItem.Id, 12, "DEF", true, new [] {
					new RequestVariant(variant1.Id, "DEF_1"),
					new RequestVariant(variant2.Id, "DEF_2"),
				}),
			};

			// Act

			var setSuppliersItemNumberCommands = lines
				.Where(x => !x.HasVariants)
				.Select(
					orderLine => new UpdateOneCommand<Item>
					{
						Filter = x => x.Eq(y => y.Id, orderLine.ItemId),
						Update = x => x.Set(y => y.Supply.LastPurchasePrice, new PurchasePrice(orderLine.OrderedPrice, "SEK"))
									   .Set(y => y.Supply.SuppliersItemReferences[supplierId], orderLine.SuppliersItemNumber)
									   .Set(y => y.Supply.LastPurchaseDate, SystemClock.Instance.GetCurrentInstant())
									   .AddToSet(y => y.Supply.Suppliers, supplierId)
					}
				)
				.Concat(
					lines
						.Where(x => x.HasVariants).SelectMany(x => x.Variants)
						.Select(orderLineVariant => new UpdateOneCommand<Item>
						{
							Filter = x => x.Eq(y => y.Id, orderLineVariant.ItemId),
							Update = x => x.Set(y => y.Supply.SuppliersItemReferences[supplierId], orderLineVariant.SuppliersItemNumber)
										   .Set(y => y.Supply.LastPurchaseDate, SystemClock.Instance.GetCurrentInstant())
						})
				).ToList();

			await repository.UpdateOneBulkAsync(setSuppliersItemNumberCommands);

			// Assert

			var persistedItems = await repository.GetAll().ToListAsync();

			persistedItems.Should().HaveCount(4).And.SatisfyRespectively(
				x => x.ShouldHaveProperty(y => y.Id, standaloneItem.Id).And
					.ShouldSatisfy(y => y.Supply.SuppliersItemReferences.Should().ContainKey(supplierId).WhichValue.Should().Be("ABC")).And
					.ShouldSatisfy(y => y.Supply.LastPurchasePrice!.OrderedPrice.Should().Be(10)).And
					.ShouldSatisfy(y => y.Supply.Suppliers.Should().ContainSingle(z => z == supplierId)),
				x => x.ShouldHaveProperty(y => y.Id, variableItem.Id),
				x => x.ShouldHaveProperty(y => y.Id, variant1.Id).And
					.ShouldSatisfy(y => y.Supply.SuppliersItemReferences.Should().ContainKey(supplierId).WhichValue.Should().Be("DEF_1")),
				x => x.ShouldHaveProperty(y => y.Id, variant2.Id).And
					.ShouldSatisfy(y => y.Supply.SuppliersItemReferences.Should().ContainKey(supplierId).WhichValue.Should().Be("DEF_2"))
			);
		}
	}
}
