using FluentAssertions;
using FluentAssertions.Common;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection.Metadata.Ecma335;
using JohnKnoop.MongoRepository;
using System.Threading.Tasks;
using Xunit;

namespace JohnKnoop.MongoRepository.IntegrationTests.ArrayFiltersTests;

class Show
{
	public Show(string title, IList<Season> seasons)
	{
		Title = title;
		Seasons = seasons;
		RatedByUserIds = new List<string>();
	}

	public string Title { get; private set; }
	public IList<Season> Seasons { get; private set; }
	public IList<string> RatedByUserIds { get; private set; }
}
class Season
{
	public Season(string year, IList<Episode> episodes)
	{
		Year = year;
		Episodes = episodes;
	}

	public string Year { get; private set; }
	public IList<Episode> Episodes { get; private set; }
}
class Episode
{
	public Episode(int number, string title)
	{
		Number = number;
		Title = title;
	}

	public int Number { get; private set; }
	public string Title { get; private set; }
}

[CollectionDefinition("IntegrationTests", DisableParallelization = true)]
public class ArrayFiltersTests : IClassFixture<LaunchSettingsFixture>
{
	private const string DbName = "TestDb";
	private const string CollectionName = "MyEntities";
	private readonly MongoClient _mongoClient;
	private readonly IRepository<Show> _repository;

	public ArrayFiltersTests(LaunchSettingsFixture launchSettingsFixture)
	{
		_mongoClient = new MongoClient(Environment.GetEnvironmentVariable("MongoDbConnectionString"));

		MongoRepository.Configure()
			.Database(DbName, x => x
				.Map<Show>(CollectionName)
			)
			.Build();

		// Empty all collections in database
		foreach (var collectionName in _mongoClient.GetDatabase(DbName).ListCollectionNames().ToEnumerable())
		{
			_mongoClient.GetDatabase(DbName).GetCollection<BsonDocument>(collectionName).DeleteMany(x => true);
		}

		_repository = _mongoClient.GetRepository<Show>();

	}

	[Fact]
	public void ArrayFiltersPathBuilder_ShouldProduceCorrectPaths()
	{
		ArrayFilters.CreateArrayFilterPath<Show>()
			.SelectEnumerable(x => x.Seasons, "a")
			.SelectEnumerable(x => x.Episodes, "b")
			.SelectProperty(x => x.Title)
			.Build()
			.Should().Be("Seasons.$[a].Episodes.$[b].Title");
	}

	[Fact]
	public void ArrayFiltersBuilder_ShouldProduceCorrectFilters()
	{
		var actual = ArrayFilters.DefineFilters<Show>()
		  .AddFilter("a", show => show.Seasons, f => f.Ne(x => x.Year, "2013"))
		  .ThenAddFilter("b", season => season.Episodes, f => f.Eq(x => x.Number, 2))
		  .AddFilter("c", show => show.RatedByUserIds, new BsonDocument("$ne", "Lars"))
		  .Cast<MongoDB.Driver.BsonDocumentArrayFilterDefinition<MongoDB.Bson.BsonValue>>()
		  .Select(x => x.Document)
		  .ToList();

		var expected = new []
		{
			new BsonDocument("a.Year", new BsonDocument("$ne", "2013")),
			new BsonDocument("b.Number", 2),
			new BsonDocument("c", new BsonDocument("$ne", "Lars")),
		};

		actual.Should().BeEquivalentTo(expected);
	}

	[Fact]
	public async Task IntegrationTest()
	{
		await _repository.InsertAsync(new Show("Game of Thrones", new[]
		{
			new Season("2012", new []
			{
				new Episode(1, "The north remembers"),
				new Episode(2, "Winter is coming"),
			}),
			new Season("2013", new []
			{
				new Episode(1, "The red wedding"),
				new Episode(2, "The dragon queen"),
			})
		}));

		await _repository.UpdateOneAsync(
			filter: x => x.Title == "Game of Thrones",
			update: x => x.Set(
				ArrayFilters.CreateArrayFilterPath<Show>()
					.SelectEnumerable(x => x.Seasons, "a")
					.SelectEnumerable(x => x.Episodes, "b")
					.SelectProperty(x => x.Title)
					.Build(),
				"Qarth"),
			options: new UpdateOptions
			{
				ArrayFilters = ArrayFilters.DefineFilters<Show>()
				  .AddFilter("a", show => show.Seasons, f => f.Eq(x => x.Year, "2013"))
				  .ThenAddFilter("b", season => season.Episodes, f => f.Eq(x => x.Number, 2))
			}
		);

		// Assert

		var fromDb = await _repository.Find(x => true).SingleAsync();

		fromDb.Should().BeEquivalentTo(new Show("Game of Thrones", new[]
		{
			new Season("2012", new []
			{
				new Episode(1, "The north remembers"),
				new Episode(2, "Winter is coming"),
			}),
			new Season("2013", new []
			{
				new Episode(1, "The red wedding"),
				new Episode(2, "Qarth"),
			})
		}));
	}

	[Fact]
	public async Task ShouldHandleDeepArraysDeepInSubDocs()
	{
		// t ex "SubDoc.SubArray.$[a].SubDoc.Property"
		throw new NotImplementedException();
	}
}
