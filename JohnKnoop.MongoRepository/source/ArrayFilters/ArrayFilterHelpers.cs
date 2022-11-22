using MongoDB.Bson.Serialization;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Newtonsoft.Json.Linq;

namespace JohnKnoop.MongoRepository
{
	public class ArrayFiltersReferenceBuilder<TEntity>
	{
		public InitializedArrayFiltersReferenceBuilder<TArrayElement> SelectEnumerable<TArrayElement>(Expression<Func<TEntity, IEnumerable<TArrayElement>>> array, string filterName)
		{
			return new InitializedArrayFiltersReferenceBuilder<TArrayElement>(
				path: new StringBuilder()
					.Append(string.Join(".", PropertyNameExtractor.GetPropertyName(array), $"$[{filterName}]"))
					.ToString()
			);
		}
	}

	public class InitializedArrayFiltersReferenceBuilder<TArrayElement>
	{
		private readonly string _path;

		public InitializedArrayFiltersReferenceBuilder(string path)
		{
			_path = path;
		}

		public InitializedArrayFiltersReferenceBuilder<TSubArrayElement> SelectEnumerable<TSubArrayElement>(Expression<Func<TArrayElement, IEnumerable<TSubArrayElement>>> array, string filterName)
		{
			return new InitializedArrayFiltersReferenceBuilder<TSubArrayElement>(
				path: new StringBuilder(_path + ".")
					.Append(string.Join(".", PropertyNameExtractor.GetPropertyName(array), $"$[{filterName}]"))
					.ToString()
			);
		}

		public InitializedArrayFiltersReferenceBuilder<TSubProperty> SelectProperty<TSubProperty>(Expression<Func<TArrayElement, TSubProperty>> prop)
		{
			return new InitializedArrayFiltersReferenceBuilder<TSubProperty>(
				path: new StringBuilder(_path + ".")
					.Append(string.Join(".", PropertyNameExtractor.GetPropertyName(prop)))
					.ToString()
			);
		}

		public string Build() => _path;
	}

	public static class ArrayFilters
	{
		public static ArrayFiltersBuilder<TEntity> DefineFilters<TEntity>() => new ArrayFiltersBuilder<TEntity>();
		public static ArrayFiltersReferenceBuilder<TEntity> CreateArrayFilterPath<TEntity>() => new ArrayFiltersReferenceBuilder<TEntity>();
	}

	public class ArrayFiltersBuilder<TEntity>
	{
		public ThenableArrayFiltersBuilder<TEntity, TChildArrayElement> AddFilter<TChildArrayElement>(
			string filterName,
			Expression<Func<TEntity, IEnumerable<TChildArrayElement>>> arraySelector,
			Func<FilterDefinitionBuilder<TChildArrayElement>, FilterDefinition<TChildArrayElement>> filter
		)
		{
			if (typeof(IEnumerable).IsAssignableFrom(arraySelector.ReturnType) == false)
			{
				throw new ArgumentException($"Selected property must be, but {PropertyNameExtractor.GetPropertyName(arraySelector)} is of type {arraySelector.ReturnType}");
			}

			var bsonDoc = filter(Builders<TChildArrayElement>.Filter).Render(
				BsonSerializer.SerializerRegistry.GetSerializer<TChildArrayElement>(),
				BsonSerializer.SerializerRegistry
			);

			return new ThenableArrayFiltersBuilder<TEntity, TChildArrayElement>(new List<ArrayFilterDefinition<BsonValue>>
		{
			new BsonDocument(
				$"{filterName}.{bsonDoc.Elements.Single().Name}",
				bsonDoc.Elements.Single().Value
			)
		});
		}
	}

	public class ThenableArrayFiltersBuilder<TEntity, TArrayElement> : IEnumerable<ArrayFilterDefinition<BsonValue>>
	{
		private readonly IList<ArrayFilterDefinition<BsonValue>> _filters;

		public ThenableArrayFiltersBuilder(IList<ArrayFilterDefinition<BsonValue>> filters)
		{
			_filters = filters;
		}

		public IEnumerator<ArrayFilterDefinition<BsonValue>> GetEnumerator() => _filters
			.Cast<ArrayFilterDefinition<BsonValue>>()
			.GetEnumerator();

		public ThenableArrayFiltersBuilder<TEntity, TChildArrayElement> ThenAddFilter<TChildArrayElement>(
			string filterName,
			Expression<Func<TArrayElement, IEnumerable<TChildArrayElement>>> arraySelector,
			Func<FilterDefinitionBuilder<TChildArrayElement>, FilterDefinition<TChildArrayElement>> filter
			)
		{
			if (typeof(IEnumerable).IsAssignableFrom(arraySelector.ReturnType) == false)
			{
				throw new ArgumentException($"Selected property must be, but {PropertyNameExtractor.GetPropertyName(arraySelector)} is of type {arraySelector.ReturnType}");
			}

			var bsonDoc = filter(Builders<TChildArrayElement>.Filter).Render(
				BsonSerializer.SerializerRegistry.GetSerializer<TChildArrayElement>(),
				BsonSerializer.SerializerRegistry
			);

			return new ThenableArrayFiltersBuilder<TEntity, TChildArrayElement>(_filters.Append(
				new BsonDocument(
					$"{filterName}.{bsonDoc.Elements.Single().Name}",
					bsonDoc.Elements.Single().Value
				)
			).ToList());
		}

		public ThenableArrayFiltersBuilder<TEntity, TChildArrayElement> ThenAddFilter<TChildArrayElement>(
			string filterName,
			Expression<Func<TArrayElement, IEnumerable<TChildArrayElement>>> arraySelector,
			BsonDocument filterDoc
			)
		{
			if (typeof(IEnumerable).IsAssignableFrom(arraySelector.ReturnType) == false)
			{
				throw new ArgumentException($"Selected property must be, but {PropertyNameExtractor.GetPropertyName(arraySelector)} is of type {arraySelector.ReturnType}");
			}

			return new ThenableArrayFiltersBuilder<TEntity, TChildArrayElement>(_filters.Append(
				new BsonDocument(
					$"{filterName}",
					filterDoc
				)
			).ToList());
		}

		public ThenableArrayFiltersBuilder<TEntity, TChildArrayElement> AddFilter<TChildArrayElement>(
			string filterName,
			Expression<Func<TEntity, IEnumerable<TChildArrayElement>>> arraySelector,
			Func<FilterDefinitionBuilder<TChildArrayElement>, FilterDefinition<TChildArrayElement>> filter
			)
		{
			if (typeof(IEnumerable).IsAssignableFrom(arraySelector.ReturnType) == false)
			{
				throw new ArgumentException($"Selected property must be, but {PropertyNameExtractor.GetPropertyName(arraySelector)} is of type {arraySelector.ReturnType}");
			}

			var bsonDoc = filter(Builders<TChildArrayElement>.Filter).Render(
				BsonSerializer.SerializerRegistry.GetSerializer<TChildArrayElement>(),
				BsonSerializer.SerializerRegistry
			);

			return new ThenableArrayFiltersBuilder<TEntity, TChildArrayElement>(_filters.Append(
				new BsonDocument(
					$"{filterName}.{bsonDoc.Elements.Single().Name}",
					bsonDoc.Elements.Single().Value
				)
			).ToList());
		}

		public ThenableArrayFiltersBuilder<TEntity, TChildArrayElement> AddFilter<TChildArrayElement>(
			string filterName,
			Expression<Func<TEntity, IEnumerable<TChildArrayElement>>> arraySelector,
			BsonDocument filterDoc
			)
		{
			if (typeof(IEnumerable).IsAssignableFrom(arraySelector.ReturnType) == false)
			{
				throw new ArgumentException($"Selected property must be, but {PropertyNameExtractor.GetPropertyName(arraySelector)} is of type {arraySelector.ReturnType}");
			}

			return new ThenableArrayFiltersBuilder<TEntity, TChildArrayElement>(_filters.Append(
				new BsonDocument(
					$"{filterName}",
					filterDoc
				)
			).ToList());
		}

		IEnumerator IEnumerable.GetEnumerator() => _filters.GetEnumerator();
	}

}
