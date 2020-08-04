using System;
using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace JohnKnoop.MongoRepository.UnitTests
{
	class Home
	{
		public Kitchen Kitchen { get; private set; }
	}

	class Kitchen
	{
		public IList<Knife> Knives { get; private set; }
		public int SomeNumber { get; private set; }
	}

	class Knife
	{
		
	}

	public class PropertyNameExtractorTests
	{
		[Fact]
		public void ShouldCreateIndexWithFullPropertyName()
		{
			PropertyNameExtractor.GetPropertyName<Home, object>(x => x.Kitchen.Knives).Should()
				.Be("Kitchen.Knives");

			PropertyNameExtractor.GetPropertyName<Home, object>(x => x.Kitchen.SomeNumber).Should()
				.Be("Kitchen.SomeNumber");

			PropertyNameExtractor.GetPropertyName<Home, object>(x => x.Kitchen).Should()
				.Be("Kitchen");
		}
	}
}