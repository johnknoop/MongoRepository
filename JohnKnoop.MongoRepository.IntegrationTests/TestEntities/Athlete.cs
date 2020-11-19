using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JohnKnoop.MongoRepository.IntegrationTests.TestEntities
{

	public class Athlete : Person
	{
		public Athlete(string name, int age, string sport) : base(name, age)
		{
			Sport = sport;
		}

		public string Sport { get; private set; }
	}
}
