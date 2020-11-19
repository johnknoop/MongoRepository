using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JohnKnoop.MongoRepository.IntegrationTests.TestEntities
{
	public class Person
	{
		public Person(string name, int age)
		{
			Id = ObjectId.GenerateNewId().ToString();
			Name = name;
			Age = age;
		}

		public string Id { get; private set; }
		public int Age { get; private set; }
		public string Name { get; private set; }
	}
}
