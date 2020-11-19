using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JohnKnoop.MongoRepository.IntegrationTests.TestEntities
{
	public class MySimpleEntity
	{
		public MySimpleEntity(string name)
		{
			Id = ObjectId.GenerateNewId().ToString();
			Name = name;
		}

		public string Id { get; private set; }
		public string Name { get; private set; }
	}
}
