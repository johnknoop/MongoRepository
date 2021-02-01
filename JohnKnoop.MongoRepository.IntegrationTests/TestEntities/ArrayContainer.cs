using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JohnKnoop.MongoRepository.IntegrationTests.TestEntities
{
	public class Dummy
	{
		public Dummy(string name)
		{
			this.Name = name;
			this.Created = DateTime.Now;
		}

		public string Name { get; private set; }
		public DateTime Created { get; private set; }
	}

	public class ArrayContainer
	{
		public ArrayContainer(IList<Dummy> dummies, string name)
		{
			this.Id = ObjectId.GenerateNewId().ToString();
			Dummies = dummies;
			Name = name;
		}

		public string Id { get; private set; }
		public string Name { get; private set; }
		public IList<Dummy> Dummies { get; private set; }
	}
}
