using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JohnKnoop.MongoRepository.Extensions
{
    public static class BsonDocumentExtensions
    {
        public static BsonDocument RemoveElement(this BsonDocument doc, string name)
		{
            doc.Remove(name);
            return doc;
		}
    }
}
