using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace JohnKnoop.MongoRepository.Extensions
{
	public static class AggregateFluentExtensions
	{
		public static IAggregateFluent<TAgg> UnionWith<TAgg, TOther>(this IAggregateFluent<TAgg> aggregation, IRepository<TOther> other, Expression<Func<TOther, TAgg>> projection)
		{
			var otherImpl = other as MongoRepository<TOther>;
			var pipelineDefinition = PipelineDefinitionBuilder.For<TOther>().Project(projection);

			return aggregation.UnionWith(
				otherImpl.Collection,
				pipelineDefinition
			);
		}
	}
}
