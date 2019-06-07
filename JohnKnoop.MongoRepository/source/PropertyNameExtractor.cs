using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace JohnKnoop.MongoRepository
{
	public class PropertyNameExtractor
	{
		public static string GetPropertyName<TSource, TProp>(Expression<Func<TSource, TProp>> memberExpression)
		{
			var outermostExpression = memberExpression.Body;
			var parts = new List<string>();

			if (outermostExpression is UnaryExpression u)
			{
				outermostExpression = u.Operand;
			}

			while (outermostExpression is MemberExpression p)
			{
				parts.Insert(0, p.Member.Name);

				outermostExpression = p.Expression;
			}

			return string.Join(".", parts);
		}
	}
}