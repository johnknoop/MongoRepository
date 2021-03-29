using FluentAssertions;
using FluentAssertions.Collections;
using FluentAssertions.Execution;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace JohnKnoop.MongoRepository.IntegrationTests
{
	public static class FluentAssertionsExtensions
	{
		public static AndConstraint<TTestable> ShouldHaveProperty<TTestable, TProperty>(this TTestable testable, Expression<Func<TTestable, TProperty>> propertySelector, TProperty expectedValue)
		{
			var memberName = PropertyNameExtractor.GetPropertyName(propertySelector);
			var property = testable.GetType().GetProperty(memberName);
			var actualValue = property.GetValue(testable);

			Execute.Assertion
				.ForCondition(actualValue.Equals(expectedValue))
				.FailWith($"Expected the property {memberName} to have value {expectedValue} but found {actualValue}");

			return new AndConstraint<TTestable>(testable);
		}

		public static AndConstraint<TTestable> ShouldSatisfy<TTestable>(this TTestable testable, Func<TTestable, bool> predicate)
		{
			predicate(testable).Should().BeTrue();
			return new AndConstraint<TTestable>(testable);
		}

		public static AndConstraint<TTestable> ShouldSatisfy<TTestable>(this TTestable testable, Action<TTestable> checker)
		{
			checker(testable);
			return new AndConstraint<TTestable>(testable);
		}

		public static AndConstraint<GenericCollectionAssertions<TTestable>> It<TTestable>(this GenericCollectionAssertions<TTestable> assertions, Action<IEnumerable<TTestable>> checker)
		{
			checker(assertions.Subject);
			return new AndConstraint<GenericCollectionAssertions<TTestable>>(assertions);
		}
	}
}
