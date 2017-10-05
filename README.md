# JohnKnoop.MongoRepository

An easy-to-configure, powerful repository for MongoDB with support for multi-tenancy


- [Getting started](#getting-started)
- [Querying](#querying)
	- [Get by id](#get-by-id)
		- [Projection](#get-by-id)
	- [Find by expression](#find-by-expression)
		- [Count, Sort, Skip, Limit, Project, Cursor](#find-by-expression)
	- [LINQ](#linq)
- [Inserting, updating and deleting](#inserting-updating-and-deleting)
	- [InsertAsync, InsertManyAsync](#insertasync-insertmanyasync)
	- [ModifyOneAsync, ModifyManyAsync](#modifyoneasync-modifymanyasync)
	- [ModifyOneBulkAsync](#modifyonebulkasync)
	- [FindOneAndModifyAsync](#findoneandmodifyasync),
	- [Aggregation](#aggregation)
	- [Deleting](#deleting)
		- [Soft-deletes](#soft-deleting)
- [Advanced features](#advanced-features)
	- [Counters](#counters)
	- [Deleting properties](#deleting-properties)
- Configuration
	- [Multi-tenancy](#multi-tenancy)
	- [Polymorphism](#polymorphism)
	- [Indices](#indices)
	- [Capped collections](#capped-collections)
	- [Unconventional id properties](#unconventional-id-properties)
	- [DI frameworks](#di-frameworks)
		- [Ninject](#ninject)
		- [.Net Core](#net-core)

## Getting started

    PM> Install-Package JohnKnoop.MongoRepository

### Configure mappings, indices, multitenancy etc with a few lines of code...

```C#
MongoRepository.Configure()
	.Database("HeadOffice", db => db
		.Map<Employee>()
	)
	.DatabasePerTenant("Zoo", db => db
		.Map<AnimalKeeper>()
		.Map<Enclosure>("Enclosures")
		.Map<Animal>("Animals", x => x
			.WithIdProperty(animal => animal.NonConventionalId)
			.WithIndex(animal => animal.Name, unique: true)
		)
	)
	.Build();
```
[See more options](#configuration)
### ...then start hacking away

```C#
var employeeRepository = mongoClient.GetRepository<Employee>();
var animalRepository = mongoClient.GetRepository<Animal>(tenantKey);
```

## Querying

### Get by id
```C#
await repository.GetAsync("id");
await repository.GetAsync<SubType>("id");

// With projection
await repository.GetAsync("id", x => x.TheOnlyPropertyIWant);
await repository.GetAsync<SubType>("id", x => new
	{
		x.SomeProperty,
		x.SomeOtherProperty
	}
);
```

### Find by expression

```C#
await repository.Find(x => x.SomeProperty == someValue);
await repository.Find<SubType>(x => x.SomeProperty == someValue);
await repository.Find(x => x.SomeProperty, regexPattern);
```
Returns an [IFindFluent](http://api.mongodb.com/csharp/current/html/T_MongoDB_Driver_IFindFluent_2.htm) which offers methods like `ToListAsync`, `CountAsync`, `Project`, `Skip` and `Limit`

Examples:

```C#
var dottedAnimals = await repository
	.Find(x => x.Coat == "dotted")
	.Limit(10)
	.Project(x => x.Species)
	.ToListAsync()
```

### LINQ
```C#
repository.Query();
repository.Query<SubType>();
```
Returns an [IMongoQueryable](http://api.mongodb.com/csharp/current/html/T_MongoDB_Driver_Linq_IMongoQueryable.htm) which offers async versions of all the standard LINQ methods.

Examples:

```C#
var dottedAnimals = await repository.Query()
	.Where(x => x.Coat == "dotted")
	.Take(10)
	.Select(x => x.Species)
	.ToListAsync()
```

## Inserting, updating and deleting
### InsertAsync, InsertManyAsync

```C#
await repository.InsertAsync(someObject);
await repository.InsertManyAsync(someCollectionOfObjects);
```

### ModifyOneAsync, ModifyManyAsync
```C#
// Modify one document
await repository.ModifyOneAsync("id", x => x.Set(y => y.SomeProperty, someValue), upsert: true);
await repository.ModifyOneAsync(x => x.SomeProperty == someValue, x => x.Push(y => y.SomeCollection, someValue));
await repository.ModifyOneAsync<SubType>(x => x.SomeProperty == someValue, x => x.Push(y => y.SomeCollection, someValue));

// Modify all documents matched by filter
await repository.ModifyManyAsync(x => x.SomeProperty == someValue, x => x.Inc(y => y.SomeProperty, 5));
```
### ModifyOneBulkAsync
Perform multiple update operations with different filters in one db roundtrip.
```C#
await repository.ModifyOneBulkAsync(new List<ModifyOneCommand<MyEntity>> {
	new ModifyOneCommand<MyEntity> {
		Filter = x => x.SomeProperty = "foo",
		Update = x => x.Set(y => y.SomeOtherProperty, 10)
	},
	new ModifyOneCommand<MyEntity> {
		Filter = x => x.SomeProperty = "bar",
		Update = x => x.Set(y => y.SomeOtherProperty, 20)
	}
});
```
### FindOneAndModifyAsync
This is a really powerful feature of MongoDB, in that it lets you update and retrieve a document atomically.
```C#
var entityAfterUpdate = await repository.FindOneAndModifyAsync(
	filter: x => x.SomeProperty.StartsWith("Hello"),
	update: x => x.AddToSet(y => y.SomeCollection, someItem)
);

var entityAfterUpdate = await repository.FindOneAndModifyAsync(
	filter: x => x.SomeProperty.StartsWith("Hello"),
	update: x => x.PullFilter(y => y.SomeCollection, y => y.SomeOtherProperty == 5),
	returnProjection: x => new {
		x.SomeCollection
	},
	returnedDocumentState: ReturnedDocumentState.AfterUpdate,
	upsert: true
);
```
### Aggregation
```C#
repository.Aggregate();
repository.Aggregate(options);
```
Returns an [IAggregateFluent](http://api.mongodb.com/csharp/current/html/T_MongoDB_Driver_IAggregateFluent_1.htm) which offers methods like `AppendStage`, `Group`, `Match`, `Unwind`, `Out`, `Lookup` etc.

### Deleting
```C#
await repository.DeleteByIdAsync("id");
await repository.DeleteManyAsync(x => x.SomeProperty === someValue);
```
#### Soft-deleting
Soft-deleting an entity will move it to a different collection, preserving type-information.
```C#
await repository.DeleteByIdAsync("id", softDelete: true);
```
Listing soft-deleted entities:
```C#
await repository.ListTrash();
```
## Advanced features

### Counters

Auto-incrementing fields is a feature of most relational databases that unfortunately isn't supported by MongoDB. To get around this, _counters_ are a way to solve the problem of incrementing a number with full concurrency support.
```C#
var value = await repository.GetCounterValueAsync();
var value = await repository.GetCounterValueAsync("MyNamedCounter");
```
Atomically increment and read the value of a counter:
```C#
var value = await repository.IncrementCounterAsync(); // Increment by 1
var value = await repository.IncrementCounterAsync(name: "MyNamedCounter", incrementBy: 5);
```
Reset a counter:
```C#
await repository.ResetCounterAsync(); // Reset to 1
await repository.ResetCounterAsync(name: "MyNamedCounter", newValue: 5);
```

### Deleting properties

Delete a property from a document:

```C#
await repository.DeletePropertyAsync(x => x.SomeProperty == someValue, x => x.PropertyToRemove);
```

## Configuration

Configuration is done once, when the application is started. Use `MongoRepository.Configure()` as shown below.

### Multi-tenancy

Database-per-tenant style multi-tenancy is supported. When defining a database, just use the `DatabasePerTenant` method:

```C#
MongoRepository.Configure()
	// Every tenant should have their own Sales database
	.DatabasePerTenant("Sales", db => db
		.Map<Order>()
		.Map<Customer>("Customers")
	)
	.Build();
```
The name of the database will be "{tenant key}_{database name}".

### Polymorphism

Mapping a type hierarchy to the same collection is easy. Just map the base type using `MapAlongWithSubclassesInSameAssembly<MyBaseType>()`. It takes all the same arguments as `Map`.

### Indices

Indices are defined when mapping a type:
```C#
MongoRepository.Configure()
	// Every tenant should have their own Sales database
	.Database("Zoo", db => db
		.Map<Animal>("Animals", x => x
			.WithIndex(a => a.Species)
			.WithIndex(a => a.EnclosureNumber, unique: true)
			.WithIndex(a => a.LastVaccinationDate, sparse: true)
		)
		.Map<FeedingRoutine>("FeedingRoutines", x => x
			// Composite index
			.WithIndex(new { Composite index })
		)
	)
	.Build();
```

### Capped collections
### Unconventional id properties

### DI frameworks
#### .NET Core

When configuring the application services, iterate through the mapped types and bind each generic repository

```C#
MongoRepository.GetMappedTypes().ForEach(entityType =>
	{
		services.AddTransient(typeof(IRepository<>).MakeGenericType(entityType), provider =>
		{
			var mongoClient = provider.GetService<IMongoClient>();
			var httpContextAccessor = provider.GetService<IHttpContextAccessor>();
			
			// In this example, we get the tenant key from the HttpContext, but you might get it from anywhere
			var tenantKey = httpContextAccessor.HttpContext?.Items["TenantKey"];

			var getRepositoryMethod = typeof(MongoConfiguration).GetMethod(nameof(MongoConfiguration.GetRepository));
			var getRepositoryMethodGeneric = getRepositoryMethod.MakeGenericMethod(entityType);
			return getRepositoryMethodGeneric.Invoke(this, new object[] { mongoClient, tenantKey });
		});
	});
```

#### Ninject

```C#
this.Bind(typeof(IRepository<>)).ToMethod(context =>
{
	Type entityType = context.GenericArguments[0];
	var mongoClient = context.Kernel.Get<IMongoClient>();
	var tenantKey = /* Pull out your tenent key from auth ticket or Owin context or what suits you best */;

	var getRepositoryMethod = typeof(MongoConfiguration).GetMethod(nameof(MongoConfiguration.GetRepository));
	var getRepositoryMethodGeneric = getRepositoryMethod.MakeGenericMethod(entityType);
	return getRepositoryMethodGeneric.Invoke(this, new object[] { mongoClient, tenantKey });
});
```