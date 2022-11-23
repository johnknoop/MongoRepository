# JohnKnoop.MongoRepository

An easy-to-configure extension to the MongoDB driver, adding support for: \
\
✔️ Multi-tenancy \
✔️ Simplified transaction handling, including support for [TransactionScope](#transactions) \
✔️ [Soft-deletes](#soft-deleting)

## Install via NuGet

    Install-Package JohnKnoop.MongoRepository

### Configure mappings, indices, multitenancy etc with a few lines of code:

```csharp
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

```csharp
var employeeRepository = mongoClient.GetRepository<Employee>();
var animalRepository = mongoClient.GetRepository<Animal>(tenantKey);
```

In the real world you'd typically resolve `IRepository<T>` through your dependency resolution system. See the section about [DI frameworks](#di-frameworks) for more info.

## Getting started

- [Querying](#querying)
    - [Get by id](#get-by-id)
        - [Projection](#get-by-id)
    - [Find by expression](#find-by-expression)
        - [Count, Sort, Skip, Limit, Project, Cursor](#find-by-expression)
    - [LINQ](#linq)
- [Inserting, updating and deleting](#inserting-updating-and-deleting)
    - [InsertAsync, InsertManyAsync](#insertasync-insertmanyasync)
    - [UpdateOneAsync, UpdateManyAsync](#updateoneasync-Updatemanyasync)
    - [UpdateOneBulkAsync](#updateonebulkasync)
    - [FindOneAndUpdateAsync](#FindOneAndUpdateasync)
    - [FindOneAndReplaceAsync](documentation under construction)
    - [FindOneOrInsertAsync](documentation under construction)
    - [UpdateOrInsertOneAsync](#UpdateOrInsertOneAsync)
    - [Aggregation](#aggregation)
    - [Deleting](#deleting)
        - [Soft-deletes](#soft-deleting)
    - [Transactions](#transactions)
    - [UnionWith](#unionwith)
    - [ArrayFilters helpers](#arrayfilters-helpers)
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
- Contribute
    - [Design philosophy](#design-philosophy)


## Querying

### Get by id
```csharp
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

```csharp
await repository.Find(x => x.SomeProperty == someValue);
await repository.Find<SubType>(x => x.SomeProperty == someValue);
await repository.Find(x => x.SomeProperty, regexPattern);
```
Returns an [IFindFluent](http://api.mongodb.com/csharp/current/html/T_MongoDB_Driver_IFindFluent_2.htm) which offers methods like `ToListAsync`, `CountAsync`, `Project`, `Skip` and `Limit`

Examples:

```csharp
var dottedAnimals = await repository
    .Find(x => x.Coat == "dotted")
    .Limit(10)
    .Project(x => x.Species)
    .ToListAsync()
```
### LINQ
```csharp
repository.Query();
repository.Query<SubType>();
```
Returns an [IMongoQueryable](http://api.mongodb.com/csharp/current/html/T_MongoDB_Driver_Linq_IMongoQueryable.htm) which offers async versions of all the standard LINQ methods.

Examples:

```csharp
var dottedAnimals = await repository.Query()
    .Where(x => x.Coat == "dotted")
    .Take(10)
    .Select(x => x.Species)
    .ToListAsync()
```
## Inserting, updating and deleting
### InsertAsync, InsertManyAsync

```csharp
await repository.InsertAsync(someObject);
await repository.InsertManyAsync(someCollectionOfObjects);
```

### UpdateOneAsync, UpdateManyAsync
```csharp
// Update one document
await repository.UpdateOneAsync("id", x => x.Set(y => y.SomeProperty, someValue), upsert: true);
await repository.UpdateOneAsync(x => x.SomeProperty == someValue, x => x.Push(y => y.SomeCollection, someValue));
await repository.UpdateOneAsync<SubType>(x => x.SomeProperty == someValue, x => x.Push(y => y.SomeCollection, someValue));

// Update all documents matched by filter
await repository.UpdateManyAsync(x => x.SomeProperty == someValue, x => x.Inc(y => y.SomeProperty, 5));
```
### UpdateOneBulkAsync
Perform multiple update operations with different filters in one db roundtrip.
```csharp
await repository.UpdateOneBulkAsync(new List<UpdateOneCommand<MyEntity>> {
    new UpdateOneCommand<MyEntity> {
        Filter = x => x.SomeProperty = "foo",
        Update = x => x.Set(y => y.SomeOtherProperty, 10)
    },
    new UpdateOneCommand<MyEntity> {
        Filter = x => x.SomeProperty = "bar",
        Update = x => x.Set(y => y.SomeOtherProperty, 20)
    }
});
```
### FindOneAndUpdateAsync
This is a really powerful feature of MongoDB, in that it lets you update and retrieve a document atomically.
```csharp
var entityAfterUpdate = await repository.FindOneAndUpdateAsync(
    filter: x => x.SomeProperty.StartsWith("Hello"),
    update: x => x.AddToSet(y => y.SomeCollection, someItem)
);

var entityAfterUpdate = await repository.FindOneAndUpdateAsync(
    filter: x => x.SomeProperty.StartsWith("Hello"),
    update: x => x.PullFilter(y => y.SomeCollection, y => y.SomeOtherProperty == 5),
    returnProjection: x => new {
        x.SomeCollection
    },
    returnedDocumentState: ReturnedDocumentState.AfterUpdate,
    upsert: true
);
```

### UpdateOrInsertOneAsync
Lets you upsert a document of type `T` using an instance of type `T` as default and then apply updates on top of that, in an atomic operation. If the filter is matched, the default instance will not be used, and only the updates will be applied.

The same result can be achieved with common UpdateOne/FindOneAndUpdate using `upsert` and a bunch of `SetOnInsert`s, but the advantage of `UpdateOrInsertOneAsync` is you don't have to add a `SetOnInsert` for each property manually.


### Aggregation
```csharp
repository.Aggregate();
repository.Aggregate(options);
```
Returns an [IAggregateFluent](http://api.mongodb.com/csharp/current/html/T_MongoDB_Driver_IAggregateFluent_1.htm) which offers methods like `AppendStage`, `Group`, `Match`, `Unwind`, `Out`, `Lookup` etc.

### Deleting
```csharp
await repository.DeleteByIdAsync("id");
// or
await repository.DeleteManyAsync(x => x.SomeProperty === someValue);
// or
var deleted = await repository.FindOneAndDeleteAsync("id");
// or
var deleted = await repository.FindOneAndDeleteAsync<DerivedType>(x => x.SomeProp == someValue);
```
#### Soft-deleting
Soft-deleting an entity will move it to a different collection, preserving type-information.
```csharp
await repository.DeleteByIdAsync("id", softDelete: true);
// or
var deleted = await repository.FindOneAndDeleteAsync("id", softDelete: true);
```
Listing soft-deleted entities:
```csharp
await repository.ListTrashAsync();
```
Restoring one (or many) soft-deleten entities
```csharp
await repository.RestoreSoftDeletedAsync("id");
await repository.RestoreSoftDeletedAsync(x => x.TimestampDeletedUtc > DateTime.Today);
```
Permanently delete soft-deleted documents
```cs
await repository.PermamentlyDeleteSoftDeletedAsync(x => x.Foo == "bar");
```

### Transactions
MongoDB 4 introduced support for multi-document transactions. We provide a simplified interface: you don't have to pass around the session object. Instead we detect any ambient transaction and uses it for all write/update/delete operations.:

```csharp
using (var transaction = repository.StartTransaction()) {
    // ...
    await transaction.CommitAsync();
}
```

Since version 5 we also support enlisting with a `TransactionScope`. This is useful to be able to put a transactional boundary around MongoDB operations and anything that is compatible with TransactionScopes.

```csharp
using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)) {
    repository.EnlistWithCurrentTransactionScope();
    // ...
    transaction.Complete();
}
```
If you configure the repository with `.AutoEnlistWithTransactionScopes()` then it will automatically enlist to any ambient TransactionScope without the need to do it explicitly like in the example above.

MongoDB replica sets sometimes encounter transient transaction errors, in which case the recommended course of action from the MongoDB team is to simply retry until it succeeds. We offer a shorthand for this:

```csharp
// Retry using standard MongoDB transaction
await repo.WithTransactionAsync(async () =>
{
    // your code here
}, maxRetries: 3);

// Retry using TransactionScope
await repo.WithTransactionAsync(async () =>
{
    // your code here
}, TransactionType.TransactionScope, maxRetries: 3);
```

`RetryAsync` also comes with an overload that takes a number representing the max number of retries.

### UnionWith

This library provides an extension method to `IAggregateFluent<T>` called `UnionWith` that accepts a repository and a projection expression.

```cs
using JohnKnoop.MongoRepository.Extensions;

var allContacts = await soccerPlayersRepository
    .Aggregate()
    .Project(x => new
    {
        PlayerName = x.SoccerPlayerName,
        TeamName = x.SoccerTeamName
    })
    .UnionWith(
        rugbyPlayersRepository,
        x => new
        {
            PlayerName = x.RugbyPlayerName,
            TeamName = x.RugbyTeamName
        }
    )
    .SortBy(x => x.PlayerName)
    .ToListAsync();
```

### ArrayFilters helpers

Working with [ArrayFilters](https://www.mongodb.com/docs/manual/reference/operator/update/positional-filtered/) using the MongoDB C# driver is an unpleasant experience in that it doesn't provide any compile-time checking. This library contains a few handy helpers that lets you replace this code:

```cs
await _repository.UpdateOneAsync(
    filter: x => x.Title == "Game of Thrones",
    update: x => x.Set(
        "Seasons.$[a].Episodes.$[b].Title",
        "Qarth"
    ),
    options: new UpdateOptions
    {
        ArrayFilters = new List<ArrayFilterDefinition<Show>>
        {
            new BsonDocument("a.Year", new BsonDocument("$ne", "2013")),
            new BsonDocument("b.Number", 2),
        }
    }
);
```
...with this:
```cs
await _repository.UpdateOneAsync(
    filter: x => x.Title == "Game of Thrones",
    update: x => x.Set(
        ArrayFilters.CreateArrayFilterPath<Show>()
            .SelectEnumerable(x => x.Seasons, "a")
            .SelectEnumerable(x => x.Episodes, "b")
            .SelectProperty(x => x.Title)
            .Build(),
        "Qarth"
    ),
    options: new UpdateOptions
    {
        ArrayFilters = ArrayFilters.DefineFilters<Show>()
            .AddFilter("a", show => show.Seasons, f => f.Eq(x => x.Year, "2013"))
            .ThenAddFilter("b", season => season.Episodes, f => f.Eq(x => x.Number, 2))
    }
);
```

This ensures already at design-time that you haven't misspelled any properties, or that you're using the wrong data type for any values.

Please note that this feature is still experimental.

## Advanced features

### Counters

Auto-incrementing fields is a feature of most relational databases that unfortunately isn't supported by MongoDB. To get around this, _counters_ are a way to solve the problem of incrementing a number with full concurrency support.
```csharp
var value = await repository.GetCounterValueAsync();
var value = await repository.GetCounterValueAsync("MyNamedCounter");
```
Atomically increment and read the value of a counter:
```csharp
var value = await repository.IncrementCounterAsync(); // Increment by 1
var value = await repository.IncrementCounterAsync(name: "MyNamedCounter", incrementBy: 5);
```
Reset a counter:
```csharp
await repository.ResetCounterAsync(); // Reset to 1
await repository.ResetCounterAsync(name: "MyNamedCounter", newValue: 5);
```

### Deleting properties

Delete a property from a document:

```csharp
await repository.DeletePropertyAsync(x => x.SomeProperty == someValue, x => x.PropertyToRemove);
```

## Configuration

Configuration is done once, when the application is started. Use `MongoRepository.Configure()` as shown below.

### Multi-tenancy

Database-per-tenant style multi-tenancy is supported. When defining a database, just use the `DatabasePerTenant` method:

```csharp
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
```csharp
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

_[To be documented]_

### Unconventional id properties

_[To be documented]_

### DI frameworks
#### .NET Core

There is an extension package called `JohnKnoop.MongoRepository.DotNetCoreDi` that registers `IRepository<T>` as a dependency with the .NET Core dependency injection framework.

See the [repository readme](https://github.com/johnknoop/MongoRepository.DotNetCoreDi) for more information.

#### Ninject

```csharp
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

## Design philosophy

This library is an extension to the MongoDB C# driver, and thus I don't mind exposing types from the MongoDB.Driver namespace, like IFindFluent or the result types of the various operations.

Any contributions to this library should be in line with the philosophy of this primarily being an extension that makes it easy to write multi-tenant applications using the MongoDB driver. I'm not looking to widen the scope of this library.
