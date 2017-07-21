# MongoRepository
An easy-to-configure, powerful repository for MongoDB with support for multi-tenancy

## Querying

...

## Inserting

...

## Updating

...

## Getting started

### Configure mappings, indices, multitenancy etc with a few lines of code

```
MongoRepository.Configure()
  .WithTypes(x => x
    .Map<Bike>
    .Map<Car>("Cars")
    .Map<Airplane>("Airplanes", p => p.AirplainId)
    )
  .WithTenantTypes(x => x
    .Map<User>
  )
  .Build();
```
