# Basalt

Basalt is a durable background-job, scheduling, and static-workflow engine for .NET with interchangeable Embedded and SQL Server storage.

## Features

- Strongly typed jobs with durable enqueue, retry, and idempotency
- Interval, daily, delayed, one-off, and cron scheduling
- Static workflow DAGs and management APIs
- Crash recovery, leases, fencing, and multiprocess coordination
- Local Embedded storage or a dedicated schema in an existing SQL Server database
- .NET 8 and .NET Framework 4.7.2/WPF consumers

## Embedded in 30 seconds

```csharp
using var basalt = Basalt.Embedded(@"C:\data\jobs");

basalt.On<SendInvoice>("billing.send",
    async (job, context, cancellationToken) =>
        await SendIdempotently(job, context.ExecutionId, cancellationToken));

await basalt.StartAsync();
await basalt.EnqueueAsync(new SendInvoice(123), key: "invoice:123", retry: 5);
await basalt.StopAsync();
```

## SQL Server

SQL Server changes only storage creation; handler and job code stays identical:

```csharp
using var basalt = Basalt.SqlServer(connectionString, sql =>
{
    sql.Schema = "MyApp_Basalt";
    sql.SchemaManagement = SchemaManagement.ValidateOnly;
});
```

SQL storage does not execute work by itself. Every process intended to execute jobs must register its handlers and call `StartAsync`.

## Documentation

- [Getting started](docs/getting-started.md)
- [Public API](docs/api.md)
- [Cookbook](docs/cookbook.md)
- [Durability contract](docs/durability.md)
- [SQL Server](docs/sql-server.md)
- [Architecture](docs/architecture.md) and [storage choices](docs/storage.md)

Basalt provides **at-least-once execution**. Durable enqueue confirms submission, not handler completion; external side effects must be idempotent or fenced.

## License

Basalt is licensed under the [MIT License](LICENSE), copyright © 2026 Vanderhell.
