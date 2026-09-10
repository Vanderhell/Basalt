# Basalt

Basalt is a durable background-job, scheduling, and static-workflow engine for .NET. Use the same strongly typed API with a local Embedded store or a shared SQL Server store.

```csharp
using var basalt = Basalt.Embedded(@"C:\data\jobs");

basalt.On<SendInvoice>("billing.send",
    async (job, context, cancellationToken) =>
        await SendIdempotently(job, context.ExecutionId, cancellationToken));

await basalt.StartAsync();
await basalt.EnqueueAsync(new SendInvoice(123), key: "invoice:123", retry: 5);
await basalt.StopAsync();
```

SQL Server changes only storage creation; job code stays identical:

```csharp
using var basalt = Basalt.SqlServer(connectionString, sql =>
{
    sql.Schema = "MyApp_Basalt";
    sql.SchemaManagement = SchemaManagement.ValidateOnly;
});
```

```csharp
await basalt.EveryAsync("sync", TimeSpan.FromMinutes(5), new SyncJob());
await basalt.Workflow("invoice")
    .Add("create", new CreateInvoice())
    .Then("send", new SendInvoice(123))
    .SubmitAsync();
```

SQL storage does not execute work by itself. Every process intended to execute jobs must register its handlers and call `StartAsync`.

Basalt provides at-least-once execution. Durable enqueue confirms submission, not handler completion; external side effects must be idempotent or fenced.

See [docs/api.md](docs/api.md) for the complete public API guide.
