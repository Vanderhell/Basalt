# Basalt

Basalt is a local durable background-job engine for .NET applications.

The v1 contract is at-least-once delivery: handlers must be idempotent. A
successful enqueue confirms durable submission, not handler completion. Native
ownership is internal to `BasaltDatabase` and `BasaltEngine`; applications do
not manage native handles.

Use the same typed API with either embedded storage or SQL Server:

```csharp
using var app = Basalt.Create(o => o.UseEmbedded(@"C:\data\jobs"));
// Add Basalt.SqlServer and use o.UseSqlServer(connectionString,
//     sql => { sql.Schema = "MyApp_Basalt"; }); for a shared durable store.

app.RegisterHandler<SendInvoice>("billing.send", async (job, context, ct) =>
    await SendIdempotently(job, context.ExecutionId, ct));

await app.EnqueueAsync("billing.send", new SendInvoice(), new EnqueueOptions {
    IdempotencyKey = "invoice:123",
    Retry = RetryOptions.Exponential(5, TimeSpan.FromSeconds(2))
});
await app.ScheduleAsync("nightly", new SendInvoice(), s =>
    s.Cron("0 2 * * *").InTimeZone("Central Europe Standard Time"));
await app.Workflow("invoice-flow").Add("send", new SendInvoice())
    .Then("notify", new SendInvoice()).SubmitAsync();
```

SQL Server objects live only in the configured Basalt schema. `AutoMigrate`
uses a transaction and an application lock; `ValidateOnly` and `Manual` never
execute DDL. `SqlSchemaManager.GenerateMigrationScript()` returns the script
for deployment-controlled environments. Grant the runtime identity DML access
only to that schema plus sequence access; keep schema-alter permission on the
migration identity.

SQL Server is a shared durable store, not an automatic job executor: each
process that should execute work must start a Basalt engine. Delivery is
at-least-once, so external effects must use an idempotency key or another
fenced/idempotent mechanism. A `BasaltUnknownCommitException` means the client
lost certainty during commit; retry the identical operation with its same
idempotency key.
