# Getting started

Basalt is consumed from the repository through project references. Applications use the same job API with either storage provider.

## Embedded

```csharp
using BasaltCore;

using var basalt = Basalt.Embedded(@"C:\MyApp\data\jobs");

basalt.On<GenerateReportJob>("reports.generate", async (job, context, ct) =>
    await GenerateReport(job.ReportId, ct));

await basalt.StartAsync();
await basalt.EnqueueAsync(new GenerateReportJob { ReportId = 42 });
await basalt.StopAsync();
```

Embedded is a durable local store. Use an application-owned writable directory and keep it across restarts.

## SQL Server

Reference both `BasaltCore` and `BasaltCore.SqlServer`:

```csharp
using BasaltCore;
using BasaltCore.SqlServer;

using var basalt = Basalt.SqlServer(connectionString, sql =>
{
    sql.Schema = "MyApp_Basalt";
    sql.SchemaManagement = SchemaManagement.AutoMigrate;
});

basalt.On<GenerateReportJob>("reports.generate", async (job, context, ct) =>
    await GenerateReport(job.ReportId, ct));

await basalt.StartAsync();
await basalt.EnqueueAsync(new GenerateReportJob { ReportId = 42 });
await basalt.StopAsync();
```

SQL Server is a shared durable store, not a worker service. Every application process intended to execute work must register handlers and call `StartAsync`.

Basalt executes at least once. Make external side effects idempotent or appropriately fenced. Continue with the [API guide](api.md) or [cookbook](cookbook.md).
