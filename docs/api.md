# Basalt public .NET API

## Getting started

Embedded storage is a local durable store:

```csharp
using var basalt = Basalt.Embedded(path);
```

SQL Server storage is a shared durable store:

```csharp
using var basalt = Basalt.SqlServer(connectionString);
```

Storage does not start workers. Every process intended to execute work must register handlers and call `StartAsync`.

## Handlers and enqueue

Register each CLR job type under an explicit durable key:

```csharp
basalt.On<MyJob>("stable.job.key", async (job, context, ct) =>
{
    await Handle(job, context.ExecutionId, ct);
});
```

The key is deliberately not derived from the CLR type name. Keep it stable across class and namespace renames.

```csharp
await basalt.EnqueueAsync(new MyJob());
await basalt.EnqueueAsync(new MyJob(), key: "business-operation:123");
await basalt.EnqueueAsync(new MyJob(), retry: 5);
await basalt.EnqueueAsync(new MyJob(), key: "business-operation:123", retry: 5);
```

The integer convenience is the total maximum attempt count. Values above one use exponential retry with a one-second initial delay and a one-minute bound. Advanced configuration remains available:

```csharp
await basalt.EnqueueAsync(new MyJob(), new EnqueueOptions
{
    IdempotencyKey = "business-operation:123",
    Retry = RetryOptions.ExponentialWithJitter(
        5, TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(500),
        TimeSpan.FromMinutes(2))
});
```

Retry strategies are `None`, `Fixed`, `Linear`, `Exponential`, and `ExponentialWithJitter`. `MaxAttempts` includes the initial attempt. Retry handles failure of one execution; recurring schedules create executions over time and are a separate concept.

Enqueue is durable submission, not successful handler completion. Delivery is at least once, so handlers with external effects must be idempotent or use an appropriate fencing mechanism. If `BasaltUnknownCommitException` is raised, retry the identical operation with the same idempotency key.

## Scheduling

```csharp
await basalt.EveryAsync("sync", TimeSpan.FromMinutes(5), new SyncJob());
await basalt.DailyAsync("backup", 2, 0, new BackupJob()); // explicitly UTC
await basalt.DailyAsync("backup", 2, 0, new BackupJob(), "Central Europe Standard Time");
await basalt.AtAsync("report-once", DateTimeOffset.UtcNow.AddHours(1), new ReportJob());
```

Stable schedule keys are required for durable management and restart behavior. `DailyAsync` uses UTC unless an explicit timezone is supplied.

Advanced scheduling uses the same scheduler path:

```csharp
await basalt.ScheduleAsync("nightly", new BackupJob(), schedule => schedule
    .Cron("0 2 * * *")
    .InTimeZone("Central Europe Standard Time")
    .Until(DateTimeOffset.UtcNow.AddYears(1))
    .OnMisfire(MisfirePolicy.RunOnce)
    .OnOverlap(OverlapPolicy.Skip)
    .WithCatchUpMax(1)
    .WithMaxOccurrences(365));
```

Builder timing methods are `Every`, `OnceAt`, `Delay`, `Cron`, and `DailyAt`. Fixed-rate measures occurrences from the schedule timeline; fixed-delay measures the next occurrence after completion. Misfire policies are `Skip`, `RunOnce`, `RunLast`, and `CatchUpAll`. Overlap policies are `Allow`, `Skip`, `QueueOne`, and `QueueAll`.

## Workflows

```csharp
await basalt.Workflow("invoice")
    .Add("create", new CreateInvoice())
    .Then("send", new SendInvoice())
    .AddAfter("audit", new AuditInvoice(), "create")
    .OnDependencyFailure(DependencyPolicy.Cancel)
    .SubmitAsync();
```

A workflow is a static durable DAG. Node names are stable within the workflow, `Then` depends on the previous node, and `AddAfter` names earlier dependencies. The dependency failure policy controls whether blocked descendants remain blocked, are cancelled, continue, or fail the workflow.

## Management

```csharp
var execution = basalt.GetExecution(id);
var executions = basalt.ListExecutions();
var stats = basalt.GetStats();
var ledger = basalt.GetLedger(id);

basalt.Cancel(id);
basalt.Requeue(id);

var schedule = basalt.GetSchedule("nightly");
basalt.Pause("nightly");
basalt.Resume("nightly");
basalt.Remove("nightly");

var workflow = basalt.GetWorkflow("invoice");
basalt.VerifyHealth();
```

States and policies are managed enums, and timestamps are `DateTimeOffset` values. `Revision` and `FencingToken` remain visible for diagnostics and advanced concurrency control; normal management calls resolve revisions internally.

## SQL Server configuration

```csharp
using var basalt = Basalt.SqlServer(connectionString, sql =>
{
    sql.Schema = "MyApp_Basalt";
    sql.SchemaManagement = SchemaManagement.ValidateOnly;
    sql.CommandTimeoutSeconds = 30;
});
```

Basalt owns only objects in its configurable dedicated schema. `AutoMigrate` creates or upgrades them, `ValidateOnly` validates without DDL, and `Manual` skips automatic schema work. Use a migration identity for DDL and normally grant the runtime identity only DML rights on the Basalt schema plus sequence access.

A connection factory is also supported and must return a fresh `Microsoft.Data.SqlClient.SqlConnection` each time:

```csharp
using var basalt = Basalt.SqlServer(() => new SqlConnection(connectionString));
```

Do not share an Entity Framework `DbContext` or its connection instance with Basalt. Basalt creates, opens, and disposes its own connections for concurrent durable operations.

## Lifecycle

```csharp
await basalt.StartAsync();
// workers execute registered work
await basalt.StopAsync();
basalt.Dispose();
```

`StartAsync` starts workers; enqueue and schedule creation do not imply worker startup. `StopAsync` performs the configured graceful stop, and `Dispose` releases the engine and owned storage. Explicit lifecycle behavior is the same for Embedded and SQL Server.

## Compatibility API

The longer configuration and explicit-key APIs remain available for advanced and existing consumers:

```csharp
using var basalt = Basalt.Create(options => options.UseEmbedded(path));
basalt.RegisterHandler<MyJob>("stable.job.key", Handler);
await basalt.EnqueueAsync("stable.job.key", new MyJob(), options);
basalt.PauseSchedule("nightly");
```
