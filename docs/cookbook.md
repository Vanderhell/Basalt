# Cookbook

These recipes assume registered job types and a started `BasaltApplication` unless shown otherwise.

## Simple background job

```csharp
basalt.On<GenerateReportJob>("reports.generate", GenerateReport);
await basalt.EnqueueAsync(new GenerateReportJob { ReportId = 42 });
```

## Retry

```csharp
await basalt.EnqueueAsync(new SendEmailJob(), retry: 5);
```

For a custom strategy, pass `new EnqueueOptions { Retry = RetryOptions.Fixed(5, TimeSpan.FromSeconds(10)) }`.

## Idempotency

```csharp
await basalt.EnqueueAsync(new SendEmailJob(), key: "invoice-email:123");
```

Reuse the same key after `BasaltUnknownCommitException`.

## Delayed job

```csharp
await basalt.ScheduleAsync("cleanup-delay", new CleanupJob(),
    schedule => schedule.Delay(TimeSpan.FromMinutes(10)));
```

## Recurring job

```csharp
await basalt.EveryAsync("sync", TimeSpan.FromMinutes(5), new SyncJob());
```

## Daily job

```csharp
await basalt.DailyAsync("backup", 2, 0, new BackupJob(), "Central Europe Standard Time");
```

## Cron

```csharp
await basalt.ScheduleAsync("weekdays", new ReportJob(), schedule => schedule
    .Cron("0 8 * * 1-5")
    .InTimeZone("Central Europe Standard Time"));
```

## Prevent overlap

```csharp
await basalt.ScheduleAsync("exclusive-sync", new SyncJob(), schedule => schedule
    .Every(TimeSpan.FromMinutes(5))
    .OnOverlap(OverlapPolicy.Skip));
```

## Linear workflow

```csharp
await basalt.Workflow("invoice")
    .Add("create", new CreateInvoiceJob())
    .Then("send", new SendInvoiceJob())
    .SubmitAsync();
```

## DAG dependency

```csharp
await basalt.Workflow("report")
    .Add("data", new LoadDataJob())
    .AddAfter("pdf", new RenderPdfJob(), "data")
    .AddAfter("audit", new AuditJob(), "data")
    .AddAfter("email", new SendEmailJob(), "pdf", "audit")
    .OnDependencyFailure(DependencyPolicy.Cancel)
    .SubmitAsync();
```

## Cancel and requeue

```csharp
basalt.Cancel(executionId);
basalt.Requeue(executionId);
```

Normal calls resolve compare-and-swap revisions internally. Requeue applies to a terminal or paused execution.

## WPF startup and shutdown

```csharp
private BasaltApplication? _basalt;

private async Task StartJobs()
{
    _basalt = Basalt.Embedded(dataPath);
    _basalt.On<CleanupJob>("maintenance.cleanup", Cleanup);
    await _basalt.StartAsync();
}

protected override void OnExit(ExitEventArgs e)
{
    if (_basalt != null)
    {
        _basalt.StopAsync().GetAwaiter().GetResult();
        _basalt.Dispose();
    }
    base.OnExit(e);
}
```

## Embedded storage

```csharp
using var basalt = Basalt.Embedded(
    Path.Combine(appDataDirectory, "BasaltJobs"));
```

Keep the directory durable and application-owned.

## SQL shared workers

```csharp
using var basalt = Basalt.SqlServer(connectionString);
basalt.On<SyncJob>("sync.run", Sync);
await basalt.StartAsync();
```

Run the same registration and lifecycle in each process intended to claim work.

## Restricted SQL runtime identity

```csharp
using var basalt = Basalt.SqlServer(runtimeConnectionString, sql =>
{
    sql.Schema = "MyApp_Basalt";
    sql.SchemaManagement = SchemaManagement.ValidateOnly;
});
```

Grant only schema DML and sequence `UPDATE`; apply migrations separately with the migration identity.

## Crash and restart

```csharp
using var basalt = Basalt.Embedded(path); // reopen the same durable path
basalt.On<SyncJob>("sync.run", Sync);
await basalt.StartAsync();                // expired unfinished work is recoverable
```

Handlers may run again after recovery, so external effects must remain idempotent or fenced.
