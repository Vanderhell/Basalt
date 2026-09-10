# Management and monitoring

Basalt management is storage-neutral. Applications and the dashboard use `BasaltApplication`; they never read Embedded files or SQL Server tables directly.

```csharp
BasaltHealth health = basalt.GetHealth();
BasaltQueueStats queue = basalt.GetQueueStats();
BasaltStats totals = basalt.GetStats();

var failed = basalt.ListExecutions(new ExecutionQuery
{
    States = new[] { ExecutionState.Failed, ExecutionState.Dead, ExecutionState.Retry },
    Take = 100
});
```

`GetHealth()` is non-destructive. `Healthy` means the provider health check succeeded and no failed, dead, or stale leased work was observed. `Degraded` indicates one of those queue conditions. `Unhealthy` means the provider health check could not be completed.

`GetQueueStats()` returns current counts for every execution state. SQL Server obtains these counts with a grouped provider query; Embedded obtains them through its storage-neutral Core management scan. `GetStats()` returns durable cumulative totals: submitted, started, completed, failed, retried, cancelled, dead, and recovered.

`ListExecutions(ExecutionQuery)` provides bounded filtering by state, job definition, schedule, workflow, creation time, and execution-id pagination. SQL Server applies these filters in its provider query; Embedded applies them while paging durable execution IDs and does not materialize the full result set. `GetExecution(id)` exposes lifecycle timestamps, retry attempt, lease expiry, fencing token, and revision. `GetLedger(id)` describes a terminal result; it is not a retry-attempt history.

Stable job, schedule, and workflow keys are stored by Basalt as management metadata when they are registered or created. Existing stores created before this feature may show an ID until that key is registered or created again.

```csharp
var schedules = basalt.ListSchedules();
basalt.Pause("nightly");
basalt.Resume("nightly");
basalt.Remove("nightly");

var workflows = basalt.ListWorkflows();
var workers = basalt.ListWorkers();
```

Schedule and workflow actions retain Basalt's optimistic revision checks. `Cancel` and `Requeue` are validated by BasaltCore, so a UI race is rejected rather than silently applied to a changed execution.

`ListWorkers()` is observational. Starting an engine registers each native worker with a machine name, process ID, start time, and a revision-checked heartbeat refreshed every five seconds. A heartbeat older than fifteen seconds is `Stale`; records older than one day are safely pruned during a later heartbeat sweep. Active execution count and lease expiry are joined from current leases. Heartbeats are never used for recovery or correctness: execution safety continues to depend on leases and fencing; a missing or stale entry never changes an execution state.

## Dashboard

`BasaltDashboard` is a separate WPF operational console. It uses only the public management API and refreshes on a non-overlapping two-second timer.

```text
dotnet run --project BasaltDashboard -- C:\data\jobs
dotnet run --project BasaltDashboard -- --sql "<connection string>"
```

The dashboard shows overview queue cards, executions, failed/dead/retrying work, schedules, workflows, observed workers, and cumulative statistics. The executions view has a state filter and selected-execution lifecycle/ledger detail. Its selected execution and schedule controls call the same public `Cancel`, `Requeue`, `PauseSchedule`, `ResumeSchedule`, and `RemoveSchedule` methods available to an application. It does not start workers or modify provider configuration.
