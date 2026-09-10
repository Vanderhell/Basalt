namespace BasaltCore;

/// <summary>The durable lifecycle state of an execution.</summary>
public enum ExecutionState : uint
{
    Created = 0, Scheduled = 1, Ready = 2, Leased = 3, Running = 4, Retry = 5,
    Blocked = 6, Done = 7, Failed = 8, Dead = 9, Cancelled = 10, Paused = 11
}

/// <summary>Describes a durable job execution. Revision and fencing values are advanced diagnostic metadata.</summary>
public sealed class BasaltExecutionInfo
{
    public BasaltExecutionInfo(ulong executionId, ulong jobDefinitionId, ulong scheduleId, ulong workflowId, uint state,
        long createdAt, long eligibleAt, long startedAt, long finishedAt, int priority, uint attempt, uint maxAttempts,
        ulong revision, long leaseExpiresAt, ulong fencingToken, byte[]? workerInstanceId = null)
    {
        ExecutionId=executionId; JobDefinitionId=jobDefinitionId; ScheduleId=scheduleId; WorkflowId=workflowId;
        State=(ExecutionState)state; CreatedAt=UnixTime.Required(createdAt); EligibleAt=UnixTime.Required(eligibleAt);
        StartedAt=UnixTime.Optional(startedAt); FinishedAt=UnixTime.Optional(finishedAt); Priority=priority;
        Attempt=attempt; MaxAttempts=maxAttempts; Revision=revision; LeaseExpiresAt=UnixTime.Optional(leaseExpiresAt);
        FencingToken=fencingToken; WorkerInstanceId=workerInstanceId == null || workerInstanceId.All(x => x == 0) ? null : BitConverter.ToString(workerInstanceId).Replace("-", string.Empty);
    }
    public ulong ExecutionId { get; }
    public ulong JobDefinitionId { get; }
    public ulong ScheduleId { get; }
    public ulong WorkflowId { get; }
    public ExecutionState State { get; }
    public DateTimeOffset CreatedAt { get; }
    public DateTimeOffset EligibleAt { get; }
    public DateTimeOffset? StartedAt { get; }
    public DateTimeOffset? FinishedAt { get; }
    public int Priority { get; }
    public uint Attempt { get; }
    public uint MaxAttempts { get; }
    public ulong Revision { get; }
    public DateTimeOffset? LeaseExpiresAt { get; }
    public ulong FencingToken { get; }
    public string? WorkerInstanceId { get; }
    /// <summary>Registered stable job key when its durable management mapping is available.</summary>
    public string? JobKey { get; internal set; }
    public TimeSpan? QueueWait => StartedAt.HasValue ? StartedAt.Value - CreatedAt : null;
    public TimeSpan? ExecutionDuration => StartedAt.HasValue && FinishedAt.HasValue ? FinishedAt.Value - StartedAt.Value : null;
}

/// <summary>Filters a bounded page of durable executions. All supplied filters are combined.</summary>
public sealed class ExecutionQuery
{
    public IReadOnlyCollection<ExecutionState>? States { get; set; }
    public ulong? JobDefinitionId { get; set; }
    public ulong? ScheduleId { get; set; }
    public ulong? WorkflowId { get; set; }
    public DateTimeOffset? CreatedFrom { get; set; }
    public DateTimeOffset? CreatedTo { get; set; }
    public int Take { get; set; } = 100;
    public ulong AfterExecutionId { get; set; }

    internal bool Matches(BasaltExecutionInfo value)
    {
        if (States != null && States.Count != 0 && !States.Contains(value.State)) return false;
        if (JobDefinitionId.HasValue && value.JobDefinitionId != JobDefinitionId.Value) return false;
        if (ScheduleId.HasValue && value.ScheduleId != ScheduleId.Value) return false;
        if (WorkflowId.HasValue && value.WorkflowId != WorkflowId.Value) return false;
        if (CreatedFrom.HasValue && value.CreatedAt < CreatedFrom.Value) return false;
        return !CreatedTo.HasValue || value.CreatedAt <= CreatedTo.Value;
    }
}

/// <summary>Current, non-cumulative execution counts grouped by durable state.</summary>
public sealed class BasaltQueueStats
{
    public BasaltQueueStats(ulong created = 0, ulong scheduled = 0, ulong ready = 0, ulong leased = 0, ulong running = 0,
        ulong retry = 0, ulong blocked = 0, ulong done = 0, ulong failed = 0, ulong dead = 0, ulong cancelled = 0, ulong paused = 0)
    { Created=created; Scheduled=scheduled; Ready=ready; Leased=leased; Running=running; Retry=retry; Blocked=blocked; Done=done; Failed=failed; Dead=dead; Cancelled=cancelled; Paused=paused; }
    public ulong Created { get; internal set; } public ulong Scheduled { get; internal set; } public ulong Ready { get; internal set; }
    public ulong Leased { get; internal set; } public ulong Running { get; internal set; } public ulong Retry { get; internal set; }
    public ulong Blocked { get; internal set; } public ulong Done { get; internal set; } public ulong Failed { get; internal set; }
    public ulong Dead { get; internal set; } public ulong Cancelled { get; internal set; } public ulong Paused { get; internal set; }
    public ulong Active => Ready + Leased + Running + Retry + Blocked;

    internal void Add(ExecutionState state)
    {
        switch (state)
        {
            case ExecutionState.Created: Created++; break; case ExecutionState.Scheduled: Scheduled++; break;
            case ExecutionState.Ready: Ready++; break; case ExecutionState.Leased: Leased++; break;
            case ExecutionState.Running: Running++; break; case ExecutionState.Retry: Retry++; break;
            case ExecutionState.Blocked: Blocked++; break; case ExecutionState.Done: Done++; break;
            case ExecutionState.Failed: Failed++; break; case ExecutionState.Dead: Dead++; break;
            case ExecutionState.Cancelled: Cancelled++; break; case ExecutionState.Paused: Paused++; break;
        }
    }
}

public enum BasaltHealthStatus { Healthy, Degraded, Unhealthy }
public enum BasaltWorkerStatus { Alive, Stale }

/// <summary>Diagnostic worker information derived from current execution leases. It does not participate in correctness.</summary>
public sealed class BasaltWorkerInfo
{
    public string WorkerId { get; internal set; } = string.Empty;
    public BasaltWorkerStatus Status { get; internal set; }
    public DateTimeOffset? LeaseExpiresAt { get; internal set; }
    public int ActiveExecutionCount { get; internal set; }
}

/// <summary>Read-only storage and queue health snapshot.</summary>
public sealed class BasaltHealth
{
    public BasaltHealth(BasaltHealthStatus status, string provider, DateTimeOffset checkedAt, BasaltQueueStats queue,
        IReadOnlyList<BasaltWorkerInfo> workers, string? diagnosticMessage)
    { Status=status; Provider=provider; CheckedAt=checkedAt; Queue=queue; Workers=workers; DiagnosticMessage=diagnosticMessage; }
    public BasaltHealthStatus Status { get; }
    public string Provider { get; }
    public DateTimeOffset CheckedAt { get; }
    public BasaltQueueStats Queue { get; }
    public IReadOnlyList<BasaltWorkerInfo> Workers { get; }
    public string? DiagnosticMessage { get; }
}

public sealed class BasaltStats
{
    public BasaltStats(ulong submittedTotal, ulong startedTotal, ulong completedTotal, ulong failedTotal, ulong retriedTotal, ulong cancelledTotal, ulong deadTotal, ulong recoveredTotal)
    { SubmittedTotal=submittedTotal; StartedTotal=startedTotal; CompletedTotal=completedTotal; FailedTotal=failedTotal; RetriedTotal=retriedTotal; CancelledTotal=cancelledTotal; DeadTotal=deadTotal; RecoveredTotal=recoveredTotal; }
    public ulong SubmittedTotal { get; } public ulong StartedTotal { get; } public ulong CompletedTotal { get; } public ulong FailedTotal { get; }
    public ulong RetriedTotal { get; } public ulong CancelledTotal { get; } public ulong DeadTotal { get; } public ulong RecoveredTotal { get; }
}

/// <summary>Records the terminal outcome of an execution.</summary>
public sealed class BasaltLedgerEntry
{
    public ulong ExecutionId { get; set; }
    public ulong JobDefinitionId { get; set; }
    public ulong ScheduleId { get; set; }
    public ulong WorkflowId { get; set; }
    public DateTimeOffset? StartedAt { get; set; }
    public DateTimeOffset? FinishedAt { get; set; }
    public TimeSpan Duration { get; set; }
    public uint Attempt { get; set; }
    public ExecutionState FinalState { get; set; }
    public int ResultCode { get; set; }
    public int ErrorCode { get; set; }
}

/// <summary>Describes a durable schedule using .NET-friendly policy and time values.</summary>
public sealed class BasaltScheduleInfo
{
    public ulong ScheduleId { get; set; }
    public ulong JobDefinitionId { get; set; }
    public ScheduleType Type { get; set; }
    public ScheduleType ScheduleType { get => Type; set => Type=value; }
    public bool Enabled { get; set; }
    public ulong TimezoneReference { get; set; }
    public TimeSpan Interval { get; set; }
    public IntervalMode IntervalMode { get; set; }
    public uint CatchUpMax { get; set; }
    public DateTimeOffset StartAt { get; set; }
    public DateTimeOffset? EndAt { get; set; }
    public DateTimeOffset? LastFireAt { get; set; }
    public DateTimeOffset? NextFireAt { get; set; }
    public ulong OccurrenceCount { get; set; }
    public ulong MaxOccurrences { get; set; }
    public ulong Revision { get; set; }
    public MisfirePolicy MisfirePolicy { get; set; }
    public OverlapPolicy OverlapPolicy { get; set; }
    public string? ScheduleKey { get; internal set; }
    public string? JobKey { get; internal set; }
}

internal static class UnixTime
{
    internal static DateTimeOffset Required(long seconds)=>DateTimeOffset.FromUnixTimeSeconds(seconds);
    internal static DateTimeOffset? Optional(long seconds)=>seconds==0?(DateTimeOffset?)null:DateTimeOffset.FromUnixTimeSeconds(seconds);
    internal static long Required(DateTimeOffset value)=>value.ToUnixTimeSeconds();
    internal static long Optional(DateTimeOffset? value)=>value?.ToUnixTimeSeconds()??0;
}
