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
        ulong revision, long leaseExpiresAt, ulong fencingToken)
    {
        ExecutionId=executionId; JobDefinitionId=jobDefinitionId; ScheduleId=scheduleId; WorkflowId=workflowId;
        State=(ExecutionState)state; CreatedAt=UnixTime.Required(createdAt); EligibleAt=UnixTime.Required(eligibleAt);
        StartedAt=UnixTime.Optional(startedAt); FinishedAt=UnixTime.Optional(finishedAt); Priority=priority;
        Attempt=attempt; MaxAttempts=maxAttempts; Revision=revision; LeaseExpiresAt=UnixTime.Optional(leaseExpiresAt);
        FencingToken=fencingToken;
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
}

internal static class UnixTime
{
    internal static DateTimeOffset Required(long seconds)=>DateTimeOffset.FromUnixTimeSeconds(seconds);
    internal static DateTimeOffset? Optional(long seconds)=>seconds==0?(DateTimeOffset?)null:DateTimeOffset.FromUnixTimeSeconds(seconds);
    internal static long Required(DateTimeOffset value)=>value.ToUnixTimeSeconds();
    internal static long Optional(DateTimeOffset? value)=>value?.ToUnixTimeSeconds()??0;
}
