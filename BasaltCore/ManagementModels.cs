namespace BasaltCore;

public sealed class BasaltExecutionInfo
{
    public BasaltExecutionInfo(ulong executionId, ulong jobDefinitionId, ulong scheduleId, ulong workflowId, uint state, long createdAt, long eligibleAt, long startedAt, long finishedAt, int priority, uint attempt, uint maxAttempts, ulong revision, long leaseExpiresAt, ulong fencingToken)
    { ExecutionId=executionId; JobDefinitionId=jobDefinitionId; ScheduleId=scheduleId; WorkflowId=workflowId; State=state; CreatedAt=createdAt; EligibleAt=eligibleAt; StartedAt=startedAt; FinishedAt=finishedAt; Priority=priority; Attempt=attempt; MaxAttempts=maxAttempts; Revision=revision; LeaseExpiresAt=leaseExpiresAt; FencingToken=fencingToken; }
    public ulong ExecutionId { get; } public ulong JobDefinitionId { get; } public ulong ScheduleId { get; } public ulong WorkflowId { get; } public uint State { get; }
    public long CreatedAt { get; } public long EligibleAt { get; } public long StartedAt { get; } public long FinishedAt { get; } public int Priority { get; }
    public uint Attempt { get; } public uint MaxAttempts { get; } public ulong Revision { get; } public long LeaseExpiresAt { get; } public ulong FencingToken { get; }
}

public sealed class BasaltStats
{
    public BasaltStats(ulong submittedTotal, ulong startedTotal, ulong completedTotal, ulong failedTotal, ulong retriedTotal, ulong cancelledTotal, ulong deadTotal, ulong recoveredTotal)
    { SubmittedTotal=submittedTotal; StartedTotal=startedTotal; CompletedTotal=completedTotal; FailedTotal=failedTotal; RetriedTotal=retriedTotal; CancelledTotal=cancelledTotal; DeadTotal=deadTotal; RecoveredTotal=recoveredTotal; }
    public ulong SubmittedTotal { get; } public ulong StartedTotal { get; } public ulong CompletedTotal { get; } public ulong FailedTotal { get; }
    public ulong RetriedTotal { get; } public ulong CancelledTotal { get; } public ulong DeadTotal { get; } public ulong RecoveredTotal { get; }
}

public sealed class BasaltLedgerEntry
{
    public ulong ExecutionId { get; set; }
    public ulong JobDefinitionId { get; set; }
    public ulong ScheduleId { get; set; }
    public ulong WorkflowId { get; set; }
    public long StartedAt { get; set; }
    public long FinishedAt { get; set; }
    public long Duration { get; set; }
    public uint Attempt { get; set; }
    public uint FinalState { get; set; }
    public int ResultCode { get; set; }
    public int ErrorCode { get; set; }
}

public sealed class BasaltScheduleInfo
{
    public ulong ScheduleId { get; set; } public ulong JobDefinitionId { get; set; } public uint ScheduleType { get; set; } public bool Enabled { get; set; } public ulong TimezoneReference { get; set; } public ulong Interval { get; set; } public uint IntervalMode { get; set; } public uint CatchUpMax { get; set; }
    public long StartAt { get; set; } public long EndAt { get; set; } public long LastFireAt { get; set; } public long NextFireAt { get; set; } public ulong OccurrenceCount { get; set; } public ulong MaxOccurrences { get; set; } public ulong Revision { get; set; }
    public uint MisfirePolicy { get; set; } public uint OverlapPolicy { get; set; }
}
