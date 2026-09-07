using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using BasaltCore.Native;

namespace BasaltCore;

public sealed class BasaltEngine : IDisposable
{
    private readonly BasaltDatabase _database;
    private readonly IntPtr _core;
    private readonly ConcurrentDictionary<ulong, NativeMethods.JobHandler> _handlers = new();
    private int _disposed;

    public BasaltEngine(BasaltDatabase database, uint workers = 1, long leaseDuration = 30)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        BasaltDatabase.Check(NativeMethods.basalt_core_create(database.Handle, workers, leaseDuration, out _core));
    }

    public BasaltEngine(BasaltDatabase database, BasaltOptions options)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        if (options == null) throw new ArgumentNullException(nameof(options));
        BasaltDatabase.Check(NativeMethods.basalt_core_create_ex(database.Handle, options.WorkerCount, options.LeaseDurationSeconds, options.StopGraceMilliseconds, out _core));
    }

    public void RegisterRawHandler(ulong jobType, Func<ReadOnlyMemory<byte>, uint, int> handler)
    {
        RegisterNativeHandler(jobType, (payload, version, context) => handler(payload, version));
    }

    private void RegisterNativeHandler(ulong jobType, Func<ReadOnlyMemory<byte>, uint, NativeMethods.ExecutionContext, int> handler)
    {
        if (jobType == 0 || handler == null) throw new ArgumentException("A job type and handler are required.");
        NativeMethods.JobHandler callback = (payload, size, version, context, data) =>
        {
            try
            {
                int length = checked((int)size.ToUInt64()); byte[] bytes = new byte[length];
                if (length != 0) Marshal.Copy(payload, bytes, 0, length);
                NativeMethods.ExecutionContext execution = context == IntPtr.Zero ? default : Marshal.PtrToStructure<NativeMethods.ExecutionContext>(context);
                return handler(bytes, version, execution);
            }
            catch { return -1; }
        };
        BasaltDatabase.Check(NativeMethods.basalt_core_register_handler(_core, jobType, callback, IntPtr.Zero));
        _handlers[jobType] = callback;
    }

    public void RegisterHandler<T>(string stableKey, Func<T, JobContext, CancellationToken, Task> handler, IJobSerializer<T>? serializer = null)
    {
        if (handler == null) throw new ArgumentNullException(nameof(handler)); serializer ??= new DataContractJobSerializer<T>();
        ulong type = JobKey.Hash(stableKey);
        RegisterNativeHandler(type, (payload, version, nativeContext) =>
        {
            try
            {
                T value = serializer.Deserialize(payload, version);
                var context = new JobContext(nativeContext.ExecutionId, stableKey, version, nativeContext.Attempt, nativeContext.FencingToken, CancellationToken.None);
                handler(value, context, context.CancellationToken).GetAwaiter().GetResult(); return 0;
            }
            catch { return -1; }
        });
    }

    public Task<ulong> EnqueueAsync<T>(string stableKey, T value, IJobSerializer<T>? serializer = null, uint maxAttempts = 1, CancellationToken cancellationToken = default)
    {
        if (value == null) throw new ArgumentNullException(nameof(value)); serializer ??= new DataContractJobSerializer<T>();
        return EnqueueAsync(JobKey.Hash(stableKey), serializer.Serialize(value), serializer.Version, maxAttempts, cancellationToken);
    }

    public void Start() { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_core_start(_core)); }
    public void Stop() { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_core_stop(_core)); }
    public Task StartAsync(CancellationToken cancellationToken = default) { cancellationToken.ThrowIfCancellationRequested(); return Task.Run(Start, cancellationToken); }
    public Task StopAsync(CancellationToken cancellationToken = default) { cancellationToken.ThrowIfCancellationRequested(); return Task.Run(Stop, cancellationToken); }
    public Task<ulong> EnqueueAsync(ulong jobType, ReadOnlyMemory<byte> payload, uint payloadVersion = 1, uint maxAttempts = 1, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed(); cancellationToken.ThrowIfCancellationRequested(); byte[] bytes = payload.ToArray();
        return Task.Run(() => { cancellationToken.ThrowIfCancellationRequested(); BasaltDatabase.Check(NativeMethods.basalt_core_enqueue(_core, jobType, bytes, (uint)bytes.Length, payloadVersion, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), maxAttempts, out ulong id)); return id; }, cancellationToken);
    }

    public Task<ulong> EnqueueAsync(string idempotencyKey, ulong jobType, ReadOnlyMemory<byte> payload, uint payloadVersion = 1, uint maxAttempts = 1, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(idempotencyKey)) throw new ArgumentException("An idempotency key is required.", nameof(idempotencyKey));
        ThrowIfDisposed(); cancellationToken.ThrowIfCancellationRequested(); byte[] bytes = payload.ToArray();
        return Task.Run(() => { cancellationToken.ThrowIfCancellationRequested(); BasaltDatabase.Check(NativeMethods.basalt_core_enqueue_idempotent(_core, idempotencyKey, jobType, bytes, (uint)bytes.Length, payloadVersion, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), maxAttempts, out ulong id)); return id; }, cancellationToken);
    }
    public Task<ulong> EnqueueRetryAsync(ulong jobType, ReadOnlyMemory<byte> payload, uint policy, uint maxAttempts, long initialDelaySeconds, long maxDelaySeconds = 0, double backoffFactor = 2, uint jitterSeconds = 0, uint payloadVersion = 1, CancellationToken cancellationToken = default)
    { ThrowIfDisposed(); cancellationToken.ThrowIfCancellationRequested(); var bytes=payload.ToArray(); return Task.Run(()=>{var retry=new NativeMethods.RetrySpec{Policy=policy,MaxAttempts=maxAttempts,InitialDelay=initialDelaySeconds,MaxDelay=maxDelaySeconds,BackoffFactor=backoffFactor,Jitter=jitterSeconds}; BasaltDatabase.Check(NativeMethods.basalt_core_enqueue_retry(_core,jobType,bytes,(uint)bytes.Length,payloadVersion,DateTimeOffset.UtcNow.ToUnixTimeSeconds(),ref retry,out var id)); return id;},cancellationToken); }

    public BasaltExecutionInfo GetExecution(ulong executionId)
    {
        ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_execution_get(_database.Handle, executionId, out var value));
        return new BasaltExecutionInfo(value.ExecutionId, value.JobDefinitionId, value.ScheduleId, value.WorkflowId, value.State, value.CreatedAt, value.EligibleAt, value.StartedAt, value.FinishedAt, value.Priority, value.Attempt, value.MaxAttempts, value.Revision, value.LeaseExpiresAt, value.FencingToken);
    }

    public IReadOnlyList<ulong> ListExecutionIds()
    {
        ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_execution_list(_database.Handle, null, UIntPtr.Zero, out var needed));
        if (needed.ToUInt64() > int.MaxValue) throw new BasaltException(JobDbResult.Limit);
        var ids = new ulong[(int)needed.ToUInt64()]; BasaltDatabase.Check(NativeMethods.basalt_execution_list(_database.Handle, ids, (UIntPtr)ids.Length, out var count));
        if ((ulong)ids.Length != count.ToUInt64()) Array.Resize(ref ids, (int)count.ToUInt64()); return ids;
    }

    public void Cancel(ulong executionId, ulong expectedRevision) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_execution_cancel(_database.Handle, executionId, expectedRevision)); }
    public void Requeue(ulong executionId, ulong expectedRevision) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_execution_requeue(_database.Handle, executionId, expectedRevision)); }
    public BasaltStats GetStats() { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_stats_get(_database.Handle, out var s)); return new BasaltStats(s.SubmittedTotal, s.StartedTotal, s.CompletedTotal, s.FailedTotal, s.RetriedTotal, s.CancelledTotal, s.DeadTotal, s.RecoveredTotal); }
    public BasaltLedgerEntry GetLedger(ulong executionId)
    {
        ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_ledger_get(_database.Handle, executionId, out var e));
        return new BasaltLedgerEntry { ExecutionId=e.ExecutionId, JobDefinitionId=e.JobDefinitionId, ScheduleId=e.ScheduleId, WorkflowId=e.WorkflowId, StartedAt=e.StartedAt, FinishedAt=e.FinishedAt, Duration=e.Duration, Attempt=e.Attempt, FinalState=e.FinalState, ResultCode=e.ResultCode, ErrorCode=e.ErrorCode };
    }
    public IReadOnlyList<ulong> ListLedgerExecutionIds()
    {
        ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_ledger_list(_database.Handle, null, UIntPtr.Zero, out var needed));
        if (needed.ToUInt64() > int.MaxValue) throw new BasaltException(JobDbResult.Limit);
        var ids = new ulong[(int)needed.ToUInt64()]; BasaltDatabase.Check(NativeMethods.basalt_ledger_list(_database.Handle, ids, (UIntPtr)ids.Length, out var count));
        if ((ulong)ids.Length != count.ToUInt64()) Array.Resize(ref ids, (int)count.ToUInt64()); return ids;
    }
    public void VerifyHealth() { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_db_health(_database.Handle)); }

    public void CreateSchedule(ulong scheduleId, ulong jobType, uint scheduleType, DateTimeOffset firstFireAt, TimeSpan interval, uint intervalMode, ulong maxOccurrences, ReadOnlyMemory<byte> payload, uint payloadVersion = 1, string? cronExpression = null, string? timezone = null, uint misfirePolicy = 2, uint overlapPolicy = 1, uint catchUpMax = 0)
    { ThrowIfDisposed(); var bytes=payload.ToArray(); BasaltDatabase.Check(NativeMethods.basalt_schedule_create(_core,scheduleId,jobType,scheduleType,firstFireAt.ToUnixTimeSeconds(),checked((long)interval.TotalSeconds),intervalMode,maxOccurrences,bytes,(uint)bytes.Length,payloadVersion,cronExpression,timezone,misfirePolicy,overlapPolicy,catchUpMax)); }
    public BasaltScheduleInfo GetSchedule(ulong scheduleId) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_get(_core,scheduleId,out var s)); return new BasaltScheduleInfo{ScheduleId=s.ScheduleId,JobDefinitionId=s.JobDefinitionId,ScheduleType=s.ScheduleType,Enabled=s.Enabled!=0,TimezoneReference=s.TimezoneReference,StartAt=s.StartAt,EndAt=s.EndAt,LastFireAt=s.LastFireAt,NextFireAt=s.NextFireAt,OccurrenceCount=s.OccurrenceCount,MaxOccurrences=s.MaxOccurrences,Revision=s.Revision,MisfirePolicy=s.MisfirePolicy,OverlapPolicy=s.OverlapPolicy}; }
    public void UpdateSchedule(BasaltScheduleInfo value, ulong expectedRevision) { if (value == null) throw new ArgumentNullException(nameof(value)); ThrowIfDisposed(); var s=new NativeMethods.ScheduleView{ScheduleId=value.ScheduleId,JobDefinitionId=value.JobDefinitionId,ScheduleType=value.ScheduleType,Enabled=value.Enabled?1u:0u,TimezoneReference=value.TimezoneReference,StartAt=value.StartAt,EndAt=value.EndAt,LastFireAt=value.LastFireAt,NextFireAt=value.NextFireAt,OccurrenceCount=value.OccurrenceCount,MaxOccurrences=value.MaxOccurrences,Revision=value.Revision,MisfirePolicy=value.MisfirePolicy,OverlapPolicy=value.OverlapPolicy}; BasaltDatabase.Check(NativeMethods.basalt_schedule_update(_core,ref s,expectedRevision)); }
     public IReadOnlyList<ulong> ListScheduleIds()
     {
         ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_list(_core, null, UIntPtr.Zero, out var needed));
         if (needed.ToUInt64() > int.MaxValue) throw new BasaltException(JobDbResult.Limit);
         var ids = new ulong[(int)needed.ToUInt64()]; BasaltDatabase.Check(NativeMethods.basalt_schedule_list(_core, ids, (UIntPtr)ids.Length, out var count));
         if ((ulong)ids.Length != count.ToUInt64()) Array.Resize(ref ids, (int)count.ToUInt64()); return ids;
     }
     public void PauseSchedule(ulong scheduleId, ulong expectedRevision) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_pause(_core,scheduleId,expectedRevision)); }
    public void ResumeSchedule(ulong scheduleId, ulong expectedRevision) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_resume(_core,scheduleId,expectedRevision)); }
    public void RemoveSchedule(ulong scheduleId, ulong expectedRevision) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_remove(_core,scheduleId,expectedRevision)); }
    public void SubmitWorkflow(ulong workflowId, IReadOnlyList<BasaltWorkflowNode> nodes, BasaltDependencyPolicy policy = BasaltDependencyPolicy.Block)
    { if(nodes==null||nodes.Count==0)throw new ArgumentException("Workflow nodes are required.",nameof(nodes)); ThrowIfDisposed(); var native=new NativeMethods.WorkflowNode[nodes.Count]; var allocations=new List<IntPtr>(); try { for(int i=0;i<nodes.Count;i++){var n=nodes[i];if(n==null||n.Dependencies.Count>8)throw new ArgumentException("Invalid workflow node.",nameof(nodes));var bytes=n.Payload??new byte[0];var ptr=Marshal.AllocHGlobal(bytes.Length==0?1:bytes.Length);allocations.Add(ptr);if(bytes.Length>0)Marshal.Copy(bytes,0,ptr,bytes.Length);native[i]=new NativeMethods.WorkflowNode{NodeId=n.NodeId,JobType=n.JobType,Payload=ptr,PayloadSize=(uint)bytes.Length,PayloadVersion=n.PayloadVersion,DependencyCount=(uint)n.Dependencies.Count,Dependencies=n.Dependencies.ToArray()};} BasaltDatabase.Check(NativeMethods.basalt_core_workflow_submit(_core,workflowId,native,(uint)native.Length,(uint)policy,DateTimeOffset.UtcNow.ToUnixTimeSeconds())); } finally {foreach(var p in allocations)Marshal.FreeHGlobal(p);} }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        NativeMethods.basalt_core_stop(_core); NativeMethods.basalt_core_destroy(_core); _handlers.Clear(); GC.SuppressFinalize(this);
    }
    public BasaltWorkflowStatus GetWorkflow(ulong workflowId) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_workflow_get(_core,workflowId,out var s)); return new BasaltWorkflowStatus{WorkflowId=s.WorkflowId,NodeCount=s.NodeCount,ReadyCount=s.ReadyCount,BlockedCount=s.BlockedCount,RunningCount=s.RunningCount,TerminalCount=s.TerminalCount,FailedCount=s.FailedCount,CancelledCount=s.CancelledCount,CancelRequested=s.CancelRequested!=0}; }
    public void CancelWorkflow(ulong workflowId) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_workflow_cancel(_core,workflowId)); }
    private void ThrowIfDisposed() { if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(BasaltEngine)); }
}
