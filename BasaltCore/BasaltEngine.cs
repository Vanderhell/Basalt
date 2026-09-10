using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text;
using BasaltCore.Native;

namespace BasaltCore;

public sealed class BasaltEngine : IDisposable
{
    private readonly BasaltDatabase? _database;
    private readonly BasaltStorage? _storage;
    private readonly BasaltCoreHandle _core;
    private readonly ConcurrentDictionary<ulong, NativeMethods.JobHandler> _handlers = new();
    private readonly object _lifecycle = new();
    private readonly bool _databaseRetained;
    private readonly bool _storageRetained;
    private int _disposed;

    public BasaltEngine(BasaltDatabase database, uint workers = 1, long leaseDuration = 30)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        database.Retain(out _databaseRetained);
        try { BasaltDatabase.Check(NativeMethods.basalt_core_create(database.Handle, workers, leaseDuration, out var core)); _core = new BasaltCoreHandle(core); }
        catch { if (_databaseRetained) database.Release(); throw; }
    }

    public BasaltEngine(BasaltDatabase database, BasaltOptions options)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        if (options == null) throw new ArgumentNullException(nameof(options));
        database.Retain(out _databaseRetained);
        try { BasaltDatabase.Check(NativeMethods.basalt_core_create_ex(database.Handle, options.WorkerCount, options.LeaseDurationSeconds, options.StopGraceMilliseconds, out var core)); _core = new BasaltCoreHandle(core); }
        catch { if (_databaseRetained) database.Release(); throw; }
    }

    public BasaltEngine(BasaltStorage storage, BasaltOptions? options = null)
    {
        _storage=storage??throw new ArgumentNullException(nameof(storage));options??=new BasaltOptions();
        storage.RetainManaged();_storageRetained=true;
        try{BasaltDatabase.Check(NativeMethods.basalt_core_create_storage_v1(storage.DangerousHandle,options.WorkerCount,options.LeaseDurationSeconds,options.StopGraceMilliseconds,out var core));_core=new BasaltCoreHandle(core);}
        catch{storage.ReleaseManaged();throw;}
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
        lock (_lifecycle) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_core_register_handler(_core.DangerousGetHandle(), jobType, callback, IntPtr.Zero)); _handlers[jobType] = callback; }
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
                using var executionCancellation = new CancellationTokenSource();
                using var drained = new ManualResetEvent(false);
                using var timer = nativeContext.CancellationRequested == IntPtr.Zero ? null : new Timer(_ =>
                {
                    if (Marshal.ReadInt32(nativeContext.CancellationRequested) != 0) executionCancellation.Cancel();
                }, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(10));
                var context = new JobContext(nativeContext.ExecutionId, stableKey, version, nativeContext.Attempt, nativeContext.FencingToken, executionCancellation.Token);
                try { handler(value, context, context.CancellationToken).GetAwaiter().GetResult(); return 0; }
                finally { if (timer != null) timer.Dispose(drained); else drained.Set(); drained.WaitOne(); }
            }
            catch { return -1; }
        });
    }

    public Task<ulong> EnqueueAsync<T>(string stableKey, T value, IJobSerializer<T>? serializer = null, uint maxAttempts = 1, CancellationToken cancellationToken = default)
    {
        if (value == null) throw new ArgumentNullException(nameof(value)); serializer ??= new DataContractJobSerializer<T>();
        return EnqueueAsync(JobKey.Hash(stableKey), serializer.Serialize(value), serializer.Version, maxAttempts, cancellationToken);
    }

    public void Start() { lock (_lifecycle) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_core_start(_core.DangerousGetHandle())); } }
    public void Stop() { lock (_lifecycle) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_core_stop(_core.DangerousGetHandle())); } }
    public Task StartAsync(CancellationToken cancellationToken = default) { cancellationToken.ThrowIfCancellationRequested(); Start(); return Task.CompletedTask; }
    public Task StopAsync(CancellationToken cancellationToken = default) { cancellationToken.ThrowIfCancellationRequested(); Stop(); return Task.CompletedTask; }
    public Task<ulong> EnqueueAsync(ulong jobType, ReadOnlyMemory<byte> payload, uint payloadVersion = 1, uint maxAttempts = 1, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested(); byte[] bytes = payload.ToArray();
        lock (_lifecycle) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_core_enqueue(_core.DangerousGetHandle(), jobType, bytes, (uint)bytes.Length, payloadVersion, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), maxAttempts, out ulong id)); return Task.FromResult(id); }
    }

    public Task<ulong> EnqueueAsync(string idempotencyKey, ulong jobType, ReadOnlyMemory<byte> payload, uint payloadVersion = 1, uint maxAttempts = 1, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(idempotencyKey)) throw new ArgumentException("An idempotency key is required.", nameof(idempotencyKey));
        cancellationToken.ThrowIfCancellationRequested(); byte[] bytes = payload.ToArray();
        lock (_lifecycle) { ThrowIfDisposed(); JobDbResult result=NativeMethods.basalt_core_enqueue_idempotent(_core.DangerousGetHandle(), idempotencyKey, jobType, bytes, (uint)bytes.Length, payloadVersion, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), maxAttempts, out ulong id); if(result==JobDbResult.UnknownCommit)throw new BasaltUnknownCommitException(idempotencyKey,id); BasaltDatabase.Check(result); return Task.FromResult(id); }
    }
    public Task<ulong> EnqueueRetryAsync(ulong jobType, ReadOnlyMemory<byte> payload, uint policy, uint maxAttempts, long initialDelaySeconds, long maxDelaySeconds = 0, double backoffFactor = 2, uint jitterSeconds = 0, uint payloadVersion = 1, CancellationToken cancellationToken = default)
    { cancellationToken.ThrowIfCancellationRequested(); var bytes=payload.ToArray(); lock (_lifecycle) { ThrowIfDisposed(); var retry=new NativeMethods.RetrySpec{Policy=policy,MaxAttempts=maxAttempts,InitialDelay=initialDelaySeconds,MaxDelay=maxDelaySeconds,BackoffFactor=backoffFactor,Jitter=jitterSeconds}; BasaltDatabase.Check(NativeMethods.basalt_core_enqueue_retry(_core.DangerousGetHandle(),jobType,bytes,(uint)bytes.Length,payloadVersion,DateTimeOffset.UtcNow.ToUnixTimeSeconds(),ref retry,out var id)); return Task.FromResult(id); } }
    public Task<ulong> EnqueueIdempotentRetryAsync(string idempotencyKey,ulong jobType,ReadOnlyMemory<byte> payload,uint policy,uint maxAttempts,long initialDelaySeconds,long maxDelaySeconds=0,double backoffFactor=2,uint jitterSeconds=0,uint payloadVersion=1,CancellationToken cancellationToken=default)
    {if(string.IsNullOrWhiteSpace(idempotencyKey))throw new ArgumentException("An idempotency key is required.",nameof(idempotencyKey));cancellationToken.ThrowIfCancellationRequested();var bytes=payload.ToArray();lock(_lifecycle){ThrowIfDisposed();var retry=new NativeMethods.RetrySpec{Policy=policy,MaxAttempts=maxAttempts,InitialDelay=initialDelaySeconds,MaxDelay=maxDelaySeconds,BackoffFactor=backoffFactor,Jitter=jitterSeconds};var result=NativeMethods.basalt_core_enqueue_idempotent_retry(_core.DangerousGetHandle(),idempotencyKey,jobType,bytes,(uint)bytes.Length,payloadVersion,DateTimeOffset.UtcNow.ToUnixTimeSeconds(),ref retry,out var id);if(result==JobDbResult.UnknownCommit)throw new BasaltUnknownCommitException(idempotencyKey,id);BasaltDatabase.Check(result);return Task.FromResult(id);}}

    public BasaltExecutionInfo GetExecution(ulong executionId)
    {
        ThrowIfDisposed(); if(_database==null)return _storage!.GetExecution(executionId); BasaltDatabase.Check(NativeMethods.basalt_execution_get(_database.Handle, executionId, out var value));
        return new BasaltExecutionInfo(value.ExecutionId, value.JobDefinitionId, value.ScheduleId, value.WorkflowId, value.State, value.CreatedAt, value.EligibleAt, value.StartedAt, value.FinishedAt, value.Priority, value.Attempt, value.MaxAttempts, value.Revision, value.LeaseExpiresAt, value.FencingToken, value.WorkerInstanceId);
    }

    public IReadOnlyList<ulong> ListExecutionIds()
    {
        ThrowIfDisposed(); if(_database==null)throw new NotSupportedException("Use the storage-neutral management API with this provider."); BasaltDatabase.Check(NativeMethods.basalt_execution_list(_database.Handle, null, UIntPtr.Zero, out var needed));
        if (needed.ToUInt64() > int.MaxValue) throw new BasaltException(JobDbResult.Limit);
        var ids = new ulong[(int)needed.ToUInt64()]; BasaltDatabase.Check(NativeMethods.basalt_execution_list(_database.Handle, ids, (UIntPtr)ids.Length, out var count));
        if ((ulong)ids.Length != count.ToUInt64()) Array.Resize(ref ids, (int)count.ToUInt64()); return ids;
    }
    public IReadOnlyList<BasaltExecutionInfo> ListExecutions(int take=100,ulong afterExecutionId=0)
    {
        if(take<1||take>1000)throw new ArgumentOutOfRangeException(nameof(take));ThrowIfDisposed();if(_database==null)return _storage!.ListExecutions(take,afterExecutionId);
        var result=new List<BasaltExecutionInfo>(take);foreach(ulong id in ListExecutionIds()){if(id<=afterExecutionId)continue;try{result.Add(GetExecution(id));}catch(BasaltException error) when(error.Result==JobDbResult.NotFound){continue;}if(result.Count==take)break;}return result;
    }

    /// <summary>Returns a bounded management page filtered by durable execution metadata.</summary>
    public IReadOnlyList<BasaltExecutionInfo> ListExecutions(ExecutionQuery query)
    {
        if (query == null) throw new ArgumentNullException(nameof(query));
        if (query.Take < 1 || query.Take > 1000) throw new ArgumentOutOfRangeException(nameof(query.Take));
        if (_database == null) return _storage!.ListExecutions(query);
        var result = new List<BasaltExecutionInfo>(query.Take);
        foreach (ulong id in ListExecutionIds().Where(x => x > query.AfterExecutionId).OrderBy(x => x))
        {
            try
            {
                var value = GetExecution(id);
                if (query.Matches(value)) result.Add(value);
            }
            catch (BasaltException error) when (error.Result == JobDbResult.NotFound) { }
            if (result.Count == query.Take) break;
        }
        return result;
    }

    /// <summary>Gets current execution counts by state without exposing storage internals.</summary>
    public BasaltQueueStats GetQueueStats()
    {
        if (_database == null) return _storage!.GetQueueStats();
        var result = new BasaltQueueStats();
        foreach (var execution in ListAllExecutions()) result.Add(execution.State);
        return result;
    }

    /// <summary>Returns worker diagnostics inferred from active leases. Leases and fencing remain the correctness mechanism.</summary>
    public IReadOnlyList<BasaltWorkerInfo> ListWorkers()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        return ListAllExecutions().Where(x => (x.State == ExecutionState.Leased || x.State == ExecutionState.Running) && x.WorkerInstanceId != null)
            .GroupBy(x => x.WorkerInstanceId!)
            .Select(group => new BasaltWorkerInfo
            {
                WorkerId = group.Key,
                ActiveExecutionCount = group.Count(),
                LeaseExpiresAt = group.Max(x => x.LeaseExpiresAt),
                Status = group.Any(x => x.LeaseExpiresAt.HasValue && x.LeaseExpiresAt.Value <= now) ? BasaltWorkerStatus.Stale : BasaltWorkerStatus.Alive
            }).OrderBy(x => x.WorkerId, StringComparer.Ordinal).ToArray();
    }

    /// <summary>Returns a non-destructive provider, queue, and lease diagnostic snapshot.</summary>
    public BasaltHealth GetHealth()
    {
        var checkedAt = DateTimeOffset.UtcNow;
        try
        {
            VerifyHealth();
            var queue = GetQueueStats(); var workers = ListWorkers();
            bool stale = workers.Any(x => x.Status == BasaltWorkerStatus.Stale);
            bool anomalous = queue.Failed != 0 || queue.Dead != 0 || stale;
            return new BasaltHealth(anomalous ? BasaltHealthStatus.Degraded : BasaltHealthStatus.Healthy,
                _database == null ? _storage!.ProviderName : "Embedded", checkedAt, queue, workers,
                anomalous ? "The queue contains failed, dead, or stale leased work." : null);
        }
        catch (Exception error)
        {
            return new BasaltHealth(BasaltHealthStatus.Unhealthy, _database == null ? _storage!.ProviderName : "Embedded",
                checkedAt, new BasaltQueueStats(), Array.Empty<BasaltWorkerInfo>(), error.Message);
        }
    }

    private IReadOnlyList<BasaltExecutionInfo> ListAllExecutions()
    {
        ThrowIfDisposed();
        if (_database != null)
        {
            var result = new List<BasaltExecutionInfo>();
            foreach (ulong id in ListExecutionIds())
                try { result.Add(GetExecution(id)); } catch (BasaltException error) when (error.Result == JobDbResult.NotFound) { }
            return result.OrderBy(x => x.ExecutionId).ToArray();
        }
        var values = new List<BasaltExecutionInfo>(); ulong after = 0;
        while (true)
        {
            var page = _storage!.ListExecutions(1000, after);
            if (page.Count == 0) break;
            values.AddRange(page); ulong last = page[page.Count - 1].ExecutionId;
            if (last <= after || page.Count < 1000) break;
            after = last;
        }
        return values.OrderBy(x => x.ExecutionId).ToArray();
    }

    public void Cancel(ulong executionId, ulong expectedRevision) { ThrowIfDisposed(); if(_database==null){_storage!.Cancel(executionId,expectedRevision);return;} BasaltDatabase.Check(NativeMethods.basalt_execution_cancel(_database.Handle, executionId, expectedRevision)); }
    public void Cancel(ulong executionId){var current=GetExecution(executionId);Cancel(executionId,current.Revision);}
    public void Requeue(ulong executionId, ulong expectedRevision) { ThrowIfDisposed(); if(_database==null){_storage!.Requeue(executionId,expectedRevision);return;} BasaltDatabase.Check(NativeMethods.basalt_execution_requeue(_database.Handle, executionId, expectedRevision)); }
    public void Requeue(ulong executionId){var current=GetExecution(executionId);Requeue(executionId,current.Revision);}
    public BasaltStats GetStats() { ThrowIfDisposed(); if(_database==null)return _storage!.GetStats(); BasaltDatabase.Check(NativeMethods.basalt_stats_get(_database.Handle, out var s)); return new BasaltStats(s.SubmittedTotal, s.StartedTotal, s.CompletedTotal, s.FailedTotal, s.RetriedTotal, s.CancelledTotal, s.DeadTotal, s.RecoveredTotal); }
    public BasaltLedgerEntry GetLedger(ulong executionId)
    {
        ThrowIfDisposed(); if(_database==null)return _storage!.GetLedger(executionId); BasaltDatabase.Check(NativeMethods.basalt_ledger_get(_database.Handle, executionId, out var e));
        return new BasaltLedgerEntry { ExecutionId=e.ExecutionId, JobDefinitionId=e.JobDefinitionId, ScheduleId=e.ScheduleId, WorkflowId=e.WorkflowId, StartedAt=UnixTime.Optional(e.StartedAt), FinishedAt=UnixTime.Optional(e.FinishedAt), Duration=TimeSpan.FromSeconds(e.Duration), Attempt=e.Attempt, FinalState=(ExecutionState)e.FinalState, ResultCode=e.ResultCode, ErrorCode=e.ErrorCode };
    }
    public IReadOnlyList<ulong> ListLedgerExecutionIds()
    {
        ThrowIfDisposed(); if(_database==null)throw new NotSupportedException("Use paginated storage management for this provider."); BasaltDatabase.Check(NativeMethods.basalt_ledger_list(_database.Handle, null, UIntPtr.Zero, out var needed));
        if (needed.ToUInt64() > int.MaxValue) throw new BasaltException(JobDbResult.Limit);
        var ids = new ulong[(int)needed.ToUInt64()]; BasaltDatabase.Check(NativeMethods.basalt_ledger_list(_database.Handle, ids, (UIntPtr)ids.Length, out var count));
        if ((ulong)ids.Length != count.ToUInt64()) Array.Resize(ref ids, (int)count.ToUInt64()); return ids;
    }
    public void VerifyHealth() { ThrowIfDisposed(); if(_database==null){_storage!.VerifyHealth();return;} BasaltDatabase.Check(NativeMethods.basalt_db_health(_database.Handle)); }

    internal void StoreManagementText(uint recordType, ulong recordId, string value)
    {
        ThrowIfDisposed(); if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("A management value is required.", nameof(value));
        byte[] bytes = Encoding.UTF8.GetBytes(value); JobDbResult result = NativeMethods.basalt_management_record_create(_core.DangerousGetHandle(), recordType, recordId, bytes, (uint)bytes.Length);
        if (result == JobDbResult.AlreadyExists)
        {
            string existing = GetManagementText(recordType, recordId);
            if (StringComparer.Ordinal.Equals(existing, value)) return;
            throw new BasaltException(JobDbResult.Conflict);
        }
        BasaltDatabase.Check(result);
    }

    internal IReadOnlyDictionary<ulong, string> ListManagementText(uint recordType)
    {
        ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_management_record_list(_core.DangerousGetHandle(), recordType, null, UIntPtr.Zero, out var needed));
        if (needed.ToUInt64() > int.MaxValue) throw new BasaltException(JobDbResult.Limit);
        var ids = new ulong[(int)needed.ToUInt64()]; BasaltDatabase.Check(NativeMethods.basalt_management_record_list(_core.DangerousGetHandle(), recordType, ids, (UIntPtr)ids.Length, out var count));
        var values = new Dictionary<ulong, string>();
        for (int i = 0; i < (int)Math.Min((ulong)ids.Length, count.ToUInt64()); i++)
            try { values[ids[i]] = GetManagementText(recordType, ids[i]); } catch (BasaltException error) when (error.Result == JobDbResult.NotFound) { }
        return values;
    }

    private string GetManagementText(uint recordType, ulong recordId)
    {
        JobDbResult result = NativeMethods.basalt_management_record_get(_core.DangerousGetHandle(), recordType, recordId, null, 0, out uint size);
        if (result != JobDbResult.Limit && result != JobDbResult.Ok) { BasaltDatabase.Check(result); }
        byte[] bytes = new byte[size]; BasaltDatabase.Check(NativeMethods.basalt_management_record_get(_core.DangerousGetHandle(), recordType, recordId, bytes, size, out uint actual));
        return Encoding.UTF8.GetString(bytes, 0, checked((int)actual));
    }

    public void CreateSchedule(ulong scheduleId, ulong jobType, uint scheduleType, DateTimeOffset firstFireAt, TimeSpan interval, uint intervalMode, ulong maxOccurrences, ReadOnlyMemory<byte> payload, uint payloadVersion = 1, string? cronExpression = null, string? timezone = null, uint misfirePolicy = 2, uint overlapPolicy = 1, uint catchUpMax = 0, DateTimeOffset? endAt=null)
    { ThrowIfDisposed(); var bytes=payload.ToArray(); BasaltDatabase.Check(NativeMethods.basalt_schedule_create_v2(_core.DangerousGetHandle(),scheduleId,jobType,scheduleType,firstFireAt.ToUnixTimeSeconds(),endAt?.ToUnixTimeSeconds()??0,checked((long)interval.TotalSeconds),intervalMode,maxOccurrences,bytes,(uint)bytes.Length,payloadVersion,cronExpression,timezone,misfirePolicy,overlapPolicy,catchUpMax)); }
    public BasaltScheduleInfo GetSchedule(ulong scheduleId) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_get(_core.DangerousGetHandle(),scheduleId,out var s)); return new BasaltScheduleInfo{ScheduleId=s.ScheduleId,JobDefinitionId=s.JobDefinitionId,Type=(ScheduleType)s.ScheduleType,Enabled=s.Enabled!=0,TimezoneReference=s.TimezoneReference,Interval=TimeSpan.FromSeconds(s.Interval),IntervalMode=(IntervalMode)s.IntervalMode,CatchUpMax=s.CatchUpMax,StartAt=UnixTime.Required(s.StartAt),EndAt=UnixTime.Optional(s.EndAt),LastFireAt=UnixTime.Optional(s.LastFireAt),NextFireAt=UnixTime.Optional(s.NextFireAt),OccurrenceCount=s.OccurrenceCount,MaxOccurrences=s.MaxOccurrences,Revision=s.Revision,MisfirePolicy=(MisfirePolicy)s.MisfirePolicy,OverlapPolicy=(OverlapPolicy)s.OverlapPolicy}; }
    public void UpdateSchedule(BasaltScheduleInfo value, ulong expectedRevision) { if (value == null) throw new ArgumentNullException(nameof(value)); ThrowIfDisposed(); var s=new NativeMethods.ScheduleView{ScheduleId=value.ScheduleId,JobDefinitionId=value.JobDefinitionId,ScheduleType=(uint)value.Type,Enabled=value.Enabled?1u:0u,TimezoneReference=value.TimezoneReference,Interval=checked((ulong)value.Interval.TotalSeconds),IntervalMode=(uint)value.IntervalMode,CatchUpMax=value.CatchUpMax,StartAt=UnixTime.Required(value.StartAt),EndAt=UnixTime.Optional(value.EndAt),LastFireAt=UnixTime.Optional(value.LastFireAt),NextFireAt=UnixTime.Optional(value.NextFireAt),OccurrenceCount=value.OccurrenceCount,MaxOccurrences=value.MaxOccurrences,Revision=value.Revision,MisfirePolicy=(uint)value.MisfirePolicy,OverlapPolicy=(uint)value.OverlapPolicy}; BasaltDatabase.Check(NativeMethods.basalt_schedule_update(_core.DangerousGetHandle(),ref s,expectedRevision)); }
     public IReadOnlyList<ulong> ListScheduleIds()
     {
         ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_list(_core.DangerousGetHandle(), null, UIntPtr.Zero, out var needed));
         if (needed.ToUInt64() > int.MaxValue) throw new BasaltException(JobDbResult.Limit);
         var ids = new ulong[(int)needed.ToUInt64()]; BasaltDatabase.Check(NativeMethods.basalt_schedule_list(_core.DangerousGetHandle(), ids, (UIntPtr)ids.Length, out var count));
         if ((ulong)ids.Length != count.ToUInt64()) Array.Resize(ref ids, (int)count.ToUInt64()); return ids;
     }
    public IReadOnlyList<BasaltScheduleInfo> ListSchedules(int take = 100, ulong afterScheduleId = 0)
    {
        if (take < 1 || take > 1000) throw new ArgumentOutOfRangeException(nameof(take));
        var result = new List<BasaltScheduleInfo>(take);
        foreach (ulong id in ListScheduleIds().Where(x => x > afterScheduleId).OrderBy(x => x))
        {
            try { result.Add(GetSchedule(id)); } catch (BasaltException error) when (error.Result == JobDbResult.NotFound) { }
            if (result.Count == take) break;
        }
        return result;
    }
     public void PauseSchedule(ulong scheduleId, ulong expectedRevision) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_pause(_core.DangerousGetHandle(),scheduleId,expectedRevision)); }
    public void ResumeSchedule(ulong scheduleId, ulong expectedRevision) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_resume(_core.DangerousGetHandle(),scheduleId,expectedRevision)); }
    public void RemoveSchedule(ulong scheduleId, ulong expectedRevision) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_schedule_remove(_core.DangerousGetHandle(),scheduleId,expectedRevision)); }
    public void PauseSchedule(ulong scheduleId){var current=GetSchedule(scheduleId);PauseSchedule(scheduleId,current.Revision);}public void ResumeSchedule(ulong scheduleId){var current=GetSchedule(scheduleId);ResumeSchedule(scheduleId,current.Revision);}public void RemoveSchedule(ulong scheduleId){var current=GetSchedule(scheduleId);RemoveSchedule(scheduleId,current.Revision);}
    private static ulong[] PadDependencies(IReadOnlyList<ulong> dependencies)
    {
        var result = new ulong[8]; for (var i = 0; i < dependencies.Count; i++) result[i] = dependencies[i]; return result;
    }
    public void SubmitWorkflow(ulong workflowId, IReadOnlyList<BasaltWorkflowNode> nodes, BasaltDependencyPolicy policy = BasaltDependencyPolicy.Block)
    { if(nodes==null||nodes.Count==0)throw new ArgumentException("Workflow nodes are required.",nameof(nodes)); ThrowIfDisposed(); var native=new NativeMethods.WorkflowNode[nodes.Count]; var allocations=new List<IntPtr>(); try { for(int i=0;i<nodes.Count;i++){var n=nodes[i];if(n==null||n.Dependencies.Count>8)throw new ArgumentException("Invalid workflow node.",nameof(nodes));var bytes=n.Payload??new byte[0];var ptr=Marshal.AllocHGlobal(bytes.Length==0?1:bytes.Length);allocations.Add(ptr);if(bytes.Length>0)Marshal.Copy(bytes,0,ptr,bytes.Length);native[i]=new NativeMethods.WorkflowNode{NodeId=n.NodeId,JobType=n.JobType,Payload=ptr,PayloadSize=(uint)bytes.Length,PayloadVersion=n.PayloadVersion,DependencyCount=(uint)n.Dependencies.Count,Dependencies=PadDependencies(n.Dependencies)};} BasaltDatabase.Check(NativeMethods.basalt_core_workflow_submit(_core.DangerousGetHandle(),workflowId,native,(uint)native.Length,(uint)policy,DateTimeOffset.UtcNow.ToUnixTimeSeconds())); } finally {foreach(var p in allocations)Marshal.FreeHGlobal(p);} }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        lock (_lifecycle) { _core.Dispose(); _handlers.Clear(); if (_databaseRetained) _database!.Release(); if(_storageRetained)_storage!.ReleaseManaged(); } GC.SuppressFinalize(this);
    }
    public BasaltWorkflowStatus GetWorkflow(ulong workflowId) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_workflow_get(_core.DangerousGetHandle(),workflowId,out var s)); return new BasaltWorkflowStatus{WorkflowId=s.WorkflowId,NodeCount=s.NodeCount,ReadyCount=s.ReadyCount,BlockedCount=s.BlockedCount,RunningCount=s.RunningCount,TerminalCount=s.TerminalCount,FailedCount=s.FailedCount,CancelledCount=s.CancelledCount,CancelRequested=s.CancelRequested!=0}; }
    public IReadOnlyList<BasaltWorkflowStatus> ListWorkflows(int take = 100, ulong afterWorkflowId = 0)
    {
        if (take < 1 || take > 1000) throw new ArgumentOutOfRangeException(nameof(take));
        var result = new List<BasaltWorkflowStatus>(take);
        foreach (ulong id in ListAllExecutions().Where(x => x.WorkflowId != 0 && x.WorkflowId > afterWorkflowId).Select(x => x.WorkflowId).Distinct().OrderBy(x => x))
        {
            try { result.Add(GetWorkflow(id)); } catch (BasaltException error) when (error.Result == JobDbResult.NotFound) { }
            if (result.Count == take) break;
        }
        return result;
    }
    public void CancelWorkflow(ulong workflowId) { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_workflow_cancel(_core.DangerousGetHandle(),workflowId)); }
    private void ThrowIfDisposed() { if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(BasaltEngine)); }
}
