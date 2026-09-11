using System.Data.Common;
using System.Reflection;
using System.Runtime.ExceptionServices;
using BasaltCore.SqlServer;

namespace BasaltCore;

public static class Basalt
{
    /// <summary>Opens or creates a local durable Basalt store.</summary>
    public static BasaltApplication Embedded(string path) => Create(options => options.UseEmbedded(path));

    /// <summary>Opens SQL Server-backed Basalt storage. Basalt creates and owns its connections.</summary>
    public static BasaltApplication SqlServer(string connectionString, Action<SqlServerOptions>? configure = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("A SQL Server connection string is required.", nameof(connectionString));
        return InvokeSqlProvider("CreateFromConnectionString", connectionString, configure);
    }

    /// <summary>Opens SQL Server-backed storage using a factory that returns a new connection per operation.</summary>
    public static BasaltApplication SqlServer(Func<DbConnection> connectionFactory, Action<SqlServerOptions>? configure = null)
    {
        if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
        return InvokeSqlProvider("CreateFromFactory", connectionFactory, configure);
    }

    public static BasaltApplication Create(Action<BasaltConfiguration> configure)
    {if(configure==null)throw new ArgumentNullException(nameof(configure));var c=new BasaltConfiguration();configure(c);return c.Build();}

    private static BasaltApplication InvokeSqlProvider(string methodName, object firstArgument, Action<SqlServerOptions>? configure)
    {
        try
        {
            Assembly provider = Assembly.Load(new AssemblyName("BasaltCore.SqlServer"));
            Type bootstrap = provider.GetType("BasaltCore.SqlServer.SqlServerBootstrap", throwOnError: true)!;
            MethodInfo method = bootstrap.GetMethod(methodName, BindingFlags.Public | BindingFlags.Static)!;
            return (BasaltApplication)method.Invoke(null, new[] { firstArgument, configure })!;
        }
        catch (TargetInvocationException error) when (error.InnerException != null)
        {
            ExceptionDispatchInfo.Capture(error.InnerException).Throw();
            throw;
        }
        catch (Exception error) when (error is FileNotFoundException || error is TypeLoadException || error is MissingMethodException)
        {
            throw new InvalidOperationException("Reference the Basalt.SqlServer package before calling Basalt.SqlServer(...).", error);
        }
    }
}

public sealed class BasaltConfiguration
{
    private string? _embeddedPath; private Func<BasaltStorage>? _storage; private readonly BasaltOptions _engine=new();
    public BasaltConfiguration UseEmbedded(string path){if(string.IsNullOrWhiteSpace(path))throw new ArgumentException("A database path is required.",nameof(path));EnsureUnset();_embeddedPath=path;return this;}
    public BasaltConfiguration UseStorage(Func<BasaltStorage> storageFactory){EnsureUnset();_storage=storageFactory??throw new ArgumentNullException(nameof(storageFactory));return this;}
    public BasaltConfiguration ConfigureEngine(Action<BasaltOptions> configure){configure?.Invoke(_engine);return this;}
    private void EnsureUnset(){if(_embeddedPath!=null||_storage!=null)throw new InvalidOperationException("Configure exactly one Basalt storage provider.");}
    internal BasaltApplication Build(){if(_embeddedPath!=null){var db=BasaltDatabase.OpenOrCreate(_embeddedPath);try{return new BasaltApplication(new BasaltEngine(db,_engine),db);}catch{db.Dispose();throw;}}if(_storage!=null){var s=_storage();try{return new BasaltApplication(new BasaltEngine(s,_engine),s);}catch{s.Dispose();throw;}}throw new InvalidOperationException("Configure UseEmbedded or a storage provider.");}
}

/// <summary>Provides the storage-neutral typed job, scheduling, workflow, management, and explicit worker lifecycle API.</summary>
public sealed class BasaltApplication : IDisposable
{
    private const uint JobKeyRecordType = 1000, ScheduleKeyRecordType = 1001, WorkflowKeyRecordType = 1002, ScheduleDetailsRecordType = 1004, WorkflowNodeRecordType = 1005;
    internal interface IRegistration{string Key{get;}ulong Type{get;}byte[] Serialize(object value);uint Version{get;}}
    private sealed class Registration<T> : IRegistration{internal readonly IJobSerializer<T> Serializer;public string Key{get;}public ulong Type{get;}public uint Version=>Serializer.Version;internal Registration(string key,IJobSerializer<T>s){Key=key;Type=JobKey.Hash(key);Serializer=s;}public byte[] Serialize(object value)=>Serializer.Serialize((T)value);}
    private readonly BasaltEngine _engine; private readonly IDisposable _ownedStorage; private readonly Dictionary<Type,IRegistration> _registrations=new(); private readonly Dictionary<ulong,string> _jobKeys=new(); private readonly Dictionary<ulong,string> _scheduleKeys=new(); private readonly Dictionary<ulong,string> _workflowKeys=new(); private readonly Dictionary<ulong,(string? Cron,string? Zone)> _scheduleDetails=new(); private readonly Dictionary<ulong,string> _workflowNodes=new(); private int _disposed;
    internal BasaltApplication(BasaltEngine engine,IDisposable ownedStorage){_engine=engine;_ownedStorage=ownedStorage;LoadKeys(JobKeyRecordType,_jobKeys);LoadKeys(ScheduleKeyRecordType,_scheduleKeys);LoadKeys(WorkflowKeyRecordType,_workflowKeys);LoadScheduleDetails();LoadWorkflowNodes();}
    /// <summary>Registers a handler under an explicit stable durable key. The key must remain stable when the CLR type is renamed.</summary>
    public BasaltApplication On<T>(string stableKey,Func<T,JobContext,CancellationToken,Task> handler,IJobSerializer<T>? serializer=null){RegisterHandler(stableKey,handler,serializer);return this;}
    public void RegisterHandler<T>(string stableKey,Func<T,JobContext,CancellationToken,Task> handler,IJobSerializer<T>? serializer=null){ThrowIfDisposed();serializer??=new DataContractJobSerializer<T>();ulong type=JobKey.Hash(stableKey);_engine.StoreManagementText(JobKeyRecordType,type,stableKey);_engine.RegisterHandler(stableKey,handler,serializer);_registrations[typeof(T)]=new Registration<T>(stableKey,serializer);_jobKeys[type]=stableKey;}
    /// <summary>Starts workers that claim and execute registered durable jobs.</summary>
    public Task StartAsync(CancellationToken cancellationToken=default){ThrowIfDisposed();return _engine.StartAsync(cancellationToken);}
    /// <summary>Stops workers using the configured graceful-stop behavior.</summary>
    public Task StopAsync(CancellationToken cancellationToken=default){ThrowIfDisposed();return _engine.StopAsync(cancellationToken);}
    public Task<ulong> EnqueueAsync<T>(string stableKey,T job,EnqueueOptions? options=null,CancellationToken cancellationToken=default){ThrowIfDisposed();options??=new EnqueueOptions();options.Validate();IJobSerializer<T> serializer=_registrations.TryGetValue(typeof(T),out var r)&&r is Registration<T> typed?typed.Serializer:new DataContractJobSerializer<T>();byte[] payload=serializer.Serialize(job!);ulong type=JobKey.Hash(stableKey);if(options.Retry.Policy!=RetryPolicy.None){if(options.IdempotencyKey!=null)return _engine.EnqueueIdempotentRetryAsync(options.IdempotencyKey,type,payload,(uint)options.Retry.Policy,options.Retry.MaxAttempts,(long)options.Retry.InitialDelay.TotalSeconds,(long)options.Retry.MaxDelay.TotalSeconds,options.Retry.BackoffFactor,(uint)options.Retry.Jitter.TotalSeconds,serializer.Version,cancellationToken);return _engine.EnqueueRetryAsync(type,payload,(uint)options.Retry.Policy,options.Retry.MaxAttempts,(long)options.Retry.InitialDelay.TotalSeconds,(long)options.Retry.MaxDelay.TotalSeconds,options.Retry.BackoffFactor,(uint)options.Retry.Jitter.TotalSeconds,serializer.Version,cancellationToken);}if(options.IdempotencyKey!=null)return _engine.EnqueueAsync(options.IdempotencyKey,type,payload,serializer.Version,options.Retry.MaxAttempts,cancellationToken);return _engine.EnqueueAsync(type,payload,serializer.Version,options.Retry.MaxAttempts,cancellationToken);}
    /// <summary>Durably enqueues a job using the stable key registered for <typeparamref name="T"/>.</summary>
    public Task<ulong> EnqueueAsync<T>(T job,CancellationToken cancellationToken=default)=>EnqueueRegisteredAsync(job,new EnqueueOptions(),cancellationToken);
    /// <summary>Durably enqueues a registered job with advanced options.</summary>
    public Task<ulong> EnqueueAsync<T>(T job,EnqueueOptions options,CancellationToken cancellationToken=default)=>EnqueueRegisteredAsync(job,options??throw new ArgumentNullException(nameof(options)),cancellationToken);
    /// <summary>Durably enqueues a registered job with an idempotency key and optional exponential retry attempts.</summary>
    public Task<ulong> EnqueueAsync<T>(T job,string? key,int retry=1,CancellationToken cancellationToken=default)=>EnqueueRegisteredAsync(job,ConvenienceOptions(key,retry),cancellationToken);
    /// <summary>Durably enqueues a registered job with the specified total number of exponential retry attempts.</summary>
    public Task<ulong> EnqueueAsync<T>(T job,int retry,CancellationToken cancellationToken=default)=>EnqueueRegisteredAsync(job,ConvenienceOptions(null,retry),cancellationToken);
    private Task<ulong> EnqueueRegisteredAsync<T>(T job,EnqueueOptions options,CancellationToken cancellationToken){ThrowIfDisposed();if(!_registrations.TryGetValue(typeof(T),out var registration))throw new InvalidOperationException($"Register {typeof(T).Name} with On<{typeof(T).Name}>(\"stable.job.key\", handler) before enqueueing it.");return EnqueueAsync(registration.Key,job,options,cancellationToken);}
    private static EnqueueOptions ConvenienceOptions(string? key,int retry){if(retry<1)throw new ArgumentOutOfRangeException(nameof(retry),"Retry attempts must be at least one.");return new EnqueueOptions{IdempotencyKey=key,Retry=retry==1?RetryOptions.None:RetryOptions.Exponential((uint)retry,TimeSpan.FromSeconds(1),TimeSpan.FromMinutes(1))};}
    /// <summary>Creates a durable schedule using the full fluent schedule configuration.</summary>
    public Task ScheduleAsync<T>(string stableScheduleKey,T job,Action<ScheduleBuilder> configure,CancellationToken cancellationToken=default){ThrowIfDisposed();cancellationToken.ThrowIfCancellationRequested();if(!_registrations.TryGetValue(typeof(T),out var registration))throw new InvalidOperationException($"Register a stable handler and serializer for {typeof(T).Name} before scheduling it.");var b=new ScheduleBuilder();configure?.Invoke(b);b.Validate();byte[] payload=registration.Serialize(job!);ulong scheduleId=JobKey.Hash("schedule:"+stableScheduleKey);_engine.StoreManagementText(ScheduleKeyRecordType,scheduleId,stableScheduleKey);_engine.StoreManagementText(ScheduleDetailsRecordType,scheduleId,Encode(b.CronExpression)+":"+Encode(b.TimeZoneId));_engine.CreateSchedule(scheduleId,registration.Type,(uint)b.Type,b.FirstFireAt,b.Interval,(uint)b.IntervalMode,b.MaxOccurrences,payload,registration.Version,b.CronExpression,b.TimeZoneId,(uint)b.MisfirePolicy,(uint)b.OverlapPolicy,b.CatchUpMax,b.EndAt);_scheduleKeys[scheduleId]=stableScheduleKey;_scheduleDetails[scheduleId]=(b.CronExpression,b.TimeZoneId);return Task.CompletedTask;}
    /// <summary>Creates a durable recurring interval schedule.</summary>
    public Task EveryAsync<T>(string stableScheduleKey,TimeSpan interval,T job,IntervalMode mode=IntervalMode.FixedRate,CancellationToken cancellationToken=default)=>ScheduleAsync(stableScheduleKey,job,s=>s.Every(interval,mode),cancellationToken);
    /// <summary>Creates a durable daily schedule. The default timezone is explicitly UTC.</summary>
    public Task DailyAsync<T>(string stableScheduleKey,int hour,int minute,T job,string timeZoneId="UTC",CancellationToken cancellationToken=default)=>ScheduleAsync(stableScheduleKey,job,s=>s.DailyAt(hour,minute).InTimeZone(timeZoneId),cancellationToken);
    /// <summary>Creates a durable one-off schedule with an explicit stable management key.</summary>
    public Task AtAsync<T>(string stableScheduleKey,DateTimeOffset when,T job,CancellationToken cancellationToken=default)=>ScheduleAsync(stableScheduleKey,job,s=>s.OnceAt(when),cancellationToken);
    /// <summary>Begins a strongly typed static durable workflow DAG with a stable management key.</summary>
    public WorkflowBuilder Workflow(string stableWorkflowKey){ThrowIfDisposed();return new WorkflowBuilder(this,stableWorkflowKey);}
    internal IRegistration RegistrationFor(Type type)=>_registrations.TryGetValue(type,out var r)?r:throw new InvalidOperationException($"Register a handler for {type.Name} before adding it to a workflow.");
    internal void Submit(string key,List<WorkflowBuilder.Node> nodes,DependencyPolicy policy){var raw=new List<BasaltWorkflowNode>(nodes.Count);foreach(var n in nodes){var r=RegistrationFor(n.Value.GetType());raw.Add(new BasaltWorkflowNode{NodeId=n.Id,JobType=r.Type,Payload=r.Serialize(n.Value),PayloadVersion=r.Version,Dependencies=n.Dependencies.ToArray()});}ulong workflowId=JobKey.Hash("workflow:"+key);_engine.StoreManagementText(WorkflowKeyRecordType,workflowId,key);foreach(var n in nodes){_engine.StoreManagementText(WorkflowNodeRecordType,n.Id,n.Name);_workflowNodes[n.Id]=n.Name;}_engine.SubmitWorkflow(workflowId,raw,(BasaltDependencyPolicy)policy);_workflowKeys[workflowId]=key;}
    public BasaltExecutionInfo GetExecution(ulong id)=>Decorate(_engine.GetExecution(id));
    public IReadOnlyList<BasaltExecutionInfo> ListExecutions(int take=100,ulong afterExecutionId=0)=>_engine.ListExecutions(take,afterExecutionId).Select(Decorate).ToArray();
    public IReadOnlyList<BasaltExecutionInfo> ListExecutions(ExecutionQuery query)=>_engine.ListExecutions(query).Select(Decorate).ToArray();
    public BasaltQueueStats GetQueueStats()=>_engine.GetQueueStats(); public BasaltHealth GetHealth()=>_engine.GetHealth(); public IReadOnlyList<BasaltWorkerInfo> ListWorkers()=>_engine.ListWorkers();
    public BasaltStats GetStats()=>_engine.GetStats();public BasaltPerformanceStats GetPerformanceStats()=>_engine.GetPerformanceStats();public BasaltLedgerEntry GetLedger(ulong id)=>_engine.GetLedger(id);public void Cancel(ulong id)=>_engine.Cancel(id);public void Requeue(ulong id)=>_engine.Requeue(id);
    public BasaltScheduleInfo GetSchedule(ulong id)=>Decorate(_engine.GetSchedule(id));public BasaltScheduleInfo GetSchedule(string key)=>GetSchedule(JobKey.Hash("schedule:"+key)); public IReadOnlyList<BasaltScheduleInfo> ListSchedules(int take=100,ulong afterScheduleId=0)=>_engine.ListSchedules(take,afterScheduleId).Select(Decorate).ToArray();
    public void Pause(string key)=>PauseSchedule(key);public void Resume(string key)=>ResumeSchedule(key);public void Remove(string key)=>RemoveSchedule(key);public void PauseSchedule(string key)=>PauseSchedule(JobKey.Hash("schedule:"+key));public void ResumeSchedule(string key)=>ResumeSchedule(JobKey.Hash("schedule:"+key));public void RemoveSchedule(string key)=>RemoveSchedule(JobKey.Hash("schedule:"+key));public void PauseSchedule(ulong id){var schedule=_engine.GetSchedule(id);_engine.PauseSchedule(id,schedule.Revision);}public void ResumeSchedule(ulong id){var schedule=_engine.GetSchedule(id);_engine.ResumeSchedule(id,schedule.Revision);}public void RemoveSchedule(ulong id){var schedule=_engine.GetSchedule(id);_engine.RemoveSchedule(id,schedule.Revision);}
    public BasaltWorkflowStatus GetWorkflow(ulong id)=>Decorate(_engine.GetWorkflow(id));public BasaltWorkflowStatus GetWorkflow(string key)=>GetWorkflow(JobKey.Hash("workflow:"+key));public IReadOnlyList<BasaltWorkflowStatus> ListWorkflows(int take=100,ulong afterWorkflowId=0)=>_engine.ListWorkflows(take,afterWorkflowId).Select(Decorate).ToArray();public IReadOnlyList<BasaltWorkflowNodeInfo> ListWorkflowNodes(ulong workflowId)=>_engine.ListWorkflowNodes(workflowId).Select(Decorate).ToArray();public IReadOnlyList<BasaltWorkflowNodeInfo> ListWorkflowNodes(string key)=>ListWorkflowNodes(JobKey.Hash("workflow:"+key));public void VerifyHealth()=>_engine.VerifyHealth();
    private void LoadKeys(uint recordType,Dictionary<ulong,string> target){foreach(var value in _engine.ListManagementText(recordType))target[value.Key]=value.Value;} private static string Encode(string? value)=>Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(value??string.Empty));private static string? Decode(string value){var decoded=System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(value));return decoded.Length==0?null:decoded;}private void LoadScheduleDetails(){foreach(var value in _engine.ListManagementText(ScheduleDetailsRecordType)){var parts=value.Value.Split(':');if(parts.Length==2)_scheduleDetails[value.Key]=(Decode(parts[0]),Decode(parts[1]));}}private void LoadWorkflowNodes()=>LoadKeys(WorkflowNodeRecordType,_workflowNodes);
    private BasaltExecutionInfo Decorate(BasaltExecutionInfo value){if(_jobKeys.TryGetValue(value.JobDefinitionId,out var key))value.JobKey=key;return value;} private BasaltScheduleInfo Decorate(BasaltScheduleInfo value){if(_scheduleKeys.TryGetValue(value.ScheduleId,out var schedule))value.ScheduleKey=schedule;if(_jobKeys.TryGetValue(value.JobDefinitionId,out var job))value.JobKey=job;if(_scheduleDetails.TryGetValue(value.ScheduleId,out var details)){value.CronExpression=details.Cron;value.TimeZoneId=details.Zone;}return value;}private BasaltWorkflowStatus Decorate(BasaltWorkflowStatus value){if(_workflowKeys.TryGetValue(value.WorkflowId,out var key))value.WorkflowKey=key;return value;}private BasaltWorkflowNodeInfo Decorate(BasaltWorkflowNodeInfo value){if(_workflowNodes.TryGetValue(value.NodeId,out var name))value.Name=name;if(_jobKeys.TryGetValue(value.JobDefinitionId,out var key))value.JobKey=key;return value;}
    public void Dispose(){if(Interlocked.Exchange(ref _disposed,1)!=0)return;_engine.Dispose();_ownedStorage.Dispose();GC.SuppressFinalize(this);}private void ThrowIfDisposed(){if(Volatile.Read(ref _disposed)!=0)throw new ObjectDisposedException(nameof(BasaltApplication));}
}

public enum RetryPolicy:uint{None=0,Fixed=1,Linear=2,Exponential=3,ExponentialWithJitter=4}
/// <summary>Defines the durable retry policy persisted with an execution.</summary>
public sealed class RetryOptions
{
    public RetryPolicy Policy{get;}public uint MaxAttempts{get;}public TimeSpan InitialDelay{get;}public TimeSpan MaxDelay{get;}public double BackoffFactor{get;}public TimeSpan Jitter{get;}
    private RetryOptions(RetryPolicy policy,uint attempts,TimeSpan delay,TimeSpan max,double factor,TimeSpan jitter){Policy=policy;MaxAttempts=attempts;InitialDelay=delay;MaxDelay=max;BackoffFactor=factor;Jitter=jitter;Validate();}
    public static RetryOptions None{get;}=new(RetryPolicy.None,1,TimeSpan.Zero,TimeSpan.Zero,1,TimeSpan.Zero);
    public static RetryOptions Fixed(uint attempts,TimeSpan delay)=>new(RetryPolicy.Fixed,attempts,delay,delay,1,TimeSpan.Zero);
    public static RetryOptions Linear(uint attempts,TimeSpan delay,TimeSpan? max=null)=>new(RetryPolicy.Linear,attempts,delay,max??TimeSpan.Zero,1,TimeSpan.Zero);
    public static RetryOptions Exponential(uint attempts,TimeSpan delay,TimeSpan? max=null,double factor=2)=>new(RetryPolicy.Exponential,attempts,delay,max??TimeSpan.Zero,factor,TimeSpan.Zero);
    public static RetryOptions ExponentialWithJitter(uint attempts,TimeSpan delay,TimeSpan jitter,TimeSpan? max=null,double factor=2)=>new(RetryPolicy.ExponentialWithJitter,attempts,delay,max??TimeSpan.Zero,factor,jitter);
    internal void Validate(){if(!Enum.IsDefined(typeof(RetryPolicy),Policy)||MaxAttempts==0||InitialDelay<TimeSpan.Zero||MaxDelay<TimeSpan.Zero||Jitter<TimeSpan.Zero||Jitter>TimeSpan.FromDays(1)||double.IsNaN(BackoffFactor)||BackoffFactor<1)throw new ArgumentOutOfRangeException(nameof(RetryOptions));}
}
/// <summary>Provides advanced enqueue configuration including idempotency and durable retry.</summary>
public sealed class EnqueueOptions{public RetryOptions Retry{get;set;}=RetryOptions.None;public string? IdempotencyKey{get;set;}internal void Validate(){Retry=(Retry??throw new ArgumentNullException(nameof(Retry))).AlsoValidate();if(IdempotencyKey!=null&&string.IsNullOrWhiteSpace(IdempotencyKey))throw new ArgumentException("IdempotencyKey cannot be blank.");}}
internal static class RetryValidation{internal static RetryOptions AlsoValidate(this RetryOptions value){value.Validate();return value;}}

public enum ScheduleType:uint{Immediate=1,Delayed=2,Absolute=3,Interval=4,Cron=5} public enum IntervalMode:uint{FixedRate=1,FixedDelay=2} public enum MisfirePolicy:uint{Skip=1,RunOnce=2,RunLast=3,CatchUpAll=4} public enum OverlapPolicy:uint{Allow=1,Skip=2,QueueOne=3,QueueAll=4}
/// <summary>Configures advanced durable schedule timing, timezone, misfire, overlap, and occurrence behavior.</summary>
public sealed class ScheduleBuilder
{
    internal ScheduleType Type{get;private set;}=ScheduleType.Immediate;internal DateTimeOffset FirstFireAt{get;private set;}=DateTimeOffset.UtcNow;internal DateTimeOffset? EndAt{get;private set;}internal TimeSpan Interval{get;private set;}internal IntervalMode IntervalMode{get;private set;}=IntervalMode.FixedRate;internal ulong MaxOccurrences{get;private set;}internal string? CronExpression{get;private set;}internal string? TimeZoneId{get;private set;}internal MisfirePolicy MisfirePolicy{get;private set;}=MisfirePolicy.RunOnce;internal OverlapPolicy OverlapPolicy{get;private set;}=OverlapPolicy.Allow;internal uint CatchUpMax{get;private set;}
    public ScheduleBuilder OnceAt(DateTimeOffset at){Type=ScheduleType.Absolute;FirstFireAt=at;return this;}public ScheduleBuilder Delay(TimeSpan delay){if(delay<TimeSpan.Zero)throw new ArgumentOutOfRangeException(nameof(delay));Type=ScheduleType.Delayed;FirstFireAt=DateTimeOffset.UtcNow+delay;return this;}public ScheduleBuilder Every(TimeSpan interval,IntervalMode mode=IntervalMode.FixedRate){Type=ScheduleType.Interval;Interval=interval;IntervalMode=mode;FirstFireAt=DateTimeOffset.UtcNow+interval;return this;}public ScheduleBuilder Cron(string expression){Type=ScheduleType.Cron;CronExpression=expression;return this;}public ScheduleBuilder DailyAt(int hour,int minute){if(hour<0||hour>23||minute<0||minute>59)throw new ArgumentOutOfRangeException();return Cron($"{minute} {hour} * * *");}public ScheduleBuilder InTimeZone(string id){TimeZoneInfo.FindSystemTimeZoneById(id);TimeZoneId=id;return this;}public ScheduleBuilder Until(DateTimeOffset endAt){EndAt=endAt;return this;}public ScheduleBuilder OnMisfire(MisfirePolicy value){MisfirePolicy=value;return this;}public ScheduleBuilder OnOverlap(OverlapPolicy value){OverlapPolicy=value;return this;}public ScheduleBuilder WithCatchUpMax(uint value){CatchUpMax=value;return this;}public ScheduleBuilder WithMaxOccurrences(ulong value){MaxOccurrences=value;return this;}
    internal void Validate(){if(Type==ScheduleType.Interval&&Interval<=TimeSpan.Zero)throw new ArgumentOutOfRangeException(nameof(Interval));if(EndAt.HasValue&&EndAt.Value<FirstFireAt)throw new ArgumentOutOfRangeException(nameof(EndAt));if(Type==ScheduleType.Cron&&(string.IsNullOrWhiteSpace(CronExpression)||string.IsNullOrWhiteSpace(TimeZoneId)))throw new InvalidOperationException("Cron/calendar schedules require an explicit expression and time zone.");if(!Enum.IsDefined(typeof(MisfirePolicy),MisfirePolicy)||!Enum.IsDefined(typeof(OverlapPolicy),OverlapPolicy))throw new ArgumentOutOfRangeException();}
}

public enum DependencyPolicy:uint{Block=1,Cancel=2,Continue=3,FailWorkflow=4}
/// <summary>Builds a static durable workflow DAG from registered typed jobs.</summary>
public sealed class WorkflowBuilder
{
    internal sealed class Node{internal string Name="";internal ulong Id;internal object Value=null!;internal List<ulong> Dependencies=new();}
    private readonly BasaltApplication _owner;private readonly string _key;private readonly List<Node> _nodes=new();private Node? _last;private DependencyPolicy _policy=DependencyPolicy.Block;
    internal WorkflowBuilder(BasaltApplication owner,string key){if(string.IsNullOrWhiteSpace(key))throw new ArgumentException("A stable workflow key is required.",nameof(key));_owner=owner;_key=key;}
    public WorkflowBuilder Add<T>(string name,T job){return AddCore(name,job!,Array.Empty<string>());}public WorkflowBuilder Then<T>(string name,T job){if(_last==null)throw new InvalidOperationException("Then requires a preceding node.");return AddCore(name,job!,new[]{NameFor(_last.Id)});}public WorkflowBuilder AddAfter<T>(string name,T job,params string[] dependencies)=>AddCore(name,job!,dependencies);public WorkflowBuilder OnDependencyFailure(DependencyPolicy policy){_policy=policy;return this;}
    private WorkflowBuilder AddCore(string name,object value,IEnumerable<string> dependencies){if(value==null)throw new ArgumentNullException(nameof(value));_owner.RegistrationFor(value.GetType());ulong id=JobKey.Hash("node:"+_key+":"+name);if(_nodes.Any(n=>n.Id==id))throw new ArgumentException("Workflow node names must be unique.",nameof(name));var node=new Node{Name=name,Id=id,Value=value};foreach(string d in dependencies){ulong dep=JobKey.Hash("node:"+_key+":"+d);if(!_nodes.Any(n=>n.Id==dep))throw new ArgumentException("Dependencies must reference an earlier named node.",nameof(dependencies));node.Dependencies.Add(dep);}_nodes.Add(node);_last=node;return this;}
    private string NameFor(ulong id)=>_nodes.First(n=>n.Id==id).Name;
    public Task SubmitAsync(CancellationToken cancellationToken=default){cancellationToken.ThrowIfCancellationRequested();if(_nodes.Count==0)throw new InvalidOperationException("A workflow needs at least one node.");_owner.Submit(_key,_nodes,_policy);return Task.CompletedTask;}
}
