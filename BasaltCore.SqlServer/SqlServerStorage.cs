using BasaltCore.Native;
namespace BasaltCore.SqlServer;

public sealed class SqlServerStorage : BasaltStorage
{
    private SqlStorageBridge? _bridge;
    private int _references=1;
    private int _disposeRequested;
    private SqlServerStorage(SqlStorageBridge bridge)=>_bridge=bridge;
    protected internal override IntPtr DangerousHandle=>_bridge?.Handle??throw new ObjectDisposedException(nameof(SqlServerStorage));
    protected internal override void RetainManaged(){lock(this){if(_disposeRequested!=0)throw new ObjectDisposedException(nameof(SqlServerStorage));checked{_references++;}}}
    protected internal override void ReleaseManaged(){SqlStorageBridge? release=null;lock(this){if(--_references==0)release=Interlocked.Exchange(ref _bridge,null);}release?.Dispose();}
    public override string ProviderName=>"SqlServer";
    public override BasaltExecutionInfo GetExecution(ulong executionId)=>Bridge.ManagementGetExecution(executionId);
    public override IReadOnlyList<BasaltExecutionInfo> ListExecutions(int take=100,ulong afterExecutionId=0)=>Bridge.ManagementListExecutions(take,afterExecutionId);
    public override BasaltStats GetStats()=>Bridge.ManagementGetStats();
    public override BasaltLedgerEntry GetLedger(ulong executionId)=>Bridge.ManagementGetLedger(executionId);
    public override void Cancel(ulong executionId,ulong expectedRevision)=>Bridge.ManagementCancel(executionId,expectedRevision);
    public override void Requeue(ulong executionId,ulong expectedRevision)=>Bridge.ManagementRequeue(executionId,expectedRevision);
    public override void VerifyHealth()=>Bridge.ManagementHealth();
    private SqlStorageBridge Bridge=>_bridge??throw new ObjectDisposedException(nameof(SqlServerStorage));
    internal JobDbResult TestClaim(byte[] worker,long lease,out BasaltCore.Native.StorageInterop.Execution execution)=>Bridge.TestClaim(worker,lease,out execution);internal JobDbResult TestStart(ulong id,byte[] worker,ulong fence)=>Bridge.TestStart(id,worker,fence);internal JobDbResult TestFinalize(ulong id,byte[] worker,ulong fence)=>Bridge.TestFinalize(id,worker,fence);
    public static async Task<SqlServerStorage> OpenAsync(string connectionString,Action<SqlServerOptions>? configure=null,CancellationToken cancellationToken=default)
    {var options=new SqlServerOptions();configure?.Invoke(options);var factory=new SqlServerConnectionFactory(connectionString);await new SqlSchemaManager(factory,options).InitializeAsync(cancellationToken).ConfigureAwait(false);return new SqlServerStorage(new SqlStorageBridge(factory,options));}
    public static async Task<SqlServerStorage> OpenAsync(Func<System.Data.Common.DbConnection> connectionFactory,Action<SqlServerOptions>? configure=null,CancellationToken cancellationToken=default)
    {var options=new SqlServerOptions();configure?.Invoke(options);var factory=new SqlServerConnectionFactory(connectionFactory);await new SqlSchemaManager(factory,options).InitializeAsync(cancellationToken).ConfigureAwait(false);return new SqlServerStorage(new SqlStorageBridge(factory,options));}
    public override void Dispose(){lock(this){if(Interlocked.Exchange(ref _disposeRequested,1)!=0)return;}ReleaseManaged();GC.SuppressFinalize(this);}
}
