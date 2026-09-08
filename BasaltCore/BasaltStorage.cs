namespace BasaltCore;

public abstract class BasaltStorage : IDisposable
{
    protected internal abstract IntPtr DangerousHandle { get; }
    protected internal abstract void RetainManaged();
    protected internal abstract void ReleaseManaged();
    public abstract string ProviderName { get; }
    public virtual BasaltExecutionInfo GetExecution(ulong executionId)=>throw new NotSupportedException($"{ProviderName} does not expose execution management.");
    public virtual IReadOnlyList<BasaltExecutionInfo> ListExecutions(int take=100,ulong afterExecutionId=0)=>throw new NotSupportedException($"{ProviderName} does not expose execution management.");
    public virtual BasaltStats GetStats()=>throw new NotSupportedException($"{ProviderName} does not expose statistics.");
    public virtual BasaltLedgerEntry GetLedger(ulong executionId)=>throw new NotSupportedException($"{ProviderName} does not expose ledger management.");
    public virtual void Cancel(ulong executionId,ulong expectedRevision)=>throw new NotSupportedException($"{ProviderName} does not expose execution management.");
    public virtual void Requeue(ulong executionId,ulong expectedRevision)=>throw new NotSupportedException($"{ProviderName} does not expose execution management.");
    public virtual void VerifyHealth()=>throw new NotSupportedException($"{ProviderName} does not expose health checks.");
    public abstract void Dispose();
}
