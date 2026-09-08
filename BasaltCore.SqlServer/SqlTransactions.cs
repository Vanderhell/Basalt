using System.Data;
using System.Data.Common;
using System.Runtime.InteropServices;
using BasaltCore.Native;
using static BasaltCore.Native.StorageInterop;

namespace BasaltCore.SqlServer;

internal static class SqlTransactions
{
    private sealed class State : IDisposable
    {
        internal readonly SqlStorageBridge Owner; internal readonly DbConnection Connection; internal readonly DbTransaction Transaction; internal bool Completed;
        internal State(SqlStorageBridge owner){Owner=owner;Connection=owner.InternalOpen();Transaction=Connection.BeginTransaction(IsolationLevel.ReadCommitted);}
        public void Dispose(){if(!Completed)try{Transaction.Rollback();}catch{}Transaction.Dispose();Connection.Dispose();Completed=true;}
    }
    private static State Get(IntPtr p)=>((GCHandle)p).Target as State??throw new InvalidOperationException("Invalid SQL transaction handle.");
    internal static JobDbResult Begin(SqlStorageBridge owner,out IntPtr handle){IntPtr p=IntPtr.Zero;var r=owner.Safe(()=>{p=GCHandle.ToIntPtr(GCHandle.Alloc(new State(owner)));return JobDbResult.Ok;});handle=p;return r;}
    internal static JobDbResult Record(IntPtr handle,uint type,ulong id,byte[] payload){State s=Get(handle);return s.Owner.Safe(()=>{s.Owner.InternalInsertRecord(s.Connection,s.Transaction,type,id,payload);return JobDbResult.Ok;});}
    internal static JobDbResult Execution(IntPtr handle,ref Execution execution){State s=Get(handle);Execution copy=execution;return s.Owner.Safe(()=>{s.Owner.InternalInsertExecution(s.Connection,s.Transaction,ref copy);return JobDbResult.Ok;});}
    internal static JobDbResult Stats(IntPtr handle,ref Stats stats,ulong revision,int exists){State s=Get(handle);Stats copy=stats;return s.Owner.Safe(()=>{using var c=s.Owner.InternalCommand(s.Connection,s.Transaction,exists!=0?$"UPDATE {s.Owner.Schema}.[Stats] SET [Submitted]=@a,[Started]=@b,[Completed]=@c,[Failed]=@d,[Retried]=@e,[Cancelled]=@f,[Dead]=@g,[Recovered]=@h,[Revision]=[Revision]+1 WHERE [Id]=1 AND [Revision]=@r":$"INSERT INTO {s.Owner.Schema}.[Stats] VALUES(1,@a,@b,@c,@d,@e,@f,@g,@h,1)");SqlStorageBridge.Parameter(c,"@a",SqlStorageBridge.Long(copy.Submitted));SqlStorageBridge.Parameter(c,"@b",SqlStorageBridge.Long(copy.Started));SqlStorageBridge.Parameter(c,"@c",SqlStorageBridge.Long(copy.Completed));SqlStorageBridge.Parameter(c,"@d",SqlStorageBridge.Long(copy.Failed));SqlStorageBridge.Parameter(c,"@e",SqlStorageBridge.Long(copy.Retried));SqlStorageBridge.Parameter(c,"@f",SqlStorageBridge.Long(copy.Cancelled));SqlStorageBridge.Parameter(c,"@g",SqlStorageBridge.Long(copy.Dead));SqlStorageBridge.Parameter(c,"@h",SqlStorageBridge.Long(copy.Recovered));if(exists!=0)SqlStorageBridge.Parameter(c,"@r",SqlStorageBridge.Long(revision));return c.ExecuteNonQuery()==1?JobDbResult.Ok:JobDbResult.Conflict;});}
    internal static JobDbResult Commit(IntPtr handle){State s=Get(handle);return s.Owner.Safe(()=>{s.Transaction.Commit();s.Completed=true;return JobDbResult.Ok;});}
    internal static void Rollback(IntPtr handle){if(handle==IntPtr.Zero)return;GCHandle g=(GCHandle)handle;if(g.Target is State s)s.Dispose();g.Free();}
    internal static JobDbResult GetStats(SqlStorageBridge owner,out Stats stats){Stats value=default;var r=owner.Safe(()=>{using var db=owner.InternalOpen();using var c=owner.InternalCommand(db,null,$"SELECT [Submitted],[Started],[Completed],[Failed],[Retried],[Cancelled],[Dead],[Recovered] FROM {owner.Schema}.[Stats] WHERE [Id]=1");using var rd=c.ExecuteReader();if(!rd.Read()){value=default;return JobDbResult.Ok;}value=new Stats{Submitted=SqlStorageBridge.ULong(rd[0]),Started=SqlStorageBridge.ULong(rd[1]),Completed=SqlStorageBridge.ULong(rd[2]),Failed=SqlStorageBridge.ULong(rd[3]),Retried=SqlStorageBridge.ULong(rd[4]),Cancelled=SqlStorageBridge.ULong(rd[5]),Dead=SqlStorageBridge.ULong(rd[6]),Recovered=SqlStorageBridge.ULong(rd[7])};return JobDbResult.Ok;});stats=value;return r;}
}
