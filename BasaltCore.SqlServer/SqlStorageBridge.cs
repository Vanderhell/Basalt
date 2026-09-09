using System.Data;
using System.Data.Common;
using System.Runtime.InteropServices;
using BasaltCore.Native;
using Microsoft.Data.SqlClient;
using static BasaltCore.Native.StorageInterop;

namespace BasaltCore.SqlServer;

internal sealed class SqlStorageBridge : IDisposable
{
    private readonly SqlServerConnectionFactory _connections;
    private readonly string _schema;
    private readonly int _timeout;
    private readonly List<Delegate> _delegates = new();
    private GCHandle _self;
    private IntPtr _providerName;
    private int _disposed;
    internal IntPtr Handle { get; private set; }

    internal SqlStorageBridge(SqlServerConnectionFactory connections, SqlServerOptions options)
    {
        _connections=connections;_schema=SqlIdentifier.Quote(options.Schema);_timeout=options.CommandTimeoutSeconds;
        _self=GCHandle.Alloc(this);_providerName=Marshal.StringToHGlobalAnsi("SqlServer");
        var v=new VTable { AbiVersion=1,Capabilities=7,ProviderName=_providerName };
        Bind(ref v.Retain,new Lifetime(_=>{}));Bind(ref v.Release,new Lifetime(_=>{}));Bind(ref v.Health,new Health(HealthCallback));Bind(ref v.UtcNow,new Clock(ClockCallback));Bind(ref v.Allocate,new Allocate(AllocateCallback));
        Bind(ref v.RecordCreate,new RecordCreate(RecordCreateCallback));Bind(ref v.RecordGet,new RecordGet(RecordGetCallback));Bind(ref v.RecordFree,new RecordFree(RecordFreeCallback));Bind(ref v.ListIds,new ListIds(ListIdsCallback));
        Bind(ref v.Enqueue,new Enqueue(EnqueueCallback));Bind(ref v.EnqueueExtra,new EnqueueExtra(EnqueueExtraCallback));Bind(ref v.EnqueueReceipt,new EnqueueReceipt(EnqueueReceiptCallback));Bind(ref v.ReceiptGet,new ReceiptGet(ReceiptGetCallback));Bind(ref v.ExecutionGet,new ExecutionGet(ExecutionGetCallback));Bind(ref v.Transition,new Transition(TransitionCallback));Bind(ref v.Start,new Start(StartCallback));Bind(ref v.Claim,new Claim(ClaimCallback));Bind(ref v.Renew,new Renew(RenewCallback));Bind(ref v.Complete,new Complete(CompleteCallback));Bind(ref v.Finalize,new Finalize(FinalizeCallback));Bind(ref v.Park,new Park(ParkCallback));Bind(ref v.Retry,new Retry(RetryCallback));
        Bind(ref v.ScheduleCreate,new ScheduleCreate(ScheduleCreateCallback));Bind(ref v.ScheduleGet,new ScheduleGet(ScheduleGetCallback));Bind(ref v.ScheduleUpdate,new ScheduleUpdate(ScheduleUpdateCallback));Bind(ref v.SchedulePause,new ScheduleCas((c,i,r)=>ScheduleEnabled(i,r,false)));Bind(ref v.ScheduleResume,new ScheduleCas((c,i,r)=>ScheduleEnabled(i,r,true)));Bind(ref v.ScheduleRemove,new ScheduleCas(ScheduleRemoveCallback));Bind(ref v.ScheduleFire,new ScheduleFire(ScheduleFireCallback));Bind(ref v.StatsGet,new StatsGet(StatsGetCallback));
        Bind(ref v.TxBegin,new TxBegin(TxBeginCallback));Bind(ref v.TxRecord,new TxRecord(TxRecordCallback));Bind(ref v.TxExecution,new TxExecution(TxExecutionCallback));Bind(ref v.TxStats,new TxStats(TxStatsCallback));Bind(ref v.TxCommit,new TxCommit(TxCommitCallback));Bind(ref v.TxRollback,new TxRollback(TxRollbackCallback));
        v.StructSize=(uint)Marshal.SizeOf<VTable>(); Check(StorageInterop.basalt_storage_create_v1(ref v,GCHandle.ToIntPtr(_self),out var handle));Handle=handle;
    }
    private void Bind<T>(ref IntPtr slot,T callback) where T:Delegate{_delegates.Add(callback);slot=Marshal.GetFunctionPointerForDelegate(callback);}
    private static void Check(JobDbResult result){if(result!=JobDbResult.Ok)throw new BasaltException(result);}
    private DbConnection Open(){DbConnection c=_connections.Create();c.Open();return c;}
    private DbCommand Command(DbConnection c,DbTransaction? tx,string sql){DbCommand cmd=c.CreateCommand();cmd.CommandText=sql;cmd.CommandTimeout=_timeout;cmd.Transaction=tx;return cmd;}
    private static void P(DbCommand c,string n,object? v){DbParameter p=c.CreateParameter();p.ParameterName=n;p.Value=v??DBNull.Value;c.Parameters.Add(p);}
    private static long L(ulong value)=>unchecked((long)value); private static ulong U(object value)=>unchecked((ulong)Convert.ToInt64(value));
    private static byte[] Bytes(IntPtr p,uint n){var b=new byte[checked((int)n)];if(n!=0)Marshal.Copy(p,b,0,b.Length);return b;}
    private static JobDbResult Error(Exception ex)=>ex is OverflowException?JobDbResult.Limit:ex is SqlException s&&s.Number is 2601 or 2627?JobDbResult.AlreadyExists:JobDbResult.Io;
    private JobDbResult Run(Func<JobDbResult> action){try{return action();}catch(Exception ex){return Error(ex);}}

    private JobDbResult HealthCallback(IntPtr c)=>Run(()=>{using var db=Open();using var cmd=Command(db,null,"SELECT 1");cmd.ExecuteScalar();return JobDbResult.Ok;});
    private JobDbResult ClockCallback(IntPtr c,out long seconds){long value=0;var r=Run(()=>{using var db=Open();using var cmd=Command(db,null,"SELECT DATEDIFF_BIG(SECOND,'19700101',SYSUTCDATETIME())");value=Convert.ToInt64(cmd.ExecuteScalar());return JobDbResult.Ok;});seconds=value;return r;}
    private JobDbResult AllocateCallback(IntPtr c,out ulong id){ulong value=0;var r=Run(()=>{using var db=Open();using var cmd=Command(db,null,$"SELECT NEXT VALUE FOR {_schema}.[ExecutionIds]");value=U(cmd.ExecuteScalar()!);return JobDbResult.Ok;});id=value;return r;}
    private JobDbResult RecordCreateCallback(IntPtr c,uint type,ulong id,IntPtr payload,uint size)=>Run(()=>{using var db=Open();using var cmd=Command(db,null,$"INSERT INTO {_schema}.[Records]([RecordType],[RecordId],[Payload]) VALUES(@t,@i,@p)");P(cmd,"@t",(int)type);P(cmd,"@i",L(id));P(cmd,"@p",Bytes(payload,size));cmd.ExecuteNonQuery();return JobDbResult.Ok;});
    private JobDbResult RecordGetCallback(IntPtr c,uint type,ulong id,out Record record){Record value=default;var r=Run(()=>{using var db=Open();if(type==5&&id==1){using var stats=Command(db,null,$"SELECT [Revision] FROM {_schema}.[Stats] WHERE [Id]=1");object? revision=stats.ExecuteScalar();if(revision==null)return JobDbResult.NotFound;value=new Record{Type=type,FormatVersion=1,Id=id,Generation=1,Revision=U(revision),Payload=Marshal.AllocHGlobal(1),PayloadSize=0};return JobDbResult.Ok;}using var cmd=Command(db,null,$"SELECT [Revision],[Payload] FROM {_schema}.[Records] WHERE [RecordType]=@t AND [RecordId]=@i");P(cmd,"@t",(int)type);P(cmd,"@i",L(id));using var rd=cmd.ExecuteReader();if(!rd.Read())return JobDbResult.NotFound;byte[] b=(byte[])rd[1];IntPtr p=Marshal.AllocHGlobal(b.Length==0?1:b.Length);if(b.Length!=0)Marshal.Copy(b,0,p,b.Length);value=new Record{Type=type,FormatVersion=1,Id=id,Generation=1,Revision=U(rd[0]),Payload=p,PayloadSize=(uint)b.Length};return JobDbResult.Ok;});record=value;return r;}
    private void RecordFreeCallback(IntPtr c,ref Record record){if(record.Payload!=IntPtr.Zero)Marshal.FreeHGlobal(record.Payload);record=default;}
    private JobDbResult ListIdsCallback(IntPtr c,uint type,IntPtr ids,UIntPtr capacity,out UIntPtr count){ulong observed=0;var r=Run(()=>{using var db=Open();string sql=type==2||type==6?$"SELECT [ScheduleId] FROM {_schema}.[Schedules] ORDER BY [ScheduleId]":type==3?$"SELECT [ExecutionId] FROM {_schema}.[Executions] ORDER BY [ExecutionId]":type==4?$"SELECT [ExecutionId] FROM {_schema}.[Ledger] ORDER BY [ExecutionId]":$"SELECT [RecordId] FROM {_schema}.[Records] WHERE [RecordType]=@t ORDER BY [RecordId]";using var cmd=Command(db,null,sql);if(type!=2&&type!=3&&type!=4&&type!=6)P(cmd,"@t",(int)type);using var rd=cmd.ExecuteReader();var values=new List<long>();while(rd.Read())values.Add(rd.GetInt64(0));observed=(ulong)values.Count;ulong cap=capacity.ToUInt64();for(int i=0;i<values.Count&&(ulong)i<cap;i++)Marshal.WriteInt64(ids,i*8,values[i]);return JobDbResult.Ok;});count=(UIntPtr)observed;return r;}

    private void InsertExecution(DbConnection db,DbTransaction tx,ref Execution e)
    {using var cmd=Command(db,tx,$"INSERT INTO {_schema}.[Executions] VALUES(@id,@j,@s,@w,@st,@c,@e,@sa,@f,@p,@a,@m,1,NULL,0,0)");P(cmd,"@id",L(e.Id));P(cmd,"@j",L(e.JobType));P(cmd,"@s",L(e.ScheduleId));P(cmd,"@w",L(e.WorkflowId));P(cmd,"@st",(int)e.State);P(cmd,"@c",e.CreatedAt);P(cmd,"@e",e.EligibleAt);P(cmd,"@sa",e.StartedAt);P(cmd,"@f",e.FinishedAt);P(cmd,"@p",e.Priority);P(cmd,"@a",(int)e.Attempt);P(cmd,"@m",(int)e.MaxAttempts);cmd.ExecuteNonQuery();}
    private void InsertRecord(DbConnection db,DbTransaction tx,uint type,ulong id,byte[] payload){using var cmd=Command(db,tx,$"INSERT INTO {_schema}.[Records]([RecordType],[RecordId],[Payload]) VALUES(@t,@i,@p)");P(cmd,"@t",(int)type);P(cmd,"@i",L(id));P(cmd,"@p",payload);cmd.ExecuteNonQuery();}
    private void IncrementStats(DbConnection db,DbTransaction tx,string column){using var cmd=Command(db,tx,$"UPDATE {_schema}.[Stats] SET [{column}]=[{column}]+1,[Revision]=[Revision]+1 WHERE [Id]=1; IF @@ROWCOUNT=0 INSERT INTO {_schema}.[Stats] VALUES(1,1,0,0,0,0,0,0,0,1);");cmd.ExecuteNonQuery();}
    private JobDbResult EnqueueCallback(IntPtr c,ref Execution e,uint type,IntPtr payload,uint size)=>EnqueueCore(ref e,type,Bytes(payload,size),null,null);
    private JobDbResult EnqueueExtraCallback(IntPtr c,ref Execution e,uint type,IntPtr payload,uint size,uint extraType,IntPtr extra,uint extraSize)=>EnqueueCore(ref e,type,Bytes(payload,size),(extraType,Bytes(extra,extraSize)),null);
    private JobDbResult EnqueueReceiptCallback(IntPtr c,ref Execution e,uint type,IntPtr payload,uint size,ulong receiptId,IntPtr receipt,uint receiptSize)=>EnqueueCore(ref e,type,Bytes(payload,size),null,(receiptId,Bytes(receipt,receiptSize)));
    private JobDbResult EnqueueCore(ref Execution e,uint type,byte[] payload,(uint,byte[])? extra,(ulong,byte[])? receipt)
    {
        Execution copy=e;
        try
        {
            using var db=Open();using var tx=db.BeginTransaction(IsolationLevel.ReadCommitted);
            try
            {
                InsertExecution(db,tx,ref copy);InsertRecord(db,tx,type,copy.Id,payload);
                if(extra.HasValue)InsertRecord(db,tx,extra.Value.Item1,copy.Id,extra.Value.Item2);
                if(receipt.HasValue){using var cmd=Command(db,tx,$"INSERT INTO {_schema}.[Receipts] VALUES(@i,@p)");P(cmd,"@i",L(receipt.Value.Item1));P(cmd,"@p",receipt.Value.Item2);cmd.ExecuteNonQuery();}
                IncrementStats(db,tx,"Submitted");
                try{tx.Commit();}
                catch(SqlException){return JobDbResult.UnknownCommit;}
                return JobDbResult.Ok;
            }
            catch{try{tx.Rollback();}catch{}throw;}
        }
        catch(Exception ex){return Error(ex);}
    }
    private JobDbResult ReceiptGetCallback(IntPtr c,ulong id,IntPtr payload,uint capacity,out uint size){uint n=0;var r=Run(()=>{using var db=Open();using var cmd=Command(db,null,$"SELECT [Payload] FROM {_schema}.[Receipts] WHERE [ReceiptId]=@i");P(cmd,"@i",L(id));object? o=cmd.ExecuteScalar();if(o==null)return JobDbResult.NotFound;byte[] b=(byte[])o;n=(uint)b.Length;if(n>capacity)return JobDbResult.Limit;if(n!=0)Marshal.Copy(b,0,payload,b.Length);return JobDbResult.Ok;});size=n;return r;}
    private Execution ReadExecution(DbDataReader r)=>new(){Id=U(r[0]),JobType=U(r[1]),ScheduleId=U(r[2]),WorkflowId=U(r[3]),State=Convert.ToUInt32(r[4]),CreatedAt=Convert.ToInt64(r[5]),EligibleAt=Convert.ToInt64(r[6]),StartedAt=Convert.ToInt64(r[7]),FinishedAt=Convert.ToInt64(r[8]),Priority=Convert.ToInt32(r[9]),Attempt=Convert.ToUInt32(r[10]),MaxAttempts=Convert.ToUInt32(r[11]),Revision=U(r[12]),WorkerId=r.IsDBNull(13)?new byte[16]:(byte[])r[13],LeaseExpiresAt=Convert.ToInt64(r[14]),FencingToken=U(r[15])};
    private JobDbResult ExecutionGetCallback(IntPtr c,ulong id,out Execution execution){Execution e=default;var result=Run(()=>{using var db=Open();using var cmd=Command(db,null,$"SELECT * FROM {_schema}.[Executions] WHERE [ExecutionId]=@i");P(cmd,"@i",L(id));using var r=cmd.ExecuteReader();if(!r.Read())return JobDbResult.NotFound;e=ReadExecution(r);return JobDbResult.Ok;});execution=e;return result;}
    private static void WriteU64(IntPtr p,ulong value){if(p!=IntPtr.Zero)Marshal.WriteInt64(p,unchecked((long)value));}
    private JobDbResult TransitionCallback(IntPtr c,ulong id,ulong revision,uint state,IntPtr output)=>Run(()=>{using var db=Open();using var cmd=Command(db,null,$"UPDATE {_schema}.[Executions] SET [State]=@s,[Revision]=[Revision]+1 WHERE [ExecutionId]=@i AND [Revision]=@r");P(cmd,"@s",(int)state);P(cmd,"@i",L(id));P(cmd,"@r",L(revision));if(cmd.ExecuteNonQuery()!=1)return Exists(db,id)?JobDbResult.Conflict:JobDbResult.NotFound;WriteU64(output,revision+1);return JobDbResult.Ok;});
    private bool Exists(DbConnection db,ulong id){using var c=Command(db,null,$"SELECT COUNT_BIG(*) FROM {_schema}.[Executions] WHERE [ExecutionId]=@i");P(c,"@i",L(id));return Convert.ToInt64(c.ExecuteScalar())!=0;}
    private JobDbResult ClaimCallback(IntPtr c,ref Worker worker,long now,long lease,out Execution execution){Execution e=default;byte[] workerBytes=worker.Bytes;var result=Run(()=>{using var db=Open();using var tx=db.BeginTransaction(IsolationLevel.ReadCommitted);using var cmd=Command(db,tx,$@"DECLARE @now bigint=DATEDIFF_BIG(SECOND,'19700101',SYSUTCDATETIME()); WITH q AS(SELECT TOP(1)* FROM {_schema}.[Executions] WITH(UPDLOCK,READPAST,ROWLOCK) WHERE ([State] IN(2,5) AND [EligibleAt]<=@now) OR ([State] IN(3,4) AND [LeaseExpiresAt]<@now) ORDER BY [Priority] DESC,[EligibleAt],[ExecutionId]) UPDATE q SET [State]=3,[WorkerId]=@w,[LeaseExpiresAt]=@now+@lease,[FencingToken]=[FencingToken]+1,[Revision]=[Revision]+1 OUTPUT inserted.*;");P(cmd,"@w",workerBytes);P(cmd,"@lease",lease);using var r=cmd.ExecuteReader();if(!r.Read()){tx.Commit();return JobDbResult.NotFound;}e=ReadExecution(r);r.Close();tx.Commit();return JobDbResult.Ok;});execution=e;return result;}
    private JobDbResult StartCallback(IntPtr c,ulong id,ref Worker worker,ulong fence,long now,IntPtr rev)=>OwnedUpdate(id,worker.Bytes,fence,"[State]=4,[StartedAt]=DATEDIFF_BIG(SECOND,'19700101',SYSUTCDATETIME())",rev,true);
    private JobDbResult RenewCallback(IntPtr c,ulong id,ref Worker worker,ulong fence,long expires)=>OwnedUpdate(id,worker.Bytes,fence,$"[LeaseExpiresAt]={expires}",IntPtr.Zero,false);
    private JobDbResult ParkCallback(IntPtr c,ulong id,ref Worker worker,ulong fence,IntPtr rev)=>OwnedUpdate(id,worker.Bytes,fence,"[State]=11,[WorkerId]=NULL,[LeaseExpiresAt]=0",rev,false);
    private JobDbResult RetryCallback(IntPtr c,ulong id,ref Worker worker,ulong fence,long eligible,IntPtr rev)=>OwnedUpdate(id,worker.Bytes,fence,$"[State]=5,[EligibleAt]={eligible},[Attempt]=[Attempt]+1,[WorkerId]=NULL,[LeaseExpiresAt]=0",rev,false);
    private JobDbResult OwnedUpdate(ulong id,byte[] worker,ulong fence,string set,IntPtr output,bool started)=>Run(()=>{using var db=Open();using var tx=db.BeginTransaction();using var cmd=Command(db,tx,$"UPDATE {_schema}.[Executions] SET {set},[Revision]=[Revision]+1 WHERE [ExecutionId]=@i AND [WorkerId]=@w AND [FencingToken]=@f AND [State] IN(3,4)");P(cmd,"@i",L(id));P(cmd,"@w",worker);P(cmd,"@f",L(fence));if(cmd.ExecuteNonQuery()!=1){tx.Rollback();return Exists(db,id)?JobDbResult.StaleLease:JobDbResult.NotFound;}if(started)IncrementStats(db,tx,"Started");using var q=Command(db,tx,$"SELECT [Revision] FROM {_schema}.[Executions] WHERE [ExecutionId]=@i");P(q,"@i",L(id));WriteU64(output,U(q.ExecuteScalar()!));tx.Commit();return JobDbResult.Ok;});
    private JobDbResult CompleteCallback(IntPtr c,ulong id,ref Worker w,ulong f)=>FinalizeCore(id,w.Bytes,f,7,0,0);
    private JobDbResult FinalizeCallback(IntPtr c,ulong id,ref Worker w,ulong f,uint state,int result,int error)=>FinalizeCore(id,w.Bytes,f,state,result,error);
    private JobDbResult FinalizeCore(ulong id,byte[] worker,ulong fence,uint state,int result,int error)=>Run(()=>{using var db=Open();using var tx=db.BeginTransaction();using var cmd=Command(db,tx,$"UPDATE {_schema}.[Executions] SET [State]=@s,[FinishedAt]=DATEDIFF_BIG(SECOND,'19700101',SYSUTCDATETIME()),[WorkerId]=NULL,[LeaseExpiresAt]=0,[Revision]=[Revision]+1 WHERE [ExecutionId]=@i AND [WorkerId]=@w AND [FencingToken]=@f AND [State] IN(3,4)");P(cmd,"@s",(int)state);P(cmd,"@i",L(id));P(cmd,"@w",worker);P(cmd,"@f",L(fence));if(cmd.ExecuteNonQuery()!=1){tx.Rollback();return Exists(db,id)?JobDbResult.StaleLease:JobDbResult.NotFound;}IncrementStats(db,tx,state==7?"Completed":state==8?"Failed":state==9?"Dead":"Cancelled");using var ledger=Command(db,tx,$"INSERT INTO {_schema}.[Ledger] SELECT [ExecutionId],[JobDefinitionId],[ScheduleId],[WorkflowId],[StartedAt],[FinishedAt],[FinishedAt]-[StartedAt],[Attempt],[State],@r,@e FROM {_schema}.[Executions] WHERE [ExecutionId]=@i");P(ledger,"@r",result);P(ledger,"@e",error);P(ledger,"@i",L(id));ledger.ExecuteNonQuery();tx.Commit();return JobDbResult.Ok;});

    // Schedule and transaction callbacks are implemented in the companion partial.
    private JobDbResult ScheduleCreateCallback(IntPtr c,ref Schedule s,uint a,IntPtr b,uint n,uint d,IntPtr e,uint f)=>SqlSchedule.Create(this,ref s,a,Bytes(b,n),d,Bytes(e,f));
    private JobDbResult ScheduleGetCallback(IntPtr c,ulong id,out Schedule s)=>SqlSchedule.Get(this,id,out s);
    private JobDbResult ScheduleUpdateCallback(IntPtr c,ref Schedule s,ulong r)=>SqlSchedule.Update(this,ref s,r);
    private JobDbResult ScheduleEnabled(ulong id,ulong r,bool enabled)=>SqlSchedule.Enabled(this,id,r,enabled);
    private JobDbResult ScheduleRemoveCallback(IntPtr c,ulong id,ulong r)=>SqlSchedule.Remove(this,id,r);
    private JobDbResult ScheduleFireCallback(IntPtr c,ulong id,ulong r,long expected,ulong eid,long fire,long next,uint state)=>SqlSchedule.Fire(this,id,r,expected,eid,fire,next,state);
    private JobDbResult StatsGetCallback(IntPtr c,out Stats stats)=>SqlTransactions.GetStats(this,out stats);
    private JobDbResult TxBeginCallback(IntPtr c,out IntPtr tx)=>SqlTransactions.Begin(this,out tx);
    private JobDbResult TxRecordCallback(IntPtr tx,uint type,ulong id,IntPtr p,uint n)=>SqlTransactions.Record(tx,type,id,Bytes(p,n));
    private JobDbResult TxExecutionCallback(IntPtr tx,ref Execution e)=>SqlTransactions.Execution(tx,ref e);
    private JobDbResult TxStatsCallback(IntPtr tx,ref Stats s,ulong r,int exists)=>SqlTransactions.Stats(tx,ref s,r,exists);
    private JobDbResult TxCommitCallback(IntPtr tx)=>SqlTransactions.Commit(tx);
    private void TxRollbackCallback(IntPtr tx)=>SqlTransactions.Rollback(tx);

    internal BasaltExecutionInfo ManagementGetExecution(ulong id){JobDbResult r=ExecutionGetCallback(IntPtr.Zero,id,out var e);Check(r);return ToInfo(e);}
    internal IReadOnlyList<BasaltExecutionInfo> ManagementListExecutions(int take,ulong after){if(take<1||take>1000)throw new ArgumentOutOfRangeException(nameof(take));var result=new List<BasaltExecutionInfo>();using var db=Open();using var c=Command(db,null,$"SELECT TOP (@take) * FROM {_schema}.[Executions] WHERE [ExecutionId]>@after ORDER BY [ExecutionId]");P(c,"@take",take);P(c,"@after",L(after));using var r=c.ExecuteReader();while(r.Read())result.Add(ToInfo(ReadExecution(r)));return result;}
    private static BasaltExecutionInfo ToInfo(Execution e)=>new(e.Id,e.JobType,e.ScheduleId,e.WorkflowId,e.State,e.CreatedAt,e.EligibleAt,e.StartedAt,e.FinishedAt,e.Priority,e.Attempt,e.MaxAttempts,e.Revision,e.LeaseExpiresAt,e.FencingToken);
    internal BasaltStats ManagementGetStats(){Check(SqlTransactions.GetStats(this,out var s));return new BasaltStats(s.Submitted,s.Started,s.Completed,s.Failed,s.Retried,s.Cancelled,s.Dead,s.Recovered);}
    internal BasaltLedgerEntry ManagementGetLedger(ulong id){using var db=Open();using var c=Command(db,null,$"SELECT * FROM {_schema}.[Ledger] WHERE [ExecutionId]=@i");P(c,"@i",L(id));using var r=c.ExecuteReader();if(!r.Read())throw new BasaltException(JobDbResult.NotFound);return new BasaltLedgerEntry{ExecutionId=U(r[0]),JobDefinitionId=U(r[1]),ScheduleId=U(r[2]),WorkflowId=U(r[3]),StartedAt=Convert.ToInt64(r[4]),FinishedAt=Convert.ToInt64(r[5]),Duration=Convert.ToInt64(r[6]),Attempt=Convert.ToUInt32(r[7]),FinalState=Convert.ToUInt32(r[8]),ResultCode=Convert.ToInt32(r[9]),ErrorCode=Convert.ToInt32(r[10])};}
    internal void ManagementCancel(ulong id,ulong revision){using var db=Open();using var tx=db.BeginTransaction();using var c=Command(db,tx,$"UPDATE {_schema}.[Executions] SET [State]=10,[FinishedAt]=DATEDIFF_BIG(SECOND,'19700101',SYSUTCDATETIME()),[WorkerId]=NULL,[LeaseExpiresAt]=0,[Revision]=[Revision]+1 WHERE [ExecutionId]=@i AND [Revision]=@r AND [State] NOT IN(7,8,9,10)");P(c,"@i",L(id));P(c,"@r",L(revision));if(c.ExecuteNonQuery()!=1){tx.Rollback();throw new BasaltException(Exists(db,id)?JobDbResult.Conflict:JobDbResult.NotFound);}IncrementStats(db,tx,"Cancelled");tx.Commit();}
    internal void ManagementRequeue(ulong id,ulong revision){using var db=Open();using var c=Command(db,null,$"UPDATE {_schema}.[Executions] SET [State]=2,[EligibleAt]=DATEDIFF_BIG(SECOND,'19700101',SYSUTCDATETIME()),[Attempt]=0,[StartedAt]=0,[FinishedAt]=0,[WorkerId]=NULL,[LeaseExpiresAt]=0,[Revision]=[Revision]+1 WHERE [ExecutionId]=@i AND [Revision]=@r AND [State] IN(7,8,9,10,11)");P(c,"@i",L(id));P(c,"@r",L(revision));if(c.ExecuteNonQuery()!=1)throw new BasaltException(Exists(db,id)?JobDbResult.Conflict:JobDbResult.NotFound);}
    internal void ManagementHealth(){Check(HealthCallback(IntPtr.Zero));}

    internal DbConnection InternalOpen()=>Open(); internal DbCommand InternalCommand(DbConnection c,DbTransaction? t,string s)=>Command(c,t,s); internal string Schema=>_schema; internal static void Parameter(DbCommand c,string n,object? v)=>P(c,n,v); internal static long Long(ulong v)=>L(v); internal static ulong ULong(object v)=>U(v); internal JobDbResult Safe(Func<JobDbResult>a)=>Run(a); internal void InternalInsertExecution(DbConnection c,DbTransaction t,ref Execution e)=>InsertExecution(c,t,ref e); internal void InternalInsertRecord(DbConnection c,DbTransaction t,uint y,ulong i,byte[]p)=>InsertRecord(c,t,y,i,p);
    public void Dispose(){if(Interlocked.Exchange(ref _disposed,1)!=0)return;if(Handle!=IntPtr.Zero){StorageInterop.basalt_storage_destroy(Handle);Handle=IntPtr.Zero;}if(_self.IsAllocated)_self.Free();if(_providerName!=IntPtr.Zero)Marshal.FreeHGlobal(_providerName);_delegates.Clear();}
}
