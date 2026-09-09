using BasaltCore;
using BasaltCore.SqlServer;
using Microsoft.Data.SqlClient;
using System.Diagnostics;

string? configured=Environment.GetEnvironmentVariable("BASALT_SQLSERVER_TEST_CONNECTION");
string connection=configured??@"Server=.\SQLEXPRESS;Database=BasaltIntegrationTests;Integrated Security=true;Encrypt=false";
if(args.Length==2&&args[0]=="--worker")
{
    string childSchema=args[1];using var childStorage=await SqlServerStorage.OpenAsync(connection,o=>{o.Schema=childSchema;o.SchemaManagement=SchemaManagement.ValidateOnly;});using var child=new BasaltEngine(childStorage,new BasaltOptions{WorkerCount=1,LeaseDurationSeconds=5});var childHandled=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    child.RegisterHandler<Payload>("process-job",async(job,ctx,ct)=>{await using var db=new SqlConnection(connection);await db.OpenAsync(ct);await using var cmd=db.CreateCommand();cmd.CommandText=$"INSERT INTO [{childSchema}].[ClaimEvidence]([ProcessId]) VALUES(@p)";cmd.Parameters.AddWithValue("@p",Environment.ProcessId);await cmd.ExecuteNonQueryAsync(ct);childHandled.TrySetResult(true);});
    await child.StartAsync();await Task.WhenAny(childHandled.Task,Task.Delay(TimeSpan.FromSeconds(12)));await child.StopAsync();return;
}
if(configured==null)
{
    var builder=new SqlConnectionStringBuilder(connection){InitialCatalog="master"};
    await using var admin=new SqlConnection(builder.ConnectionString);await admin.OpenAsync();
    await using var create=admin.CreateCommand();create.CommandText="IF DB_ID(N'BasaltIntegrationTests') IS NULL CREATE DATABASE [BasaltIntegrationTests];";await create.ExecuteNonQueryAsync();
}
string schema="BasaltIt_"+Environment.ProcessId;
await using(var app=new SqlConnection(connection)){await app.OpenAsync();await using var cmd=app.CreateCommand();cmd.CommandText="IF OBJECT_ID(N'dbo.BasaltForeignSentinel',N'U') IS NULL CREATE TABLE dbo.BasaltForeignSentinel(Id int NOT NULL PRIMARY KEY, Value nvarchar(20) NULL);";await cmd.ExecuteNonQueryAsync();}
using var storage=await SqlServerStorage.OpenAsync(connection,o=>{o.Schema=schema;o.SchemaManagement=SchemaManagement.AutoMigrate;});
using var engine1=new BasaltEngine(storage,new BasaltOptions{WorkerCount=1,LeaseDurationSeconds=10});
using var engine2=new BasaltEngine(storage,new BasaltOptions{WorkerCount=1,LeaseDurationSeconds=10});
int handled=0;var done=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
Func<Payload,JobContext,CancellationToken,Task> handler=(job,ctx,ct)=>{if(Interlocked.Increment(ref handled)==1)done.TrySetResult(true);return Task.CompletedTask;};
engine1.RegisterHandler("sql-job",handler);engine2.RegisterHandler("sql-job",handler);
ulong first=await engine1.EnqueueAsync("sql-job",new Payload{Value=42});
ulong duplicate=await engine1.EnqueueAsync("sql-idempotency",JobKeyForTest(),new byte[]{1,2,3},1,2);
ulong same=await engine1.EnqueueAsync("sql-idempotency",JobKeyForTest(),new byte[]{1,2,3},1,2);
if(duplicate!=same)throw new Exception("Idempotency did not return the original execution.");
await engine1.StartAsync();await engine2.StartAsync();
await done.Task.WaitAsync(TimeSpan.FromSeconds(20));await engine1.StopAsync();await engine2.StopAsync();
if(handled!=1)throw new Exception($"Expected one claim, observed {handled}.");
await using(var verify=new SqlConnection(connection)){await verify.OpenAsync();await using var cmd=verify.CreateCommand();cmd.CommandText=$"SELECT COUNT(*) FROM [{schema}].[Executions] WHERE [ExecutionId]=@id AND [State]=7; SELECT COUNT(*) FROM dbo.BasaltForeignSentinel;";cmd.Parameters.AddWithValue("@id",checked((long)first));await using var reader=await cmd.ExecuteReaderAsync();if(!await reader.ReadAsync()||reader.GetInt32(0)!=1)throw new Exception("SQL execution did not finish durably.");await reader.NextResultAsync();if(!await reader.ReadAsync())throw new Exception("Foreign table was changed.");}
Console.WriteLine($"SQL integration passed ({schema}, executions {first}/{duplicate}).");
string processSchema="BasaltProcesses_"+Environment.ProcessId;ulong processExecution;
using(var processStorage=await SqlServerStorage.OpenAsync(connection,o=>o.Schema=processSchema)){using var seed=new BasaltEngine(processStorage);processExecution=await seed.EnqueueAsync("process-job",new Payload{Value=77});await using var db=new SqlConnection(connection);await db.OpenAsync();await using var cmd=db.CreateCommand();cmd.CommandText=$"CREATE TABLE [{processSchema}].[ClaimEvidence]([Id] int IDENTITY PRIMARY KEY,[ProcessId] int NOT NULL)";await cmd.ExecuteNonQueryAsync();}
string host=Environment.ProcessPath??throw new Exception("Cannot resolve integration test host.");string assembly=Environment.GetCommandLineArgs()[0];
Process StartWorker(){var info=new ProcessStartInfo(host){UseShellExecute=false};if(Path.GetFileNameWithoutExtension(host).Equals("dotnet",StringComparison.OrdinalIgnoreCase))info.ArgumentList.Add(assembly);info.ArgumentList.Add("--worker");info.ArgumentList.Add(processSchema);info.Environment["BASALT_SQLSERVER_TEST_CONNECTION"]=connection;return Process.Start(info)??throw new Exception("Could not start SQL worker process.");}
using Process worker1=StartWorker(),worker2=StartWorker();int evidence=0,state=0;
for(int i=0;i<100;i++){await Task.Delay(100);await using var db=new SqlConnection(connection);await db.OpenAsync();await using var cmd=db.CreateCommand();cmd.CommandText=$"SELECT COUNT(*) FROM [{processSchema}].[ClaimEvidence]; SELECT [State] FROM [{processSchema}].[Executions] WHERE [ExecutionId]=@i";cmd.Parameters.AddWithValue("@i",checked((long)processExecution));await using var reader=await cmd.ExecuteReaderAsync();await reader.ReadAsync();evidence=reader.GetInt32(0);await reader.NextResultAsync();await reader.ReadAsync();state=reader.GetInt32(0);if(state==7)break;}
await Task.WhenAll(worker1.WaitForExitAsync(),worker2.WaitForExitAsync());if(worker1.ExitCode!=0||worker2.ExitCode!=0||evidence!=1||state!=7)throw new Exception($"Independent-process claim failed: evidence={evidence}, state={state}, exits={worker1.ExitCode}/{worker2.ExitCode}.");
Console.WriteLine($"SQL independent-process claim passed ({processSchema}).");
string fenceSchema="BasaltFence_"+Environment.ProcessId;using(var fenceStorage=await SqlServerStorage.OpenAsync(connection,o=>o.Schema=fenceSchema)){using(var seed=new BasaltEngine(fenceStorage)){await seed.EnqueueAsync("fence-job",new Payload{Value=88});}byte[] owner1=Enumerable.Repeat((byte)1,16).ToArray(),owner2=Enumerable.Repeat((byte)2,16).ToArray();if(fenceStorage.TestClaim(owner1,30,out var lease1)!=BasaltCore.Native.JobDbResult.Ok)throw new Exception("Initial SQL claim failed.");await using(var expire=new SqlConnection(connection)){await expire.OpenAsync();await using var cmd=expire.CreateCommand();cmd.CommandText=$"UPDATE [{fenceSchema}].[Executions] SET [LeaseExpiresAt]=0 WHERE [ExecutionId]=@i";cmd.Parameters.AddWithValue("@i",checked((long)lease1.Id));await cmd.ExecuteNonQueryAsync();}if(fenceStorage.TestClaim(owner2,30,out var lease2)!=BasaltCore.Native.JobDbResult.Ok||lease2.Id!=lease1.Id||lease2.FencingToken<=lease1.FencingToken)throw new Exception("Expired SQL lease was not fenced and reclaimed.");if(fenceStorage.TestFinalize(lease1.Id,owner1,lease1.FencingToken)!=BasaltCore.Native.JobDbResult.StaleLease)throw new Exception("Old SQL owner bypassed fencing.");if(fenceStorage.TestStart(lease2.Id,owner2,lease2.FencingToken)!=BasaltCore.Native.JobDbResult.Ok||fenceStorage.TestFinalize(lease2.Id,owner2,lease2.FencingToken)!=BasaltCore.Native.JobDbResult.Ok)throw new Exception("New SQL owner could not finalize.");}
Console.WriteLine("SQL lease reclaim and stale-owner fencing passed.");
string manualSchema="BasaltManual_"+Environment.ProcessId;var manualOptions=new SqlServerOptions{Schema=manualSchema,SchemaManagement=SchemaManagement.Manual};var manualManager=new SqlSchemaManager(new SqlServerConnectionFactory(connection),manualOptions);string manualScript=manualManager.GenerateMigrationScript();if(!manualScript.Contains("VALUES(2",StringComparison.Ordinal)||!manualScript.Contains("varbinary(max)",StringComparison.OrdinalIgnoreCase))throw new Exception("Manual migration script is incomplete.");
try{await manualManager.InitializeAsync();throw new Exception("Manual mode unexpectedly created a missing schema.");}catch(InvalidOperationException){}
await using(var verifyManual=new SqlConnection(connection)){await verifyManual.OpenAsync();await using var cmd=verifyManual.CreateCommand();cmd.CommandText="SELECT SCHEMA_ID(@s)";cmd.Parameters.AddWithValue("@s",manualSchema);if(await cmd.ExecuteScalarAsync()!=DBNull.Value)throw new Exception("Manual mode executed DDL.");}
string futureSchema="BasaltFuture_"+Environment.ProcessId;using(var future=await SqlServerStorage.OpenAsync(connection,o=>o.Schema=futureSchema)){}await using(var alter=new SqlConnection(connection)){await alter.OpenAsync();await using var cmd=alter.CreateCommand();cmd.CommandText=$"INSERT INTO [{futureSchema}].[SchemaHistory] VALUES(99,REPLICATE('0',64),'future',SYSUTCDATETIME())";await cmd.ExecuteNonQueryAsync();}
try{using var future=await SqlServerStorage.OpenAsync(connection,o=>{o.Schema=futureSchema;o.SchemaManagement=SchemaManagement.ValidateOnly;});throw new Exception("A future schema version was accepted.");}catch(InvalidOperationException ex)when(ex.Message.Contains("newer",StringComparison.OrdinalIgnoreCase)){}
Console.WriteLine("SQL manual/validate/future migration modes passed.");
string cancelSchema="BasaltCancel_"+Environment.ProcessId;using(var cancelling=Basalt.Create(o=>o.UseSqlServer(connection,s=>s.Schema=cancelSchema).ConfigureEngine(e=>{e.WorkerCount=1;e.LeaseDurationSeconds=1;}))){var entered=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);var cancelled=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);cancelling.RegisterHandler<Payload>("cancel-job",async(job,ctx,ct)=>{entered.TrySetResult(true);try{await Task.Delay(Timeout.Infinite,ct);}catch(OperationCanceledException){cancelled.TrySetResult(true);throw;}});ulong cancelId=await cancelling.EnqueueAsync("cancel-job",new Payload{Value=9});await cancelling.StartAsync();await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));cancelling.Cancel(cancelId);await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));await cancelling.StopAsync();if(cancelling.GetExecution(cancelId).State!=10)throw new Exception("Durable cancellation state was not preserved.");}
Console.WriteLine("SQL managed cancellation propagation passed.");
string unknownSchema="BasaltUnknown_"+Environment.ProcessId;bool loseAcknowledgement=true;using(var unknownStorage=await SqlServerStorage.OpenAsync(connection,o=>{o.Schema=unknownSchema;o.SimulateLostCommitAcknowledgement=()=>{if(!loseAcknowledgement)return false;loseAcknowledgement=false;return true;};})){using var unknownEngine=new BasaltEngine(unknownStorage);ulong proposed;try{await unknownEngine.EnqueueAsync("unknown-key",JobKeyForTest(),new byte[]{7,8,9});throw new Exception("Lost commit acknowledgement was reported as success.");}catch(BasaltUnknownCommitException ex){proposed=ex.ProposedExecutionId;if(ex.IdempotencyKey!="unknown-key")throw;}ulong resolved=await unknownEngine.EnqueueAsync("unknown-key",JobKeyForTest(),new byte[]{7,8,9});if(resolved!=proposed)throw new Exception("Unknown commit did not resolve to the committed execution.");}
Console.WriteLine("SQL unknown-commit idempotent resolution passed.");
string facadeSchema="BasaltFacade_"+Environment.ProcessId;
var opens=await Task.WhenAll(SqlServerStorage.OpenAsync(connection,o=>o.Schema=facadeSchema),SqlServerStorage.OpenAsync(connection,o=>o.Schema=facadeSchema));foreach(var opened in opens)opened.Dispose();
using(var validated=await SqlServerStorage.OpenAsync(connection,o=>{o.Schema=facadeSchema;o.SchemaManagement=SchemaManagement.ValidateOnly;})){}
using var basalt=Basalt.Create(o=>o.UseSqlServer(connection,s=>{s.Schema=facadeSchema;s.SchemaManagement=SchemaManagement.ValidateOnly;}));
int facadeHandled=0;var facadeDone=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
basalt.RegisterHandler<Payload>("facade-job",(job,ctx,ct)=>{if(Interlocked.Increment(ref facadeHandled)>=4)facadeDone.TrySetResult(true);return Task.CompletedTask;});
ulong facadeExecution=await basalt.EnqueueAsync("facade-job",new Payload{Value=1},new EnqueueOptions{IdempotencyKey="facade-retry-idempotency",Retry=RetryOptions.Fixed(2,TimeSpan.FromSeconds(1))});
ulong facadeDuplicate=await basalt.EnqueueAsync("facade-job",new Payload{Value=1},new EnqueueOptions{IdempotencyKey="facade-retry-idempotency",Retry=RetryOptions.Fixed(2,TimeSpan.FromSeconds(1))});
if(facadeExecution!=facadeDuplicate)throw new Exception("Atomic retry/idempotency enqueue did not resolve to one execution.");
await basalt.ScheduleAsync("once",new Payload{Value=2},s=>s.Delay(TimeSpan.FromMilliseconds(100)).Until(DateTimeOffset.UtcNow.AddHours(1)));
if(basalt.GetSchedule("once").EndAt==0)throw new Exception("Typed schedule EndAt was not persisted.");basalt.PauseSchedule("once");if(basalt.GetSchedule("once").Enabled)throw new Exception("Schedule pause convenience failed.");basalt.ResumeSchedule("once");
await basalt.Workflow("wf").Add("first",new Payload{Value=3}).Then("second",new Payload{Value=4}).SubmitAsync();
await basalt.StartAsync();await facadeDone.Task.WaitAsync(TimeSpan.FromSeconds(20));await basalt.StopAsync();
if(basalt.GetExecution(facadeExecution).State!=7||basalt.ListExecutions(2).Count!=2||basalt.GetStats().CompletedTotal<4||basalt.GetLedger(facadeExecution).FinalState!=7)throw new Exception("Storage-neutral SQL management returned inconsistent state.");basalt.VerifyHealth();
Console.WriteLine($"SQL facade/schedule/workflow passed ({facadeSchema}).");
static ulong JobKeyForTest(){const string value="sql-raw";ulong h=1469598103934665603UL;foreach(byte b in System.Text.Encoding.UTF8.GetBytes(value)){h^=b;h*=1099511628211UL;}return h==0?1:h;}
public sealed class Payload{public int Value{get;set;}}
