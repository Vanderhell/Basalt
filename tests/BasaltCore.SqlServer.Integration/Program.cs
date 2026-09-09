using BasaltCore;
using BasaltCore.SqlServer;
using Microsoft.Data.SqlClient;

string? configured=Environment.GetEnvironmentVariable("BASALT_SQLSERVER_TEST_CONNECTION");
string connection=configured??@"Server=.\SQLEXPRESS;Database=BasaltIntegrationTests;Integrated Security=true;Encrypt=false";
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
string facadeSchema="BasaltFacade_"+Environment.ProcessId;
var opens=await Task.WhenAll(SqlServerStorage.OpenAsync(connection,o=>o.Schema=facadeSchema),SqlServerStorage.OpenAsync(connection,o=>o.Schema=facadeSchema));foreach(var opened in opens)opened.Dispose();
using(var validated=await SqlServerStorage.OpenAsync(connection,o=>{o.Schema=facadeSchema;o.SchemaManagement=SchemaManagement.ValidateOnly;})){}
using var basalt=Basalt.Create(o=>o.UseSqlServer(connection,s=>{s.Schema=facadeSchema;s.SchemaManagement=SchemaManagement.ValidateOnly;}));
int facadeHandled=0;var facadeDone=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
basalt.RegisterHandler<Payload>("facade-job",(job,ctx,ct)=>{if(Interlocked.Increment(ref facadeHandled)>=4)facadeDone.TrySetResult(true);return Task.CompletedTask;});
ulong facadeExecution=await basalt.EnqueueAsync("facade-job",new Payload{Value=1},new EnqueueOptions{IdempotencyKey="facade-retry-idempotency",Retry=RetryOptions.Fixed(2,TimeSpan.FromSeconds(1))});
ulong facadeDuplicate=await basalt.EnqueueAsync("facade-job",new Payload{Value=1},new EnqueueOptions{IdempotencyKey="facade-retry-idempotency",Retry=RetryOptions.Fixed(2,TimeSpan.FromSeconds(1))});
if(facadeExecution!=facadeDuplicate)throw new Exception("Atomic retry/idempotency enqueue did not resolve to one execution.");
await basalt.ScheduleAsync("once",new Payload{Value=2},s=>s.Delay(TimeSpan.FromMilliseconds(100)));
await basalt.Workflow("wf").Add("first",new Payload{Value=3}).Then("second",new Payload{Value=4}).SubmitAsync();
await basalt.StartAsync();await facadeDone.Task.WaitAsync(TimeSpan.FromSeconds(20));await basalt.StopAsync();
if(basalt.GetExecution(facadeExecution).State!=7||basalt.GetStats().CompletedTotal<4||basalt.GetLedger(facadeExecution).FinalState!=7)throw new Exception("Storage-neutral SQL management returned inconsistent state.");basalt.VerifyHealth();
Console.WriteLine($"SQL facade/schedule/workflow passed ({facadeSchema}).");
static ulong JobKeyForTest(){const string value="sql-raw";ulong h=1469598103934665603UL;foreach(byte b in System.Text.Encoding.UTF8.GetBytes(value)){h^=b;h*=1099511628211UL;}return h==0?1:h;}
public sealed class Payload{public int Value{get;set;}}
