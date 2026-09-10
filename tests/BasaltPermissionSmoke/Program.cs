using System.Runtime.Serialization;
using BasaltCore;
using BasaltCore.SqlServer;
using Microsoft.Data.SqlClient;

const string schema = "basalt_permission_runtime";
const string database = "BasaltPermissionTests";
const string server = @".\SQLEXPRESS";
string connection = $"Server={server};Database={database};Integrated Security=true;Encrypt=false";

if (args.SingleOrDefault() == "--migrate")
{
    using var basalt = Basalt.SqlServer(connection, options => { options.Schema = schema; options.SchemaManagement = SchemaManagement.AutoMigrate; });
    await using var db = new SqlConnection(connection);
    await db.OpenAsync();
    await using var command = db.CreateCommand();
    command.CommandText = $"IF OBJECT_ID(N'dbo.BasaltPermissionSentinel',N'U') IS NULL CREATE TABLE dbo.BasaltPermissionSentinel(Id int NOT NULL PRIMARY KEY, Value nvarchar(20) NULL); GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::{schema} TO [VANDERHELL\\BasaltRuntime]; GRANT UPDATE ON OBJECT::{schema}.[ExecutionIds] TO [VANDERHELL\\BasaltRuntime];";
    await command.ExecuteNonQueryAsync();
    Console.WriteLine("Permission smoke schema migrated and runtime grants applied.");
    return;
}

if (args.SingleOrDefault() == "--cleanup")
{
    await using var db = new SqlConnection(connection);
    await db.OpenAsync();
    await using var command = db.CreateCommand();
    command.CommandText = $"IF SCHEMA_ID(N'{schema}') IS NOT NULL BEGIN DROP TABLE IF EXISTS [{schema}].[Ledger]; DROP TABLE IF EXISTS [{schema}].[Receipts]; DROP TABLE IF EXISTS [{schema}].[Executions]; DROP TABLE IF EXISTS [{schema}].[Schedules]; DROP TABLE IF EXISTS [{schema}].[Records]; DROP TABLE IF EXISTS [{schema}].[Stats]; DROP TABLE IF EXISTS [{schema}].[SchemaHistory]; DROP SEQUENCE IF EXISTS [{schema}].[ExecutionIds]; DROP SCHEMA [{schema}]; END IF OBJECT_ID(N'dbo.BasaltPermissionSentinel',N'U') IS NOT NULL DROP TABLE dbo.BasaltPermissionSentinel;";
    await command.ExecuteNonQueryAsync();
    Console.WriteLine("Permission smoke schema and sentinel removed.");
    return;
}

using (var basalt = Basalt.SqlServer(connection, options => { options.Schema = schema; options.SchemaManagement = SchemaManagement.ValidateOnly; }))
{
    int attempts = 0;
    var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    basalt.On<ProbeJob>("permission.probe", (_, _, _) =>
    {
        if (Interlocked.Increment(ref attempts) == 1) throw new InvalidOperationException("retry");
        done.TrySetResult(true); return Task.CompletedTask;
    });
    ulong execution = await basalt.EnqueueAsync(new ProbeJob(1), key: "permission.probe:1", retry: 2);
    Require(execution == await basalt.EnqueueAsync(new ProbeJob(1), key: "permission.probe:1", retry: 2), "Idempotent enqueue did not resolve.");
    await basalt.StartAsync();
    await done.Task.WaitAsync(TimeSpan.FromSeconds(20));
    await WaitForAsync(() => basalt.GetExecution(execution).State == ExecutionState.Done, TimeSpan.FromSeconds(10));
    Require(attempts == 2, "Retry did not execute exactly once after the first failure.");
    Require(basalt.ListExecutions(new ExecutionQuery { States = new[] { ExecutionState.Done }, Take = 10 }).Any(x => x.ExecutionId == execution), "Runtime identity could not read management state.");
    Require(basalt.GetHealth().Status == BasaltHealthStatus.Healthy, "Runtime identity health check failed.");
    await basalt.StopAsync();
}

bool foreignDenied;
try
{
    await using var db = new SqlConnection(connection);
    await db.OpenAsync();
    await using var command = db.CreateCommand();
    command.CommandText = "UPDATE dbo.BasaltPermissionSentinel SET Value=N'blocked' WHERE Id=1";
    await command.ExecuteNonQueryAsync();
    foreignDenied = false;
}
catch (SqlException)
{
    foreignDenied = true;
}
Require(foreignDenied, "Runtime identity unexpectedly modified a foreign dbo table.");
Console.WriteLine("Restricted SQL runtime permission smoke passed.");

static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    while (DateTime.UtcNow < deadline)
    {
        if (condition()) return;
        await Task.Delay(50);
    }
    throw new TimeoutException("Durable execution did not reach Done.");
}

static void Require(bool value, string message)
{
    if (!value) throw new InvalidOperationException(message);
}

[DataContract]
public sealed class ProbeJob
{
    public ProbeJob() { }
    public ProbeJob(int value) => Value = value;
    [DataMember] public int Value { get; set; }
}
