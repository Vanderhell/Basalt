using System.Runtime.Serialization;
using BasaltCore;
using Microsoft.Data.SqlClient;

var database = "BasaltObservabilitySmoke_" + Guid.NewGuid().ToString("N");
var master = @"Server=.\SQLEXPRESS;Database=master;Integrated Security=true;Encrypt=false";
var connection = @"Server=.\SQLEXPRESS;Database=" + database + @";Integrated Security=true;Encrypt=false";

try
{
    await using (var db = new SqlConnection(master))
    {
        await db.OpenAsync();
        await using var command = db.CreateCommand();
        command.CommandText = "CREATE DATABASE [" + database + "]";
        await command.ExecuteNonQueryAsync();
    }

    using (var basalt = Basalt.SqlServer(connection, options => options.Schema = "basalt"))
    {
        var handled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        basalt.On<ProbeJob>("sql.observability.probe", (_, _, _) => { handled.TrySetResult(true); return Task.CompletedTask; });
        ulong executionId = await basalt.EnqueueAsync(new ProbeJob(1));
        await basalt.StartAsync();
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await WaitForAsync(() => basalt.GetExecution(executionId).State == ExecutionState.Done, TimeSpan.FromSeconds(10));
        Require(basalt.GetQueueStats().Done == 1, "SQL queue aggregate did not report completion.");
        Require(basalt.ListExecutions(new ExecutionQuery { States = new[] { ExecutionState.Done }, Take = 10 }).Single().JobKey == "sql.observability.probe", "SQL filtered execution did not resolve its job key.");
        Require(basalt.ListWorkers().Any(x => x.LastHeartbeatAt.HasValue && x.MachineName != null && x.ProcessId.HasValue), "SQL worker heartbeat was not observable.");
        await basalt.StopAsync();
    }

    Console.WriteLine("SQL observability smoke passed");
}
finally
{
    await using var db = new SqlConnection(master);
    await db.OpenAsync();
    await using var command = db.CreateCommand();
    command.CommandText = "IF DB_ID(N'" + database + "') IS NOT NULL BEGIN ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + database + "] END";
    await command.ExecuteNonQueryAsync();
}

static void Require(bool value, string message)
{
    if (!value) throw new InvalidOperationException(message);
}

static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    while (DateTime.UtcNow < deadline)
    {
        if (condition()) return;
        await Task.Delay(50);
    }
    throw new TimeoutException("Durable execution did not reach the expected state.");
}

[DataContract]
public sealed class ProbeJob
{
    public ProbeJob() { }
    public ProbeJob(int value) => Value = value;
    [DataMember] public int Value { get; set; }
}
