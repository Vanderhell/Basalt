using BasaltCore;
using BasaltCore.SqlServer;

string root = Path.Combine(Path.GetTempPath(), "basalt-simplified-api-" + Guid.NewGuid().ToString("N"));
try
{
    await RunScenario(() => Basalt.Embedded(root), "Embedded");

    string? sqlConnection = Environment.GetEnvironmentVariable("BASALT_SQL_CONNECTION");
    if (!string.IsNullOrWhiteSpace(sqlConnection))
    {
        string schema = Environment.GetEnvironmentVariable("BASALT_SQL_SCHEMA") ?? "BasaltApiSmoke";
        await RunScenario(() => Basalt.SqlServer(sqlConnection, sql =>
        {
            sql.Schema = schema;
            sql.SchemaManagement = SchemaManagement.AutoMigrate;
        }), "SQL Server");
    }
}
finally
{
    if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
}

static async Task RunScenario(Func<BasaltApplication> create, string provider)
{
    int attempts = 0;
    ulong durableId;
    using (var basalt = create())
    {
        basalt.On<SmokeJob>("smoke.job", (job, context, ct) => Task.CompletedTask);
        basalt.On<RetryJob>("smoke.retry", (job, context, ct) =>
        {
            if (Interlocked.Increment(ref attempts) == 1) throw new InvalidOperationException("expected first-attempt failure");
            return Task.CompletedTask;
        });

        durableId = await basalt.EnqueueAsync(new SmokeJob { Value = 1 }, key: provider + ":durable");
        ulong sameId = await basalt.EnqueueAsync(new SmokeJob { Value = 1 }, key: provider + ":durable");
        if (sameId != durableId) throw new Exception(provider + " idempotency mismatch");

        ulong cancelled = await basalt.EnqueueAsync(new SmokeJob { Value = 2 });
        basalt.Cancel(cancelled);
        if (basalt.GetExecution(cancelled).State != ExecutionState.Cancelled) throw new Exception(provider + " cancel failed");
        basalt.Requeue(cancelled);
        if (basalt.GetExecution(cancelled).State != ExecutionState.Ready) throw new Exception(provider + " requeue failed before start");

        ulong retryId = await basalt.EnqueueAsync(new RetryJob(), retry: 2);

        await basalt.EveryAsync("interval", TimeSpan.FromHours(1), new SmokeJob());
        basalt.Pause("interval");
        basalt.Resume("interval");
        basalt.Remove("interval");
        await basalt.DailyAsync("daily", 2, 0, new SmokeJob(), "UTC");
        basalt.Remove("daily");
        await basalt.AtAsync("once", DateTimeOffset.UtcNow.AddHours(1), new SmokeJob());
        basalt.Remove("once");

        await basalt.Workflow("smoke-flow")
            .Add("first", new SmokeJob { Value = 3 })
            .Then("second", new SmokeJob { Value = 4 })
            .SubmitAsync();

        await basalt.StartAsync();
        await WaitForTerminal(basalt, durableId);
        await WaitForTerminal(basalt, cancelled);
        await WaitForTerminal(basalt, retryId);
        await basalt.StopAsync();

        if (attempts != 2 || basalt.GetExecution(retryId).State != ExecutionState.Done)
            throw new Exception(provider + " retry failed");
        if (basalt.ListExecutions().Count == 0 || basalt.GetWorkflow("smoke-flow").NodeCount != 2)
            throw new Exception(provider + " management failed");
        _ = basalt.GetStats();
        _ = basalt.GetLedger(durableId);
        basalt.VerifyHealth();
    }

    using (var reopened = create())
    {
        reopened.On<SmokeJob>("smoke.job", (job, context, ct) => Task.CompletedTask);
        reopened.On<RetryJob>("smoke.retry", (job, context, ct) => Task.CompletedTask);
        if (reopened.GetExecution(durableId).State != ExecutionState.Done)
            throw new Exception(provider + " reopen lost durable execution");
        await reopened.StartAsync();
        await reopened.StopAsync();
    }

    Console.WriteLine(provider + " simplified public API passed");
}

static async Task WaitForTerminal(BasaltApplication basalt, ulong id)
{
    for (int i = 0; i < 1200; i++)
    {
        BasaltExecutionInfo? execution = basalt.ListExecutions().FirstOrDefault(item => item.ExecutionId == id);
        if (execution != null && execution.State is ExecutionState.Done or ExecutionState.Failed or ExecutionState.Dead or ExecutionState.Cancelled) return;
        await Task.Delay(25);
    }
    throw new TimeoutException("Execution did not reach a terminal state: " + id);
}

public sealed class SmokeJob { public int Value { get; set; } }
public sealed class RetryJob { }

// Compile-only coverage for retained long-form and connection-factory APIs.
static class CompatibilitySurface
{
    public static async Task Compile(string path, string connectionString)
    {
        using var embedded = Basalt.Create(options => options.UseEmbedded(path));
        embedded.RegisterHandler<SmokeJob>("compat.job", (job, context, ct) => Task.CompletedTask);
        await embedded.EnqueueAsync("compat.job", new SmokeJob(), new EnqueueOptions());
        embedded.PauseSchedule("compat.schedule");
        embedded.ResumeSchedule("compat.schedule");
        embedded.RemoveSchedule("compat.schedule");

        using var sql = Basalt.Create(options => options.UseSqlServer(connectionString));
        using var factory = Basalt.SqlServer(() => new Microsoft.Data.SqlClient.SqlConnection(connectionString));
    }
}
