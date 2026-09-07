using BasaltCore;

var path = Path.Combine(Path.GetTempPath(), "basalt-consumer-" + Guid.NewGuid().ToString("N"));
var backup = path + "-backup";
var jobType = JobKeyFor("consumer.job");
try
{
    using (var database = BasaltDatabase.OpenOrCreate(path))
    using (var engine = new BasaltEngine(database))
    {
        engine.RegisterRawHandler(jobType, (payload, version) => 0);
        engine.Start();
        var id = await engine.EnqueueAsync("consumer-idempotency", jobType, new byte[] { 1, 2, 3 }, 1, 1);
        var same = await engine.EnqueueAsync("consumer-idempotency", jobType, new byte[] { 1, 2, 3 }, 1, 1);
        if (id != same) throw new Exception("idempotency receipt mismatch");
        _ = await engine.EnqueueRetryAsync(jobType, new byte[] { 4 }, 1, 2, 1);
        engine.CreateSchedule(9001, jobType, 2, DateTimeOffset.UtcNow.AddMinutes(5), TimeSpan.Zero, 1, 1, new byte[] { 5 });
        _ = engine.GetSchedule(9001);
        _ = engine.ListScheduleIds();
        var scheduleRevision = engine.GetSchedule(9001).Revision;
        engine.PauseSchedule(9001, scheduleRevision);
        engine.ResumeSchedule(9001, engine.GetSchedule(9001).Revision);
        engine.RemoveSchedule(9001, engine.GetSchedule(9001).Revision);
        engine.SubmitWorkflow(9100, new[] { new BasaltWorkflowNode { NodeId = 1, JobType = jobType, Payload = new byte[] { 6 } } });
        _ = engine.GetWorkflow(9100);
        engine.CancelWorkflow(9100);
        var info = engine.GetExecution(id);
        if (info.State == 0) throw new Exception("execution inspection failed");
        for (var i = 0; i < 250 && engine.GetExecution(id).State < 7; i++) await Task.Delay(20);
        if (engine.GetExecution(id).State == 7 && engine.ListLedgerExecutionIds().Contains(id) && engine.GetLedger(id).FinalState != 7) throw new Exception("ledger inspection failed");
        _ = engine.GetStats();
        _ = engine.ListExecutionIds();
        _ = engine.ListLedgerExecutionIds();
        _ = engine.ListScheduleIds();
        engine.VerifyHealth();
        engine.Stop();
        database.Backup(backup);
    }
    BasaltDatabase.Verify(backup);
    Console.WriteLine("clean NuGet consumer passed");
}
finally
{
    if (Directory.Exists(path)) Directory.Delete(path, true);
    if (Directory.Exists(backup)) Directory.Delete(backup, true);
}

static ulong JobKeyFor(string value)
{
    ulong hash = 1469598103934665603UL;
    foreach (var b in System.Text.Encoding.UTF8.GetBytes(value)) { hash ^= b; hash *= 1099511628211UL; }
    return hash == 0 ? 1 : hash;
}
