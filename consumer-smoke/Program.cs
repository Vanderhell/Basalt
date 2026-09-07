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
