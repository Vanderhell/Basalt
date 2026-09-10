using BasaltCore;
using System.Runtime.Serialization;

var path = Path.Combine(Path.GetTempPath(), "basalt-observability-" + Guid.NewGuid().ToString("N"));
try
{
    ulong executionId;
    using (var basalt = Basalt.Embedded(path))
    {
        basalt.On<ProbeJob>("diagnostics.probe", (_, _, _) => Task.CompletedTask);

        executionId = await basalt.EnqueueAsync(new ProbeJob(1));
        var queue = basalt.GetQueueStats();
        Require(queue.Ready == 1, "Expected one ready execution.");

        var page = basalt.ListExecutions(new ExecutionQuery { States = new[] { ExecutionState.Ready }, Take = 10 });
        Require(page.Count == 1 && page[0].ExecutionId == executionId, "State query did not return the queued execution.");
        Require(page[0].JobKey == "diagnostics.probe", "Registered stable job key was not resolved.");

        await basalt.EveryAsync("diagnostics.schedule", TimeSpan.FromMinutes(5), new ProbeJob(2));
        var schedules = basalt.ListSchedules();
        Require(schedules.Count == 1 && schedules[0].ScheduleKey == "diagnostics.schedule", "Schedule list did not return the stable schedule key.");

        await basalt.Workflow("diagnostics.workflow").Add("first", new ProbeJob(3)).Then("second", new ProbeJob(4)).SubmitAsync();
        var workflows = basalt.ListWorkflows();
        Require(workflows.Count == 1 && workflows[0].WorkflowKey == "diagnostics.workflow" && workflows[0].NodeCount == 2, "Workflow list did not return the submitted workflow.");

        var health = basalt.GetHealth();
        Require(health.Status == BasaltHealthStatus.Healthy && health.Provider == "Embedded", "Embedded health snapshot is not healthy.");
    }

    using (var observer = Basalt.Embedded(path))
    {
        Require(observer.GetExecution(executionId).JobKey == "diagnostics.probe", "Persisted job key was not available after reopen.");
        Require(observer.ListSchedules().Single().ScheduleKey == "diagnostics.schedule", "Persisted schedule key was not available after reopen.");
        Require(observer.ListWorkflows().Single().WorkflowKey == "diagnostics.workflow", "Persisted workflow key was not available after reopen.");
    }
    Console.WriteLine("Basalt observability smoke passed");
}
finally
{
    if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
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
