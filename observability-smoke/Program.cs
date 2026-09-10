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
        basalt.PauseSchedule(schedules[0].ScheduleId);
        Require(!basalt.GetSchedule(schedules[0].ScheduleId).Enabled, "Pause management action did not persist.");
        basalt.ResumeSchedule(schedules[0].ScheduleId);
        Require(basalt.GetSchedule(schedules[0].ScheduleId).Enabled, "Resume management action did not persist.");

        await basalt.Workflow("diagnostics.workflow").Add("first", new ProbeJob(3)).Then("second", new ProbeJob(4)).SubmitAsync();
        var workflows = basalt.ListWorkflows();
        Require(workflows.Count == 1 && workflows[0].WorkflowKey == "diagnostics.workflow" && workflows[0].NodeCount == 2, "Workflow list did not return the submitted workflow.");
        Require(basalt.ListExecutions(new ExecutionQuery { WorkflowId = workflows[0].WorkflowId, Take = 10 }).Count == 2, "Workflow execution filter did not return workflow nodes.");
        Require(basalt.ListExecutions(new ExecutionQuery { JobDefinitionId = page[0].JobDefinitionId, Take = 10 }).Count >= 1, "Job execution filter did not return the registered job.");
        Require(basalt.ListExecutions(new ExecutionQuery { CreatedFrom = DateTimeOffset.UtcNow.AddMinutes(1), Take = 10 }).Count == 0, "Creation time filter did not exclude older work.");
        var first = basalt.ListExecutions(new ExecutionQuery { Take = 1 }).Single();
        Require(basalt.ListExecutions(new ExecutionQuery { AfterExecutionId = first.ExecutionId, Take = 10 }).Count >= 1, "Execution pagination did not advance after the first ID.");

        var health = basalt.GetHealth();
        Require(health.Status == BasaltHealthStatus.Healthy && health.Provider == "Embedded", "Embedded health snapshot is not healthy.");
        await basalt.StartAsync();
        Require(basalt.ListWorkers().Any(x => x.LastHeartbeatAt.HasValue && x.MachineName != null && x.ProcessId.HasValue), "Worker heartbeat registration was not observable.");
        await basalt.StopAsync();
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
