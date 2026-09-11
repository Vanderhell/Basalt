using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using BasaltCore;
using BasaltCore.SqlServer;

namespace BasaltConsumerWpf;

/// <summary>A real net472/WPF consumer. It intentionally uses only the public managed Basalt API.</summary>
public partial class MainWindow : Window
{
    private static int _emailAttempts;
    private readonly string _embeddedPath = Path.Combine(Path.GetTempPath(), "basalt-wpf-demo");
    public MainWindow() { InitializeComponent(); ConnectionString.Text = Environment.GetEnvironmentVariable("BASALT_DEMO_SQL") ?? string.Empty; if (!string.IsNullOrWhiteSpace(ConnectionString.Text)) Provider.SelectedIndex = 1; }

    private async void RunClick(object sender, RoutedEventArgs e)
    {
        RunButton.IsEnabled = false; Output.Clear();
        try { await RunCompleteDemoAsync(); Status.Text = "PASS — every public demo operation completed."; }
        catch (Exception error) { Status.Text = "FAIL — " + error.GetType().Name; Write(error.ToString()); }
        finally { RunButton.IsEnabled = true; }
    }

    internal Task RunEmbeddedSmokeAsync()
    {
        Provider.SelectedIndex = 0;
        return RunCompleteDemoAsync();
    }
    internal Task RunSqlSmokeAsync()
    {
        Provider.SelectedIndex = 1;
        return RunCompleteDemoAsync();
    }

    private async Task RunCompleteDemoAsync()
    {
        bool sql = ((ComboBoxItem)Provider.SelectedItem).Content.ToString() == "SQL Server";
        if (sql && string.IsNullOrWhiteSpace(ConnectionString.Text)) throw new InvalidOperationException("Enter a SQL Server connection string or use Embedded.");
        string runKey = "wpf-demo-" + Guid.NewGuid().ToString("N");
        Interlocked.Exchange(ref _emailAttempts, 0);
        BasaltApplication basalt = Open(sql);
        try
        {
            Register(basalt);
            ulong recoverable = await basalt.EnqueueAsync(new CleanupJob { KeepDays = 30 });
            basalt.Cancel(recoverable); basalt.Requeue(recoverable);
            await basalt.StartAsync();
            ulong report = await basalt.EnqueueAsync(new GenerateReportJob { ReportId = 42 });
            var options = new EnqueueOptions { IdempotencyKey = runKey + ":email", Retry = RetryOptions.Exponential(3, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1)) };
            ulong emailOne = await basalt.EnqueueAsync(new SendEmailJob { ReportId = 42, Address = "team@example.com" }, options);
            ulong emailTwo = await basalt.EnqueueAsync(new SendEmailJob { ReportId = 42, Address = "team@example.com" }, options);
            Require(emailOne == emailTwo, "Idempotent enqueue returned different executions.");
            await basalt.AtAsync(runKey + ":at", DateTimeOffset.UtcNow.AddMinutes(10), new CleanupJob { KeepDays = 7 });
            await basalt.EveryAsync(runKey + ":every", TimeSpan.FromHours(1), new CleanupJob { KeepDays = 14 });
            await basalt.DailyAsync(runKey + ":daily", 3, 30, new CleanupJob { KeepDays = 21 });
            await basalt.ScheduleAsync(runKey + ":cron", new CleanupJob { KeepDays = 28 }, s => s.Cron("0 2 * * *").InTimeZone("UTC").OnOverlap(OverlapPolicy.Skip).OnMisfire(MisfirePolicy.RunOnce));
            await basalt.Workflow(runKey + ":workflow").Add("report", new GenerateReportJob { ReportId = 43 }).Then("email", new SendEmailJob { ReportId = 43, Address = "team@example.com" }).AddAfter("cleanup", new CleanupJob { KeepDays = 30 }, "report", "email").SubmitAsync();
            BasaltScheduleInfo schedule = basalt.GetSchedule(runKey + ":every");
            basalt.PauseSchedule(schedule.ScheduleId); Require(basalt.GetSchedule(schedule.ScheduleId).Status == "Paused", "Pause was not persisted.");
            basalt.ResumeSchedule(schedule.ScheduleId); basalt.RemoveSchedule(schedule.ScheduleId);
            await WaitForAsync(() => basalt.GetExecution(report).State == ExecutionState.Done, TimeSpan.FromSeconds(10));
            await WaitForAsync(() => basalt.GetExecution(emailOne).State == ExecutionState.Done, TimeSpan.FromSeconds(10));
            Require(basalt.GetExecution(emailOne).Attempt >= 2, "The configured retry did not execute.");
            BasaltHealth health = basalt.GetHealth(); BasaltQueueStats queue = basalt.GetQueueStats(); BasaltPerformanceStats performance = basalt.GetPerformanceStats();
            BasaltWorkflowStatus workflow = basalt.GetWorkflow(runKey + ":workflow"); var nodes = basalt.ListWorkflowNodes(workflow.WorkflowId);
            Require(nodes.Count == 3 && nodes.Single(x => x.Name == "cleanup").Dependencies.Count == 2, "Workflow DAG detail is incomplete.");
            Require(basalt.ListExecutions(new ExecutionQuery { JobDefinitionId = basalt.GetExecution(report).JobDefinitionId, Take = 100 }).Any(), "Execution query returned no job.");
            Require(basalt.ListWorkers().Any(x => x.LastHeartbeatAt.HasValue), "Worker monitoring is unavailable.");
            Write("Provider: " + health.Provider + "; health: " + health.Status);
            Write("Queue ready/done: " + queue.Ready + "/" + queue.Done + "; success: " + performance.SuccessRate.ToString("P1"));
            Write("Workflow nodes: " + string.Join(", ", nodes.Select(n => n.Name + "=" + n.State)));
            await basalt.StopAsync();
        }
        finally { basalt.Dispose(); }
        using (BasaltApplication reopened = Open(sql))
        {
            Register(reopened);
            Require(reopened.ListExecutions(new ExecutionQuery { Take = 1 }).Any(), "Reopen did not expose durable work.");
            Write("Restart/reopen: durable work and management API available.");
        }
    }

    private BasaltApplication Open(bool sql) => sql ? Basalt.SqlServer(ConnectionString.Text, o => o.SchemaManagement = SchemaManagement.AutoMigrate) : Basalt.Embedded(_embeddedPath);
    private static void Register(BasaltApplication basalt) { basalt.On<GenerateReportJob>("reports.generate", (job, context, ct) => Task.CompletedTask); basalt.On<SendEmailJob>("email.send", (job, context, ct) => { if (Interlocked.Increment(ref _emailAttempts) == 1) throw new InvalidOperationException("Demo retry."); return Task.CompletedTask; }); basalt.On<CleanupJob>("maintenance.cleanup", (job, context, ct) => Task.CompletedTask); }
    private static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout) { DateTime deadline = DateTime.UtcNow + timeout; while (DateTime.UtcNow < deadline) { if (condition()) return; await Task.Delay(50); } throw new TimeoutException("A demo execution did not reach Done."); }
    private void Write(string value) => Output.AppendText(value + Environment.NewLine);
    private static void Require(bool value, string message) { if (!value) throw new InvalidOperationException(message); }
    public sealed class GenerateReportJob { public int ReportId { get; set; } }
    public sealed class SendEmailJob { public int ReportId { get; set; } public string Address { get; set; } = string.Empty; }
    public sealed class CleanupJob { public int KeepDays { get; set; } }
}
