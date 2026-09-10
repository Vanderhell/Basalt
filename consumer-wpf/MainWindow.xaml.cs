using System;
using System.IO;
using System.Threading.Tasks;
using System.Windows;
using BasaltCore;

namespace BasaltConsumerWpf;

public partial class MainWindow : Window
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), "basalt-wpf-consumer");
    private BasaltApplication? _basalt;
    private bool _started;

    public MainWindow()
    {
        InitializeComponent();
        Loaded += async (_, _) => await RunScenario();
        Closed += (_, _) => ShutdownBasalt();
    }

    private async Task RunScenario()
    {
        try
        {
            _basalt = Basalt.Embedded(_path);

            _basalt.On<GenerateReportJob>("reports.generate", GenerateReport);
            _basalt.On<SendEmailJob>("email.send", SendEmail);
            _basalt.On<CleanupJob>("maintenance.cleanup", Cleanup);

            // Cancel/requeue is deterministic before workers start claiming work.
            var recoverableId = await _basalt.EnqueueAsync(new CleanupJob { KeepDays = 30 });
            _basalt.Cancel(recoverableId);
            _basalt.Requeue(recoverableId);

            await _basalt.StartAsync();
            _started = true;

            var reportId = await _basalt.EnqueueAsync(new GenerateReportJob { ReportId = 42 });
            await _basalt.EnqueueAsync(
                new SendEmailJob { ReportId = 42, Address = "team@example.com" },
                key: "report-email:42",
                retry: 5);

            await _basalt.EveryAsync(
                "cleanup",
                TimeSpan.FromHours(6),
                new CleanupJob { KeepDays = 30 });

            await _basalt.Workflow("monthly-report")
                .Add("generate", new GenerateReportJob { ReportId = 43 })
                .Then("email", new SendEmailJob { ReportId = 43, Address = "team@example.com" })
                .SubmitAsync();

            var execution = _basalt.GetExecution(reportId);
            var stats = _basalt.GetStats();
            Status.Text = "Report " + execution.ExecutionId + " is " + execution.State
                + "; submitted: " + stats.SubmittedTotal;
        }
        catch (Exception ex)
        {
            Status.Text = "Basalt error: " + ex.GetType().Name;
        }
    }

    private static Task GenerateReport(GenerateReportJob job, JobContext context, System.Threading.CancellationToken ct)
    {
        // Generate the report idempotently using context.ExecutionId as the operation identity.
        return Task.CompletedTask;
    }

    private static Task SendEmail(SendEmailJob job, JobContext context, System.Threading.CancellationToken ct)
    {
        // The external email operation should use an idempotency key.
        return Task.CompletedTask;
    }

    private static Task Cleanup(CleanupJob job, JobContext context, System.Threading.CancellationToken ct)
        => Task.CompletedTask;

    private void ShutdownBasalt()
    {
        if (_basalt == null) return;
        try
        {
            if (_started) _basalt.StopAsync().GetAwaiter().GetResult();
        }
        finally
        {
            _basalt.Dispose();
            _basalt = null;
            TryDelete(_path);
        }
    }

    public sealed class GenerateReportJob { public int ReportId { get; set; } }
    public sealed class SendEmailJob { public int ReportId { get; set; } public string Address { get; set; } = ""; }
    public sealed class CleanupJob { public int KeepDays { get; set; } }

    private static void TryDelete(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, true); } catch { }
    }
}
