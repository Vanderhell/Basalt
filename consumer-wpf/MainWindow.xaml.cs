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

    public MainWindow()
    {
        InitializeComponent();
        Loaded += async (_, _) => await RunScenario();
        Closed += (_, _) => { _basalt?.Dispose(); TryDelete(_path); };
    }

    private async Task RunScenario()
    {
        try
        {
            _basalt = Basalt.Embedded(_path);
            _basalt.On<WpfJob>("wpf.job", (job, context, ct) => Task.CompletedTask);
            await _basalt.StartAsync();
            var id = await _basalt.EnqueueAsync(new WpfJob { Value = 1 }, key: "wpf:1", retry: 2);
            var info = _basalt.GetExecution(id);
            _ = _basalt.GetStats();
            _ = _basalt.ListExecutions();
            _basalt.VerifyHealth();
            Status.Text = "Package consumer OK: execution " + info.ExecutionId;
        }
        catch (Exception ex)
        {
            Status.Text = "Basalt error: " + ex.GetType().Name;
        }
    }

    public sealed class WpfJob { public int Value { get; set; } }

    private static void TryDelete(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, true); } catch { }
    }
}
