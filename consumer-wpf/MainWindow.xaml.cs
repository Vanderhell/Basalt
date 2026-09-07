using System;
using System.IO;
using System.Threading.Tasks;
using System.Windows;
using BasaltCore;

namespace BasaltConsumerWpf;

public partial class MainWindow : Window
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), "basalt-wpf-consumer");
    private BasaltDatabase? _database;
    private BasaltEngine? _engine;

    public MainWindow()
    {
        InitializeComponent();
        Loaded += async (_, _) => await RunScenario();
        Closed += (_, _) => { _engine?.Dispose(); _database?.Dispose(); TryDelete(_path); };
    }

    private async Task RunScenario()
    {
        try
        {
            _database = BasaltDatabase.OpenOrCreate(_path);
            _engine = new BasaltEngine(_database);
            _engine.RegisterRawHandler(0xB4517, (payload, version) => 0);
            _engine.Start();
            var id = await _engine.EnqueueAsync(0xB4517, new byte[] { 1 }, 1, 1);
            var info = _engine.GetExecution(id);
            _ = _engine.GetStats();
            _ = _engine.ListExecutionIds();
            _engine.VerifyHealth();
            Status.Text = "Package consumer OK: execution " + info.ExecutionId;
        }
        catch (Exception ex)
        {
            Status.Text = "Basalt error: " + ex.GetType().Name;
        }
    }

    private static void TryDelete(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, true); } catch { }
    }
}
