using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;

namespace BasaltConsumerWpf;

public partial class App : Application
{
    protected override async void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);
        var window = new MainWindow();
        if (!e.Args.Contains("--smoke", StringComparer.OrdinalIgnoreCase)) { MainWindow = window; window.Show(); return; }
        ShutdownMode = ShutdownMode.OnExplicitShutdown;
        try { if (e.Args.Contains("--smoke-sql", StringComparer.OrdinalIgnoreCase)) await window.RunSqlSmokeAsync(); else await window.RunEmbeddedSmokeAsync(); Environment.ExitCode = 0; }
        catch { Environment.ExitCode = 1; }
        finally { window.Close(); Shutdown(); }
    }
}
