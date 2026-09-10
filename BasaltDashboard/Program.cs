using System.Windows;
using System.IO;
using BasaltCore;
using BasaltCore.SqlServer;

namespace BasaltDashboard;

internal static class Program
{
    [STAThread]
    private static void Main(string[] args)
    {
        using BasaltApplication basalt = Open(args);
        var application = new Application();
        application.Run(new DashboardWindow(basalt));
    }

    private static BasaltApplication Open(string[] args)
    {
        if (args.Length >= 2 && string.Equals(args[0], "--sql", StringComparison.OrdinalIgnoreCase))
            return Basalt.SqlServer(args[1], options => options.SchemaManagement = SchemaManagement.ValidateOnly);

        string path = args.Length == 0
            ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "Basalt", "dashboard")
            : args[0];
        return Basalt.Embedded(path);
    }
}
