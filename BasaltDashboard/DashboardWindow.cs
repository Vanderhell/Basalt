using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Media;
using System.Windows.Threading;
using BasaltCore;

namespace BasaltDashboard;

/// <summary>A storage-neutral operational view. It receives only the public managed Basalt API.</summary>
public sealed class DashboardWindow : Window
{
    private readonly DashboardViewModel _viewModel;
    private readonly DispatcherTimer _timer;
    private readonly CancellationTokenSource _closing = new();

    public DashboardWindow(BasaltApplication basalt)
    {
        if (basalt == null) throw new ArgumentNullException(nameof(basalt));
        Title = "Basalt Dashboard"; Width = 1280; Height = 780; MinWidth = 960; MinHeight = 600;
        Background = new SolidColorBrush(Color.FromRgb(247, 249, 252));
        _viewModel = new DashboardViewModel(basalt); DataContext = _viewModel;
        Content = CreateLayout();
        _timer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(2) };
        _timer.Tick += async (_, _) => await _viewModel.RefreshAsync(_closing.Token);
        Loaded += async (_, _) => { await _viewModel.RefreshAsync(_closing.Token); _timer.Start(); };
        Closed += (_, _) => { _timer.Stop(); _closing.Cancel(); _closing.Dispose(); };
    }

    private UIElement CreateLayout()
    {
        var root = new DockPanel { Margin = new Thickness(24) };
        var header = new StackPanel { Orientation = Orientation.Horizontal, Margin = new Thickness(0, 0, 0, 18) };
        var title = new TextBlock { Text = "BASALT", FontSize = 24, FontWeight = FontWeights.Bold, Foreground = Brushes.MidnightBlue, VerticalAlignment = VerticalAlignment.Center };
        var health = new TextBlock { Margin = new Thickness(18, 4, 0, 0), FontSize = 14, FontWeight = FontWeights.SemiBold };
        health.SetBinding(TextBlock.TextProperty, new Binding(nameof(DashboardViewModel.HealthText)));
        health.SetBinding(TextBlock.ForegroundProperty, new Binding(nameof(DashboardViewModel.HealthBrush)));
        header.Children.Add(title); header.Children.Add(health); DockPanel.SetDock(header, Dock.Top); root.Children.Add(header);

        var tabs = new TabControl { BorderThickness = new Thickness(0), Background = Brushes.Transparent };
        tabs.Items.Add(Tab("Overview", Overview()));
        tabs.Items.Add(Tab("Jobs / Executions", ExecutionPage()));
        tabs.Items.Add(Tab("Failed & Dead", GridFor("Failures")));
        tabs.Items.Add(Tab("Schedules", SchedulePage()));
        tabs.Items.Add(Tab("Workflows", GridFor("Workflows")));
        tabs.Items.Add(Tab("Workers", GridFor("Workers")));
        tabs.Items.Add(Tab("Statistics", Statistics()));
        root.Children.Add(tabs); return root;
    }

    private TabItem Tab(string title, UIElement content) => new() { Header = title, Content = content };

    private UIElement Overview()
    {
        var panel = new DockPanel();
        var cards = new ItemsControl { Margin = new Thickness(0, 0, 0, 16) };
        cards.ItemsPanel = new ItemsPanelTemplate(new FrameworkElementFactory(typeof(WrapPanel)));
        cards.ItemTemplate = new DataTemplate(typeof(DashboardMetric));
        var card = new FrameworkElementFactory(typeof(Border)); card.SetValue(Border.BackgroundProperty, Brushes.White); card.SetValue(Border.CornerRadiusProperty, new CornerRadius(6)); card.SetValue(Border.PaddingProperty, new Thickness(16)); card.SetValue(Border.MarginProperty, new Thickness(0, 0, 12, 0));
        var text = new FrameworkElementFactory(typeof(TextBlock)); text.SetBinding(TextBlock.TextProperty, new Binding(nameof(DashboardMetric.Text))); text.SetValue(TextBlock.FontSizeProperty, 14.0); card.AppendChild(text); cards.ItemTemplate.VisualTree = card;
        cards.SetBinding(ItemsControl.ItemsSourceProperty, new Binding(nameof(DashboardViewModel.Metrics))); DockPanel.SetDock(cards, Dock.Top); panel.Children.Add(cards);
        panel.Children.Add(GridFor("RecentExecutions")); return panel;
    }

    private UIElement Statistics()
    {
        var text = new TextBlock { FontSize = 16, Background = Brushes.White, Padding = new Thickness(20) };
        text.SetBinding(TextBlock.TextProperty, new Binding(nameof(DashboardViewModel.StatisticsText))); return text;
    }

    private UIElement ExecutionPage()
    {
        var panel = new DockPanel();
        var actions = new StackPanel { Orientation = Orientation.Horizontal, Margin = new Thickness(0, 0, 0, 10) };
        actions.Children.Add(Button("Cancel selected", async (_, _) => await _viewModel.CancelSelectedAsync()));
        actions.Children.Add(Button("Requeue selected", async (_, _) => await _viewModel.RequeueSelectedAsync()));
        DockPanel.SetDock(actions, Dock.Top); panel.Children.Add(actions);
        var grid = (DataGrid)GridFor("Executions"); grid.SelectionChanged += (_, _) => _viewModel.SelectedExecution = grid.SelectedItem as BasaltExecutionInfo; panel.Children.Add(grid); return panel;
    }

    private UIElement SchedulePage()
    {
        var panel = new DockPanel();
        var actions = new StackPanel { Orientation = Orientation.Horizontal, Margin = new Thickness(0, 0, 0, 10) };
        actions.Children.Add(Button("Pause selected", async (_, _) => await _viewModel.PauseSelectedScheduleAsync()));
        actions.Children.Add(Button("Resume selected", async (_, _) => await _viewModel.ResumeSelectedScheduleAsync()));
        actions.Children.Add(Button("Remove selected", async (_, _) => await _viewModel.RemoveSelectedScheduleAsync()));
        DockPanel.SetDock(actions, Dock.Top); panel.Children.Add(actions);
        var grid = (DataGrid)GridFor("Schedules"); grid.SelectionChanged += (_, _) => _viewModel.SelectedSchedule = grid.SelectedItem as BasaltScheduleInfo; panel.Children.Add(grid); return panel;
    }

    private static Button Button(string text, RoutedEventHandler handler)
    {
        var button = new Button { Content = text, Margin = new Thickness(0, 0, 8, 0), Padding = new Thickness(10, 5, 10, 5), MinWidth = 108, IsDefault = false };
        button.Click += handler;
        return button;
    }

    private UIElement GridFor(string property)
    {
        var grid = new DataGrid { IsReadOnly = true, AutoGenerateColumns = true, CanUserAddRows = false, HeadersVisibility = DataGridHeadersVisibility.Column, Background = Brushes.White, BorderThickness = new Thickness(0) };
        grid.SetBinding(ItemsControl.ItemsSourceProperty, new Binding(property)); return grid;
    }
}

public sealed class DashboardViewModel : INotifyPropertyChanged
{
    private readonly BasaltApplication _basalt; private readonly SemaphoreSlim _refresh = new(1, 1);
    private string _healthText = "Checking health..."; private Brush _healthBrush = Brushes.Gray; private string _statisticsText = string.Empty;
    public DashboardViewModel(BasaltApplication basalt) => _basalt = basalt;
    public ObservableCollection<DashboardMetric> Metrics { get; } = new(); public ObservableCollection<BasaltExecutionInfo> RecentExecutions { get; } = new(); public ObservableCollection<BasaltExecutionInfo> Executions { get; } = new(); public ObservableCollection<BasaltExecutionInfo> Failures { get; } = new(); public ObservableCollection<BasaltScheduleInfo> Schedules { get; } = new(); public ObservableCollection<BasaltWorkflowStatus> Workflows { get; } = new(); public ObservableCollection<BasaltWorkerInfo> Workers { get; } = new();
    public string HealthText { get => _healthText; private set { _healthText=value; OnChanged(); } } public Brush HealthBrush { get => _healthBrush; private set { _healthBrush=value; OnChanged(); } } public string StatisticsText { get => _statisticsText; private set { _statisticsText=value; OnChanged(); } }
    public event PropertyChangedEventHandler? PropertyChanged; private void OnChanged([CallerMemberName] string? name = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    public BasaltExecutionInfo? SelectedExecution { get; set; }
    public BasaltScheduleInfo? SelectedSchedule { get; set; }

    public async Task CancelSelectedAsync() { var value = SelectedExecution; if (value == null) return; await Task.Run(() => _basalt.Cancel(value.ExecutionId)); await RefreshAsync(CancellationToken.None); }
    public async Task RequeueSelectedAsync() { var value = SelectedExecution; if (value == null) return; await Task.Run(() => _basalt.Requeue(value.ExecutionId)); await RefreshAsync(CancellationToken.None); }
    public async Task PauseSelectedScheduleAsync() { var value = SelectedSchedule; if (value == null) return; await Task.Run(() => _basalt.PauseSchedule(value.ScheduleId)); await RefreshAsync(CancellationToken.None); }
    public async Task ResumeSelectedScheduleAsync() { var value = SelectedSchedule; if (value == null) return; await Task.Run(() => _basalt.ResumeSchedule(value.ScheduleId)); await RefreshAsync(CancellationToken.None); }
    public async Task RemoveSelectedScheduleAsync() { var value = SelectedSchedule; if (value == null) return; await Task.Run(() => _basalt.RemoveSchedule(value.ScheduleId)); await RefreshAsync(CancellationToken.None); }

    public async Task RefreshAsync(CancellationToken cancellationToken)
    {
        if (!await _refresh.WaitAsync(0, cancellationToken).ConfigureAwait(true)) return;
        try
        {
            var snapshot = await Task.Run(() => new DashboardSnapshot(_basalt), cancellationToken).ConfigureAwait(true);
            HealthText = $"{snapshot.Health.Status.ToString().ToUpperInvariant()}  |  {snapshot.Health.Provider}";
            HealthBrush = snapshot.Health.Status == BasaltHealthStatus.Healthy ? Brushes.ForestGreen : snapshot.Health.Status == BasaltHealthStatus.Degraded ? Brushes.DarkOrange : Brushes.Firebrick;
            Replace(Metrics, snapshot.Metrics); Replace(RecentExecutions, snapshot.Recent); Replace(Executions, snapshot.Executions); Replace(Failures, snapshot.Failures); Replace(Schedules, snapshot.Schedules); Replace(Workflows, snapshot.Workflows); Replace(Workers, snapshot.Workers);
            StatisticsText = snapshot.Statistics;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception error) { HealthText = "UNHEALTHY  |  " + error.Message; HealthBrush = Brushes.Firebrick; }
        finally { _refresh.Release(); }
    }

    private static void Replace<T>(ObservableCollection<T> target, IEnumerable<T> source) { target.Clear(); foreach (var item in source) target.Add(item); }
}

public sealed class DashboardMetric { public DashboardMetric(string text) => Text = text; public string Text { get; } }

internal sealed class DashboardSnapshot
{
    public DashboardSnapshot(BasaltApplication basalt)
    {
        Health = basalt.GetHealth(); var stats = basalt.GetStats();
        Metrics = new[] { new DashboardMetric($"READY\n{Health.Queue.Ready}"), new DashboardMetric($"RUNNING\n{Health.Queue.Running}"), new DashboardMetric($"RETRY\n{Health.Queue.Retry}"), new DashboardMetric($"SCHEDULED\n{Health.Queue.Scheduled}"), new DashboardMetric($"BLOCKED\n{Health.Queue.Blocked}"), new DashboardMetric($"DEAD\n{Health.Queue.Dead}") };
        Recent = basalt.ListExecutions(new ExecutionQuery { Take = 25 }); Executions = basalt.ListExecutions(new ExecutionQuery { Take = 250 }); Failures = basalt.ListExecutions(new ExecutionQuery { States = new[] { ExecutionState.Failed, ExecutionState.Dead, ExecutionState.Retry }, Take = 250 });
        Schedules = basalt.ListSchedules(250); Workflows = basalt.ListWorkflows(250); Workers = basalt.ListWorkers();
        Statistics = $"Submitted: {stats.SubmittedTotal}\nCompleted: {stats.CompletedTotal}\nFailed: {stats.FailedTotal}\nRetried: {stats.RetriedTotal}\nRecovered: {stats.RecoveredTotal}";
    }
    public BasaltHealth Health { get; } public IReadOnlyList<DashboardMetric> Metrics { get; } public IReadOnlyList<BasaltExecutionInfo> Recent { get; } public IReadOnlyList<BasaltExecutionInfo> Executions { get; } public IReadOnlyList<BasaltExecutionInfo> Failures { get; } public IReadOnlyList<BasaltScheduleInfo> Schedules { get; } public IReadOnlyList<BasaltWorkflowStatus> Workflows { get; } public IReadOnlyList<BasaltWorkerInfo> Workers { get; } public string Statistics { get; }
}
