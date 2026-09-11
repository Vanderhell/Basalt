# Basalt.NET WPF demo

`ConsumerWpf` is a real .NET Framework 4.7.2 WPF application using only the public managed Basalt.NET API.

Run it with `dotnet run --project consumer-wpf/ConsumerWpf.csproj`. Choose **Embedded** for a local durable store, or choose **SQL Server** and enter a connection string. The SQL demo uses Basalt's standard `basalt` schema in the selected database, so use an application-owned or test database.

The **Run complete demo** button verifies typed handlers, durable enqueue, idempotency and an actual retry, delayed/interval/daily/cron schedules, pause/resume/remove, a three-node DAG workflow, execution filtering, health, queue/performance statistics, worker visibility, shutdown, and reopen.

For non-interactive verification, `ConsumerWpf.exe --smoke` runs the Embedded scenario. Set `BASALT_DEMO_SQL` and use `ConsumerWpf.exe --smoke-sql` for the same SQL Server scenario.
