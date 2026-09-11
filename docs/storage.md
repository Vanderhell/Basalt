# Choosing Basalt.NET storage

| | Embedded | SQL Server |
|---|---|---|
| Best for | Desktop apps, local services, single-machine durable work | Shared applications, multiple processes, central operations |
| Location | Application-owned local directory | Dedicated schema in an existing SQL Server database |
| Coordination | Local durable store | Shared workers with transactional coordination |
| Setup | `Basalt.Embedded(path)` | `Basalt.SqlServer(connectionString)` |
| Operations | Back up and protect the local directory | Manage schema migrations and database permissions |

Both providers use the same typed handlers, enqueue, schedules, workflows, management API, leases, fencing, recovery, and at-least-once execution contract.

Choose Embedded when the work belongs to one installed application and should survive its restarts without requiring a database server. Choose SQL Server when multiple application processes must share durable work or operations require central database management.

Do not place Embedded files in a temporary directory in production. For SQL Server, use a dedicated configurable schema; Basalt does not require a shared filesystem and does not modify unrelated application tables.
