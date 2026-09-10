# SQL Server provider

## Configuration

```csharp
using BasaltCore;
using BasaltCore.SqlServer;

using var basalt = Basalt.SqlServer(connectionString, sql =>
{
    sql.Schema = "MyApp_Basalt";
    sql.SchemaManagement = SchemaManagement.ValidateOnly;
    sql.CommandTimeoutSeconds = 30;
});
```

All Basalt-owned tables, records, statistics, receipts, ledger rows, and the execution ID sequence live in the configured schema. Basalt does not require a common directory and does not modify foreign `dbo` tables.

## Schema management

- `AutoMigrate` transactionally creates or upgrades the Basalt schema. Use it with a migration identity that has the required DDL rights.
- `ValidateOnly` checks the existing schema without executing DDL. Use it for restricted runtime identities.
- `Manual` does not execute migrations; the current implementation still validates the deployed schema when opening storage.

For controlled deployment, generate the migration SQL through `SqlSchemaManager.GenerateMigrationScript()` and apply it with the migration identity before runtime starts.

## Identities and permissions

Keep DDL permission on the migration identity. A runtime identity normally needs only access to the database, DML rights on the Basalt schema, and permission to advance the sequence:

```sql
CREATE USER [MyAppRuntime] FOR LOGIN [MyAppRuntime];
GRANT SELECT, INSERT, UPDATE, DELETE
    ON SCHEMA::[MyApp_Basalt] TO [MyAppRuntime];
GRANT UPDATE
    ON OBJECT::[MyApp_Basalt].[ExecutionIds] TO [MyAppRuntime];
```

Use `ValidateOnly` for this runtime identity. Do not grant it schema ownership, `ALTER`, `CONTROL`, or rights on unrelated application schemas unless the application independently requires them.

### Restricted runtime verification

`tests/BasaltPermissionSmoke` is a manual Windows-authentication verification for this model. Its `--migrate` mode must run under the migration identity; it creates a dedicated Basalt schema and grants only the permissions above to the runtime identity. Run the normal mode in a process started as that runtime Windows account. It verifies typed idempotent enqueue, retry, claim/completion, management reads, `ValidateOnly`, health, and denial of an attempted update to a foreign `dbo` sentinel table. Run `--cleanup` under the migration identity afterwards; it removes the dedicated schema and sentinel.

The smoke does not require SQL authentication and never grants DDL rights to the runtime identity.

## Connections and workers

Basalt creates, opens, and disposes its own SQL connections. Supply either a connection string or a factory returning a fresh `Microsoft.Data.SqlClient.SqlConnection`:

```csharp
using var basalt = Basalt.SqlServer(
    () => new SqlConnection(connectionString),
    sql => sql.SchemaManagement = SchemaManagement.ValidateOnly);
```

Do not share an Entity Framework `DbContext` instance or its connection with Basalt.

Multiple processes may use the same database and schema. Every process that should execute jobs must register the same stable handler keys and call `StartAsync`; SQL Server storage alone does not execute work.

SQL integration requires a provisioned server and identity, so the credential-free GitHub-hosted CI workflow builds the provider but does not claim an integration PASS.
