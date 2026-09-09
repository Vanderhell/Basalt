using System.Data;
using System.Data.Common;
using System.Security.Cryptography;
using System.Text;

namespace BasaltCore.SqlServer;

public sealed class SqlSchemaManager
{
    public const int CurrentVersion = 2;
    private readonly SqlServerConnectionFactory _connections;
    private readonly SqlServerOptions _options;
    public SqlSchemaManager(SqlServerConnectionFactory connections, SqlServerOptions options)
    { _connections = connections ?? throw new ArgumentNullException(nameof(connections)); _options = options ?? throw new ArgumentNullException(nameof(options)); }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        using DbConnection connection = _connections.Create();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        if (_options.SchemaManagement == SchemaManagement.AutoMigrate) await MigrateAsync(connection, cancellationToken).ConfigureAwait(false);
        else await ValidateAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    public string GenerateMigrationScript() { string s=SqlIdentifier.Quote(_options.Schema); return Migration1(s)+Environment.NewLine+Migration2(s); }

    private async Task MigrateAsync(DbConnection connection, CancellationToken token)
    {
        using DbTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        try
        {
            using DbCommand command = Create(connection, transaction, "DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=@resource,@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=@timeout; IF @r<0 THROW 51000,'Unable to acquire Basalt schema migration lock.',1;");
            Add(command, "@resource", "Basalt:migrate:" + _options.Schema); Add(command, "@timeout", _options.CommandTimeoutSeconds * 1000);
            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            using DbCommand migrate = Create(connection, transaction, Migration1(SqlIdentifier.Quote(_options.Schema)));
            await migrate.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            using DbCommand migrate2 = Create(connection, transaction, Migration2(SqlIdentifier.Quote(_options.Schema)));
            await migrate2.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            transaction.Commit();
        }
        catch { transaction.Rollback(); throw; }
        await ValidateAsync(connection, token).ConfigureAwait(false);
    }

    private async Task ValidateAsync(DbConnection connection, CancellationToken token)
    {
        string schema = SqlIdentifier.Quote(_options.Schema);
        using DbCommand command = Create(connection, null, $"SELECT TOP(1) [Version],[Checksum] FROM {schema}.[SchemaHistory] ORDER BY [Version] DESC;");
        try
        {
            using DbDataReader reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
            if (!await reader.ReadAsync(token).ConfigureAwait(false)) throw new InvalidOperationException("The Basalt schema has no migration history.");
            int version = reader.GetInt32(0); string checksum = reader.GetString(1);
            if (version != CurrentVersion || !StringComparer.Ordinal.Equals(checksum, Checksum(Migration2Body(schema))))
                throw new InvalidOperationException(version > CurrentVersion ? "The Basalt SQL schema is newer than this provider." : "The Basalt SQL schema is incompatible or its migration checksum differs.");
        }
        catch (DbException ex) { throw new InvalidOperationException("The Basalt SQL schema is missing, inaccessible, or invalid. ValidateOnly and Manual modes never execute DDL.", ex); }
    }

    private DbCommand Create(DbConnection connection, DbTransaction? transaction, string sql)
    { DbCommand c=connection.CreateCommand(); c.CommandText=sql; c.CommandTimeout=_options.CommandTimeoutSeconds; c.Transaction=transaction; return c; }
    private static void Add(DbCommand command,string name,object value){DbParameter p=command.CreateParameter();p.ParameterName=name;p.Value=value;command.Parameters.Add(p);}
    private static string Checksum(string value){using SHA256 h=SHA256.Create();return BitConverter.ToString(h.ComputeHash(Encoding.UTF8.GetBytes(value))).Replace("-","").ToLowerInvariant();}

    private static string Migration1(string s)
    {
        string body=MigrationBody(s), checksum=Checksum(body);
        return $@"IF SCHEMA_ID(N'{s.Trim('[',']')}') IS NULL EXEC(N'CREATE SCHEMA {s}');
IF OBJECT_ID(N'{s}.[SchemaHistory]',N'U') IS NULL BEGIN
{body}
INSERT INTO {s}.[SchemaHistory]([Version],[Checksum],[ProviderVersion],[AppliedAtUtc]) VALUES(1,'{checksum}','1',SYSUTCDATETIME());
END ELSE IF NOT EXISTS(SELECT 1 FROM {s}.[SchemaHistory] WHERE [Version]=1 AND [Checksum]='{checksum}') THROW 51001,'Incompatible Basalt schema history.',1;";
    }
    private static string Migration2(string s)
    {
        string body=Migration2Body(s), checksum=Checksum(body), previous=Checksum(MigrationBody(s));
        return $@"IF NOT EXISTS(SELECT 1 FROM {s}.[SchemaHistory] WHERE [Version]=1 AND [Checksum]='{previous}') THROW 51001,'Incompatible Basalt schema history.',1;
IF NOT EXISTS(SELECT 1 FROM {s}.[SchemaHistory] WHERE [Version]=2) BEGIN
{body}
INSERT INTO {s}.[SchemaHistory]([Version],[Checksum],[ProviderVersion],[AppliedAtUtc]) VALUES(2,'{checksum}','2',SYSUTCDATETIME());
END ELSE IF NOT EXISTS(SELECT 1 FROM {s}.[SchemaHistory] WHERE [Version]=2 AND [Checksum]='{checksum}') THROW 51001,'Incompatible Basalt schema history.',1;";
    }
    private static string Migration2Body(string s) => $@"ALTER TABLE {s}.[Receipts] ALTER COLUMN [Payload] varbinary(max) NOT NULL;";
    private static string MigrationBody(string s) => $@"CREATE TABLE {s}.[SchemaHistory]([Version] int NOT NULL PRIMARY KEY,[Checksum] char(64) NOT NULL,[ProviderVersion] nvarchar(32) NOT NULL,[AppliedAtUtc] datetime2(7) NOT NULL);
CREATE SEQUENCE {s}.[ExecutionIds] AS bigint START WITH 1 INCREMENT BY 1;
CREATE TABLE {s}.[Records]([RecordType] int NOT NULL,[RecordId] bigint NOT NULL,[Revision] bigint NOT NULL CONSTRAINT [DF_Basalt_RecordRevision] DEFAULT(1),[Payload] varbinary(max) NOT NULL,CONSTRAINT [PK_Basalt_Records] PRIMARY KEY([RecordType],[RecordId]));
CREATE TABLE {s}.[Executions]([ExecutionId] bigint NOT NULL PRIMARY KEY,[JobDefinitionId] bigint NOT NULL,[ScheduleId] bigint NOT NULL,[WorkflowId] bigint NOT NULL,[State] int NOT NULL,[CreatedAt] bigint NOT NULL,[EligibleAt] bigint NOT NULL,[StartedAt] bigint NOT NULL,[FinishedAt] bigint NOT NULL,[Priority] int NOT NULL,[Attempt] int NOT NULL,[MaxAttempts] int NOT NULL,[Revision] bigint NOT NULL,[WorkerId] binary(16) NULL,[LeaseExpiresAt] bigint NOT NULL,[FencingToken] bigint NOT NULL);
CREATE INDEX [IX_Basalt_Claim] ON {s}.[Executions]([State],[EligibleAt],[Priority],[ExecutionId]); CREATE INDEX [IX_Basalt_Lease] ON {s}.[Executions]([State],[LeaseExpiresAt]);
CREATE TABLE {s}.[Schedules]([ScheduleId] bigint NOT NULL PRIMARY KEY,[JobDefinitionId] bigint NOT NULL,[ScheduleType] int NOT NULL,[Enabled] bit NOT NULL,[TimezoneReference] bigint NOT NULL,[Interval] bigint NOT NULL,[IntervalMode] int NOT NULL,[CatchUpMax] int NOT NULL,[StartAt] bigint NOT NULL,[EndAt] bigint NOT NULL,[LastFireAt] bigint NOT NULL,[NextFireAt] bigint NOT NULL,[OccurrenceCount] bigint NOT NULL,[MaxOccurrences] bigint NOT NULL,[MisfirePolicy] int NOT NULL,[OverlapPolicy] int NOT NULL,[Revision] bigint NOT NULL); CREATE INDEX [IX_Basalt_ScheduleFire] ON {s}.[Schedules]([Enabled],[NextFireAt]);
CREATE TABLE {s}.[Receipts]([ReceiptId] bigint NOT NULL PRIMARY KEY,[Payload] varbinary(8000) NOT NULL);
CREATE TABLE {s}.[Stats]([Id] int NOT NULL PRIMARY KEY CHECK([Id]=1),[Submitted] bigint NOT NULL,[Started] bigint NOT NULL,[Completed] bigint NOT NULL,[Failed] bigint NOT NULL,[Retried] bigint NOT NULL,[Cancelled] bigint NOT NULL,[Dead] bigint NOT NULL,[Recovered] bigint NOT NULL,[Revision] bigint NOT NULL);
CREATE TABLE {s}.[Ledger]([ExecutionId] bigint NOT NULL PRIMARY KEY,[JobDefinitionId] bigint NOT NULL,[ScheduleId] bigint NOT NULL,[WorkflowId] bigint NOT NULL,[StartedAt] bigint NOT NULL,[FinishedAt] bigint NOT NULL,[Duration] bigint NOT NULL,[Attempt] int NOT NULL,[FinalState] int NOT NULL,[ResultCode] int NOT NULL,[ErrorCode] int NOT NULL);";
}
