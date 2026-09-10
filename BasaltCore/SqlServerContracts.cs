using System.Data.Common;

namespace BasaltCore.SqlServer;

/// <summary>Controls how the SQL Server provider handles its dedicated schema at startup.</summary>
public enum SchemaManagement
{
    /// <summary>Create or transactionally upgrade the Basalt schema.</summary>
    AutoMigrate,
    /// <summary>Validate the existing schema without requiring DDL permission.</summary>
    ValidateOnly,
    /// <summary>Open storage without running or validating migrations.</summary>
    Manual
}

/// <summary>Configures Basalt's dedicated SQL Server storage.</summary>
public sealed class SqlServerOptions
{
    private string _schema = "Basalt";

    /// <summary>Gets or sets the dedicated schema containing every Basalt-owned object.</summary>
    public string Schema
    {
        get => _schema;
        set
        {
            if (string.IsNullOrWhiteSpace(value) || value.Length > 128)
                throw new ArgumentException("A SQL identifier of at most 128 characters is required.", nameof(value));
            foreach (char c in value)
                if (!(char.IsLetterOrDigit(c) || c == '_'))
                    throw new ArgumentException("SQL identifiers may contain only letters, digits, and underscore.", nameof(value));
            _schema = value;
        }
    }

    /// <summary>Gets or sets schema migration behavior. The default is <see cref="SchemaManagement.AutoMigrate"/>.</summary>
    public SchemaManagement SchemaManagement { get; set; } = SchemaManagement.AutoMigrate;

    /// <summary>Gets or sets the command timeout in seconds.</summary>
    public int CommandTimeoutSeconds { get; set; } = 30;

    internal Func<bool>? SimulateLostCommitAcknowledgement { get; set; }
}

/// <summary>Creates independent SQL Server connections for Basalt operations.</summary>
public sealed class SqlServerConnectionFactory
{
    private readonly Func<DbConnection> _create;

    public SqlServerConnectionFactory(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("A SQL Server connection string is required.", nameof(connectionString));
        _create = () => CreateSqlConnection(connectionString);
    }

    public SqlServerConnectionFactory(Func<DbConnection> create) =>
        _create = create ?? throw new ArgumentNullException(nameof(create));

    internal DbConnection Create()
    {
        DbConnection connection = _create() ?? throw new InvalidOperationException("The connection factory returned null.");
        Type? sqlConnectionType = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient");
        if (sqlConnectionType == null || !sqlConnectionType.IsInstanceOfType(connection))
        {
            connection.Dispose();
            throw new InvalidOperationException("The Basalt SQL provider requires Microsoft.Data.SqlClient.SqlConnection.");
        }
        return connection;
    }

    private static DbConnection CreateSqlConnection(string connectionString)
    {
        Type type = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", throwOnError: true)!;
        return (DbConnection)Activator.CreateInstance(type, connectionString)!;
    }
}
