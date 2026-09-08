using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace BasaltCore.SqlServer;

public enum SchemaManagement { AutoMigrate, ValidateOnly, Manual }

public sealed class SqlServerOptions
{
    private string _schema = "Basalt";
    public string Schema { get => _schema; set => _schema = SqlIdentifier.Validate(value, nameof(value)); }
    public SchemaManagement SchemaManagement { get; set; } = SchemaManagement.AutoMigrate;
    public int CommandTimeoutSeconds { get; set; } = 30;
}

internal static class SqlIdentifier
{
    internal static string Validate(string? value, string parameter)
    {
        if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("A SQL identifier of at most 128 characters is required.", parameter);
        string identifier = value!;
        if (identifier.Length > 128) throw new ArgumentException("A SQL identifier of at most 128 characters is required.", parameter);
        foreach (char c in identifier) if (!(char.IsLetterOrDigit(c) || c == '_')) throw new ArgumentException("SQL identifiers may contain only letters, digits, and underscore.", parameter);
        return identifier;
    }
    internal static string Quote(string value) => "[" + value.Replace("]", "]]") + "]";
}

public sealed class SqlServerConnectionFactory
{
    private readonly Func<DbConnection> _create;
    public SqlServerConnectionFactory(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("A SQL Server connection string is required.", nameof(connectionString));
        _create = () => new SqlConnection(connectionString);
    }
    public SqlServerConnectionFactory(Func<DbConnection> create) => _create = create ?? throw new ArgumentNullException(nameof(create));
    internal DbConnection Create()
    {
        DbConnection connection = _create() ?? throw new InvalidOperationException("The connection factory returned null.");
        if (connection is not SqlConnection) { connection.Dispose(); throw new InvalidOperationException("The Basalt SQL provider requires Microsoft.Data.SqlClient.SqlConnection."); }
        return connection;
    }
}
