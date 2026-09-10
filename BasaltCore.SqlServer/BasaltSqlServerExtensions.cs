using System.Data.Common;

namespace BasaltCore.SqlServer;

public static class BasaltSqlServerExtensions
{
    public static BasaltConfiguration UseSqlServer(this BasaltConfiguration configuration,string connectionString,Action<SqlServerOptions>? configure=null)
    {if(configuration==null)throw new ArgumentNullException(nameof(configuration));return configuration.UseStorage(()=>SqlServerStorage.OpenAsync(connectionString,configure).GetAwaiter().GetResult());}
    public static BasaltConfiguration UseSqlServer(this BasaltConfiguration configuration,Func<DbConnection> connectionFactory,Action<SqlServerOptions>? configure=null)
    {if(configuration==null)throw new ArgumentNullException(nameof(configuration));return configuration.UseStorage(()=>SqlServerStorage.OpenAsync(connectionFactory,configure).GetAwaiter().GetResult());}
}

internal static class SqlServerBootstrap
{
    public static BasaltApplication CreateFromConnectionString(string connectionString,Action<SqlServerOptions>? configure)
        => Basalt.Create(configuration => configuration.UseSqlServer(connectionString, configure));

    public static BasaltApplication CreateFromFactory(Func<DbConnection> connectionFactory,Action<SqlServerOptions>? configure)
        => Basalt.Create(configuration => configuration.UseSqlServer(connectionFactory, configure));
}
