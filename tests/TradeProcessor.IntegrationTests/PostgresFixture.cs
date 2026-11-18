using Npgsql;
using Testcontainers.PostgreSql;

namespace TradeProcessor.IntegrationTests;

/// <summary>
/// One real Postgres container for the whole test run, with the production schema applied.
/// Starting a container per test would add several seconds to every test.
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container =
        new PostgreSqlBuilder("postgres:17").Build();

    public NpgsqlDataSource Db { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        Db = NpgsqlDataSource.Create(_container.GetConnectionString());

        var schema = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "init.sql"));
        await using var cmd = Db.CreateCommand(schema);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Empties both tables so each test starts from a clean database.</summary>
    public async Task ResetAsync()
    {
        await using var cmd = Db.CreateCommand("TRUNCATE trades, positions");
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        await Db.DisposeAsync();
        await _container.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>
{
    public const string Name = "Postgres";
}
