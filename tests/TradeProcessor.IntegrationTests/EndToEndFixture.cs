using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace TradeProcessor.IntegrationTests;

/// <summary>Real Kafka and real Postgres, started once and shared by the end-to-end tests.</summary>
public sealed class EndToEndFixture : IAsyncLifetime
{
    private readonly KafkaContainer _kafka = new KafkaBuilder("apache/kafka:4.3.1").Build();
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17").Build();

    public NpgsqlDataSource Db { get; private set; } = null!;
    public string BootstrapServers => _kafka.GetBootstrapAddress();
    public string ConnectionString => _postgres.GetConnectionString();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_kafka.StartAsync(), _postgres.StartAsync());
        Db = NpgsqlDataSource.Create(ConnectionString);
        await using var cmd = Db.CreateCommand(
            await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "init.sql")));
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Fresh topics for one test (3-partition trades, 1-partition DLQ, like production) and an empty database.</summary>
    public async Task<Topics> NewRunAsync()
    {
        var id = Guid.NewGuid().ToString("N")[..8];
        var topics = new Topics($"trades-{id}", $"trades-{id}.dlq");

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
        await admin.CreateTopicsAsync(
        [
            new TopicSpecification { Name = topics.Trades, NumPartitions = 3, ReplicationFactor = 1 },
            new TopicSpecification { Name = topics.Dlq, NumPartitions = 1, ReplicationFactor = 1 },
        ]);

        await using var truncate = Db.CreateCommand("TRUNCATE trades, positions");
        await truncate.ExecuteNonQueryAsync();
        return topics;
    }

    public async Task DisposeAsync()
    {
        await Db.DisposeAsync();
        await Task.WhenAll(_kafka.DisposeAsync().AsTask(), _postgres.DisposeAsync().AsTask());
    }
}

public sealed record Topics(string Trades, string Dlq);

[CollectionDefinition(Name)]
public sealed class EndToEndCollection : ICollectionFixture<EndToEndFixture>
{
    public const string Name = "EndToEnd";
}
