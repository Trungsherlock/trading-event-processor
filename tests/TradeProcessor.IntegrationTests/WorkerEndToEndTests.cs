using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using TradeProcessor.Core;
using TradeProcessor.Core.Persistence;
using TradeProcessor.Core.Processing;
using TradeProcessor.Infrastructure;
using TradeProcessor.Worker;
using TradeProducer;

namespace TradeProcessor.IntegrationTests;

/// <summary>
/// Runs the real worker (same DI wiring as Program.cs) against real Kafka and Postgres.
/// These cover the consumer loop: the offset is committed only after a message is handled.
/// </summary>
[Collection(EndToEndCollection.Name)]
public sealed class WorkerEndToEndTests(EndToEndFixture fx)
{
    [Theory]
    [InlineData(CommitMode.PerMessage)]
    [InlineData(CommitMode.Background)]
    public async Task Mixed_stream_ends_with_exactly_the_expected_trades_positions_and_dlq_entries(CommitMode mode)
    {
        var topics = await fx.NewRunAsync();
        var gen = new TradeGenerator(seed: 1, accounts: 5, invalidRate: 0.05, duplicateRate: 0.05);
        await ProduceAsync(topics.Trades, Enumerable.Range(0, 400).Select(_ => gen.Next()));

        using var host = await StartWorkerAsync(topics, "group-1", mode);
        await WaitUntilAsync(async () => await TradeCountAsync() == gen.Valid && DlqCount(topics) == gen.Invalid,
            "every message to be handled");

        Assert.Equal(Expected(gen), await ReadPositionsAsync());
        Assert.True(gen.Duplicates > 0 && gen.Invalid > 0, "the stream should contain both kinds of bad input");
    }

    [Fact]
    public async Task Replaying_every_message_changes_nothing()
    {
        var topics = await fx.NewRunAsync();
        var gen = new TradeGenerator(seed: 2, accounts: 5, invalidRate: 0.05, duplicateRate: 0.05);
        await ProduceAsync(topics.Trades, Enumerable.Range(0, 300).Select(_ => gen.Next()));

        using (var first = await StartWorkerAsync(topics, "first", CommitMode.PerMessage))
        {
            await WaitUntilAsync(async () => await TradeCountAsync() == gen.Valid && DlqCount(topics) == gen.Invalid,
                "the first pass to finish");
            await first.StopAsync();
        }

        // A new consumer group has no committed offsets, so it re-reads the whole topic:
        // the worst case after a crash, as if every offset had been lost.
        using var replay = await StartWorkerAsync(topics, "replay", CommitMode.PerMessage);
        await WaitUntilAsync(() => CaughtUpAsync(topics, "replay"), "the replay to reach the end of the topic");

        Assert.Equal(gen.Valid, await TradeCountAsync());
        Assert.Equal(Expected(gen), await ReadPositionsAsync());
        Assert.Equal(2 * gen.Invalid, DlqCount(topics));   // bad messages are dead-lettered again: the DLQ is at-least-once too
    }

    [Theory]
    [InlineData(CommitMode.PerMessage)]
    [InlineData(CommitMode.Background)]
    public async Task Message_that_keeps_failing_is_not_committed_and_is_stored_after_restart(CommitMode mode)
    {
        var topics = await fx.NewRunAsync();
        var gen = new TradeGenerator(seed: 3, accounts: 1, invalidRate: 0, duplicateRate: 0);
        await ProduceAsync(topics.Trades, [gen.Next()]);

        using (var failing = await StartWorkerAsync(topics, "group", mode,
                   services => services.Replace(ServiceDescriptor.Singleton<ITradeRepository>(new DatabaseDownRepository()))))
        {
            var worker = failing.Services.GetServices<IHostedService>().OfType<TradeConsumerWorker>().Single();
            // The worker retries, gives up, and stops.
            await Assert.ThrowsAsync<DatabaseDownException>(() => worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(60)));
        }

        Assert.Equal(0, await CommittedCountAsync(topics, "group"));   // the failed message was not marked as done

        using var healthy = await StartWorkerAsync(topics, "group", mode);
        await WaitUntilAsync(async () => await TradeCountAsync() == 1, "the redelivered trade to be stored");
    }

    // ---------- helpers ----------

    private async Task<IHost> StartWorkerAsync(Topics topics, string groupId, CommitMode mode,
        Action<IServiceCollection>? configure = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:Trades"] = fx.ConnectionString,
            ["Kafka:BootstrapServers"] = fx.BootstrapServers,
            ["Kafka:Topic"] = topics.Trades,
            ["Kafka:DeadLetterTopic"] = topics.Dlq,
            ["Kafka:GroupId"] = groupId,
            ["Kafka:CommitMode"] = mode.ToString(),
        });
        builder.Services.AddLogging();
        builder.Services.AddTradeProcessor(builder.Configuration);
        // Same retry behaviour, shorter waits, so the failure test doesn't take seconds.
        builder.Services.Replace(ServiceDescriptor.Singleton(new RetryPolicy(3, TimeSpan.FromMilliseconds(10))));
        configure?.Invoke(builder.Services);

        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    private async Task ProduceAsync(string topic, IEnumerable<(string Key, string Value)> messages)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = fx.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
        }).Build();
        foreach (var (key, value) in messages)
            await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = value });
    }

    private async Task<long> TradeCountAsync()
    {
        await using var cmd = fx.Db.CreateCommand("SELECT count(*) FROM trades");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task<Dictionary<(string, string), decimal>> ReadPositionsAsync()
    {
        var result = new Dictionary<(string, string), decimal>();
        await using var cmd = fx.Db.CreateCommand("SELECT account_id, symbol, net_quantity FROM positions");
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            result[(reader.GetString(0), reader.GetString(1))] = reader.GetDecimal(2);
        return result;
    }

    private static Dictionary<(string, string), decimal> Expected(TradeGenerator gen) =>
        gen.Positions.ToDictionary(p => (p.Key.Account, p.Key.Symbol), p => p.Value);

    private long DlqCount(Topics topics) => HighWatermarks(topics.Dlq).Values.Sum();

    private Dictionary<int, long> HighWatermarks(string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = fx.BootstrapServers }).Build();
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = fx.BootstrapServers,
            GroupId = "test-inspector",
        }).Build();
        return admin.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics[0].Partitions.ToDictionary(
            p => p.PartitionId,
            p => consumer.QueryWatermarkOffsets(new TopicPartition(topic, p.PartitionId), TimeSpan.FromSeconds(10)).High.Value);
    }

    private async Task<Dictionary<int, long>> CommittedOffsetsAsync(Topics topics, string groupId)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = fx.BootstrapServers }).Build();
        var partitions = Enumerable.Range(0, 3).Select(p => new TopicPartition(topics.Trades, p)).ToList();
        var result = await admin.ListConsumerGroupOffsetsAsync([new ConsumerGroupTopicPartitions(groupId, partitions)]);
        return result.Single().Partitions.ToDictionary(
            p => p.Partition.Value, p => p.Offset == Offset.Unset ? 0 : p.Offset.Value);
    }

    private async Task<long> CommittedCountAsync(Topics topics, string groupId) =>
        (await CommittedOffsetsAsync(topics, groupId)).Values.Sum();

    private async Task<bool> CaughtUpAsync(Topics topics, string groupId)
    {
        var committed = await CommittedOffsetsAsync(topics, groupId);
        return HighWatermarks(topics.Trades).All(end => committed.GetValueOrDefault(end.Key) == end.Value);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string what, int timeoutSeconds = 90)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException($"Timed out waiting for {what}");
            await Task.Delay(250);
        }
    }

    private sealed class DatabaseDownException() : Exception("database is down");

    private sealed class DatabaseDownRepository : ITradeRepository
    {
        public Task<StoreResult> SaveAsync(Trade trade, CancellationToken ct) => throw new DatabaseDownException();
    }
}
