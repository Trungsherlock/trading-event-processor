using Confluent.Kafka;
using TradeProcessor.Core.Processing;
using TradeProcessor.Infrastructure;

namespace TradeProcessor.Worker;

/// <summary>
/// Reads trades from Kafka one at a time and commits each offset only after the message is
/// handled (stored, duplicate, or dead-lettered). If handling still fails after the retries,
/// the worker stops without committing, so the message is redelivered after a restart.
/// </summary>
public sealed class TradeConsumerWorker(TradeMessageHandler handler, RetryPolicy retry,
    KafkaOptions options, ILogger<TradeConsumerWorker> log) : BackgroundService
{
    // Consume() blocks a thread, so Task.Run keeps it from blocking host startup.
    protected override Task ExecuteAsync(CancellationToken ct) => Task.Run(() => RunAsync(ct), ct);

    private async Task RunAsync(CancellationToken ct)
    {
        var background = options.CommitMode == CommitMode.Background;
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = background,            // PerMessage: we commit each offset ourselves
            EnableAutoOffsetStore = false,            // only offsets we mark as done get committed
        };

        using var consumer = new ConsumerBuilder<Ignore, string?>(config).Build();
        consumer.Subscribe(options.Topic);
        log.LogInformation("Consuming {Topic} as group {Group}, commit mode {Mode}",
            options.Topic, options.GroupId, options.CommitMode);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);

                var outcome = await retry.ExecuteAsync(
                    token => handler.HandleAsync(cr.Message.Value, token),
                    (ex, attempt, wait) => log.LogWarning(ex,
                        "Handling p{Partition}@{Offset} failed, retry {Attempt} in {Wait}ms",
                        cr.Partition.Value, cr.Offset.Value, attempt, wait.TotalMilliseconds),
                    ct);

                // Only after the DB commit or the DLQ publish
                if (background) consumer.StoreOffset(cr);   // committed in the background
                else consumer.Commit(cr);                   // committed now, waiting for the broker

                var level = outcome switch
                {
                    HandleOutcome.Stored => LogLevel.Debug,
                    HandleOutcome.Duplicate => LogLevel.Information,
                    _ => LogLevel.Warning,
                };
                log.Log(level, "{Outcome} p{Partition}@{Offset}", outcome, cr.Partition.Value, cr.Offset.Value);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // normal shutdown
        }
        catch (Exception ex)
        {
            log.LogCritical(ex, "Stopping: a message could not be handled after retries. " +
                "Its offset was not committed, so it will be redelivered after restart.");
            throw;                                    // stops the host; Program.cs turns it into exit code 1
        }
        finally
        {
            consumer.Close();                         // leave the group cleanly so rebalancing is fast
        }
    }
}
