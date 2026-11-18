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
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            EnableAutoCommit = false,                 // we decide when a message is done
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        using var consumer = new ConsumerBuilder<Ignore, string?>(config).Build();
        consumer.Subscribe(options.Topic);
        log.LogInformation("Consuming {Topic} as group {Group}", options.Topic, options.GroupId);

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

                consumer.Commit(cr);                  // only after the DB commit or the DLQ publish

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
            Environment.ExitCode = 1;
            throw;                                    // stops the host
        }
        finally
        {
            consumer.Close();                         // leave the group cleanly so rebalancing is fast
        }
    }
}
