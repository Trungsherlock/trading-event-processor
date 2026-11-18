using System.Text.Json;
using Confluent.Kafka;
using TradeProcessor.Core.Processing;

namespace TradeProcessor.Infrastructure;

/// <summary>
/// Publishes {"original", "errors", "failedAt"} to the DLQ topic and waits for the broker to
/// acknowledge it, so the caller only commits the source offset once the DLQ copy is durable.
/// </summary>
public sealed class KafkaDeadLetterPublisher(IProducer<Null, string> producer, KafkaOptions options,
    TimeProvider clock) : IDeadLetterPublisher
{
    // Relaxed escaping keeps quotes readable ("x" instead of "x"). Safe here because the
    // envelope is never embedded in HTML.
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    public async Task PublishAsync(string? originalJson, IReadOnlyList<string> errors, CancellationToken ct)
    {
        var envelope = JsonSerializer.Serialize(
            new { original = originalJson, errors, failedAt = clock.GetUtcNow() }, Json);

        await producer.ProduceAsync(options.DeadLetterTopic, new Message<Null, string> { Value = envelope }, ct);
    }

    public static IProducer<Null, string> CreateProducer(KafkaOptions options) =>
        new ProducerBuilder<Null, string>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,              // wait until the broker has stored it
            EnableIdempotence = true,     // the producer's own retries can't create duplicates
        }).Build();
}
