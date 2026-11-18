namespace TradeProcessor.Infrastructure;

/// <summary>Bound from the "Kafka" section of appsettings.json.</summary>
public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "trades";
    public string DeadLetterTopic { get; set; } = "trades.dlq";
    public string GroupId { get; set; } = "trade-processor";
}
