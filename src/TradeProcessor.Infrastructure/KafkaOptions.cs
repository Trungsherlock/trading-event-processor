namespace TradeProcessor.Infrastructure;

/// <summary>Bound from the "Kafka" section of appsettings.json.</summary>
public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "trades";
    public string DeadLetterTopic { get; set; } = "trades.dlq";
    public string GroupId { get; set; } = "trade-processor";
    public CommitMode CommitMode { get; set; } = CommitMode.Background;   // measured 1.4× faster, see WORKLOG.md
}

public enum CommitMode
{
    /// <summary>Commit each offset to the broker, and wait for it, after each message.</summary>
    PerMessage,

    /// <summary>
    /// Mark each offset as done after each message; the client commits the latest one in the
    /// background (every 5 s by default) and on clean shutdown. Still at-least-once: only handled
    /// messages are marked. A crash replays up to ~5 s of messages, which the idempotent write absorbs.
    /// </summary>
    Background,
}
