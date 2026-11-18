namespace TradeProcessor.UnitTests.TestData;

/// <summary>A clock that always returns the same time, so tests control what "now" is.</summary>
public sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => now;
}
