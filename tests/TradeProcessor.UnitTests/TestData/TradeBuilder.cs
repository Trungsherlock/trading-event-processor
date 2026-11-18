using TradeProcessor.Core;

namespace TradeProcessor.UnitTests.TestData;

public static class TradeBuilder
{
    public static readonly DateTimeOffset ExecutedAt = new(2026, 11, 17, 14, 30, 0, TimeSpan.Zero);

    public static Trade Valid() => new(Guid.NewGuid(), "ACC-1001", "AAPL", TradeSide.Buy,
        100m, 189.25m, ExecutedAt);
}
