using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Validation;

public class NotInFutureRuleTests
{
    private static readonly DateTimeOffset Now = TradeBuilder.ExecutedAt;
    private readonly NotInFutureRule _rule = new(new FixedTimeProvider(Now));

    [Fact]
    public void Rejects_trade_executed_after_now()
    {
        var trade = TradeBuilder.Valid() with { ExecutedAt = Now.AddSeconds(1) };
        Assert.NotNull(_rule.Check(trade));
    }

    [Fact]
    public void Accepts_trade_executed_exactly_now() =>
        Assert.Null(_rule.Check(TradeBuilder.Valid() with { ExecutedAt = Now }));

    [Fact]
    public void Accepts_trade_executed_in_the_past() =>
        Assert.Null(_rule.Check(TradeBuilder.Valid() with { ExecutedAt = Now.AddDays(-1) }));

    [Fact]
    public void Compares_instants_not_clock_times()
    {
        // 09:30 in New York (UTC-5) is 14:30 UTC, the same instant as Now, so it's not in the future.
        var sameInstant = new DateTimeOffset(2026, 11, 17, 9, 30, 0, TimeSpan.FromHours(-5));
        Assert.Null(_rule.Check(TradeBuilder.Valid() with { ExecutedAt = sameInstant }));
    }
}
