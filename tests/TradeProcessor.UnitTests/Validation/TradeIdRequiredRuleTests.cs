using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Validation;

public class TradeIdRequiredRuleTests
{
    [Fact]
    public void Rejects_empty_trade_id()
    {
        var trade = TradeBuilder.Valid() with { TradeId = Guid.Empty };
        Assert.NotNull(new TradeIdRequiredRule().Check(trade));
    }

    [Fact]
    public void Accepts_trade_id() =>
        Assert.Null(new TradeIdRequiredRule().Check(TradeBuilder.Valid()));
}
