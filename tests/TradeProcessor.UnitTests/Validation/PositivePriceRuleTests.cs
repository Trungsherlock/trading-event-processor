using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Validation;

public class PositivePriceRuleTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Rejects_non_positive_price(int price)
    {
        var trade = TradeBuilder.Valid() with { Price = price };
        Assert.NotNull(new PositivePriceRule().Check(trade));
    }

    [Fact]
    public void Accepts_positive_price() =>
        Assert.Null(new PositivePriceRule().Check(TradeBuilder.Valid()));
}
