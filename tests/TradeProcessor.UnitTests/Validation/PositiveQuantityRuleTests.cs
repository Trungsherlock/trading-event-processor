using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Validation;

public class PositiveQuantityRuleTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Rejects_non_positive_quantity(int qty)   // attributes can't take decimal, so use int
    {
        var trade = TradeBuilder.Valid() with { Quantity = qty };
        Assert.NotNull(new PositiveQuantityRule().Check(trade));
    }

    [Fact]
    public void Accepts_positive_quantity() =>
        Assert.Null(new PositiveQuantityRule().Check(TradeBuilder.Valid()));
}
