using TradeProcessor.Core;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests;

public class TradeTests
{
    [Fact]
    public void Buy_adds_to_position() =>
        Assert.Equal(100m, (TradeBuilder.Valid() with { Side = TradeSide.Buy }).SignedQuantity);

    [Fact]
    public void Sell_subtracts_from_position() =>
        Assert.Equal(-100m, (TradeBuilder.Valid() with { Side = TradeSide.Sell }).SignedQuantity);
}
