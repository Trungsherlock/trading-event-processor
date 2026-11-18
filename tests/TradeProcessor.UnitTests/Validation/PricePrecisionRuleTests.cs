using System.Globalization;
using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Validation;

// trades.price is NUMERIC(18,6): at most 12 digits before the decimal point and 6 after.
public class PricePrecisionRuleTests
{
    [Theory]
    [InlineData("0.0000001")]          // 7 decimal places
    [InlineData("1000000000000")]      // 10^12
    public void Rejects_price_that_does_not_fit_the_column(string price) =>
        Assert.NotNull(new PricePrecisionRule().Check(WithPrice(price)));

    [Theory]
    [InlineData("0.000001")]
    [InlineData("999999999999.999999")]
    public void Accepts_price_that_fits_the_column(string price) =>
        Assert.Null(new PricePrecisionRule().Check(WithPrice(price)));

    private static Core.Trade WithPrice(string price) =>
        TradeBuilder.Valid() with { Price = decimal.Parse(price, CultureInfo.InvariantCulture) };
}
