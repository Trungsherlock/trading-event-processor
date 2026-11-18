using System.Globalization;
using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Validation;

// trades.quantity is NUMERIC(18,4): at most 14 digits before the decimal point and 4 after.
public class QuantityPrecisionRuleTests
{
    [Theory]
    [InlineData("0.00001")]            // 5 decimal places: Postgres would silently round it
    [InlineData("100000000000000")]    // 10^14: Postgres would throw "numeric field overflow"
    public void Rejects_quantity_that_does_not_fit_the_column(string qty) =>
        Assert.NotNull(new QuantityPrecisionRule().Check(WithQuantity(qty)));

    [Theory]
    [InlineData("0.0001")]
    [InlineData("99999999999999.9999")]
    [InlineData("100.50000")]          // trailing zeros don't add precision
    public void Accepts_quantity_that_fits_the_column(string qty) =>
        Assert.Null(new QuantityPrecisionRule().Check(WithQuantity(qty)));

    private static Core.Trade WithQuantity(string qty) =>
        TradeBuilder.Valid() with { Quantity = decimal.Parse(qty, CultureInfo.InvariantCulture) };
}
