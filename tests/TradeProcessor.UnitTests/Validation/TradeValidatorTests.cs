using TradeProcessor.Core.Validation;
using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Validation;

public class TradeValidatorTests
{
    private static ITradeRule[] AllRules() =>
    [
        new PositiveQuantityRule(),
        new PositivePriceRule(),
        new SymbolFormatRule(),
        new AccountIdRequiredRule(),
        new TradeIdRequiredRule(),
        new NotInFutureRule(new FixedTimeProvider(TradeBuilder.ExecutedAt)),
        new QuantityPrecisionRule(),
        new PricePrecisionRule(),
    ];

    [Fact]
    public void Valid_trade_passes_every_rule()
    {
        var result = new TradeValidator(AllRules()).Validate(TradeBuilder.Valid());

        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Collects_every_error_not_just_the_first()
    {
        var trade = TradeBuilder.Valid() with { Quantity = -5, Symbol = "bad", AccountId = "" };

        var result = new TradeValidator(AllRules()).Validate(trade);

        Assert.False(result.IsValid);
        Assert.Equal(3, result.Errors.Count);
    }

    [Fact]
    public void No_rules_means_valid() =>
        Assert.True(new TradeValidator([]).Validate(TradeBuilder.Valid()).IsValid);
}
