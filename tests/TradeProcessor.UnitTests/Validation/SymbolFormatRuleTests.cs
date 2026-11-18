using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Validation;

public class SymbolFormatRuleTests
{
    [Theory]
    [InlineData("")]
    [InlineData("aapl")]      // lowercase
    [InlineData("ABCDEF")]    // too long
    [InlineData("BRK.B")]     // punctuation
    [InlineData(" AAPL")]     // whitespace
    [InlineData("AAPL\n")]    // trailing newline: a regex ending in $ would let this through
    [InlineData(null)]        // missing from the JSON
    public void Rejects_bad_symbol(string? symbol)
    {
        var trade = TradeBuilder.Valid() with { Symbol = symbol! };
        Assert.NotNull(new SymbolFormatRule().Check(trade));
    }

    [Theory]
    [InlineData("A")]
    [InlineData("AAPL")]
    [InlineData("GOOGL")]
    public void Accepts_one_to_five_uppercase_letters(string symbol)
    {
        var trade = TradeBuilder.Valid() with { Symbol = symbol };
        Assert.Null(new SymbolFormatRule().Check(trade));
    }
}
