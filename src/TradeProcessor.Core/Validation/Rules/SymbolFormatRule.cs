using System.Text.RegularExpressions;

namespace TradeProcessor.Core.Validation.Rules;

public sealed partial class SymbolFormatRule : ITradeRule
{
    // \z, not $: $ also matches before a trailing newline, so "AAPL\n" would pass.
    [GeneratedRegex(@"^[A-Z]{1,5}\z")]
    private static partial Regex Pattern();

    public string? Check(Trade trade) =>
        trade.Symbol is not null && Pattern().IsMatch(trade.Symbol)
            ? null
            : "Symbol must be 1-5 uppercase letters";
}
