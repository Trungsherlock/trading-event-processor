namespace TradeProcessor.Core.Validation.Rules;

/// <summary>The price must fit trades.price, NUMERIC(18,6). See QuantityPrecisionRule for why.</summary>
public sealed class PricePrecisionRule : ITradeRule
{
    private const int DecimalPlaces = 6;
    private const decimal Limit = 1_000_000_000_000m;   // 10^12: 12 digits before the point

    public string? Check(Trade trade) =>
        Math.Abs(trade.Price) < Limit && trade.Price == Math.Round(trade.Price, DecimalPlaces)
            ? null
            : "Price must be less than 10^12 with at most 6 decimal places";
}
