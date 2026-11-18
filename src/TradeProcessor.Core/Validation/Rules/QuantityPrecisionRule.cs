namespace TradeProcessor.Core.Validation.Rules;

/// <summary>
/// The quantity must fit trades.quantity, NUMERIC(18,4). Otherwise Postgres either rounds it
/// silently (too many decimals) or throws on insert (too large), and a value that always
/// fails the insert would be redelivered forever instead of going to the DLQ.
/// </summary>
public sealed class QuantityPrecisionRule : ITradeRule
{
    private const int DecimalPlaces = 4;
    private const decimal Limit = 100_000_000_000_000m;   // 10^14: 14 digits before the point

    public string? Check(Trade trade) =>
        Math.Abs(trade.Quantity) < Limit && trade.Quantity == Math.Round(trade.Quantity, DecimalPlaces)
            ? null
            : "Quantity must be less than 10^14 with at most 4 decimal places";
}
