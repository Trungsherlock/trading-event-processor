namespace TradeProcessor.Core.Validation.Rules;

public sealed class PositiveQuantityRule : ITradeRule
{
    public string? Check(Trade trade) => trade.Quantity > 0 ? null : "Quantity must be positive";
}
