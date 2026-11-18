namespace TradeProcessor.Core.Validation.Rules;

public sealed class PositivePriceRule : ITradeRule
{
    public string? Check(Trade trade) => trade.Price > 0 ? null : "Price must be positive";
}
