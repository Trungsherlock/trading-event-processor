namespace TradeProcessor.Core.Validation.Rules;

public sealed class TradeIdRequiredRule : ITradeRule
{
    public string? Check(Trade trade) =>
        trade.TradeId == Guid.Empty ? "TradeId is required" : null;
}
