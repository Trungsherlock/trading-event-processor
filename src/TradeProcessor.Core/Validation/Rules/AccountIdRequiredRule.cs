namespace TradeProcessor.Core.Validation.Rules;

public sealed class AccountIdRequiredRule : ITradeRule
{
    public string? Check(Trade trade) =>
        string.IsNullOrWhiteSpace(trade.AccountId) ? "AccountId is required" : null;
}
