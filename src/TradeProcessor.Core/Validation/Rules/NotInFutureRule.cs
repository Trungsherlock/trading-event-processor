namespace TradeProcessor.Core.Validation.Rules;

/// <summary>The clock is injected so tests can control what "now" is.</summary>
public sealed class NotInFutureRule(TimeProvider clock) : ITradeRule
{
    public string? Check(Trade trade) =>
        trade.ExecutedAt <= clock.GetUtcNow() ? null : "ExecutedAt must not be in the future";
}
