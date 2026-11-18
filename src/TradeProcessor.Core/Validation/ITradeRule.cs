namespace TradeProcessor.Core.Validation;

public interface ITradeRule
{
    /// <summary>Returns null if the trade passes, otherwise the error message.</summary>
    string? Check(Trade trade);
}
