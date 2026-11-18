namespace TradeProcessor.Core;

public enum TradeSide { Buy, Sell }

public sealed record Trade(Guid TradeId, string AccountId, string Symbol, TradeSide Side,
    decimal Quantity, decimal Price, DateTimeOffset ExecutedAt)
{
    /// <summary>How much this trade changes the position: positive for a buy, negative for a sell.</summary>
    public decimal SignedQuantity => Side == TradeSide.Buy ? Quantity : -Quantity;
}
