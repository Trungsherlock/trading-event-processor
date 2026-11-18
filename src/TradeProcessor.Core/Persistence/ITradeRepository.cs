namespace TradeProcessor.Core.Persistence;

public enum StoreResult { Inserted, Duplicate }

public interface ITradeRepository
{
    /// <summary>
    /// Stores the trade and updates its position in one transaction. Saving a trade whose
    /// TradeId is already stored changes nothing and returns <see cref="StoreResult.Duplicate"/>.
    /// </summary>
    Task<StoreResult> SaveAsync(Trade trade, CancellationToken ct);
}
