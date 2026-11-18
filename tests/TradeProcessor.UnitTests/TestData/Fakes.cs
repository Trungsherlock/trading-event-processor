using TradeProcessor.Core;
using TradeProcessor.Core.Persistence;
using TradeProcessor.Core.Processing;

namespace TradeProcessor.UnitTests.TestData;

public sealed class FakeTradeRepository : ITradeRepository
{
    public List<Trade> Saved { get; } = [];

    /// <summary>When set, SaveAsync throws this instead of saving.</summary>
    public Exception? Throw { get; set; }

    public Task<StoreResult> SaveAsync(Trade trade, CancellationToken ct)
    {
        if (Throw is not null) throw Throw;
        if (Saved.Any(s => s.TradeId == trade.TradeId)) return Task.FromResult(StoreResult.Duplicate);
        Saved.Add(trade);
        return Task.FromResult(StoreResult.Inserted);
    }
}

public sealed class FakeDeadLetterPublisher : IDeadLetterPublisher
{
    public List<(string? Json, IReadOnlyList<string> Errors)> Published { get; } = [];

    /// <summary>When set, PublishAsync throws this instead of publishing.</summary>
    public Exception? Throw { get; set; }

    public Task PublishAsync(string? originalJson, IReadOnlyList<string> errors, CancellationToken ct)
    {
        if (Throw is not null) throw Throw;
        Published.Add((originalJson, errors));
        return Task.CompletedTask;
    }
}
