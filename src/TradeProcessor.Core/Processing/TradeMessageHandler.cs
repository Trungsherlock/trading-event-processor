using TradeProcessor.Core.Persistence;
using TradeProcessor.Core.Validation;

namespace TradeProcessor.Core.Processing;

public enum HandleOutcome { Stored, Duplicate, DeadLettered }

public interface IDeadLetterPublisher
{
    Task PublishAsync(string? originalJson, IReadOnlyList<string> errors, CancellationToken ct);
}

/// <summary>
/// Decides what happens to one message. Permanent failures (malformed, invalid, rejected by the
/// database) go to the DLQ and count as handled. Anything else propagates, so the caller doesn't
/// commit the offset and the message is redelivered.
/// </summary>
public sealed class TradeMessageHandler(ITradeValidator validator, ITradeRepository repo,
    IDeadLetterPublisher dlq)
{
    public async Task<HandleOutcome> HandleAsync(string? json, CancellationToken ct)
    {
        Trade trade;
        try
        {
            trade = TradeMessage.Parse(json);
        }
        catch (MalformedTradeException ex)
        {
            await dlq.PublishAsync(json, [$"Malformed: {ex.Message}"], ct);
            return HandleOutcome.DeadLettered;
        }

        var result = validator.Validate(trade);
        if (!result.IsValid)
        {
            await dlq.PublishAsync(json, result.Errors, ct);
            return HandleOutcome.DeadLettered;
        }

        try
        {
            return await repo.SaveAsync(trade, ct) == StoreResult.Inserted
                ? HandleOutcome.Stored
                : HandleOutcome.Duplicate;
        }
        catch (TradeRejectedException ex)
        {
            await dlq.PublishAsync(json, [$"Rejected by database: {ex.Message}"], ct);
            return HandleOutcome.DeadLettered;
        }
        // Other exceptions (database down, timeout) are NOT caught → no offset commit → redelivery.
    }
}
