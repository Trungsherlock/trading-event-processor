namespace TradeProcessor.Core.Persistence;

/// <summary>
/// The store permanently refused the trade (for example, a value overflowed a column).
/// Retrying can never succeed, so the message belongs in the DLQ.
/// </summary>
public sealed class TradeRejectedException(string reason, Exception? inner = null)
    : Exception(reason, inner);
