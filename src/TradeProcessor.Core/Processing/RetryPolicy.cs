namespace TradeProcessor.Core.Processing;

/// <summary>
/// Retries a failing action with exponential backoff (baseDelay, 2×, 4×, ...), then rethrows the
/// last error. Cancellation is never retried. The delay is injectable so tests don't wait.
/// </summary>
public sealed class RetryPolicy(int maxRetries, TimeSpan baseDelay,
    Func<TimeSpan, CancellationToken, Task>? delay = null)
{
    private readonly Func<TimeSpan, CancellationToken, Task> _delay = delay ?? Task.Delay;

    /// <param name="onRetry">Called before each wait with the error, the retry number (1-based) and the wait.</param>
    public async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> action,
        Action<Exception, int, TimeSpan>? onRetry, CancellationToken ct)
    {
        for (var retry = 0; ; retry++)
        {
            try
            {
                return await action(ct);
            }
            catch (Exception ex) when (retry < maxRetries && ex is not OperationCanceledException)
            {
                var wait = baseDelay * Math.Pow(2, retry);
                onRetry?.Invoke(ex, retry + 1, wait);
                await _delay(wait, ct);
            }
        }
    }
}
