using TradeProcessor.Core.Processing;

namespace TradeProcessor.UnitTests.Processing;

public class RetryPolicyTests
{
    private readonly List<TimeSpan> _delays = [];
    private readonly RetryPolicy _policy;

    public RetryPolicyTests() =>
        _policy = new RetryPolicy(maxRetries: 3, baseDelay: TimeSpan.FromMilliseconds(200),
            delay: (wait, _) => { _delays.Add(wait); return Task.CompletedTask; });

    /// <summary>An action that throws for the first <paramref name="failures"/> calls, then returns 42.</summary>
    private static Func<CancellationToken, Task<int>> FailingTimes(int failures, Func<int, Exception>? error = null)
    {
        var calls = 0;
        return _ => ++calls <= failures
            ? throw (error?.Invoke(calls) ?? new TimeoutException($"attempt {calls}"))
            : Task.FromResult(42);
    }

    [Fact]
    public async Task Success_on_first_attempt_does_not_wait()
    {
        Assert.Equal(42, await _policy.ExecuteAsync(FailingTimes(0), onRetry: null, default));
        Assert.Empty(_delays);
    }

    [Fact]
    public async Task Retries_until_the_action_succeeds()
    {
        Assert.Equal(42, await _policy.ExecuteAsync(FailingTimes(2), onRetry: null, default));
        Assert.Equal(2, _delays.Count);
    }

    [Fact]
    public async Task Doubles_the_wait_after_each_failure()
    {
        await _policy.ExecuteAsync(FailingTimes(3), onRetry: null, default);

        Assert.Equal(new double[] { 200, 400, 800 }, _delays.Select(d => d.TotalMilliseconds));
    }

    [Fact]
    public async Task Gives_up_after_max_retries_and_rethrows_the_last_error()
    {
        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => _policy.ExecuteAsync(FailingTimes(10), onRetry: null, default));

        Assert.Equal("attempt 4", ex.Message);   // 1 attempt + 3 retries
    }

    [Fact]
    public async Task Does_not_retry_cancellation()
    {
        await Assert.ThrowsAsync<OperationCanceledException>(() => _policy.ExecuteAsync(
            FailingTimes(10, _ => new OperationCanceledException()), onRetry: null, default));

        Assert.Empty(_delays);
    }

    [Fact]
    public async Task Reports_each_retry()
    {
        var reported = new List<int>();

        await _policy.ExecuteAsync(FailingTimes(2), onRetry: (_, attempt, _) => reported.Add(attempt), default);

        Assert.Equal(new[] { 1, 2 }, reported);
    }
}
