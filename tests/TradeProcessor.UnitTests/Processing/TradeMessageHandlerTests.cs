using TradeProcessor.Core.Persistence;
using TradeProcessor.Core.Processing;
using TradeProcessor.Core.Validation;
using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Processing;

public class TradeMessageHandlerTests
{
    private const string ValidJson = """
        {"tradeId":"8f14e45f-ea2b-4c1a-9b7e-3f0d2a1c5b6e","accountId":"ACC-1","symbol":"AAPL",
         "side":"BUY","quantity":10,"price":10,"executedAt":"2026-11-17T14:30:00Z"}
        """;

    private readonly FakeTradeRepository _repo = new();
    private readonly FakeDeadLetterPublisher _dlq = new();
    private readonly TradeMessageHandler _handler;

    public TradeMessageHandlerTests() =>
        _handler = new TradeMessageHandler(
            new TradeValidator([new PositiveQuantityRule(), new SymbolFormatRule()]), _repo, _dlq);

    [Fact]
    public async Task Valid_trade_is_stored()
    {
        Assert.Equal(HandleOutcome.Stored, await _handler.HandleAsync(ValidJson, default));
        Assert.Single(_repo.Saved);
        Assert.Empty(_dlq.Published);
    }

    [Fact]
    public async Task Same_message_twice_is_reported_as_duplicate()
    {
        await _handler.HandleAsync(ValidJson, default);

        Assert.Equal(HandleOutcome.Duplicate, await _handler.HandleAsync(ValidJson, default));
        Assert.Single(_repo.Saved);
    }

    [Fact]
    public async Task Invalid_trade_goes_to_dlq_with_every_error_and_is_not_stored()
    {
        var json = ValidJson.Replace("\"quantity\":10", "\"quantity\":-5").Replace("AAPL", "bad");

        Assert.Equal(HandleOutcome.DeadLettered, await _handler.HandleAsync(json, default));
        Assert.Empty(_repo.Saved);
        var (original, errors) = Assert.Single(_dlq.Published);
        Assert.Equal(json, original);
        Assert.Equal(2, errors.Count);
    }

    [Theory]
    [InlineData("{not json")]
    [InlineData("null")]
    [InlineData(null)]
    public async Task Malformed_message_goes_to_dlq(string? json)
    {
        Assert.Equal(HandleOutcome.DeadLettered, await _handler.HandleAsync(json, default));
        Assert.Empty(_repo.Saved);
        Assert.Single(_dlq.Published);
    }

    [Fact]
    public async Task Trade_rejected_by_the_database_goes_to_dlq()
    {
        _repo.Throw = new TradeRejectedException("22003: numeric field overflow");

        Assert.Equal(HandleOutcome.DeadLettered, await _handler.HandleAsync(ValidJson, default));
        var (_, errors) = Assert.Single(_dlq.Published);
        Assert.Contains("numeric field overflow", Assert.Single(errors));
    }

    [Fact]
    public async Task Other_repository_errors_propagate_so_the_offset_is_not_committed()
    {
        _repo.Throw = new TimeoutException("database unreachable");

        await Assert.ThrowsAsync<TimeoutException>(() => _handler.HandleAsync(ValidJson, default));
        Assert.Empty(_dlq.Published);
    }

    [Fact]
    public async Task Dlq_errors_propagate_so_the_offset_is_not_committed()
    {
        _dlq.Throw = new TimeoutException("kafka unreachable");

        await Assert.ThrowsAsync<TimeoutException>(() => _handler.HandleAsync("{not json", default));
    }
}
