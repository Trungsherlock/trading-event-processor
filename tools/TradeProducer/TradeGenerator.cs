using System.Text.Json;

namespace TradeProducer;

/// <summary>
/// Generates a reproducible stream of trade messages (same seed → same messages, except
/// executedAt, which is always shortly before now so it passes the not-in-future rule) and tracks
/// what the database should contain once the worker has processed all of them.
/// Mix: about <c>invalidRate</c> invalid or malformed messages, about <c>duplicateRate</c>
/// exact resends of an earlier valid trade, and valid trades for the rest.
/// </summary>
public sealed class TradeGenerator(int seed, int accounts, double invalidRate, double duplicateRate)
{
    private static readonly string[] Symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM"];
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly Random _rng = new(seed);
    private readonly List<(string Key, string Value)> _sentValid = [];
    private readonly Dictionary<(string Account, string Symbol), decimal> _positions = [];

    public int Valid { get; private set; }
    public int Invalid { get; private set; }
    public int Duplicates { get; private set; }

    public IReadOnlyDictionary<(string Account, string Symbol), decimal> Positions => _positions;

    /// <summary>The next message: Kafka key (the account ID) and JSON value.</summary>
    public (string Key, string Value) Next()
    {
        var roll = _rng.NextDouble();

        if (roll < duplicateRate && _sentValid.Count > 0)
        {
            Duplicates++;
            return _sentValid[_rng.Next(_sentValid.Count)];   // same key, so same partition as the original
        }

        if (roll < duplicateRate + invalidRate)
        {
            Invalid++;
            return NextInvalid();
        }

        var trade = NewTrade();
        var signed = trade.side == "BUY" ? trade.quantity : -trade.quantity;
        var key = (trade.accountId, trade.symbol);
        _positions[key] = _positions.GetValueOrDefault(key) + signed;
        Valid++;

        var message = (trade.accountId, JsonSerializer.Serialize(trade, Json));
        _sentValid.Add(message);
        return message;
    }

    private Guid NewId()
    {
        Span<byte> bytes = stackalloc byte[16];
        _rng.NextBytes(bytes);
        return new Guid(bytes);
    }

    private TradeDto NewTrade() => new(
        tradeId: NewId(),
        accountId: $"ACC-{1001 + _rng.Next(accounts)}",
        symbol: Symbols[_rng.Next(Symbols.Length)],
        side: _rng.Next(2) == 0 ? "BUY" : "SELL",
        quantity: _rng.Next(1, 100_000) / 100m,          // 0.01 to 999.99
        price: _rng.Next(100, 100_000) / 100m,           // 1.00 to 999.99
        executedAt: DateTimeOffset.UtcNow.AddSeconds(-_rng.Next(1, 3600)));

    /// <summary>One of five kinds of bad message, each of which the worker must send to the DLQ.</summary>
    private (string Key, string Value) NextInvalid()
    {
        var t = NewTrade();
        var value = _rng.Next(5) switch
        {
            0 => JsonSerializer.Serialize(t with { quantity = -t.quantity }, Json),        // negative quantity
            1 => JsonSerializer.Serialize(t with { price = 0m }, Json),                    // zero price
            2 => JsonSerializer.Serialize(t with { symbol = t.symbol.ToLowerInvariant() }, Json), // bad symbol
            3 => JsonSerializer.Serialize(t, Json)[..20],                                  // truncated JSON
            _ => JsonSerializer.Serialize(new { t.tradeId, t.accountId, t.symbol }, Json), // missing fields
        };
        return (t.accountId, value);
    }

    // Lowercase names so the JSON matches the message format exactly.
    private sealed record TradeDto(Guid tradeId, string accountId, string symbol, string side,
        decimal quantity, decimal price, DateTimeOffset executedAt);
}
