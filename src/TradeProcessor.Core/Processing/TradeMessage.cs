using System.Text.Json;

namespace TradeProcessor.Core.Processing;

/// <summary>The message is not a well-formed trade, so it can never be processed.</summary>
public sealed class MalformedTradeException(string reason, Exception? inner = null)
    : Exception(reason, inner);

/// <summary>
/// Turns the raw Kafka message into a <see cref="Trade"/>. Every way that can fail throws
/// <see cref="MalformedTradeException"/>, so the handler has exactly one exception to send to the DLQ.
/// </summary>
public static class TradeMessage
{
    // camelCase, case-insensitive names. Numbers must be JSON numbers, not strings.
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.Strict,
    };

    // Every field is optional here, so a missing field is reported by name instead of becoming a default value.
    private sealed record Dto(Guid? TradeId, string? AccountId, string? Symbol, string? Side,
        decimal? Quantity, decimal? Price, DateTimeOffset? ExecutedAt);

    public static Trade Parse(string? json)
    {
        // A leading byte order mark is an encoding artifact (Windows tools add one); RFC 8259 lets parsers ignore it.
        if (json is not null && json.StartsWith('﻿'))
            json = json[1..];

        if (string.IsNullOrWhiteSpace(json))
            throw new MalformedTradeException("Message is empty");

        Dto? dto;
        try
        {
            dto = JsonSerializer.Deserialize<Dto>(json, Options);
        }
        catch (JsonException ex)
        {
            throw new MalformedTradeException($"Invalid JSON: {ex.Message}", ex);
        }

        if (dto is null)
            throw new MalformedTradeException("Message is not a JSON object");

        var missing = new List<string>();
        if (dto.TradeId is null) missing.Add("tradeId");
        if (dto.AccountId is null) missing.Add("accountId");
        if (dto.Symbol is null) missing.Add("symbol");
        if (dto.Side is null) missing.Add("side");
        if (dto.Quantity is null) missing.Add("quantity");
        if (dto.Price is null) missing.Add("price");
        if (dto.ExecutedAt is null) missing.Add("executedAt");
        if (missing.Count > 0)
            throw new MalformedTradeException($"Missing fields: {string.Join(", ", missing)}");

        var side = dto.Side switch
        {
            "BUY" => TradeSide.Buy,
            "SELL" => TradeSide.Sell,
            _ => throw new MalformedTradeException($"Unknown side '{dto.Side}' (expected BUY or SELL)"),
        };

        return new Trade(dto.TradeId!.Value, dto.AccountId!, dto.Symbol!, side,
            dto.Quantity!.Value, dto.Price!.Value, dto.ExecutedAt!.Value);
    }
}
