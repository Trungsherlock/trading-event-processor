using System.Text.Json.Nodes;
using TradeProcessor.Core;
using TradeProcessor.Core.Processing;

namespace TradeProcessor.UnitTests.Processing;

public class TradeMessageTests
{
    private const string ValidJson = """
        {"tradeId":"8f14e45f-ea2b-4c1a-9b7e-3f0d2a1c5b6e","accountId":"ACC-1001","symbol":"AAPL",
         "side":"BUY","quantity":100.5,"price":189.123456,"executedAt":"2026-11-17T14:30:00Z"}
        """;

    /// <summary>The valid message with one field replaced (or removed, when value is null).</summary>
    private static string With(string field, JsonNode? value)
    {
        var obj = JsonNode.Parse(ValidJson)!.AsObject();
        if (value is null) obj.Remove(field); else obj[field] = value;
        return obj.ToJsonString();
    }

    [Fact]
    public void Parses_every_field()
    {
        var trade = TradeMessage.Parse(ValidJson);

        Assert.Equal(Guid.Parse("8f14e45f-ea2b-4c1a-9b7e-3f0d2a1c5b6e"), trade.TradeId);
        Assert.Equal("ACC-1001", trade.AccountId);
        Assert.Equal("AAPL", trade.Symbol);
        Assert.Equal(TradeSide.Buy, trade.Side);
        Assert.Equal(100.5m, trade.Quantity);
        Assert.Equal(189.123456m, trade.Price);   // decimal, no floating-point rounding
        Assert.Equal(new DateTimeOffset(2026, 11, 17, 14, 30, 0, TimeSpan.Zero), trade.ExecutedAt);
    }

    [Fact]
    public void Parses_sell() =>
        Assert.Equal(TradeSide.Sell, TradeMessage.Parse(With("side", "SELL")).Side);

    [Theory]
    [InlineData(null)]                 // empty Kafka message (tombstone)
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("null")]               // valid JSON, but not an object
    [InlineData("[]")]
    [InlineData("\"AAPL\"")]
    [InlineData("{not json")]
    public void Rejects_message_that_is_not_a_trade_object(string? json) =>
        Assert.Throws<MalformedTradeException>(() => TradeMessage.Parse(json));

    public static TheoryData<string, JsonNode> BadFields => new()
    {
        { "side", "HOLD" },                        // unknown side
        { "side", "buy" },                         // sides are uppercase
        { "tradeId", "not-a-guid" },
        { "executedAt", "yesterday" },
        { "quantity", "100" },                     // a string, not a number
        { "quantity", JsonNode.Parse("1e30")! },   // too large for decimal
        { "price", JsonNode.Parse("true")! },
    };

    [Theory]
    [MemberData(nameof(BadFields))]
    public void Rejects_field_with_wrong_type_or_value(string field, JsonNode value) =>
        Assert.Throws<MalformedTradeException>(() => TradeMessage.Parse(With(field, value)));

    [Fact]
    public void Lists_every_missing_field()
    {
        var json = JsonNode.Parse(With("accountId", null))!.AsObject();
        json.Remove("price");

        var ex = Assert.Throws<MalformedTradeException>(() => TradeMessage.Parse(json.ToJsonString()));

        Assert.Contains("accountId", ex.Message);
        Assert.Contains("price", ex.Message);
    }

    [Fact]
    public void Ignores_a_leading_byte_order_mark() =>
        // Windows tools often prepend one; RFC 8259 lets parsers ignore it.
        Assert.Equal("AAPL", TradeMessage.Parse("﻿" + ValidJson).Symbol);

    [Fact]
    public void Ignores_unknown_fields() =>
        Assert.Equal("AAPL", TradeMessage.Parse(With("venue", "NASDAQ")).Symbol);
}
