using Npgsql;
using TradeProcessor.Core;
using TradeProcessor.Core.Persistence;
using TradeProcessor.Infrastructure;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.IntegrationTests;

[Collection(PostgresCollection.Name)]
public sealed class NpgsqlTradeRepositoryTests(PostgresFixture pg) : IAsyncLifetime
{
    private readonly NpgsqlTradeRepository _repo = new(pg.Db);

    public Task InitializeAsync() => pg.ResetAsync();
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Same_trade_twice_counts_position_once()
    {
        var trade = TradeBuilder.Valid();

        Assert.Equal(StoreResult.Inserted, await _repo.SaveAsync(trade, default));
        Assert.Equal(StoreResult.Duplicate, await _repo.SaveAsync(trade, default));

        Assert.Equal(100m, await PositionAsync("ACC-1001", "AAPL"));
        Assert.Equal(1, await TradeCountAsync());
    }

    [Fact]
    public async Task Concurrent_copies_of_one_trade_are_stored_once()
    {
        // e.g. two consumers both handling the same message during a rebalance
        var trade = TradeBuilder.Valid();

        var results = await Task.WhenAll(
            Enumerable.Range(0, 8).Select(_ => _repo.SaveAsync(trade, default)));

        Assert.Single(results, r => r == StoreResult.Inserted);
        Assert.Equal(100m, await PositionAsync("ACC-1001", "AAPL"));
    }

    [Fact]
    public async Task Stores_every_field_exactly()
    {
        var trade = TradeBuilder.Valid() with { Side = TradeSide.Sell, Quantity = 12.3456m, Price = 189.123456m };

        await _repo.SaveAsync(trade, default);

        await using var cmd = pg.Db.CreateCommand(
            "SELECT account_id, symbol, side, quantity, price, executed_at FROM trades WHERE trade_id = $1");
        cmd.Parameters.Add(new() { Value = trade.TradeId });
        await using var row = await cmd.ExecuteReaderAsync();
        Assert.True(await row.ReadAsync());
        Assert.Equal("ACC-1001", row.GetString(0));
        Assert.Equal("AAPL", row.GetString(1));
        Assert.Equal("SELL", row.GetString(2));
        Assert.Equal(12.3456m, row.GetDecimal(3));
        Assert.Equal(189.123456m, row.GetDecimal(4));
        Assert.Equal(trade.ExecutedAt, row.GetFieldValue<DateTimeOffset>(5));
    }

    [Fact]
    public async Task Accepts_executed_at_with_a_non_utc_offset()
    {
        // Npgsql only writes timestamptz from a UTC offset; the repository must convert.
        var newYork = new DateTimeOffset(2026, 11, 17, 9, 30, 0, TimeSpan.FromHours(-5));
        var trade = TradeBuilder.Valid() with { ExecutedAt = newYork };

        Assert.Equal(StoreResult.Inserted, await _repo.SaveAsync(trade, default));
    }

    [Fact]
    public async Task Buys_and_sells_net_out_per_account_and_symbol()
    {
        var buy = TradeBuilder.Valid() with { Quantity = 100m };
        await _repo.SaveAsync(buy, default);
        await _repo.SaveAsync(buy with { TradeId = Guid.NewGuid(), Side = TradeSide.Sell, Quantity = 30m }, default);
        await _repo.SaveAsync(buy with { TradeId = Guid.NewGuid(), Symbol = "MSFT", Quantity = 5m }, default);
        await _repo.SaveAsync(buy with { TradeId = Guid.NewGuid(), AccountId = "ACC-2002", Quantity = 7m }, default);

        Assert.Equal(70m, await PositionAsync("ACC-1001", "AAPL"));
        Assert.Equal(5m, await PositionAsync("ACC-1001", "MSFT"));
        Assert.Equal(7m, await PositionAsync("ACC-2002", "AAPL"));
    }

    [Fact]
    public async Task Failed_position_update_rolls_back_the_trade_insert_and_is_reported_as_permanent()
    {
        // Each trade fits its column, but the sum overflows positions.net_quantity (NUMERIC(18,4)).
        var big = TradeBuilder.Valid() with { Quantity = 99_999_999_999_999m };
        await _repo.SaveAsync(big, default);

        var second = big with { TradeId = Guid.NewGuid() };
        var ex = await Assert.ThrowsAsync<TradeRejectedException>(() => _repo.SaveAsync(second, default));

        // A permanent data error, so the handler sends it to the DLQ instead of retrying forever
        Assert.Equal(PostgresErrorCodes.NumericValueOutOfRange, Assert.IsType<PostgresException>(ex.InnerException).SqlState);
        Assert.Equal(1, await TradeCountAsync());            // the second insert was rolled back
        Assert.Equal(99_999_999_999_999m, await PositionAsync("ACC-1001", "AAPL"));
    }

    private async Task<decimal?> PositionAsync(string account, string symbol)
    {
        await using var cmd = pg.Db.CreateCommand(
            "SELECT net_quantity FROM positions WHERE account_id = $1 AND symbol = $2");
        cmd.Parameters.Add(new() { Value = account });
        cmd.Parameters.Add(new() { Value = symbol });
        return await cmd.ExecuteScalarAsync() as decimal?;
    }

    private async Task<long> TradeCountAsync()
    {
        await using var cmd = pg.Db.CreateCommand("SELECT count(*) FROM trades");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}
