using Npgsql;
using TradeProcessor.Core;
using TradeProcessor.Core.Persistence;

namespace TradeProcessor.Infrastructure;

/// <summary>
/// Plain ADO.NET. The insert is idempotent (ON CONFLICT DO NOTHING on trade_id), and the
/// position is only updated when the insert added a row, so a replayed message changes nothing.
/// </summary>
public sealed class NpgsqlTradeRepository(NpgsqlDataSource db) : ITradeRepository
{
    public async Task<StoreResult> SaveAsync(Trade trade, CancellationToken ct)
    {
        try
        {
            return await SaveInTransactionAsync(trade, ct);
        }
        catch (PostgresException ex) when (IsPermanentDataError(ex))
        {
            // The transaction has already rolled back. Retrying would fail the same way every time.
            throw new TradeRejectedException($"{ex.SqlState}: {ex.MessageText}", ex);
        }
    }

    /// <summary>
    /// Class 22 (data exception, e.g. numeric overflow) and class 23 (constraint violation) depend
    /// only on the data. Connection errors, timeouts, deadlocks and the like are worth retrying.
    /// </summary>
    private static bool IsPermanentDataError(PostgresException ex) =>
        ex.SqlState.StartsWith("22", StringComparison.Ordinal) ||
        ex.SqlState.StartsWith("23", StringComparison.Ordinal);

    private async Task<StoreResult> SaveInTransactionAsync(Trade t, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        await using var insert = new NpgsqlCommand("""
            INSERT INTO trades (trade_id, account_id, symbol, side, quantity, price, executed_at)
            VALUES (@id, @acc, @sym, @side, @qty, @price, @at)
            ON CONFLICT (trade_id) DO NOTHING
            """, conn, tx);
        insert.Parameters.AddWithValue("id", t.TradeId);
        insert.Parameters.AddWithValue("acc", t.AccountId);
        insert.Parameters.AddWithValue("sym", t.Symbol);
        insert.Parameters.AddWithValue("side", t.Side == TradeSide.Buy ? "BUY" : "SELL");
        insert.Parameters.AddWithValue("qty", t.Quantity);
        insert.Parameters.AddWithValue("price", t.Price);
        insert.Parameters.AddWithValue("at", t.ExecutedAt.ToUniversalTime()); // Npgsql rejects non-UTC offsets for timestamptz

        if (await insert.ExecuteNonQueryAsync(ct) == 0)
        {
            await tx.RollbackAsync(ct);
            return StoreResult.Duplicate;     // replayed message: don't touch positions
        }

        await using var upsert = new NpgsqlCommand("""
            INSERT INTO positions (account_id, symbol, net_quantity) VALUES (@acc, @sym, @delta)
            ON CONFLICT (account_id, symbol) DO UPDATE
            SET net_quantity = positions.net_quantity + EXCLUDED.net_quantity,
                updated_at   = now()
            """, conn, tx);
        upsert.Parameters.AddWithValue("acc", t.AccountId);
        upsert.Parameters.AddWithValue("sym", t.Symbol);
        upsert.Parameters.AddWithValue("delta", t.SignedQuantity);
        await upsert.ExecuteNonQueryAsync(ct);

        await tx.CommitAsync(ct);
        return StoreResult.Inserted;
    }
}
