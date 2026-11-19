using System.Text.Json;

namespace TradeProducer;

/// <summary>What the database and DLQ should contain after the worker processes one producer run.</summary>
public sealed record Expected(
    int Seed,
    int Sent,
    int ValidTrades,
    int InvalidMessages,
    int DuplicateMessages,
    long DlqOffsetBefore,
    List<ExpectedPosition> Positions)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public void Save(string path) => File.WriteAllText(path, JsonSerializer.Serialize(this, Json));

    public static Expected Load(string path) =>
        JsonSerializer.Deserialize<Expected>(File.ReadAllText(path), Json)
        ?? throw new InvalidOperationException($"{path} is empty");
}

public sealed record ExpectedPosition(string AccountId, string Symbol, decimal NetQuantity);
