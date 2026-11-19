using System.Diagnostics;
using System.Globalization;
using Confluent.Kafka;
using Npgsql;
using TradeProducer;

// Usage:
//   TradeProducer <count> [--seed N] [--invalid-rate 0.05] [--duplicate-rate 0.02] [--accounts 20]
//   TradeProducer verify [--wait-seconds 0]
// Shared options: --bootstrap localhost:9092  --topic trades  --dlq-topic trades.dlq
//                 --db "<connection string>"   --expected expected.json

var opts = ParseOptions(args.Skip(1));
string Opt(string name, string fallback) => opts.GetValueOrDefault(name, fallback);

var bootstrap = Opt("bootstrap", "localhost:9092");
var topic = Opt("topic", "trades");
var dlqTopic = Opt("dlq-topic", "trades.dlq");
var expectedPath = Opt("expected", "expected.json");
var db = Opt("db", "Host=localhost;Port=5433;Database=trades;Username=trades;Password=trades");

if (args.Length > 0 && int.TryParse(args[0], out var count) && count > 0)
    return Produce(count);
if (args.Length > 0 && args[0] == "verify")
    return await VerifyAsync();

Console.Error.WriteLine("Usage: TradeProducer <count> [options] | TradeProducer verify [options]");
return 2;

int Produce(int n)
{
    var seed = int.Parse(Opt("seed", Random.Shared.Next().ToString()), CultureInfo.InvariantCulture);
    var gen = new TradeGenerator(seed,
        accounts: int.Parse(Opt("accounts", "20"), CultureInfo.InvariantCulture),
        invalidRate: double.Parse(Opt("invalid-rate", "0.05"), CultureInfo.InvariantCulture),
        duplicateRate: double.Parse(Opt("duplicate-rate", "0.02"), CultureInfo.InvariantCulture));

    var dlqBefore = DlqHighWatermark();   // so verify can count only this run's DLQ entries

    using var producer = new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrap,
        Acks = Acks.All,
        EnableIdempotence = true,
        LingerMs = 5,                      // batch messages for speed
    }).Build();

    var failed = 0;
    var sw = Stopwatch.StartNew();
    for (var i = 0; i < n; i++)
    {
        var (key, value) = gen.Next();
        while (true)
        {
            try
            {
                producer.Produce(topic, new Message<string, string> { Key = key, Value = value },
                    report => { if (report.Error.IsError) Interlocked.Increment(ref failed); });
                break;
            }
            catch (ProduceException<string, string> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                producer.Poll(TimeSpan.FromMilliseconds(100));   // local buffer full: wait for deliveries
            }
        }
    }
    producer.Flush(TimeSpan.FromMinutes(2));
    sw.Stop();

    if (failed > 0)
    {
        Console.Error.WriteLine($"{failed} messages failed to deliver; expected results would be wrong.");
        return 1;
    }

    var positions = gen.Positions
        .Select(p => new ExpectedPosition(p.Key.Account, p.Key.Symbol, p.Value))
        .OrderBy(p => p.AccountId, StringComparer.Ordinal).ThenBy(p => p.Symbol, StringComparer.Ordinal)
        .ToList();
    new Expected(seed, n, gen.Valid, gen.Invalid, gen.Duplicates, dlqBefore, positions).Save(expectedPath);

    Console.WriteLine($"Sent {n:N0} messages to '{topic}' in {sw.Elapsed.TotalSeconds:N1}s ({n / sw.Elapsed.TotalSeconds:N0} msg/s), seed {seed}");
    Console.WriteLine($"  valid trades:      {gen.Valid:N0}   <- expected rows in trades");
    Console.WriteLine($"  invalid/malformed: {gen.Invalid:N0}   <- expected DLQ entries");
    Console.WriteLine($"  duplicate resends: {gen.Duplicates:N0}   <- must not change any position");
    Console.WriteLine($"  positions:         {positions.Count} account/symbol pairs");
    foreach (var p in positions.Take(5))
        Console.WriteLine($"    {p.AccountId} {p.Symbol,-5} {p.NetQuantity,12}");
    if (positions.Count > 5) Console.WriteLine($"    ... all {positions.Count} in {expectedPath}");
    return 0;
}

async Task<int> VerifyAsync()
{
    var expected = Expected.Load(expectedPath);
    var waitSeconds = int.Parse(Opt("wait-seconds", "0"), CultureInfo.InvariantCulture);
    await using var dataSource = NpgsqlDataSource.Create(db);

    async Task<long> TradeCountAsync()
    {
        await using var cmd = dataSource.CreateCommand("SELECT count(*) FROM trades");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    // Optionally wait for the worker to catch up.
    var deadline = DateTime.UtcNow.AddSeconds(waitSeconds);
    var trades = await TradeCountAsync();
    while (trades < expected.ValidTrades && DateTime.UtcNow < deadline)
    {
        await Task.Delay(1000);
        trades = await TradeCountAsync();
    }

    var actual = new Dictionary<(string, string), decimal>();
    await using (var cmd = dataSource.CreateCommand("SELECT account_id, symbol, net_quantity FROM positions"))
    await using (var reader = await cmd.ExecuteReaderAsync())
        while (await reader.ReadAsync())
            actual[(reader.GetString(0), reader.GetString(1))] = reader.GetDecimal(2);

    var mismatches = expected.Positions
        .Where(p => actual.GetValueOrDefault((p.AccountId, p.Symbol)) != p.NetQuantity
                    || !actual.ContainsKey((p.AccountId, p.Symbol)))
        .Select(p => $"{p.AccountId}/{p.Symbol}: expected {p.NetQuantity}, got {(actual.TryGetValue((p.AccountId, p.Symbol), out var a) ? a : "missing")}")
        .Concat(actual.Keys.Except(expected.Positions.Select(p => (p.AccountId, p.Symbol)))
            .Select(k => $"{k.Item1}/{k.Item2}: unexpected row"))
        .ToList();

    var dlqEntries = DlqHighWatermark() - expected.DlqOffsetBefore;

    var tradesOk = trades == expected.ValidTrades;
    var positionsOk = mismatches.Count == 0;
    var dlqOk = dlqEntries >= expected.InvalidMessages;   // more is possible: a replayed DLQ publish

    Console.WriteLine($"{(tradesOk ? "PASS" : "FAIL")}  trades rows:     expected {expected.ValidTrades:N0}, actual {trades:N0}");
    Console.WriteLine($"{(positionsOk ? "PASS" : "FAIL")}  positions:       {expected.Positions.Count} expected, {mismatches.Count} mismatched");
    foreach (var m in mismatches.Take(10)) Console.WriteLine($"        {m}");
    Console.WriteLine($"{(dlqOk ? "PASS" : "FAIL")}  DLQ entries:     expected {expected.InvalidMessages:N0}, actual {dlqEntries:N0}" +
        (dlqEntries > expected.InvalidMessages ? $" ({dlqEntries - expected.InvalidMessages} replayed)" : ""));

    return tradesOk && positionsOk && dlqOk ? 0 : 1;
}

long DlqHighWatermark()
{
    using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
    var partitions = admin.GetMetadata(dlqTopic, TimeSpan.FromSeconds(10)).Topics[0].Partitions;

    using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
    {
        BootstrapServers = bootstrap,
        GroupId = "trade-producer-tool",   // required by the client; nothing is ever committed
    }).Build();
    return partitions.Sum(p => consumer
        .QueryWatermarkOffsets(new TopicPartition(dlqTopic, p.PartitionId), TimeSpan.FromSeconds(10)).High.Value);
}

static Dictionary<string, string> ParseOptions(IEnumerable<string> args)
{
    var result = new Dictionary<string, string>();
    using var it = args.GetEnumerator();
    while (it.MoveNext())
    {
        if (!it.Current.StartsWith("--")) throw new ArgumentException($"Unexpected argument '{it.Current}'");
        var name = it.Current[2..];
        result[name] = it.MoveNext() ? it.Current : throw new ArgumentException($"--{name} needs a value");
    }
    return result;
}
