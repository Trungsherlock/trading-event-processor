using Npgsql;
using TradeProcessor.Core.Persistence;
using TradeProcessor.Core.Processing;
using TradeProcessor.Core.Validation;
using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.Infrastructure;
using TradeProcessor.Worker;

var builder = Host.CreateApplicationBuilder(args);

var kafka = builder.Configuration.GetSection("Kafka").Get<KafkaOptions>() ?? new KafkaOptions();
builder.Services.AddSingleton(kafka);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton(
    NpgsqlDataSource.Create(builder.Configuration.GetConnectionString("Trades")
        ?? throw new InvalidOperationException("Missing connection string 'Trades'")));
builder.Services.AddSingleton(_ => KafkaDeadLetterPublisher.CreateProducer(kafka));

// DI injects all of these as IEnumerable<ITradeRule>
builder.Services.AddSingleton<ITradeRule, PositiveQuantityRule>();
builder.Services.AddSingleton<ITradeRule, PositivePriceRule>();
builder.Services.AddSingleton<ITradeRule, SymbolFormatRule>();
builder.Services.AddSingleton<ITradeRule, AccountIdRequiredRule>();
builder.Services.AddSingleton<ITradeRule, TradeIdRequiredRule>();
builder.Services.AddSingleton<ITradeRule, NotInFutureRule>();
builder.Services.AddSingleton<ITradeRule, QuantityPrecisionRule>();
builder.Services.AddSingleton<ITradeRule, PricePrecisionRule>();

builder.Services.AddSingleton<ITradeValidator, TradeValidator>();
builder.Services.AddSingleton<ITradeRepository, NpgsqlTradeRepository>();
builder.Services.AddSingleton<IDeadLetterPublisher, KafkaDeadLetterPublisher>();
builder.Services.AddSingleton<TradeMessageHandler>();

// 3 retries at 200 ms, 400 ms, 800 ms; after that the worker stops (see TradeConsumerWorker).
builder.Services.AddSingleton(new RetryPolicy(maxRetries: 3, baseDelay: TimeSpan.FromMilliseconds(200)));
builder.Services.AddHostedService<TradeConsumerWorker>();

builder.Build().Run();
