using Npgsql;
using TradeProcessor.Core.Persistence;
using TradeProcessor.Core.Processing;
using TradeProcessor.Core.Validation;
using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.Infrastructure;

namespace TradeProcessor.Worker;

public static class ServiceRegistration
{
    /// <summary>
    /// Everything the worker needs. Program.cs and the tests both call this, so the tests check
    /// the real wiring.
    /// </summary>
    public static IServiceCollection AddTradeProcessor(this IServiceCollection services, IConfiguration config)
    {
        var kafka = config.GetSection("Kafka").Get<KafkaOptions>() ?? new KafkaOptions();
        services.AddSingleton(kafka);
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton(NpgsqlDataSource.Create(config.GetConnectionString("Trades")
            ?? throw new InvalidOperationException("Missing connection string 'Trades'")));
        services.AddSingleton(_ => KafkaDeadLetterPublisher.CreateProducer(kafka));

        // DI injects all of these as IEnumerable<ITradeRule>
        services.AddSingleton<ITradeRule, PositiveQuantityRule>();
        services.AddSingleton<ITradeRule, PositivePriceRule>();
        services.AddSingleton<ITradeRule, SymbolFormatRule>();
        services.AddSingleton<ITradeRule, AccountIdRequiredRule>();
        services.AddSingleton<ITradeRule, TradeIdRequiredRule>();
        services.AddSingleton<ITradeRule, NotInFutureRule>();
        services.AddSingleton<ITradeRule, QuantityPrecisionRule>();
        services.AddSingleton<ITradeRule, PricePrecisionRule>();

        services.AddSingleton<ITradeValidator, TradeValidator>();
        services.AddSingleton<ITradeRepository, NpgsqlTradeRepository>();
        services.AddSingleton<IDeadLetterPublisher, KafkaDeadLetterPublisher>();
        services.AddSingleton<TradeMessageHandler>();

        // 3 retries at 200 ms, 400 ms, 800 ms; after that the worker stops (see TradeConsumerWorker).
        services.AddSingleton(new RetryPolicy(maxRetries: 3, baseDelay: TimeSpan.FromMilliseconds(200)));
        services.AddHostedService<TradeConsumerWorker>();
        return services;
    }
}
