using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TradeProcessor.Core.Processing;
using TradeProcessor.Core.Validation;
using TradeProcessor.Worker;

namespace TradeProcessor.IntegrationTests;

/// <summary>
/// Checks the worker's real DI wiring (the same AddTradeProcessor call Program.cs makes).
/// No Docker needed: nothing here connects to Kafka or Postgres.
/// </summary>
public class ServiceRegistrationTests
{
    private static ServiceProvider BuildProvider()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:Trades"] = "Host=localhost;Database=trades;Username=trades;Password=trades",
            ["Kafka:BootstrapServers"] = "localhost:9092",
        }).Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddTradeProcessor(config);
        // ValidateOnBuild fails if any registered service has a dependency nobody registered.
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });
    }

    [Fact]
    public void Every_service_can_be_constructed()
    {
        using var provider = BuildProvider();

        Assert.NotNull(provider.GetRequiredService<TradeMessageHandler>());
        Assert.Single(provider.GetServices<IHostedService>().OfType<TradeConsumerWorker>());
    }

    [Fact]
    public void Every_validation_rule_in_core_is_registered_exactly_once()
    {
        // Adding a rule class without registering it would otherwise pass every other test.
        var ruleClasses = typeof(ITradeRule).Assembly.GetTypes()
            .Where(t => typeof(ITradeRule).IsAssignableFrom(t) && t is { IsClass: true, IsAbstract: false })
            .Select(t => t.Name).Order().ToList();

        using var provider = BuildProvider();
        var registered = provider.GetServices<ITradeRule>().Select(r => r.GetType().Name).Order().ToList();

        Assert.Equal(8, ruleClasses.Count);
        Assert.Equal(ruleClasses, registered);
    }
}
