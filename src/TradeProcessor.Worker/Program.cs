using TradeProcessor.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddTradeProcessor(builder.Configuration);

using var host = builder.Build();

// Look the workers up before running: RunAsync disposes the host (and its services) when it ends.
var workers = host.Services.GetServices<IHostedService>().OfType<BackgroundService>().ToList();
await host.RunAsync();

// A worker that gave up after retries stops the host (see TradeConsumerWorker). Report that with
// exit code 1 so a supervisor knows to restart it; a normal shutdown exits with 0.
return workers.Any(w => w.ExecuteTask is { IsFaulted: true }) ? 1 : 0;
