using System.Diagnostics.CodeAnalysis;
using Akka.Streams;
using Arcane.Operator.Services.Base.Operators;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.HostedServices;

[ExcludeFromCodeCoverage(Justification = "Trivial")]
public class HostedStreamingClassOperatorService : BackgroundService
{
    private readonly ILogger<HostedStreamingClassOperatorService> logger;
    private readonly IMaterializer materializer;
    private readonly IStreamClassOperatorService streamClassOperatorService;

    public HostedStreamingClassOperatorService(
        ILogger<HostedStreamingClassOperatorService> logger,
        IStreamClassOperatorService streamClassOperatorService,
        IMaterializer materializer)
    {
        this.logger = logger;
        this.streamClassOperatorService = streamClassOperatorService;
        this.materializer = materializer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(message: "Activated {service}", nameof(HostedStreamingClassOperatorService));
        while (!stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Started listening for stream class events");
            await streamClassOperatorService
                .GetStreamClassEventsGraph(stoppingToken)
                .Run(materializer);
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation(message: "Stopping {service}", nameof(HostedStreamingClassOperatorService));
        return base.StopAsync(cancellationToken);
    }
}
