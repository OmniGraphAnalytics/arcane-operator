using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.Util;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Base.Operators;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OmniModels.Extensions;

namespace Arcane.Operator.Services.Operators;

/// <inheritdoc cref="IStreamClassOperatorService"/>
public class StreamClassOperatorService : IStreamClassOperatorService
{
    private const int PARALLELISM = 1;

    private readonly StreamClassOperatorServiceConfiguration configuration;

    private readonly ILogger<StreamClassOperatorService> logger;
    private readonly IMetricsReporter metricsService;
    private readonly CustomResourceApiRequest request;
    private readonly IStreamClassRepository streamClassRepository;
    private readonly ICommandHandler<SetStreamClassStatusCommand> streamClassStatusCommandHandler;
    private readonly IStreamOperatorService streamOperatorService;

    public StreamClassOperatorService(IOptions<StreamClassOperatorServiceConfiguration> streamOperatorServiceOptions,
        IStreamClassRepository streamClassRepository,
        IMetricsReporter metricsService,
        ILogger<StreamClassOperatorService> logger,
        ICommandHandler<SetStreamClassStatusCommand> streamClassStatusCommandHandler,
        IStreamOperatorService streamOperatorService)
    {
        configuration = streamOperatorServiceOptions.Value;
        this.logger = logger;
        this.streamClassRepository = streamClassRepository;
        this.metricsService = metricsService;
        this.streamOperatorService = streamOperatorService;
        this.streamClassStatusCommandHandler = streamClassStatusCommandHandler;
        request = new CustomResourceApiRequest(
            Namespace: configuration.NameSpace,
            ApiGroup: configuration.ApiGroup,
            ApiVersion: configuration.Version,
            PluralName: configuration.Plural
        );
    }

    /// <inheritdoc cref="IStreamClassOperatorService.GetStreamClassEventsGraph"/>
    public IRunnableGraph<Task> GetStreamClassEventsGraph(CancellationToken cancellationToken)
    {
        var sink = Sink.ForEachAsync<SetStreamClassStatusCommand>(parallelism: PARALLELISM, action: command =>
        {
            streamClassStatusCommandHandler.Handle(command);
            return streamClassRepository.InsertOrUpdate(streamClass: command.streamClass, phase: command.phase, conditions: command.conditions,
                pluralName: command.request.PluralName);
        });

        return streamClassRepository.GetEvents(request: request, maxBufferCapacity: configuration.MaxBufferCapacity)
            .Via(cancellationToken.AsFlow<ResourceEvent<IStreamClass>>(true))
            .Select(OnEvent)
            .CollectOption()
            .Select(streamClass => metricsService.ReportStatusMetrics(streamClass))
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(HandleError))
            .ToMaterialized(sink: sink, combine: Keep.Right);
    }

    private Option<SetStreamClassStatusCommand> OnEvent(ResourceEvent<IStreamClass> resourceEvent)
    {
        return resourceEvent switch
        {
            (WatchEventType.Added, var streamClass) => Attach(streamClass),
            (WatchEventType.Deleted, var streamClass) => Detach(streamClass),
            _ => Option<SetStreamClassStatusCommand>.None,
        };
    }

    private SetStreamClassReady Attach(IStreamClass streamClass)
    {
        streamOperatorService.Attach(streamClass);
        return new SetStreamClassReady(resourceName: streamClass.Name(), request: request, streamClass: streamClass);
    }

    private Option<SetStreamClassStatusCommand> Detach(IStreamClass streamClass)
    {
        streamOperatorService.Detach(streamClass);
        return Option<SetStreamClassStatusCommand>.None;
    }

    private Directive HandleError(Exception exception)
    {
        logger.LogError(exception: exception, message: "Failed to handle stream definition event");
        return exception switch
        {
            BufferOverflowException => Directive.Stop,
            _ => Directive.Resume,
        };
    }
}
