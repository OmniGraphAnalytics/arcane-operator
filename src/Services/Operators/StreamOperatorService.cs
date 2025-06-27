using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.EventFilters;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Base.Operators;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using OmniModels.Extensions;

namespace Arcane.Operator.Services.Operators;

public sealed class StreamOperatorService(
    IMetricsReporter metricsReporter,
    ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler,
    ICommandHandler<SetAnnotationCommand<V1Job>> setAnnotationCommandHandler,
    ICommandHandler<StreamingJobCommand> streamingJobCommandHandler,
    ILogger<StreamOperatorService> logger,
    IMaterializer materializer,
    IReactiveResourceCollection<IStreamDefinition> streamDefinitionSource,
    IEventFilter<IStreamDefinition> eventFilter,
    IStreamingJobCollection streamingJobCollection)
    : IStreamOperatorService, IDisposable
{
    private const int PARALLELISM = 1;
    private const int BUFFER_SIZE = 1000;

    private readonly CancellationTokenSource cancellationTokenSource = new();
    private readonly Dictionary<string, UniqueKillSwitch> killSwitches = new();

    public void Dispose()
    {
        this.cancellationTokenSource?.Cancel();
    }

    public void Attach(IStreamClass streamClass)
    {
        if (this.killSwitches.ContainsKey(streamClass.ToStreamClassId()))
        {
            return;
        }

        var request = new CustomResourceApiRequest(
            streamClass.Namespace(),
            streamClass.ApiGroupRef,
            streamClass.VersionRef,
            streamClass.PluralNameRef
        );


        var restartSource = RestartSource
            .WithBackoff(() => streamDefinitionSource.GetEvents(request, streamClass.MaxBufferCapacity), streamClass.RestartSettings)
            .ViaMaterialized(KillSwitches.Single<ResourceEvent<IStreamDefinition>>(), Keep.Right);

        var ks = restartSource
            .Recover(cause =>
            {
                logger.LogError(cause, "Stream class {streamClassId} has been stopped due to an exception", streamClass.ToStreamClassId());
                this.Detach(streamClass);
                throw cause;
            })
            .ToMaterialized(this.Sink.Value, Keep.Left).Run(materializer);
        this.killSwitches[streamClass.ToStreamClassId()] = ks;
    }

    public void Detach(IStreamClass streamClass)
    {
        if (this.killSwitches.TryGetValue(streamClass.ToStreamClassId(), out var ks))
        {
            ks.Shutdown();
            this.killSwitches.Remove(streamClass.ToStreamClassId());
        }
        logger.LogInformation("THe stream class with id {streamClassId} has been detached", streamClass.ToStreamClassId());
    }

    private Lazy<Sink<ResourceEvent<IStreamDefinition>, NotUsed>> Sink =>
        new(() => this.BuildSink(this.cancellationTokenSource.Token).Run(materializer));

    private IRunnableGraph<Sink<ResourceEvent<IStreamDefinition>, NotUsed>> BuildSink(CancellationToken cancellationToken)
    {
        return MergeHub.Source<ResourceEvent<IStreamDefinition>>(perProducerBufferSize: BUFFER_SIZE)
            .Via(eventFilter.Filter())
            .CollectOption()
            .Via(cancellationToken.AsFlow<ResourceEvent<IStreamDefinition>>(true))
            .Select(metricsReporter.ReportTrafficMetrics)
            .SelectAsync(PARALLELISM,
                ev => streamingJobCollection.Get(ev.kubernetesObject.Namespace(),
                        ev.kubernetesObject.StreamId)
                    .Map(job => (ev, job)))
            .Select(this.OnEvent)
            .SelectMany(e => e)
            .To(Akka.Streams.Dsl.Sink.ForEachAsync<KubernetesCommand>(PARALLELISM, this.HandleCommand))
            .WithAttributes(new Attributes(new ActorAttributes.SupervisionStrategy(this.LogAndResumeDecider)));
    }

    private Decider LogAndResumeDecider => cause =>
    {
        logger.LogWarning(cause, "Queue element dropped due to exception in processing code");
        return Directive.Resume;
    };

    private List<KubernetesCommand> OnEvent((ResourceEvent<IStreamDefinition>, Option<V1Job>) resourceEvent)
    {
        return resourceEvent switch
        {
            ((WatchEventType.Added, var sd), var maybeJob) => this.OnAdded(sd, maybeJob).AsList(),
            ((WatchEventType.Modified, var sd), var maybeJob) => this.OnModified(sd, maybeJob),
            _ => new List<KubernetesCommand>()
        };
    }

    private KubernetesCommand OnAdded(IStreamDefinition streamDefinition, Option<V1Job> maybeJob)
    {
        logger.LogInformation("Added a stream definition with id {streamId}", streamDefinition.StreamId);
        return maybeJob switch
        {
            { HasValue: true, Value: var job } when job.IsReloading() => new Reloading(streamDefinition),
            { HasValue: true, Value: var job } when !job.IsReloading() => new Running(streamDefinition),
            { HasValue: true, Value: var job } when streamDefinition.Suspended => new StopJob(job.Name(), job.Namespace()),
            { HasValue: false } when streamDefinition.Suspended => new Suspended(streamDefinition),
            { HasValue: false } when !streamDefinition.Suspended => new StartJob(streamDefinition, true),
            _ => throw new ArgumentOutOfRangeException(nameof(maybeJob), maybeJob, null)
        };
    }

    private List<KubernetesCommand> OnModified(IStreamDefinition streamDefinition, Option<V1Job> maybeJob)
    {
        logger.LogInformation("Modified a stream definition with id {streamId}", streamDefinition.StreamId);
        return maybeJob switch
        {
            { HasValue: false } when streamDefinition.CrashLoopDetected => new SetCrashLoopStatusCommand(streamDefinition).AsList(),
            { HasValue: false } when streamDefinition.Suspended => new Suspended(streamDefinition).AsList(),
            { HasValue: false } when streamDefinition.ReloadRequested => new List<KubernetesCommand>
            {
                new StartJob(streamDefinition, true),
            },
            { HasValue: false } => new StartJob(streamDefinition, false).AsList(),

            { HasValue: true, Value: var job } when streamDefinition.CrashLoopDetected => new
                List<KubernetesCommand>
                {
                    new SetCrashLoopStatusCommand(streamDefinition)
                },
            { HasValue: true, Value: var job } when streamDefinition.Suspended => new
                List<KubernetesCommand>
                {
                    new StopJob(job.Name(), job.Namespace()),
                },
            { HasValue: true, Value: var job } when !job.ConfigurationMatches(streamDefinition) => new
                List<KubernetesCommand>
                {
                    new RequestJobRestartCommand(job),
                },
            { HasValue: true, Value: var job } when streamDefinition.ReloadRequested => new
                List<KubernetesCommand>
                {
                    new RequestJobReloadCommand(job),
                },
            { HasValue: true, Value: var job } when job.ConfigurationMatches(streamDefinition) =>
                new List<KubernetesCommand>(),
            _ => new List<KubernetesCommand>()
        };
    }

    private Task HandleCommand(KubernetesCommand response) => response switch
    {
        UpdateStatusCommand sdc => updateStatusCommandHandler.Handle(sdc),
        StreamingJobCommand sjc => streamingJobCommandHandler.Handle(sjc),
        RequestJobRestartCommand rrc => setAnnotationCommandHandler.Handle(rrc),
        RequestJobReloadCommand rrc => setAnnotationCommandHandler.Handle(rrc),
        SetAnnotationCommand<V1Job> sac => setAnnotationCommandHandler.Handle(sac),
        _ => throw new ArgumentOutOfRangeException(nameof(response), response, null)
    };
}
