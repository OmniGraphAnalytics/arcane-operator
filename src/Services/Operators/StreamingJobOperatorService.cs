using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OmniModels.Extensions;
using OmniModels.Services.Kubernetes;

namespace Arcane.Operator.Services.Operators;

public class StreamingJobOperatorService(
    ILogger<StreamingJobOperatorService> logger,
    IOptions<StreamingJobOperatorServiceConfiguration> options,
    IMetricsReporter metricsReporter,
    IResourceCollection<IStreamDefinition> streamDefinitionCollection,
    ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler,
    ICommandHandler<SetAnnotationCommand<IStreamDefinition>> setAnnotationCommandHandler,
    ICommandHandler<StreamingJobCommand> streamingJobCommandHandler,
    ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>> removeAnnotationHandler,
    IStreamingJobCollection streamingJobCollection)
    : IStreamingJobOperatorService
{
    private const int parallelism = 1;
    private readonly StreamingJobOperatorServiceConfiguration configuration = options.Value;


    public IRunnableGraph<Task> GetJobEventsGraph(CancellationToken cancellationToken)
    {
        return streamingJobCollection.GetEvents(this.configuration.Namespace, this.configuration.MaxBufferCapacity)
            .Via(cancellationToken.AsFlow<ResourceEvent<V1Job>>(true))
            .Select(metricsReporter.ReportTrafficMetrics)
            .SelectAsync(parallelism, this.OnJobEvent)
            .SelectMany(e => e)
            .CollectOption()
            .ToMaterialized(Sink.ForEachAsync<KubernetesCommand>(parallelism, this.HandleCommand), Keep.Right);
    }

    private Task<List<Option<KubernetesCommand>>> OnJobEvent(ResourceEvent<V1Job> valueTuple)
    {
        return valueTuple switch
        {
            (WatchEventType.Added, var job) => this.OnJobAdded(job),
            (WatchEventType.Modified, var job) => Task.FromResult(new List<Option<KubernetesCommand>> { this.OnJobModified(job) }),
            (WatchEventType.Deleted, var job) => this.OnJobDelete(job),
            _ => Task.FromResult(new List<Option<KubernetesCommand>>())
        };
    }

    private Task<List<Option<KubernetesCommand>>> OnJobAdded(V1Job job)
    {
        return streamDefinitionCollection
            .Get(job.Name(), job.ToOwnerApiRequest())
            .Map(maybeSd => maybeSd switch
            {
                { HasValue: true, Value: var sd } when job.IsReloading() && sd.ReloadRequested => new
                    List<Option<KubernetesCommand>>
                    {
                        new RemoveReloadRequestedAnnotation(sd),
                        new Reloading(sd)
                    },
                { HasValue: true, Value: var sd } when job.IsReloading() && !sd.ReloadRequested => new
                    List<Option<KubernetesCommand>>
                    {
                        new Reloading(sd)
                    },
                _ => new List<Option<KubernetesCommand>>()
            });
    }

    private Option<KubernetesCommand> OnJobModified(V1Job job)
    {
        var streamId = job.GetStreamId();
        if (job.IsStopping())
        {
            logger.LogInformation("Streaming job for stream with id {streamId} is already stopping",
                streamId);
            return Option<KubernetesCommand>.None;
        }

        if (job.IsReloadRequested() || job.IsRestartRequested())
        {
            return new StopJob(job.Name(), job.Namespace());
        }

        return Option<KubernetesCommand>.None;
    }

    private Task<List<Option<KubernetesCommand>>> OnJobDelete(V1Job job)
    {
        return streamDefinitionCollection
            .Get(job.Name(), job.ToOwnerApiRequest())
            .Map(maybeSd => maybeSd switch
            {
                { HasValue: true, Value: var sd } when job.IsFailed() => new List<Option<KubernetesCommand>>
                {
                    new SetCrashLoopStatusCommand(sd),
                    new SetCrashLoopStatusAnnotationCommand(sd)
                },
                { HasValue: true, Value: var sd } when sd.Suspended => new List<Option<KubernetesCommand>>
                {
                    new Suspended(sd)
                },
                { HasValue: true, Value: var sd } when sd.CrashLoopDetected => new List<Option<KubernetesCommand>>
                {
                    new SetCrashLoopStatusCommand(sd)
                },
                { HasValue: true, Value: var sd } when !sd.Suspended => new List<Option<KubernetesCommand>>
                {
                    new StartJob(sd, job.IsReloadRequested() || job.IsSchemaMismatch())
                },
                { HasValue: false } => new List<Option<KubernetesCommand>>(),
                _ => throw new ArgumentOutOfRangeException(nameof(maybeSd), maybeSd, null)
            });
    }

    private Task HandleCommand(KubernetesCommand response) => response switch
    {
        UpdateStatusCommand sdc => updateStatusCommandHandler.Handle(sdc),
        StreamingJobCommand sjc => streamingJobCommandHandler.Handle(sjc),
        SetAnnotationCommand<IStreamDefinition> sac => setAnnotationCommandHandler.Handle(sac),
        RemoveAnnotationCommand<IStreamDefinition> command => removeAnnotationHandler.Handle(command),
        _ => throw new ArgumentOutOfRangeException(nameof(response), response, null)
    };
}
