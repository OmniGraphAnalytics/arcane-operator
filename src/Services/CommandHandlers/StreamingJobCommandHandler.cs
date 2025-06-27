using System.Net;
using Akka;
using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using k8s.Autorest;
using k8s.Models;
using Microsoft.Extensions.Logging;
using OmniModels.Extensions;
using OmniModels.Services.Base;
using OmniModels.Services.Kubernetes;

namespace Arcane.Operator.Services.CommandHandlers;

/// <inheritdoc cref="ICommandHandler{T}" />
public class StreamingJobCommandHandler(
    IStreamClassRepository streamClassRepository,
    IKubeCluster kubeCluster,
    ILogger<StreamingJobCommandHandler> logger,
    ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler,
    IStreamingJobTemplateRepository streamingJobTemplateRepository)
    : ICommandHandler<StreamingJobCommand>
{
    /// <inheritdoc cref="ICommandHandler{T}.Handle" />
    public Task Handle(StreamingJobCommand command)
    {
        return command switch
        {
            StartJob startJob => streamClassRepository
                .Get(nameSpace: startJob.streamDefinition.Namespace(), streamDefinitionKind: startJob.streamDefinition.Kind)
                .TryMap(maybeSc => maybeSc switch
                {
                    { HasValue: true, Value: var sc } => StartJob(streamDefinition: startJob.streamDefinition, isBackfilling: startJob.IsBackfilling,
                        streamClass: sc),
                    { HasValue: false } => throw new InvalidOperationException($"Stream class not found for {startJob.streamDefinition.Kind}"),
                }),
            StopJob stopJob => kubeCluster.DeleteJob(jobId: stopJob.name, jobNamespace: stopJob.nameSpace),
            _ => throw new ArgumentOutOfRangeException(paramName: nameof(command), actualValue: command, message: null),
        };
    }

    private Task StartJob(IStreamDefinition streamDefinition, bool isBackfilling, IStreamClass streamClass)
    {
        var template = streamDefinition.GetJobTemplate(isBackfilling);

        return streamingJobTemplateRepository
            .GetStreamingJobTemplate(kind: template.Kind, jobNamespace: streamDefinition.Namespace(), templateName: template.Name)
            .FlatMap(t => TryStartJobFromTemplate(jobTemplate: t, streamDefinition: streamDefinition, streamClass: streamClass,
                isBackfilling: isBackfilling, reference: template))
            .FlatMap(async command =>
            {
                await updateStatusCommandHandler.Handle(command);
                return NotUsed.Instance;
            });
    }

    private Task<UpdateStatusCommand> TryStartJobFromTemplate(Option<IStreamingJobTemplate> jobTemplate,
        IStreamDefinition streamDefinition,
        IStreamClass streamClass,
        bool isBackfilling,
        V1TypedLocalObjectReference reference)
    {
        if (!jobTemplate.HasValue)
        {
            var message = $"Failed to find job template with kind {reference.Kind} and name {reference.Name}";
            var condition = V1Alpha1StreamCondition.CustomErrorCondition(message);
            var command = new SetInternalErrorStatus(affectedResource: streamDefinition, conditions: condition);
            return Task.FromResult<UpdateStatusCommand>(command);
        }


        try
        {
            var job = BuildJob(jobTemplate: jobTemplate, streamDefinition: streamDefinition, streamClass: streamClass, isBackfilling: isBackfilling);
            logger.LogInformation(message: "Starting a new stream job with an id {streamId}", streamDefinition.StreamId);
            return kubeCluster
                .SendJob(job: job, jobNamespace: streamDefinition.Metadata.Namespace(), cancellationToken: CancellationToken.None)
                .TryMap(
                    selector: _ => isBackfilling ? new Reloading(streamDefinition) : new Running(streamDefinition),
                    errorHandler: ex => HandleError(exception: ex, streamDefinition: streamDefinition, isBackfilling: isBackfilling));
        }
        catch (Exception ex)
        {
            var condition = V1Alpha1StreamCondition.CustomErrorCondition($"Failed to build job: {ex.Message}");
            return Task.FromResult<UpdateStatusCommand>(new SetInternalErrorStatus(affectedResource: streamDefinition, conditions: condition));
        }
    }

    private UpdateStatusCommand HandleError(Exception exception, IStreamDefinition streamDefinition, bool isBackfilling)
    {
        if (exception is HttpOperationException { Response.StatusCode: HttpStatusCode.Conflict })
        {
            logger.LogWarning(exception: exception, message: "Streaming job with ID {streamId} already exists", streamDefinition.StreamId);
            return isBackfilling ? new Reloading(streamDefinition) : new Running(streamDefinition);
        }

        logger.LogError(exception: exception, message: "Failed to send job");
        var condition = V1Alpha1StreamCondition.CustomErrorCondition($"Failed to start job: {exception.Message}");
        return new SetInternalErrorStatus(affectedResource: streamDefinition, conditions: condition);
    }

    private V1Job BuildJob(Option<IStreamingJobTemplate> jobTemplate, IStreamDefinition streamDefinition,
        IStreamClass streamClass, bool isBackfilling)
    {
        return jobTemplate
            .Value
            .GetJob()
            .WithStreamingJobLabels(streamId: streamDefinition.StreamId, isBackfilling: isBackfilling, streamKind: streamDefinition.Kind)
            .WithStreamingJobAnnotations(streamDefinition.GetConfigurationChecksum())
            .WithMetadataAnnotations(streamClass)
            .WithCustomEnvironment(streamDefinition.ToV1EnvFromSources(streamClass))
            .WithCustomEnvironment(streamDefinition.ToEnvironment(isBackfilling: isBackfilling, streamClass: streamClass))
            .WithOwnerReference(apiVersion: streamDefinition.ApiVersion, kind: streamDefinition.Kind, metadata: streamDefinition.Metadata)
            .WithName(streamDefinition.StreamId);
    }
}
