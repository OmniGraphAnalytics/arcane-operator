using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using k8s.Models;
using Microsoft.Extensions.Logging;
using OmniModels.Extensions;
using OmniModels.Services.Base;

namespace Arcane.Operator.Services.CommandHandlers;

public class UpdateStatusCommandHandler(
    IKubeCluster kubeCluster,
    IStreamClassRepository streamClassRepository,
    ILogger<UpdateStatusCommandHandler> logger)
    : ICommandHandler<UpdateStatusCommand>,
        ICommandHandler<SetStreamClassStatusCommand>
{
    public Task Handle(SetStreamClassStatusCommand command)
    {
        var status = new V1Alpha1StreamStatus
        {
            Phase = command.phase.ToString(),
            Conditions = command.conditions.ToArray(),
        };

        return kubeCluster.UpdateCustomResourceStatus(
                group: command.request.ApiGroup,
                version: command.request.ApiVersion,
                plural: command.request.PluralName,
                crdNamespace: command.request.Namespace,
                resourceName: command.resourceName,
                status: status,
                converter: element => element.AsOptionalStreamClass())
            .TryMap(selector: success => OnSuccess(maybeStreamClass: success, phase: command.phase),
                errorHandler: exception => OnFailure(exception: exception, request: command.request));
    }

    /// <inheritdoc cref="ICommandHandler{T}.Handle" />
    public Task Handle(UpdateStatusCommand command)
    {
        var ((nameSpace, kind, streamId), conditions, phase) = command;
        return streamClassRepository.Get(nameSpace: nameSpace, streamDefinitionKind: kind).FlatMap(crdConf =>
        {
            if (crdConf is { HasValue: false })
            {
                logger.LogError(message: "Failed to get configuration for kind {kind}", kind);
                return Task.FromResult(Option<IStreamDefinition>.None);
            }

            var status = new V1Alpha1StreamStatus { Phase = phase.ToString(), Conditions = conditions };

            logger.LogInformation(
                message: "Status and phase of stream with kind {kind} and id {streamId} changed to {statuses}, {phase}",
                kind,
                streamId,
                string.Join(separator: ", ", values: conditions.Select(sc => sc.Type)),
                phase);

            return kubeCluster.UpdateCustomResourceStatus(
                group: crdConf.Value.ApiGroupRef,
                version: crdConf.Value.VersionRef,
                plural: crdConf.Value.PluralNameRef,
                crdNamespace: nameSpace,
                resourceName: streamId,
                status: status,
                converter: element => element.AsOptionalStreamDefinition());
        });
    }

    private Option<IStreamClass> OnSuccess(Option<IStreamClass> maybeStreamClass, StreamClassPhase phase)
    {
        if (maybeStreamClass is { HasValue: false })
        {
            logger.LogError("Failed to get stream definition");
        }

        logger.LogInformation(message: "The phase of the stream class {namespace}/{name} changed to {status}",
            maybeStreamClass.Value.Metadata.Namespace(),
            maybeStreamClass.Value.Metadata.Name,
            phase);

        return maybeStreamClass;
    }

    private Option<IStreamClass> OnFailure(Exception exception, CustomResourceApiRequest request)
    {
        logger.LogError(exception: exception, message: "Failed to update stream class status for {@request}", request);
        return Option<IStreamClass>.None;
    }
}
