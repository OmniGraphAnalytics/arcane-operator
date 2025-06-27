using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using k8s.Models;
using Microsoft.Extensions.Logging;
using OmniModels.Extensions;
using OmniModels.Services.Base;

namespace Arcane.Operator.Services.CommandHandlers;

public class AnnotationCommandHandler(
    IStreamClassRepository streamClassRepository,
    IKubeCluster kubeCluster,
    ILogger<AnnotationCommandHandler> logger)
    :
        ICommandHandler<SetAnnotationCommand<IStreamDefinition>>,
        ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>>,
        ICommandHandler<SetAnnotationCommand<V1Job>>
{
    public Task Handle(SetAnnotationCommand<IStreamDefinition> command)
    {
        var ((nameSpace, kind, name), annotationKey, annotationValue) = command;
        return streamClassRepository.Get(nameSpace, kind).Map(crdConf =>
        {
            if (crdConf is { HasValue: false })
            {
                logger.LogError("Failed to get configuration for kind {kind}", kind);
                return Task.CompletedTask;
            }

            return kubeCluster.AnnotateObject(crdConf.Value.ToNamespacedCrd(),
                annotationKey,
                annotationValue,
                name,
                nameSpace);
        });
    }

    public Task Handle(SetAnnotationCommand<V1Job> command)
    {
        var ((nameSpace, name), annotationKey, annotationValue) = command;
        return kubeCluster.AnnotateJob(name, nameSpace, annotationKey, annotationValue)
            .TryMap(job => job.AsOption(),
                exception =>
                {
                    logger.LogError(exception, "Failed to annotate {streamId} with {annotationKey}:{annotationValue}",
                        command.affectedResource, command.annotationKey, command.annotationValue);
                    return Option<V1Job>.None;
                });
    }

    public Task Handle(RemoveAnnotationCommand<IStreamDefinition> command)
    {
        var ((nameSpace, kind, name), annotationKey) = command;
        return streamClassRepository.Get(nameSpace, kind).FlatMap(crdConf =>
        {
            if (crdConf is { HasValue: false })
            {
                logger.LogError("Failed to get configuration for kind {kind}", kind);
                return Task.FromResult(Option<object>.None);
            }

            var crd = crdConf.Value.ToNamespacedCrd();
            return kubeCluster
                .RemoveObjectAnnotation(crd, annotationKey, name, nameSpace)
                .TryMap(result => result.AsOption(), exception =>
                {
                    logger.LogError(exception,
                        "Failed to remove annotation {annotationKey} from {nameSpace}/{name}",
                        annotationKey,
                        nameSpace,
                        name);
                    return default;
                });
        });
    }
}
