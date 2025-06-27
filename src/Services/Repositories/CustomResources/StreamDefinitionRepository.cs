﻿using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Resources.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using k8s;
using OmniModels.Services.Base;

namespace Arcane.Operator.Services.Repositories.CustomResources;

public class StreamDefinitionRepository(IKubeCluster kubeCluster) : IReactiveResourceCollection<IStreamDefinition>,
    IResourceCollection<IStreamDefinition>
{
    /// <inheritdoc cref="IReactiveResourceCollection{TResourceType}.GetEvents"/>
    public Source<ResourceEvent<IStreamDefinition>, NotUsed> GetEvents(CustomResourceApiRequest request,
        int maxBufferCapacity)
    {
        var listTask = kubeCluster.ListCustomResources<StreamDefinition>(
            group: request.ApiGroup,
            version: request.ApiVersion,
            plural: request.PluralName,
            crdNamespace: request.Namespace);

        var initialSync = Source
            .FromTask(listTask)
            .SelectMany(sd => sd)
            .Select(sd => new ResourceEvent<IStreamDefinition>(EventType: WatchEventType.Modified, kubernetesObject: sd));


        var subscriptionSource = kubeCluster.StreamCustomResourceEvents<StreamDefinition>(
                crdNamespace: request.Namespace,
                group: request.ApiGroup,
                version: request.ApiVersion,
                plural: request.PluralName,
                maxBufferCapacity: maxBufferCapacity,
                overflowStrategy: OverflowStrategy.Fail)
            .Select(tuple => new ResourceEvent<IStreamDefinition>(EventType: tuple.Item1, kubernetesObject: tuple.Item2));

        return initialSync.Concat(subscriptionSource);
    }

    /// <inheritdoc cref="IResourceCollection{TResourceType}.Get"/>
    public Task<Option<IStreamDefinition>> Get(string name, CustomResourceApiRequest request)
    {
        return kubeCluster.GetCustomResource(group: request.ApiGroup,
            version: request.ApiVersion,
            plural: request.PluralName,
            crdNamespace: request.Namespace,
            name: name,
            converter: element => element.AsOptionalStreamDefinition());
    }
}
