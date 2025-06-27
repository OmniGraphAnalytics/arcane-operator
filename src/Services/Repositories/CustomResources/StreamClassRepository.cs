using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.Resources.StreamClass.V1Beta1;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Microsoft.Extensions.Caching.Memory;
using OmniModels.Services.Base;

namespace Arcane.Operator.Services.Repositories.CustomResources;

public class StreamClassRepository(IMemoryCache memoryCache, IKubeCluster kubeCluster) : IStreamClassRepository
{
    public Task<Option<IStreamClass>> Get(string nameSpace, string streamDefinitionKind)
    {
        return memoryCache.Get<V1Beta1StreamClass>(streamDefinitionKind) switch
        {
            null => Task.FromResult(Option<IStreamClass>.None),
            var streamClass => Task.FromResult(Option<IStreamClass>.Create(streamClass)),
        };
    }

    public Task InsertOrUpdate(IStreamClass streamClass, StreamClassPhase phase, IEnumerable<V1Alpha1StreamCondition> conditions, string pluralName)
    {
        memoryCache.Set(key: streamClass.KindRef, value: streamClass);
        return Task.CompletedTask;
    }

    /// <inheritdoc cref="IReactiveResourceCollection{TResourceType}.GetEvents"/>>
    public Source<ResourceEvent<IStreamClass>, NotUsed> GetEvents(CustomResourceApiRequest request, int maxBufferCapacity)
    {
        return kubeCluster.StreamCustomResourceEvents<V1Beta1StreamClass>(
                crdNamespace: request.Namespace,
                group: request.ApiGroup,
                version: request.ApiVersion,
                plural: request.PluralName,
                maxBufferCapacity: maxBufferCapacity,
                overflowStrategy: OverflowStrategy.Fail)
            .Select((tuple) => new ResourceEvent<IStreamClass>(EventType: tuple.Item1, kubernetesObject: tuple.Item2));
    }
}
