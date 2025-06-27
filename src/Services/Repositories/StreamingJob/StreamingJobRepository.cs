using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using k8s.Models;
using Microsoft.Extensions.Logging;
using OmniModels.Extensions;
using OmniModels.Services.Base;

namespace Arcane.Operator.Services.Repositories.StreamingJob;

public class StreamingJobRepository(IKubeCluster kubeCluster, ILogger<StreamingJobRepository> logger) : IStreamingJobCollection
{
    public Source<ResourceEvent<V1Job>, NotUsed> GetEvents(string nameSpace, int maxBufferCapacity)
    {
        return kubeCluster
            .StreamJobEvents(jobNamespace: nameSpace, maxBufferCapacity: maxBufferCapacity, overflowStrategy: OverflowStrategy.Fail)
            .Select(tuple => new ResourceEvent<V1Job>(EventType: tuple.Item1, kubernetesObject: tuple.Item2));
    }

    public Task<Option<V1Job>> Get(string nameSpace, string name)
    {
        return kubeCluster.GetJob(jobId: name, jobNamespace: nameSpace)
            .TryMap(selector: job => job?.AsOption() ?? Option<V1Job>.None, errorHandler: exception =>
            {
                logger.LogWarning(exception: exception, message: "The job resource {jobName} not found", name);
                return Option<V1Job>.None;
            });
    }
}
