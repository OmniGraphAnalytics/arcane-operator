using System;
using System.Collections.Generic;
using Arcane.Operator.Contracts;
using Arcane.Operator.Extensions;
using k8s.Models;
using OmniModels.Services.Kubernetes;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;

namespace Arcane.Operator.Tests.Services.TestCases;

public static class JobTestCases
{
    public static V1Job FailedJob => CreateJob([new V1JobCondition { Type = "Failed", Status = "True" }])
        .WithStreamingJobLabels(streamId: "1", isBackfilling: false, streamKind: string.Empty)
        .WithMetadataAnnotations(StreamClass);

    public static V1Job CompletedJob => CreateJob([new V1JobCondition { Type = "Complete", Status = "True" }])
        .WithStreamingJobLabels(streamId: "1", isBackfilling: false, streamKind: string.Empty)
        .WithMetadataAnnotations(StreamClass);


    public static V1Job ReloadRequestedJob => CompletedJob
        .Clone()
        .WithAnnotations(new Dictionary<string, string>
        {
            { Annotations.STATE_ANNOTATION_KEY, Annotations.RELOADING_STATE_ANNOTATION_VALUE },
        });

    public static V1Job TerminatingJob => CompletedJob
        .Clone()
        .WithAnnotations(new Dictionary<string, string>
        {
            { Annotations.STATE_ANNOTATION_KEY, Annotations.TERMINATING_STATE_ANNOTATION_VALUE },
        });

    public static V1Job ReloadingJob => CreateJob(new List<V1JobCondition>
            { new() { Type = "Complete", Status = "True" } })
        .Clone()
        .WithMetadataAnnotations(StreamClass)
        .WithStreamingJobLabels(streamId: Guid.NewGuid().ToString(), isBackfilling: true, streamKind: string.Empty);

    public static V1Job RunningJob => CreateJob(null)
        .WithMetadataAnnotations(StreamClass)
        .WithStreamingJobLabels(streamId: "1", isBackfilling: false, streamKind: string.Empty);

    public static V1Job SchemaMismatchJob => RunningJob
        .Clone()
        .WithAnnotations(new Dictionary<string, string>
        {
            { Annotations.STATE_ANNOTATION_KEY, Annotations.SCHEMA_MISMATCH_STATE_ANNOTATION_VALUE },
        });

    public static V1Job JobWithChecksum(string checksum)
    {
        return RunningJob
            .Clone()
            .WithStreamingJobAnnotations(checksum);
    }

    private static V1Job CreateJob(List<V1JobCondition> conditions)
    {
        var job = new V1Job
        {
            Metadata = new V1ObjectMeta
            {
                Name = "stream",
            },
            Spec = new V1JobSpec
            {
                Template = new V1PodTemplateSpec
                {
                    Metadata = new V1ObjectMeta(),
                },
            },
            Status = new V1JobStatus { Conditions = conditions },
        };
        return job;
    }
}
