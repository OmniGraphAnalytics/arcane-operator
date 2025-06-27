using System;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.TestCases;

/// <summary>
/// A streaming job templatethat throws an exception (for tests)
/// </summary>
public class FailedStreamingJobTemplate : IStreamingJobTemplate
{
    private readonly Exception exception;

    public FailedStreamingJobTemplate(Exception exception)
    {
        this.exception = exception;
    }

    public string ApiVersion
    {
        get => throw exception;
        set => throw exception;
    }

    public string Kind
    {
        get => throw exception;
        set => throw exception;
    }

    public V1ObjectMeta Metadata
    {
        get => throw exception;
        set => throw exception;
    }

    public V1Job GetJob()
    {
        throw exception;
    }
}
