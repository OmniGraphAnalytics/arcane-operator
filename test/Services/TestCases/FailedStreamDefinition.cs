using System;
using System.Collections.Generic;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.TestCases;

/// <summary>
/// A stream definition that throws an exception (for tests)
/// </summary>
public class FailedStreamDefinition : IStreamDefinition
{
    private readonly Exception exception;

    public FailedStreamDefinition(Exception exception)
    {
        this.exception = exception;
    }

    public V1TypedLocalObjectReference JobTemplateRef => throw exception;
    public V1TypedLocalObjectReference ReloadingJobTemplateRef => throw exception;

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

    public string StreamId => throw exception;
    public bool Suspended => throw exception;
    public bool ReloadRequested => throw exception;

    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources(IStreamClass streamDefinition)
    {
        throw exception;
    }

    public Dictionary<string, string> ToEnvironment(bool isBackfilling, IStreamClass streamDefinition)
    {
        throw exception;
    }

    public string GetConfigurationChecksum()
    {
        throw exception;
    }

    public V1TypedLocalObjectReference GetJobTemplate(bool isBackfilling)
    {
        throw exception;
    }

    public void Deconstruct(out string nameSpace, out string kind, out string streamId)
    {
        throw exception;
    }

    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources()
    {
        throw exception;
    }

    public Dictionary<string, string> ToEnvironment(bool isBackfilling)
    {
        throw exception;
    }
}
