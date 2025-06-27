using System;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using k8s.Models;
using OmniModels.Models.OmniPulse.Kubernetes;

namespace Arcane.Operator.Tests.Services.TestCases;

/// <summary>
/// A stream Class that throws an exception (for tests)
/// </summary>
public class FailedStreamClass : IStreamClass
{
    private readonly Exception exception;

    public FailedStreamClass(Exception exception)
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

    public string ToStreamClassId()
    {
        throw exception;
    }

    public string ApiGroupRef => throw exception;
    public string VersionRef => throw exception;
    public string PluralNameRef => throw exception;
    public string KindRef => throw exception;
    public int MaxBufferCapacity => throw exception;

    public NamespacedCrd ToNamespacedCrd()
    {
        throw exception;
    }

    public bool IsSecretRef(string propertyName)
    {
        throw exception;
    }
}
