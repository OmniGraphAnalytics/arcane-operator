using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arcane.Operator.Contracts;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using k8s.Models;

namespace Arcane.Operator.Models.Resources.StreamDefinitions;

[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamDefinition : IStreamDefinition
{
    private const string ENV_PREFIX = "STREAMCONTEXT__";

    /// <summary>
    /// Stream configuration
    /// </summary>
    [JsonPropertyName("spec")]
    public JsonElement Spec { get; set; }

    private V1TypedLocalObjectReference? JobTemplateRef =>
        Spec.GetProperty("jobTemplateRef").Deserialize<V1TypedLocalObjectReference>();

    private V1TypedLocalObjectReference? BackfillingJobTemplateRef =>
        Spec.GetProperty("backfillJobTemplateRef").Deserialize<V1TypedLocalObjectReference>();

    /// <summary>
    /// Api version
    /// </summary>
    [JsonPropertyName("apiVersion")]
    public string? ApiVersion { get; set; }

    /// <summary>
    /// Object kind (should always be "SqlServerStream")
    /// </summary>
    [JsonPropertyName("kind")]
    public string? Kind { get; set; }

    /// <summary>
    /// Object metadata see <see cref="V1ObjectMeta"/>
    /// </summary>
    [JsonPropertyName("metadata")]
    public V1ObjectMeta? Metadata { get; set; }

    /// <inheritdoc cref="IStreamDefinition"/>
    [JsonIgnore]
    public bool Suspended
        =>
            Metadata?.Annotations != null
            && Metadata.Annotations.TryGetValue(key: Annotations.STATE_ANNOTATION_KEY, value: out var value)
            && value == Annotations.SUSPENDED_STATE_ANNOTATION_VALUE;

    /// <inheritdoc cref="IStreamDefinition"/>
    [JsonIgnore]
    public bool ReloadRequested =>
        Metadata?.Annotations != null
        && Metadata.Annotations.TryGetValue(key: Annotations.STATE_ANNOTATION_KEY, value: out var value)
        && value == Annotations.RELOADING_STATE_ANNOTATION_VALUE;


    /// <summary>
    /// Stream identifier
    /// </summary>
    [JsonIgnore]
    public string StreamId => Metadata?.Name ?? throw new InvalidOperationException("StreamId is not set. Metadata.Name is null.");

    /// <inheritdoc cref="IStreamDefinition"/>
    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources(IStreamClass streamDefinition)
    {
        return Spec.EnumerateObject()
            .Where(s => streamDefinition.IsSecretRef(s.Name))
            .Select(p => new V1EnvFromSource(secretRef: p.Value.Deserialize<V1SecretEnvSource>()));
    }

    /// <summary>
    /// Encode Stream runner configuration to dictionary that can be passed as environment variables.
    /// </summary>
    /// <param name="isBackfilling">True if stream runner should run in backfill mode.</param>
    /// <param name="streamClass">Stream class associated with the stream definition.</param>
    /// <returns>Dictionary of strings</returns>
    public Dictionary<string, string> ToEnvironment(bool isBackfilling, IStreamClass streamClass)
    {
        return SelfToEnvironment(isBackfilling)
            .Concat(SpecToEnvironment(streamClass))
            .ToDictionary(keySelector: x => x.Key, elementSelector: x => x.Value);
    }

    /// <inheritdoc cref="IStreamDefinition.GetConfigurationChecksum"/>
    public string GetConfigurationChecksum()
    {
        var base64Hash = Convert.ToBase64String(GetSpecHash());
        return base64Hash[..7].ToLowerInvariant();
    }

    /// <inheritdoc cref="IStreamDefinition.GetJobTemplate"/>
    public V1TypedLocalObjectReference GetJobTemplate(bool isBackfilling)
    {
        return isBackfilling ? BackfillingJobTemplateRef : JobTemplateRef;
    }

    /// <inheritdoc cref="IStreamDefinition.Deconstruct"/>
    public void Deconstruct(out string nameSpace, out string kind, out string streamId)
    {
        nameSpace = Metadata.NamespaceProperty;
        kind = Kind;
        streamId = StreamId;
    }

    /// <summary>
    /// Converts the stream configuration .spec field to to JSON document and removes secret fields
    /// Unfortunately, we cannot serialize the JSONElement directly to string, because the JSONElement is immutable.
    /// To work around this, we clone the JSONElement to a new object and then serialize it.
    /// </summary>
    /// <param name="streamClass">StreamClass object containing stream metadata.</param>
    /// <returns>Serialized KeyValuePair containing the stream definition.</returns>
    private IEnumerable<KeyValuePair<string, string>> SpecToEnvironment(IStreamClass streamClass)
    {
        var newObj = Spec.Clone().Deserialize<Dictionary<string, object>>();
        foreach (var property in Spec.EnumerateObject().Where(property => streamClass.IsSecretRef(property.Name)))
        {
            newObj.Remove(property.Name);
        }

        return new KeyValuePair<string, string>[]
        {
            new(key: $"{ENV_PREFIX}SPEC", value: JsonSerializer.Serialize(newObj)),
        };
    }


    private byte[] GetSpecHash()
    {
        return SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(Spec)));
    }

    private Dictionary<string, string> SelfToEnvironment(bool backfill)
    {
        return new Dictionary<string, string>
        {
            { $"{ENV_PREFIX}STREAM_ID", StreamId },
            { $"{ENV_PREFIX}STREAM_KIND", Kind },
            { $"{ENV_PREFIX}BACKFILL", backfill.ToString().ToLowerInvariant() },
        };
    }
}
