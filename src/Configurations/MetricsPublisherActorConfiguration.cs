using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Services.Metrics.Actors;

namespace Arcane.Operator.Configurations;

/// <summary>
/// The configuration for the <see cref="MetricsPublisherActor"/>
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class MetricsPublisherActorConfiguration
{
    /// <summary>
    /// Interval to publish metrics
    /// </summary>
    public TimeSpan UpdateInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Initial delay for the first metrics publication
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(10);
};
