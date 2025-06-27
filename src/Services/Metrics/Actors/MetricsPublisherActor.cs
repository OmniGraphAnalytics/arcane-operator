using Akka.Actor;
using Akka.Event;
using Arcane.Operator.Configurations;
using OmniModels.Services.Base;

namespace Arcane.Operator.Services.Metrics.Actors;

/// <summary>
/// Add stream class metrics message. Once received, the metrics will be added to the
/// metrics collection in the <see cref="StreamKindRef"/> actor.
/// </summary>
/// <param name="MetricName">Name of the stream kind referenced by the stream class</param>
/// <param name="MetricTags">Name of the metric to report</param>
/// <param name="MetricTags">Tags of the metric to report</param>
public record AddStreamClassMetricsMessage(
    string StreamKindRef,
    string MetricName,
    SortedDictionary<string, string> MetricTags);

/// <summary>
/// Remove stream class metrics message. Once received, the metrics will be removed from the
/// metrics collection in the <see cref="StreamKindRef"/> actor.
/// </summary>
/// <param name="StreamKindRef">Name of the stream kind referenced by the stream class</param>
public record RemoveStreamClassMetricsMessage(string StreamKindRef);

/// <summary>
/// Emit metrics message. Once received, the metrics will be emitted to the metrics service.
/// This message is emitted periodically by the <see cref="MetricsPublisherActor"/> actor.
/// </summary>
public record EmitMetricsMessage;

/// <summary>
/// A metric collection element for a stream class.
/// </summary>
public class StreamClassMetric
{
    /// <summary>
    /// Name of the metric to report.
    /// </summary>
    public string? MetricName { get; init; }


    /// <summary>
    /// Tags of the metric to report.
    /// </summary>
    public SortedDictionary<string, string> MetricTags { get; init; } = new();

    /// <summary>
    /// Metric Value
    /// </summary>
    public int MetricValue { get; set; } = 1;
}

/// <summary>
/// Stream class service actor. This actor is responsible for collecting metrics for stream classes
/// that should be emitted periodically.
/// </summary>
public class MetricsPublisherActor : ReceiveActor, IWithTimers
{
    private readonly MetricsPublisherActorConfiguration configuration;
    private readonly ILoggingAdapter log = Context.GetLogger();
    private readonly Dictionary<string, StreamClassMetric> streamClassMetrics = new();

    public MetricsPublisherActor(MetricsPublisherActorConfiguration configuration, MetricsService metricsService)
    {
        this.configuration = configuration;
        Receive<AddStreamClassMetricsMessage>(s =>
        {
            log.Debug(format: "Adding stream class metrics for {streamKindRef}", arg1: s.StreamKindRef);
            streamClassMetrics[s.StreamKindRef] = new StreamClassMetric
            {
                MetricTags = s.MetricTags,
                MetricName = s.MetricName,
            };
        });

        Receive<RemoveStreamClassMetricsMessage>(s =>
        {
            if (!streamClassMetrics.Remove(s.StreamKindRef))
            {
                log.Warning(format: "Stream class {streamKindRef} not found in metrics collection", arg1: s.StreamKindRef);
            }
        });

        Receive<EmitMetricsMessage>(_ =>
        {
            log.Debug("Start emitting stream class metrics");
            foreach (var (_, metric) in streamClassMetrics)
            {
                metricsService.Count(metricName: metric.MetricName, metricValue: metric.MetricValue, tags: metric.MetricTags);
            }
        });
    }

    public ITimerScheduler Timers { get; set; } = null!;

    protected override void PreStart()
    {
        Timers.StartPeriodicTimer(key: nameof(EmitMetricsMessage),
            msg: new EmitMetricsMessage(),
            initialDelay: configuration.InitialDelay,
            interval: configuration.UpdateInterval);
    }
}
