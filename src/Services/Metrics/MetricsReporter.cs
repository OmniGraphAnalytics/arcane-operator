using Akka.Actor;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Metrics.Actors;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Options;
using OmniModels.Services.Base;

namespace Arcane.Operator.Services.Metrics;

/// <summary>
/// The IMetricsReporter implementation.
/// </summary>
public class MetricsReporter : IMetricsReporter
{
    private readonly MetricsService metricsService;
    private readonly IActorRef statusActor;

    public MetricsReporter(MetricsService metricsService, ActorSystem actorSystem,
        IOptions<MetricsReporterConfiguration> metricsReporterConfiguration)
    {
        this.metricsService = metricsService;
        statusActor = actorSystem.ActorOf(props: Props.Create(() => new MetricsPublisherActor(
                metricsReporterConfiguration.Value.MetricsPublisherActorConfiguration,
                metricsService)),
            name: nameof(MetricsPublisherActor));
    }

    /// <inheritdoc cref="IMetricsReporter.ReportStatusMetrics"/>
    public SetStreamClassStatusCommand ReportStatusMetrics(SetStreamClassStatusCommand command)
    {
        if (command.phase.IsFinal())
        {
            statusActor.Tell(new RemoveStreamClassMetricsMessage(command.streamClass.KindRef));
        }
        else
        {
            var msg = new AddStreamClassMetricsMessage(StreamKindRef: command.streamClass.KindRef, MetricName: "stream_class",
                MetricTags: command.GetMetricsTags());
            statusActor.Tell(msg);
        }

        return command;
    }

    /// <inheritdoc cref="IMetricsReporter.ReportTrafficMetrics"/>
    public (WatchEventType, V1Job) ReportTrafficMetrics((WatchEventType, V1Job) jobEvent)
    {
        metricsService.Count(metricName: jobEvent.Item1.TrafficMetricName(), metricValue: 1, tags: jobEvent.Item2.GetMetricsTags());
        return jobEvent;
    }

    /// <inheritdoc cref="IMetricsReporter.ReportTrafficMetrics"/>
    public ResourceEvent<TResource> ReportTrafficMetrics<TResource>(ResourceEvent<TResource> ev) where TResource : IKubernetesObject<V1ObjectMeta>
    {
        metricsService.Count(metricName: ev.EventType.TrafficMetricName(), metricValue: 1, tags: ev.kubernetesObject.GetMetricsTags());
        return ev;
    }
}
