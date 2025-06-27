using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Configurations.Common;
using Arcane.Operator.Contracts;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.Resources.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.EventFilters;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Base.Operators;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Operators;
using Arcane.Operator.Services.Repositories.CustomResources;
using Arcane.Operator.Services.Repositories.StreamingJob;
using Arcane.Operator.Tests.Extensions;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.Helpers;
using Arcane.Operator.Tests.Services.TestCases;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using OmniModels.Services.Base;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.JobTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamDefinitionTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamingJobTemplateTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamOperatorServiceTests : IClassFixture<LoggerFixture>, IDisposable
{
    // Akka service and test helpers
    private readonly ActorSystem actorSystem = ActorSystem.Create(nameof(StreamOperatorServiceTests));
    private readonly CancellationTokenSource cts = new();

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly LoggerFixture loggerFixture;
    private readonly ActorMaterializer materializer;
    private readonly Mock<IStreamClassRepository> streamClassRepositoryMock = new();
    private readonly Mock<IReactiveResourceCollection<IStreamDefinition>> streamDefinitionRepositoryMock = new();
    private readonly Mock<IStreamingJobCollection> streamingJobOperatorServiceMock = new();
    private readonly Mock<IStreamingJobTemplateRepository> streamingJobTemplateRepositoryMock = new();
    private readonly TaskCompletionSource tcs = new();

    public StreamOperatorServiceTests(LoggerFixture loggerFixture)
    {
        this.loggerFixture = loggerFixture;
        materializer = actorSystem.Materializer();
        streamClassRepositoryMock
            .Setup(c => c.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        cts.Token.Register(tcs.SetResult);
        streamingJobTemplateRepositoryMock
            .Setup(s => s.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamingJobTemplate.AsOption<IStreamingJobTemplate>());
    }

    public void Dispose()
    {
        actorSystem?.Dispose();
        materializer?.Dispose();
        cts?.Dispose();
    }

    public static IEnumerable<object[]> GenerateSynchronizationTestCases()
    {
        yield return [WatchEventType.Added, StreamDefinitionTestCases.StreamDefinition, true, false, false, false];
        yield return [WatchEventType.Modified, StreamDefinitionTestCases.StreamDefinition, false, true, true, false];
        yield return [WatchEventType.Modified, StreamDefinitionTestCases.StreamDefinition, false, true, false, false];

        yield return [WatchEventType.Added, SuspendedStreamDefinition, false, false, false, false];
        yield return [WatchEventType.Deleted, SuspendedStreamDefinition, false, false, false, false];
        yield return [WatchEventType.Modified, SuspendedStreamDefinition, false, false, true, true];
        yield return [WatchEventType.Modified, SuspendedStreamDefinition, false, false, false, false];
    }

    [Theory]
    [MemberData(nameof(GenerateSynchronizationTestCases))]
    public async Task TestHandleAddedStreamEvent(WatchEventType eventType,
        StreamDefinition streamDefinition,
        bool expectBackfill,
        bool expectRestart,
        bool streamingJobExists,
        bool expectTermination)
    {
        // Arrange
        SetupEventMock(eventType: eventType, streamDefinition: streamDefinition);
        streamingJobOperatorServiceMock
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(streamingJobExists ? JobWithChecksum("checksum").AsOption() : Option<V1Job>.None);

        var task = tcs.Task;
        kubeClusterMock
            .Setup(service => service.SendJob(
                It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(() => tcs.SetResult());

        kubeClusterMock.Setup(c => c.AnnotateJob(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>()))
            .Callback(() => tcs.SetResult());

        kubeClusterMock
            .Setup(service => service.DeleteJob(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>(),
                It.IsAny<PropagationPolicy>()))
            .Callback(() => tcs.SetResult());

        // Act
        var sp = CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert

        kubeClusterMock.Verify(expression: service
                => service.SendJob(
                    It.Is<V1Job>(job => job.IsBackfilling()),
                    It.IsAny<string>(), It.IsAny<CancellationToken>()),
            times: Times.Exactly(expectBackfill ? 1 : 0));

        kubeClusterMock
            .Verify(expression: c => c.AnnotateJob(It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.Is<string>(a => a == Annotations.STATE_ANNOTATION_KEY),
                    It.Is<string>(a => a == Annotations.RESTARTING_STATE_ANNOTATION_VALUE)),
                times: Times.Exactly(expectRestart && streamingJobExists ? 1 : 0));

        kubeClusterMock.Verify(expression: service
                => service.SendJob(
                    It.Is<V1Job>(job => !job.IsBackfilling()),
                    It.IsAny<string>(), It.IsAny<CancellationToken>()),
            times: Times.Exactly(expectRestart && !streamingJobExists ? 1 : 0));

        kubeClusterMock.Verify(expression: service
                => service.DeleteJob(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>(),
                    It.IsAny<PropagationPolicy>()),
            times: Times.Exactly(expectTermination ? 1 : 0));
    }


    public static IEnumerable<object[]> GenerateModifiedTestCases()
    {
        yield return [StreamDefinitionTestCases.StreamDefinition, true, true];
        yield return [StreamDefinitionTestCases.StreamDefinition, false, false];
    }

    [Theory]
    [MemberData(nameof(GenerateModifiedTestCases))]
    public async Task TestJobModificationEvent(StreamDefinition streamDefinition,
        bool jobChecksumChanged, bool expectRestart)
    {
        // Arrange
        SetupEventMock(eventType: WatchEventType.Modified, streamDefinition: streamDefinition);
        var mockJob = JobWithChecksum(jobChecksumChanged ? "checksum" : streamDefinition.GetConfigurationChecksum());
        streamingJobOperatorServiceMock
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(mockJob.AsOption());

        var task = tcs.Task;
        kubeClusterMock.Setup(c => c.AnnotateJob(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>()))
            .Callback(() => tcs.SetResult());

        // Act
        var sp = CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        kubeClusterMock
            .Verify(expression: c => c.AnnotateJob(It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.Is<string>(a => a == Annotations.STATE_ANNOTATION_KEY),
                    It.Is<string>(a => a == Annotations.RESTARTING_STATE_ANNOTATION_VALUE)),
                times: Times.Exactly(expectRestart ? 1 : 0));
    }


    public static IEnumerable<object[]> GenerateReloadTestCases()
    {
        yield return [ReloadRequestedStreamDefinition, true, true];
        yield return [ReloadRequestedStreamDefinition, false, false];
    }

    [Theory]
    [MemberData(nameof(GenerateReloadTestCases))]
    public async Task TestStreamReload(StreamDefinition streamDefinition, bool jobExists, bool expectReload)
    {
        // Arrange
        SetupEventMock(eventType: WatchEventType.Modified, streamDefinition: streamDefinition);
        var mockJob = JobWithChecksum(streamDefinition.GetConfigurationChecksum());
        streamingJobOperatorServiceMock
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(jobExists ? mockJob.AsOption() : Option<V1Job>.None);

        var task = tcs.Task;
        kubeClusterMock.Setup(c => c.AnnotateJob(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>()))
            .Callback(() => tcs.SetResult());
        kubeClusterMock
            .Setup(service => service.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(tcs.SetResult);

        // Act
        var sp = CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        kubeClusterMock
            .Verify(expression: c => c.AnnotateJob(It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.Is<string>(a => a == Annotations.STATE_ANNOTATION_KEY),
                    It.Is<string>(a => a == Annotations.RELOADING_STATE_ANNOTATION_VALUE)),
                times: Times.Exactly(expectReload ? 1 : 0));

        kubeClusterMock.Verify(expression: service
                => service.SendJob(It.Is<V1Job>(job => job.IsBackfilling()),
                    It.IsAny<string>(), It.IsAny<CancellationToken>()),
            times: Times.Exactly(expectReload ? 0 : 1));
    }

    public static IEnumerable<object[]> GenerateAddTestCases()
    {
        yield return [ReloadRequestedStreamDefinition, true, true, false];
        yield return [ReloadRequestedStreamDefinition, false, false, true];
        yield return [ReloadRequestedStreamDefinition, true, false, false];

        yield return [StreamDefinitionTestCases.StreamDefinition, true, true, false];
        yield return [StreamDefinitionTestCases.StreamDefinition, false, false, true];
        yield return [StreamDefinitionTestCases.StreamDefinition, true, false, false];

        yield return [SuspendedStreamDefinition, true, true, false];
        yield return [SuspendedStreamDefinition, false, false, false];
        yield return [SuspendedStreamDefinition, true, false, false];
    }

    [Theory]
    [MemberData(nameof(GenerateAddTestCases))]
    public async Task TestStreamAdded(StreamDefinition streamDefinition, bool jobExists, bool jobIsReloading,
        bool expectStart)
    {
        // Arrange
        SetupEventMock(eventType: WatchEventType.Added, streamDefinition: streamDefinition);
        streamingJobOperatorServiceMock
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(jobExists ? jobIsReloading ? ReloadingJob : RunningJob : Option<V1Job>.None);

        var task = tcs.Task;
        kubeClusterMock
            .Setup(service => service.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(tcs.SetResult);

        // Act
        var sp = CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        kubeClusterMock
            .Verify(expression: c => c.AnnotateJob(It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.Is<string>(a => a == Annotations.STATE_ANNOTATION_KEY),
                    It.Is<string>(a => a == Annotations.RELOADING_STATE_ANNOTATION_VALUE)),
                times: Times.Exactly(0));

        kubeClusterMock.Verify(expression: service
                => service.SendJob(It.Is<V1Job>(job => job.IsBackfilling()), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            times: Times.Exactly(expectStart ? 1 : 0));
    }

    public static IEnumerable<object[]> GenerateRecoverableTestCases()
    {
        yield return [FailedStreamDefinition(new JsonException())];
    }

    [Theory]
    [MemberData(nameof(GenerateRecoverableTestCases))]
    public async Task HandleBrokenStreamDefinition(FailedStreamDefinition streamDefinition)
    {
        // Arrange
        streamDefinitionRepositoryMock.Setup(cluster => cluster.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.Single(new ResourceEvent<IStreamDefinition>(EventType: WatchEventType.Added, kubernetesObject: streamDefinition)));

        streamingJobOperatorServiceMock
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        var task = tcs.Task;

        // Act
        var sp = CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        streamingJobOperatorServiceMock.Verify(expression: service
            => service.Get(It.IsAny<string>(), It.IsAny<string>()), times: Times.Never);
    }

    public static IEnumerable<object[]> GenerateFatalTestCases()
    {
        yield return [FailedStreamDefinition(new BufferOverflowException("test"))];
    }

    [Theory]
    [MemberData(nameof(GenerateFatalTestCases))]
    public async Task HandleFatalException(FailedStreamDefinition streamDefinition)
    {
        // Arrange
        streamDefinitionRepositoryMock.Setup(s =>
                s.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.Single(new ResourceEvent<IStreamDefinition>(EventType: WatchEventType.Added, kubernetesObject: streamDefinition)));

        var task = tcs.Task;
        streamingJobOperatorServiceMock
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        // Act
        var sp = CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert that code above didn't throw
        Assert.True(task.IsCompleted);
    }

    [Fact]
    public async Task HandleBrokenStreamRepository()
    {
        // Arrange
        streamDefinitionRepositoryMock.Setup(s
                => s.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(
                Source.Single(new ResourceEvent<IStreamDefinition>(EventType: WatchEventType.Added,
                    kubernetesObject: StreamDefinitionTestCases.StreamDefinition)));

        streamingJobOperatorServiceMock
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        kubeClusterMock
            .Setup(service
                => service.UpdateCustomResourceStatus(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Func<JsonElement, It.IsAnyType>>()
                ))
            .ThrowsAsync(new Exception());

        var task = tcs.Task;

        // Act
        var sp = CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert that code above didn't throw
        Assert.True(task.IsCompleted);
    }


    private void SetupEventMock(WatchEventType eventType, IStreamDefinition streamDefinition)
    {
        streamDefinitionRepositoryMock
            .Setup(service => service.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.Single(new ResourceEvent<IStreamDefinition>(EventType: eventType, kubernetesObject: streamDefinition)));
    }


    private ServiceProvider CreateServiceProvider()
    {
        var optionsMock = new Mock<IOptionsSnapshot<CustomResourceConfiguration>>();
        optionsMock
            .Setup(m => m.Get(It.IsAny<string>()))
            .Returns(new CustomResourceConfiguration());
        var metricsReporterConfiguration = Options.Create(new MetricsReporterConfiguration
        {
            MetricsPublisherActorConfiguration = new MetricsPublisherActorConfiguration
            {
                InitialDelay = TimeSpan.FromSeconds(30),
                UpdateInterval = TimeSpan.FromSeconds(10),
            },
        });
        return new ServiceCollection()
            .AddSingleton(materializer)
            .AddSingleton(actorSystem)
            .AddSingleton(kubeClusterMock.Object)
            .AddSingleton<IMaterializer>(actorSystem.Materializer())
            .AddSingleton(streamingJobOperatorServiceMock.Object)
            .AddSingleton(streamDefinitionRepositoryMock.Object)
            .AddSingleton(streamingJobTemplateRepositoryMock.Object)
            .AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<SetAnnotationCommand<V1Job>>, AnnotationCommandHandler>()
            .AddSingleton<ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>()
            .AddSingleton<ICommandHandler<StreamingJobCommand>, StreamingJobCommandHandler>()
            .AddSingleton(streamClassRepositoryMock.Object)
            .AddSingleton<IMetricsReporter, MetricsReporter>()
            .AddSingleton(Mock.Of<MetricsService>())
            .AddSingleton(metricsReporterConfiguration)
            .AddSingleton<IEventFilter<IStreamDefinition>, EmptyEventFilter<IStreamDefinition>>()
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamOperatorService>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamDefinitionRepository>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamingJobOperatorService>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamingJobRepository>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<AnnotationCommandHandler>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<UpdateStatusCommandHandler>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamingJobCommandHandler>())
            .AddSingleton(streamingJobOperatorServiceMock.Object)
            .AddSingleton(optionsMock.Object)
            .AddSingleton<IStreamOperatorService, StreamOperatorService>()
            .BuildServiceProvider();
    }
}
