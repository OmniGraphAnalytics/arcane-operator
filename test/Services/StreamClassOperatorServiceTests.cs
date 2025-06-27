using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.Resources.StreamClass.V1Beta1;
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
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.Helpers;
using Arcane.Operator.Tests.Services.TestCases;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using OmniModels.Services.Base;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamingJobTemplateTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamClassOperatorServiceTests : IClassFixture<LoggerFixture>, IClassFixture<AkkaFixture>
{
    // Akka service and test helpers
    private readonly ActorSystem actorSystem = ActorSystem.Create(nameof(StreamClassOperatorServiceTests));
    private readonly CancellationTokenSource cts = new();

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly LoggerFixture loggerFixture;
    private readonly ActorMaterializer materializer;
    private readonly Mock<IStreamClassRepository> streamClassRepositoryMock = new();
    private readonly Mock<IReactiveResourceCollection<IStreamDefinition>> streamDefinitionSourceMock = new();
    private readonly Mock<IStreamingJobCollection> streamingJobCollectionMock = new();
    private readonly Mock<IStreamingJobTemplateRepository> streamingJobTemplateRepositoryMock = new();
    private readonly TaskCompletionSource tcs = new();

    public StreamClassOperatorServiceTests(LoggerFixture loggerFixture)
    {
        this.loggerFixture = loggerFixture;
        materializer = actorSystem.Materializer();
        streamingJobTemplateRepositoryMock
            .Setup(s => s.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamingJobTemplate.AsOption<IStreamingJobTemplate>());
        cts.CancelAfter(TimeSpan.FromSeconds(60));
        cts.Token.Register(() => tcs.TrySetResult());
    }

    [Fact]
    public async Task TestStreamClassAdded()
    {
        // Arrange
        kubeClusterMock
            .Setup(m => m.StreamCustomResourceEvents<V1Beta1StreamClass>(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<OverflowStrategy>(),
                It.IsAny<TimeSpan?>()))
            .Returns(Source.Single<(WatchEventType, V1Beta1StreamClass)>((WatchEventType.Added,
                (V1Beta1StreamClass)StreamClass)));

        kubeClusterMock.Setup(service => service.SendJob(
                It.IsAny<V1Job>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => tcs.TrySetResult());

        streamDefinitionSourceMock
            .Setup(m => m.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.From(
                new List<ResourceEvent<IStreamDefinition>>
                {
                    new(EventType: WatchEventType.Added, kubernetesObject: StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(EventType: WatchEventType.Added, kubernetesObject: StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(EventType: WatchEventType.Added, kubernetesObject: StreamDefinitionTestCases.NamedStreamDefinition()),
                }));

        streamClassRepositoryMock
            .Setup(m => m.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());

        var task = tcs.Task;

        // Act
        var sp = CreateServiceProvider();
        await sp.GetRequiredService<IStreamClassOperatorService>()
            .GetStreamClassEventsGraph(CancellationToken.None)
            .Run(materializer);
        await task;

        // Assert
        kubeClusterMock.Verify(service => service.SendJob(It.IsAny<V1Job>(),
            It.IsAny<string>(),
            It.IsAny<CancellationToken>()));
    }

    [Fact]
    public async Task TestStreamClassDeleted()
    {
        // Arrange
        kubeClusterMock
            .Setup(m => m.StreamCustomResourceEvents<V1Beta1StreamClass>(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<OverflowStrategy>(),
                It.IsAny<TimeSpan?>()))
            .Returns(Source.Single<(WatchEventType, V1Beta1StreamClass)>((WatchEventType.Deleted,
                (V1Beta1StreamClass)StreamClass)));

        kubeClusterMock.Setup(service => service.SendJob(
                It.IsAny<V1Job>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => tcs.TrySetResult());

        streamDefinitionSourceMock
            .Setup(m => m.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.From(
                new List<ResourceEvent<IStreamDefinition>>
                {
                    new(EventType: WatchEventType.Added, kubernetesObject: StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(EventType: WatchEventType.Added, kubernetesObject: StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(EventType: WatchEventType.Added, kubernetesObject: StreamDefinitionTestCases.NamedStreamDefinition()),
                }));
        var task = tcs.Task;

        // Act
        var sp = CreateServiceProvider();
        await sp.GetRequiredService<IStreamClassOperatorService>()
            .GetStreamClassEventsGraph(CancellationToken.None)
            .Run(materializer);
        await task;

        // Assert
        kubeClusterMock.Verify(
            expression: service => service.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            times: Times.Never
        );
    }

    [Fact]
    public async Task TestFailedStreamClassAdded()
    {
        var streamClassMockEvents = new List<ResourceEvent<IStreamClass>>
        {
            new(EventType: WatchEventType.Added, kubernetesObject: FailedStreamClass(new Exception("Test exception"))),
            new(EventType: WatchEventType.Added, kubernetesObject: StreamClass),
        };

        // Arrange
        streamClassRepositoryMock.Setup(s => s.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.From(streamClassMockEvents));

        streamClassRepositoryMock.Setup(s => s.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());

        kubeClusterMock.Setup(service => service.SendJob(
                It.IsAny<V1Job>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => tcs.TrySetResult());

        streamDefinitionSourceMock
            .Setup(m => m.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.From(
                new List<ResourceEvent<IStreamDefinition>>
                {
                    new(EventType: WatchEventType.Added, kubernetesObject: StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(EventType: WatchEventType.Added, kubernetesObject: StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(EventType: WatchEventType.Added, kubernetesObject: StreamDefinitionTestCases.NamedStreamDefinition()),
                }));

        var task = tcs.Task;

        // Act
        var sp = CreateServiceProvider(streamClassRepositoryMock.Object);
        await sp.GetRequiredService<IStreamClassOperatorService>()
            .GetStreamClassEventsGraph(CancellationToken.None)
            .Run(materializer);
        await task;

        // Assert
        kubeClusterMock.Verify(service => service.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()));
    }

    private ServiceProvider CreateServiceProvider(IStreamClassRepository streamClassRepository = null)
    {
        return new ServiceCollection()
            .AddSingleton<IMaterializer>(actorSystem.Materializer())
            .AddSingleton(actorSystem)
            .AddSingleton(kubeClusterMock.Object)
            .AddSingleton(streamingJobCollectionMock.Object)
            .AddSingleton(streamDefinitionSourceMock.Object)
            .AddSingleton(streamingJobTemplateRepositoryMock.Object)
            .AddSingleton(sp => streamClassRepository ??
                                new StreamClassRepository(memoryCache: sp.GetRequiredService<IMemoryCache>(),
                                    kubeCluster: sp.GetRequiredService<IKubeCluster>()))
            .AddMemoryCache()
            .AddSingleton<IStreamOperatorService, StreamOperatorService>()
            .AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<SetStreamClassStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<SetAnnotationCommand<V1Job>>, AnnotationCommandHandler>()
            .AddSingleton<ICommandHandler<StreamingJobCommand>, StreamingJobCommandHandler>()
            .AddSingleton<IMetricsReporter, MetricsReporter>()
            .AddSingleton<IEventFilter<IStreamDefinition>, EmptyEventFilter<IStreamDefinition>>()
            .AddSingleton(Mock.Of<MetricsService>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamOperatorService>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamClassOperatorService>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<AnnotationCommandHandler>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<UpdateStatusCommandHandler>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamingJobCommandHandler>())
            .AddSingleton(Options.Create(new StreamClassOperatorServiceConfiguration
            {
                MaxBufferCapacity = 100,
            }))
            .AddSingleton<IStreamClassOperatorService, StreamClassOperatorService>()
            .BuildServiceProvider();
    }
}
