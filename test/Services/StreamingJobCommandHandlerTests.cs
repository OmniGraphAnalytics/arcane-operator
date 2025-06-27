using System;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Util.Extensions;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Tests.Extensions;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.TestCases;
using k8s.Autorest;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using OmniModels.Services.Base;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.StreamDefinitionTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamingJobTemplateTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamingJobCommandHandlerTests(LoggerFixture loggerFixture) : IClassFixture<LoggerFixture>,
    IClassFixture<AkkaFixture>
{
    // Akka service and test helpers

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly Mock<IStreamClassRepository> streamClassRepositoryMock = new();
    private readonly Mock<IStreamingJobTemplateRepository> streamingJobTemplateRepositoryMock = new();

    [Fact]
    public async Task HandleStreamStopCommand()
    {
        // Arrange
        var command = new StopJob(name: "job-name", nameSpace: "job-namespace");
        var service = CreateServiceProvider().GetRequiredService<ICommandHandler<StreamingJobCommand>>();

        // Act
        await service.Handle(command);

        // Assert
        kubeClusterMock.Verify(k => k.DeleteJob(command.name,
            command.nameSpace,
            It.IsAny<CancellationToken>(),
            It.IsAny<PropagationPolicy>()));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HandleStreamStartCommand(bool isBackfilling)
    {
        // Arrange
        var command = new StartJob(streamDefinition: StreamDefinition, IsBackfilling: isBackfilling);
        var service = CreateServiceProvider().GetRequiredService<ICommandHandler<StreamingJobCommand>>();
        streamClassRepositoryMock
            .Setup(scr => scr.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());
        streamingJobTemplateRepositoryMock
            .Setup(sjtr => sjtr.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(((IStreamingJobTemplate)StreamingJobTemplate).AsOption());
        var expectedState = isBackfilling ? nameof(StreamPhase.RELOADING) : nameof(StreamPhase.RUNNING);

        // Act
        await service.Handle(command);

        // Assert
        kubeClusterMock.Verify(k =>
            k.SendJob(It.Is<V1Job>(job => job.IsBackfilling() == isBackfilling), It.IsAny<string>(), It.IsAny<CancellationToken>()));
        kubeClusterMock.Verify(expression: k => k.UpdateCustomResourceStatus(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                StreamDefinition.Namespace(),
                StreamDefinition.Name(),
                It.Is<V1Alpha1StreamStatus>(s => s.Phase == expectedState),
                It.IsAny<Func<JsonElement, It.IsAnyType>>()),
            times: Times.Once);
    }

    [Theory]
    [InlineData(HttpStatusCode.Conflict, StreamPhase.RUNNING)]
    [InlineData(HttpStatusCode.InternalServerError, StreamPhase.FAILED)]
    [InlineData(HttpStatusCode.BadRequest, StreamPhase.FAILED)]
    public async Task HandleStreamStartFail(HttpStatusCode statusCode, StreamPhase expectedPhase)
    {
        // Arrange
        var command = new StartJob(streamDefinition: StreamDefinition, IsBackfilling: false);
        var service = CreateServiceProvider().GetRequiredService<ICommandHandler<StreamingJobCommand>>();
        kubeClusterMock.Setup(k => k.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromException<V1JobStatus>(new HttpOperationException
            {
                Response = new HttpResponseMessageWrapper(httpResponse: new HttpResponseMessage { StatusCode = statusCode }, content: ""),
            }));
        streamClassRepositoryMock
            .Setup(scr => scr.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());
        streamingJobTemplateRepositoryMock
            .Setup(sjtr => sjtr.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(((IStreamingJobTemplate)StreamingJobTemplate).AsOption());
        var expectedState = expectedPhase.ToString();

        // Act
        await service.Handle(command);

        // Assert
        kubeClusterMock.Verify(expression: k => k.UpdateCustomResourceStatus(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                StreamDefinition.Namespace(),
                StreamDefinition.Name(),
                It.Is<V1Alpha1StreamStatus>(s => s.Phase == expectedState),
                It.IsAny<Func<JsonElement, It.IsAnyType>>()),
            times: Times.Once);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HandleFailedStreamTemplate(bool isBackfilling)
    {
        // Arrange
        var command = new StartJob(streamDefinition: StreamDefinition, IsBackfilling: isBackfilling);
        var service = CreateServiceProvider().GetRequiredService<ICommandHandler<StreamingJobCommand>>();
        streamClassRepositoryMock
            .Setup(scr => scr.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());
        streamingJobTemplateRepositoryMock
            .Setup(sjtr => sjtr.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(((IStreamingJobTemplate)new FailedStreamingJobTemplate(new Exception())).AsOption());

        // Act
        await service.Handle(command);

        // Assert
        kubeClusterMock.Verify(expression: k => k.UpdateCustomResourceStatus(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                StreamDefinition.Namespace(),
                StreamDefinition.Name(),
                It.Is<V1Alpha1StreamStatus>(s => s.Phase == nameof(StreamPhase.FAILED)),
                It.IsAny<Func<JsonElement, It.IsAnyType>>()),
            times: Times.Once);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HandleFailedSendJob(bool isBackfilling)
    {
        // Arrange
        var command = new StartJob(streamDefinition: StreamDefinition, IsBackfilling: isBackfilling);
        var service = CreateServiceProvider().GetRequiredService<ICommandHandler<StreamingJobCommand>>();

        streamClassRepositoryMock
            .Setup(scr => scr.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());

        streamingJobTemplateRepositoryMock
            .Setup(sjtr => sjtr.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(((IStreamingJobTemplate)new FailedStreamingJobTemplate(new Exception())).AsOption());

        kubeClusterMock
            .Setup(k => k.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Throws<Exception>();

        // Act
        await service.Handle(command);

        // Assert
        kubeClusterMock.Verify(expression: k => k.UpdateCustomResourceStatus(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                StreamDefinition.Namespace(),
                StreamDefinition.Name(),
                It.Is<V1Alpha1StreamStatus>(s => s.Phase == nameof(StreamPhase.FAILED)),
                It.IsAny<Func<JsonElement, It.IsAnyType>>()),
            times: Times.Once);
    }

    private ServiceProvider CreateServiceProvider()
    {
        return new ServiceCollection()
            .AddSingleton(kubeClusterMock.Object)
            .AddSingleton(streamClassRepositoryMock.Object)
            .AddSingleton(streamingJobTemplateRepositoryMock.Object)
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamingJobCommandHandler>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<UpdateStatusCommandHandler>())
            .AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<StreamingJobCommand>, StreamingJobCommandHandler>()
            .BuildServiceProvider();
    }
}
