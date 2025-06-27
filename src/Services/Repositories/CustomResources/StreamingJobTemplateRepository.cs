using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.Resources.JobTemplates.V1Beta1;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OmniModels.Extensions;
using OmniModels.Services.Base;

namespace Arcane.Operator.Services.Repositories.CustomResources;

public class StreamingJobTemplateRepository(
    IKubeCluster kubeCluster,
    IOptions<StreamingJobTemplateRepositoryConfiguration> configuration,
    ILogger<StreamingJobTemplateRepository> logger)
    : IStreamingJobTemplateRepository
{
    private readonly StreamingJobTemplateRepositoryConfiguration configuration = configuration.Value;

    public Task<Option<IStreamingJobTemplate>> GetStreamingJobTemplate(string kind, string jobNamespace,
        string templateName)
    {
        var jobTemplateResourceConfiguration = configuration.ResourceConfiguration;

        if (jobTemplateResourceConfiguration is { ApiGroup: not null, Version: not null, Plural: not null })
        {
            logger.LogError("Failed to get job template configuration for kind {kind}", kind);
            return Task.FromResult(Option<IStreamingJobTemplate>.None);
        }

        return kubeCluster
            .GetCustomResource<V1Beta1StreamingJobTemplate>(
                jobTemplateResourceConfiguration.ApiGroup,
                jobTemplateResourceConfiguration.Version,
                jobTemplateResourceConfiguration.Plural,
                jobNamespace,
                templateName)
            .TryMap(resource => resource.AsOption<IStreamingJobTemplate>(),
                _ =>
            {
                logger.LogError("Failed to get job template {templateName} for kind {kind} in namespace {jobNamespace}",
                    templateName, kind, jobNamespace);
                return Option<IStreamingJobTemplate>.None;
            });
    }
}
