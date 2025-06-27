FROM ubuntu:noble AS datadog-tracer
ARG TRACER_VERSION="3.18.0"

ADD https://github.com/DataDog/dd-trace-dotnet/releases/download/v${TRACER_VERSION}/datadog-dotnet-apm-${TRACER_VERSION}.tar.gz ./datadog-dotnet-apm-${TRACER_VERSION}.tar.gz

RUN mkdir -p /empty/ \
    && mkdir -p /opt/datadog \
    && tar -C /opt/datadog -xzf datadog-dotnet-apm-${TRACER_VERSION}.tar.gz \
    && rm datadog-dotnet-apm-${TRACER_VERSION}.tar.gz \
    && /opt/datadog/createLogPath.sh

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:9.0-noble AS build
ARG PROJECT_NAME="Arcane"
ARG COMPONENT="Operator"
ARG TARGETARCH

WORKDIR /app

# Copy csproj
COPY "src/${PROJECT_NAME}.${COMPONENT}/*.csproj" ./
COPY "src/${PROJECT_NAME}.${COMPONENT}/packages.lock.json" ./
RUN --mount=type=secret,id=nuget_token,env=NUGET_TOKEN dotnet nuget add source --username USERNAME --password "$NUGET_TOKEN" --store-password-in-clear-text --name github "https://nuget.pkg.github.com/OmniGraphAnalytics/index.json"

# Copy everything else and build
COPY src/. ./
RUN dotnet publish "${PROJECT_NAME}.${COMPONENT}/${PROJECT_NAME}.${COMPONENT}.csproj" -c Release -o out -a "$TARGETARCH"

WORKDIR /app/out
RUN ln -s ${PROJECT_NAME}.${COMPONENT}.dll app.dll

# Build runtime image
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/aspnet:9.0-noble-chiseled
ARG DD_GIT_REPOSITORY_URL
ARG DD_GIT_COMMIT_SHA

WORKDIR /app

# Copy the tracer & empty log directory
COPY --from=datadog-tracer /opt/datadog /opt/datadog
COPY --chown=$APP_UID --from=datadog-tracer /empty/ /var/log/datadog/dotnet/

# Copy the built application
COPY --from=build /app/out .

USER $APP_UID

ENV CORECLR_PROFILER={846F5F1C-F9AE-4B07-969E-05C26BC060D8}
ENV CORECLR_PROFILER_PATH=/opt/datadog/Datadog.Trace.ClrProfiler.Native.so
ENV DD_DOTNET_TRACER_HOME=/opt/datadog
ENV DD_GIT_REPOSITORY_URL=${DD_GIT_REPOSITORY_URL}
ENV DD_GIT_COMMIT_SHA=${DD_GIT_COMMIT_SHA}

ENTRYPOINT ["/usr/bin/dotnet", "app.dll"]
