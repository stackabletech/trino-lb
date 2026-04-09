use std::sync::Arc;

use opentelemetry::trace::TracerProvider;
use opentelemetry::{Context, global};
use opentelemetry_http::HeaderInjector;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::SdkMeterProvider,
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider, Tracer},
};
use snafu::{ResultExt, Snafu};
use tracing::{level_filters::LevelFilter, subscriber::SetGlobalDefaultError};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt};
use trino_lb_core::config::{Config, TrinoLbTracingConfig};
use trino_lb_persistence::PersistenceImplementation;

use crate::metrics::Metrics;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to build OTLP span exporter"))]
    BuildSpanExporter {
        source: opentelemetry_otlp::ExporterBuildError,
    },

    #[snafu(display("Failed to create OpenTelemetry Prometheus exporter"))]
    CreateOpenTelemetryPrometheusExporter {
        source: opentelemetry_sdk::error::OTelSdkError,
    },

    #[snafu(display("Failed to set global tracing subscriber"))]
    SetGlobalTracingSubscriber { source: SetGlobalDefaultError },
}

pub fn init(
    tracing_config: Option<&TrinoLbTracingConfig>,
    persistence: Arc<PersistenceImplementation>,
    config: &Config,
) -> Result<(Metrics, Option<SdkTracerProvider>), Error> {
    let env_filter_layer = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let console_output_layer = tracing_subscriber::fmt::layer().with_filter(env_filter_layer);
    let mut layers = vec![console_output_layer.boxed()];

    let mut tracer_provider = None;
    if let Some(tracing_config) = tracing_config
        && tracing_config.enabled
    {
        let env_filter_layer = EnvFilter::builder()
            .with_default_directive(LevelFilter::DEBUG.into())
            .from_env_lossy();
        let (tracer, provider) = otel_tracer(tracing_config)?;
        tracer_provider = Some(provider);
        layers.push(
            tracing_opentelemetry::layer()
                .with_error_records_to_exceptions(true)
                .with_tracer(tracer)
                .with_filter(env_filter_layer)
                .boxed(),
        );
    }

    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .context(CreateOpenTelemetryPrometheusExporterSnafu)?;

    let meter_provider = SdkMeterProvider::builder().with_reader(exporter).build();

    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layers))
        .context(SetGlobalTracingSubscriberSnafu)?;
    // TODO: Have a look at how we can ship Prometheus and oltp metrics at the same time.
    opentelemetry::global::set_meter_provider(meter_provider);
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let metrics = Metrics::new(registry, persistence, config);

    Ok((metrics, tracer_provider))
}

fn otel_tracer(
    tracing_config: &TrinoLbTracingConfig,
) -> Result<(Tracer, SdkTracerProvider), Error> {
    let exporter = build_span_exporter(tracing_config)?;

    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_id_generator(RandomIdGenerator::default())
        .with_max_attributes_per_span(16)
        .with_max_events_per_span(16)
        .with_resource(Resource::builder().with_service_name("trino-lb").build())
        .with_batch_exporter(exporter)
        .build();

    global::set_tracer_provider(provider.clone());
    let tracer = provider.tracer("trino-lb");
    Ok((tracer, provider))
}

fn build_span_exporter(tracing_config: &TrinoLbTracingConfig) -> Result<SpanExporter, Error> {
    let mut builder = SpanExporter::builder().with_tonic();
    if let Some(endpoint) = &tracing_config.otlp_endpoint {
        builder = builder.with_endpoint(endpoint.as_str());
    }
    if let Some(protocol) = tracing_config.otlp_protocol {
        builder = builder.with_protocol(protocol);
    }
    if let Some(compression) = tracing_config.otlp_compression {
        builder = builder.with_compression(compression);
    }

    // In case endpoint and protocol are not set here, they will still be read from the env vars
    // OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_EXPORTER_OTLP_PROTOCOL
    builder.build().context(BuildSpanExporterSnafu)
}

pub fn add_current_context_to_client_request(
    context: Context,
    headers: &mut reqwest::header::HeaderMap,
) {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(headers));
    });
}
