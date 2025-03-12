use std::{sync::Arc, time::Duration};

use opentelemetry::{
    global,
    metrics::MetricsError,
    trace::{TraceError, TracerProvider},
    Context, KeyValue,
};
use opentelemetry_http::HeaderInjector;
use opentelemetry_otlp::{TonicExporterBuilder, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector},
        Aggregation, Instrument, SdkMeterProvider, Stream,
    },
    propagation::TraceContextPropagator,
    trace::{self, RandomIdGenerator, Sampler},
    Resource,
};
use snafu::{ResultExt, Snafu};
use tracing::{level_filters::LevelFilter, subscriber::SetGlobalDefaultError};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};
use trino_lb_core::config::{Config, TrinoLbTracingConfig};
use trino_lb_persistence::PersistenceImplementation;

use crate::metrics::{self, Metrics};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to install tokio batch runtime"))]
    InstallTokioBatchRuntime { source: TraceError },

    #[snafu(display("Failed to create metrics pipeline"))]
    CreateMetricsPipeline { source: MetricsError },

    #[snafu(display("Failed to create OpenTelemetry Prometheus exporter"))]
    CreateOpenTelemetryPrometheusExporter { source: MetricsError },

    #[snafu(display("Failed to set global tracing subscriber"))]
    SetGlobalTracingSubscriber { source: SetGlobalDefaultError },

    #[snafu(display("Failed to set up metrics"))]
    SetUpMetrics { source: metrics::Error },
}

pub fn init(
    tracing_config: Option<&TrinoLbTracingConfig>,
    persistence: Arc<PersistenceImplementation>,
    config: &Config,
) -> Result<Metrics, Error> {
    let env_filter_layer = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let console_output_layer = tracing_subscriber::fmt::layer().with_filter(env_filter_layer);
    let mut layers = vec![console_output_layer.boxed()];

    if let Some(tracing_config) = tracing_config {
        if tracing_config.enabled {
            let env_filter_layer = EnvFilter::builder()
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env_lossy();
            layers.push(
                tracing_opentelemetry::layer()
                    .with_error_records_to_exceptions(true)
                    .with_tracer(otel_tracer(tracing_config)?)
                    .with_filter(env_filter_layer)
                    .boxed(),
            );
        }
    }

    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .context(CreateOpenTelemetryPrometheusExporterSnafu)?;

    let meter_provider = SdkMeterProvider::builder()
        .with_view(setup_custom_metrics)
        .with_reader(exporter)
        .build();

    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layers))
        .context(SetGlobalTracingSubscriberSnafu)?;
    // TODO: Have a look at how we can ship Prometheus and oltp metrics at the same time.
    opentelemetry::global::set_meter_provider(meter_provider);
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let metrics = Metrics::new(registry, persistence, config).context(SetUpMetricsSnafu)?;

    Ok(metrics)
}

fn otel_tracer(tracing_config: &TrinoLbTracingConfig) -> Result<trace::Tracer, Error> {
    let provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter(tracing_config))
        .with_trace_config(
            trace::Config::default()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default())
                .with_max_attributes_per_span(16)
                .with_max_events_per_span(16)
                .with_resource(Resource::new(vec![KeyValue::new(
                    "service.name",
                    "trino-lb",
                )])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .context(InstallTokioBatchRuntimeSnafu)?;

    global::set_tracer_provider(provider.clone());
    Ok(provider.tracer("trino-lb"))
}

fn _otel_meter(tracing_config: &TrinoLbTracingConfig) -> Result<SdkMeterProvider, Error> {
    opentelemetry_otlp::new_pipeline()
        .metrics(opentelemetry_sdk::runtime::Tokio)
        .with_exporter(exporter(tracing_config))
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            "trino-lb",
        )]))
        .with_period(Duration::from_secs(3))
        .with_timeout(Duration::from_secs(10))
        .with_aggregation_selector(DefaultAggregationSelector::new())
        .with_temporality_selector(DefaultTemporalitySelector::new())
        .build()
        .context(CreateMetricsPipelineSnafu)
}

fn exporter(tracing_config: &TrinoLbTracingConfig) -> TonicExporterBuilder {
    let mut exporter = opentelemetry_otlp::new_exporter().tonic();
    if let Some(endpoint) = &tracing_config.otlp_endpoint {
        exporter = exporter.with_endpoint(endpoint.as_str());
    }
    if let Some(protocol) = tracing_config.otlp_protocol {
        exporter = exporter.with_protocol(protocol);
    }
    if let Some(compression) = tracing_config.otlp_compression {
        exporter = exporter.with_compression(compression);
    }

    // In case endpoint and protocol are not set here, they will still be read from the env vars
    // OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_EXPORTER_OTLP_PROTOCOL

    exporter
}

fn setup_custom_metrics(i: &Instrument) -> Option<Stream> {
    if i.name == "query_queued_duration" {
        Some(
            Stream::new()
                .name(i.name.clone())
                .description(i.description.clone())
                .unit(i.unit.clone())
                .aggregation(Aggregation::ExplicitBucketHistogram {
                    // Copied and adopted from https://github.com/open-telemetry/opentelemetry-rust/blob/7d0b80ea852eb3218504b722476484063802a9a4/opentelemetry-sdk/src/metrics/reader.rs#L151-L154
                    boundaries: vec![
                        0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0,
                        2500.0, 5000.0, 7500.0, 10000.0, 25000.0, 50000.0, 75000.0, 100000.0,
                        250000.0, 500000.0, 750000.0, 1000000.0, 2500000.0,
                    ],
                    record_min_max: true,
                }),
        )
    } else {
        None
    }
}

pub fn add_current_context_to_client_request(
    context: Context,
    headers: &mut reqwest::header::HeaderMap,
) {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(headers));
    });
}
