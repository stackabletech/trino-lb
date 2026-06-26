use std::{sync::Arc, time::Duration};

use opentelemetry::{Context, global, trace::TracerProvider};
use opentelemetry_http::HeaderInjector;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{Aggregation, Instrument, PeriodicReader, SdkMeterProvider, Stream, Temporality},
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use snafu::{ResultExt, Snafu};
use tracing::{level_filters::LevelFilter, subscriber::SetGlobalDefaultError};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt};
use trino_lb_core::config::{Config, TrinoLbTracingConfig};
use trino_lb_persistence::PersistenceImplementation;

use crate::metrics::Metrics;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to build the OTLP span exporter"))]
    BuildSpanExporter {
        source: opentelemetry_otlp::ExporterBuildError,
    },

    #[snafu(display("Failed to build the OTLP metric exporter"))]
    BuildMetricExporter {
        source: opentelemetry_otlp::ExporterBuildError,
    },

    #[snafu(display("Failed to create OpenTelemetry Prometheus exporter"))]
    CreateOpenTelemetryPrometheusExporter {
        source: opentelemetry_sdk::error::OTelSdkError,
    },

    #[snafu(display("Failed to set global tracing subscriber"))]
    SetGlobalTracingSubscriber { source: SetGlobalDefaultError },
}

/// Sets up tracing and metrics.
///
/// Returns the [`Metrics`] handle and, if OTLP tracing is enabled, the [`SdkTracerProvider`] so the
/// caller can flush and shut it down on exit (the global `shutdown_tracer_provider` helper was
/// removed in opentelemetry 0.32).
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

        let provider = otel_tracer_provider(tracing_config)?;
        layers.push(
            tracing_opentelemetry::layer()
                .with_error_records_to_exceptions(true)
                .with_tracer(provider.tracer("trino-lb"))
                .with_filter(env_filter_layer)
                .boxed(),
        );
        tracer_provider = Some(provider);
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

    let metrics = Metrics::new(registry, persistence, config);

    Ok((metrics, tracer_provider))
}

fn otel_tracer_provider(tracing_config: &TrinoLbTracingConfig) -> Result<SdkTracerProvider, Error> {
    let exporter = configure_exporter(SpanExporter::builder().with_tonic(), tracing_config)
        .build()
        .context(BuildSpanExporterSnafu)?;

    // The batch span processor manages its own background thread as of opentelemetry_sdk 0.32, so
    // there is no longer an explicit runtime (previously `install_batch(runtime::Tokio)`). The span
    // limits and sampler that used to live on `trace::Config` are now set directly on the builder.
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::AlwaysOn)
        .with_id_generator(RandomIdGenerator::default())
        .with_max_attributes_per_span(16)
        .with_max_events_per_span(16)
        .with_resource(Resource::builder().with_service_name("trino-lb").build())
        .build();

    global::set_tracer_provider(provider.clone());
    Ok(provider)
}

/// Currently unused, see the TODO in [`init`] about shipping Prometheus and OTLP metrics at the
/// same time. Kept (and migrated to the opentelemetry 0.32 API) as a reference for the OTLP metrics
/// push path.
fn _otel_meter(tracing_config: &TrinoLbTracingConfig) -> Result<SdkMeterProvider, Error> {
    let exporter = configure_exporter(MetricExporter::builder().with_tonic(), tracing_config)
        // `DefaultAggregationSelector`/`DefaultTemporalitySelector` were removed in 0.32. The
        // default temporality (cumulative) matches the previous `DefaultTemporalitySelector`, and
        // aggregation is now picked by the SDK/views instead of an exporter-level selector.
        .with_temporality(Temporality::default())
        .with_timeout(Duration::from_secs(10))
        .build()
        .context(BuildMetricExporterSnafu)?;

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(3))
        .build();

    Ok(SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(Resource::builder().with_service_name("trino-lb").build())
        .build())
}

/// Applies the configured endpoint, protocol and compression to an OTLP exporter builder.
///
/// Works for both the span and metric tonic exporter builders, which both implement
/// [`WithExportConfig`] and [`WithTonicConfig`].
fn configure_exporter<B: WithExportConfig + WithTonicConfig>(
    mut builder: B,
    tracing_config: &TrinoLbTracingConfig,
) -> B {
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

    builder
}

fn setup_custom_metrics(i: &Instrument) -> Option<Stream> {
    if i.name() == "query_queued_duration" {
        // `Instrument` no longer exposes its description as a field/getter, so the view only sets
        // the name and aggregation. Description and unit are inherited from the instrument when not
        // overridden, so the resulting stream is identical to before.
        Stream::builder()
            .with_name(i.name().to_string())
            .with_aggregation(Aggregation::ExplicitBucketHistogram {
                // Copied and adopted from https://github.com/open-telemetry/opentelemetry-rust/blob/7d0b80ea852eb3218504b722476484063802a9a4/opentelemetry-sdk/src/metrics/reader.rs#L151-L154
                boundaries: vec![
                    0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0,
                    5000.0, 7500.0, 10000.0, 25000.0, 50000.0, 75000.0, 100000.0, 250000.0,
                    500000.0, 750000.0, 1000000.0, 2500000.0,
                ],
                record_min_max: true,
            })
            .build()
            .ok()
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
