use std::sync::Arc;

use clap::Parser;
use cluster_group_manager::ClusterGroupManager;
use main_error::MainError;
use maintenance::{
    leftover_queries::LeftoverQueryDetector, query_count_fetcher,
    query_count_fetcher::QueryCountFetcher,
};
use opentelemetry::global::shutdown_tracer_provider;
use routing::Router;
use scaling::Scaler;
use snafu::{ResultExt, Snafu};
use tokio::task::JoinError;
use trino_lb_core::config::{self, Config, PersistenceConfig};
use trino_lb_persistence::{
    in_memory::InMemoryPersistence,
    postgres::{self, PostgresPersistence},
    redis::{self, RedisPersistence},
    PersistenceImplementation,
};

use crate::{args::Args, http_server::start_http_server};

mod args;
mod cluster_group_manager;
mod http_server;
mod maintenance;
mod metrics;
mod routing;
mod scaling;
mod tracing;
mod trino_client;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to spawn dedicated tokio runtime for tracing and metrics"))]
    SpawnTokioRuntimeForTracingAndMetrics { source: JoinError },

    #[snafu(display("Failed to set up tracing"))]
    SetUpTracing { source: tracing::Error },

    #[snafu(display("Failed to read configuration"))]
    ReadConfig { source: config::Error },

    #[snafu(display("Failed to create redis persistence client"))]
    CreateRedisPersistenceClient { source: redis::Error },

    #[snafu(display("Failed to create postgres persistence client"))]
    CreatePostgresPersistenceClient { source: postgres::Error },

    #[snafu(display("Failed to create router"))]
    CreateRouter { source: routing::Error },

    #[snafu(display("Failed to create cluster group manager"))]
    CreateClusterGroupManager {
        source: cluster_group_manager::Error,
    },

    #[snafu(display("Failed to create query count fetcher"))]
    CreateQueryCountFetcher { source: query_count_fetcher::Error },

    #[snafu(display("Failed to create scaler"))]
    CreateScaler { source: scaling::Error },

    #[snafu(display("Failed to start HTTP server"))]
    StartHttpServer { source: http_server::Error },
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();

    let config = Config::read_from_file(&args.config_file)
        .await
        .context(ReadConfigSnafu)?;
    let cluster_groups = config.trino_cluster_groups.keys().cloned().collect();

    let persistence: Arc<PersistenceImplementation> =
        Arc::new(match &config.trino_lb.persistence {
            PersistenceConfig::InMemory {} => InMemoryPersistence::default().into(),
            PersistenceConfig::Redis(redis_config) => {
                if redis_config.cluster_mode {
                    RedisPersistence::<
                        ::redis::cluster_async::ClusterConnection<
                            ::redis::aio::MultiplexedConnection,
                        >,
                    >::new(redis_config, cluster_groups)
                    .await
                    .context(CreateRedisPersistenceClientSnafu)?
                    .into()
                } else {
                    RedisPersistence::<::redis::aio::MultiplexedConnection>::new(
                        redis_config,
                        cluster_groups,
                    )
                    .await
                    .context(CreateRedisPersistenceClientSnafu)?
                    .into()
                }
            }
            PersistenceConfig::Postgres(postgres_config) => {
                PostgresPersistence::new(postgres_config)
                    .await
                    .context(CreatePostgresPersistenceClientSnafu)?
                    .into()
            }
        });

    // https://github.com/open-telemetry/opentelemetry-rust/issues/1376#issuecomment-1987102217
    // For now, I was able to get around it by launching the meter provider in its own dedicated Tokio runtime in a new
    // thread. The separate Tokio runtime seems to avoid issues with the tasks in the primary Tokio runtime.
    let persistence_for_metrics = Arc::clone(&persistence);
    let config_for_metrics = config.clone();
    let metrics = Arc::new(
        tokio::task::spawn_blocking(move || {
            tracing::init(
                config_for_metrics.trino_lb.tracing.as_ref(),
                persistence_for_metrics,
                &config_for_metrics,
            )
        })
        .await
        .context(SpawnTokioRuntimeForTracingAndMetricsSnafu)?
        .context(SetUpTracingSnafu)?,
    );

    let cluster_group_manager = ClusterGroupManager::new(
        Arc::clone(&persistence),
        &config,
        config.trino_cluster_groups_ignore_cert,
    )
    .context(CreateClusterGroupManagerSnafu)?;

    let router = Router::new(&config).context(CreateRouterSnafu)?;

    let scaler = Scaler::new_if_configured(&config, Arc::clone(&persistence))
        .await
        .context(CreateScalerSnafu)?;
    if let Some(scaler) = scaler {
        scaler.start_loop();
    }

    let query_count_fetcher = QueryCountFetcher::new(
        Arc::clone(&persistence),
        &config.trino_cluster_groups,
        config.trino_cluster_groups_ignore_cert,
        &config.trino_lb.refresh_query_counter_interval,
        Arc::clone(&metrics),
    )
    .context(CreateQueryCountFetcherSnafu)?;
    query_count_fetcher.start_loop();

    LeftoverQueryDetector::new(Arc::clone(&persistence)).start_loop();

    start_http_server(
        config,
        persistence,
        cluster_group_manager,
        router,
        Arc::clone(&metrics),
    )
    .await
    .context(StartHttpServerSnafu)?;

    shutdown_tracer_provider();

    Ok(())
}
