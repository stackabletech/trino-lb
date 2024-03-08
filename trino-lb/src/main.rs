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

/// We can not use the `#[tokio::main]` macro, as we need at least 3 worker threads because of some magic happening
/// in metric collection that are related to <https://github.com/open-telemetry/opentelemetry-rust/issues/1376#issuecomment-1816813128>
fn main() -> Result<(), MainError> {
    const ENV_WORKER_THREADS: &str = "TOKIO_WORKER_THREADS";

    let worker_threads = match std::env::var(ENV_WORKER_THREADS) {
        Ok(s) => {
            let n = s.parse().unwrap_or_else(|e| {
                panic!("ENV_WORKER_THREADS:? must be usize, error: {e}, value: {s}")
            });
            assert!(n > 0, "{ENV_WORKER_THREADS:?} cannot be set to 0");
            n
        }
        // We default to at least 2 workers
        Err(std::env::VarError::NotPresent) => usize::max(3, num_cpus::get()),
        Err(std::env::VarError::NotUnicode(e)) => {
            panic!("{ENV_WORKER_THREADS:?} must be valid unicode, error: {e:?}")
        }
    };

    // We can not emit tracing messages here, as tracing is not set up yet.
    // println!("Starting tokio runtime with {worker_threads} workers");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap()
        .block_on(start())
}

async fn start() -> Result<(), MainError> {
    let args = Args::parse();

    let config = Config::read_from_file(&args.config_file)
        .await
        .context(ReadConfigSnafu)?;
    let cluster_groups = config.trino_cluster_groups.keys().cloned().collect();

    let persistence: Arc<PersistenceImplementation> =
        Arc::new(match &config.trino_lb.persistence {
            PersistenceConfig::InMemory {} => InMemoryPersistence::default().into(),
            PersistenceConfig::Redis(redis_config) => {
                RedisPersistence::new(redis_config, cluster_groups)
                    .await
                    .context(CreateRedisPersistenceClientSnafu)?
                    .into()
            }
            PersistenceConfig::Postgres(postgres_config) => {
                PostgresPersistence::new(postgres_config)
                    .await
                    .context(CreatePostgresPersistenceClientSnafu)?
                    .into()
            }
        });

    let metrics = Arc::new(
        tracing::init(
            config.trino_lb.tracing.as_ref(),
            Arc::clone(&persistence),
            &config,
        )
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
