use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, RwLock},
};

use futures::future::try_join_all;
use opentelemetry::{
    metrics::{Counter, Histogram, MetricsError, Unit},
    KeyValue,
};
use prometheus::Registry;
use snafu::{ResultExt, Snafu};
use tokio::{
    runtime::Builder,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tracing::error;
use trino_lb_core::{
    config::{Config, TrinoClusterGroupConfig},
    trino_cluster::ClusterState,
    TrinoClusterName,
};
use trino_lb_persistence::{Persistence, PersistenceImplementation};

use crate::trino_client::ClusterInfo;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to register metrics callback"))]
    RegisterMetricsCallback { source: MetricsError },
}

pub struct Metrics {
    pub registry: Registry,
    pub http_counter: Counter<u64>,
    pub queued_time: Histogram<u64>,

    /// We cant use [`tokio::sync::RwLock`] because of <https://github.com/open-telemetry/opentelemetry-rust/issues/1376>.
    /// As setting the HashMap values is not in a critical path should be fine (tm).
    pub cluster_infos: Arc<RwLock<HashMap<TrinoClusterName, ClusterInfo>>>,
}

impl Metrics {
    pub fn new(
        registry: Registry,
        persistence: Arc<PersistenceImplementation>,
        config: &Config,
    ) -> Result<Self, Error> {
        let meter = opentelemetry::global::meter("trino-lb");

        let http_counter = meter
            .u64_counter("http_requests_total")
            .with_unit(Unit::new("requests"))
            .with_description("Total number of HTTP requests made.")
            .init();

        let queued_time = meter
            .u64_histogram("query_queued_duration")
            .with_unit(Unit::new("ms"))
            .with_description("The time queries where queued in trino-lb")
            .init();

        let cluster_infos = Arc::new(RwLock::new(HashMap::<TrinoClusterName, ClusterInfo>::new()));

        let cluster_counts_per_state_metric = meter
            .u64_observable_gauge("cluster_counts_per_state")
            .with_unit(Unit::new("clusters"))
            .with_description("The number of active or inactive clusters for each cluster group")
            .init();

        let cluster_queries_metric = meter
            .u64_observable_gauge("cluster_queries")
            .with_unit(Unit::new("queries"))
            .with_description(
                "The number of running, queued or blocked queries on a specific Trino cluster",
            )
            .init();

        let queued_queries_metric = meter
            .u64_observable_gauge("queued_queries")
            .with_unit(Unit::new("queries"))
            .with_description("The number of queries queued across all trino-lb instances")
            .init();

        let cluster_infos_for_callback = Arc::clone(&cluster_infos);
        meter
            .register_callback(&[cluster_queries_metric.as_any()], move |observer| {
                if let Ok(cluster_query_counters) = cluster_infos_for_callback.read() {
                    for (cluster, counter) in cluster_query_counters.deref() {
                        observer.observe_u64(
                            &cluster_queries_metric,
                            counter.running_queries,
                            [
                                KeyValue::new("cluster", cluster.to_string()),
                                KeyValue::new("state", "running"),
                            ]
                            .as_ref(),
                        );
                        observer.observe_u64(
                            &cluster_queries_metric,
                            counter.queued_queries,
                            [
                                KeyValue::new("cluster", cluster.to_string()),
                                KeyValue::new("state", "queued"),
                            ]
                            .as_ref(),
                        );
                        observer.observe_u64(
                            &cluster_queries_metric,
                            counter.blocked_queries,
                            [
                                KeyValue::new("cluster", cluster.to_string()),
                                KeyValue::new("state", "blocked"),
                            ]
                            .as_ref(),
                        );
                    }
                }
            })
            .context(RegisterMetricsCallbackSnafu)?;

        // All of this mess can be removed once https://github.com/open-telemetry/opentelemetry-rust/issues/1376 is supported.
        let (ping_sender, ping_receiver) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (metrics_sender, metrics_receiver) =
            tokio::sync::mpsc::unbounded_channel::<HashMap<String, u64>>();
        let metrics_receiver = RwLock::new(metrics_receiver);

        // This needs to go on a dedicated runtime, as otherwise systems with <= 2 cores will only have only one tokio
        // worker thread and would deadlock.
        let trino_cluster_groups = config.trino_cluster_groups.clone();
        let persistence_clone = Arc::clone(&persistence);
        std::thread::spawn(move || {
            let metrics_runtime = Builder::new_current_thread().enable_all().build().unwrap();
            metrics_runtime.block_on(queued_query_counts_metrics_handler(
                ping_receiver,
                metrics_sender,
                persistence_clone,
                &trino_cluster_groups,
            ))
        });

        meter
            .register_callback(&[queued_queries_metric.as_any()], move |observer| {
                ping_sender.send(()).unwrap();
                let queued_queries = std::thread::scope(|s| {
                    s.spawn(|| metrics_receiver.write().unwrap().blocking_recv().unwrap())
                        .join()
                        .unwrap()
                });

                for (cluster_group, queued) in queued_queries {
                    observer.observe_u64(
                        &queued_queries_metric,
                        queued,
                        [KeyValue::new("cluster-group", cluster_group)].as_ref(),
                    );
                }
            })
            .context(RegisterMetricsCallbackSnafu)?;

        // All of this mess can be removed once https://github.com/open-telemetry/opentelemetry-rust/issues/1376 is supported.
        let (ping_sender, ping_receiver) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (metrics_sender, metrics_receiver) =
            tokio::sync::mpsc::unbounded_channel::<HashMap<String, HashMap<ClusterState, u64>>>();
        let metrics_receiver = RwLock::new(metrics_receiver);

        // This needs to go on a dedicated runtime, as otherwise systems with <= 2 cores will only have only one tokio
        // worker thread and would deadlock.
        let trino_cluster_groups = config.trino_cluster_groups.clone();
        let persistence_clone = Arc::clone(&persistence);
        std::thread::spawn(move || {
            let metrics_runtime = Builder::new_current_thread().enable_all().build().unwrap();
            metrics_runtime.block_on(cluster_counts_per_state_metrics_handler(
                ping_receiver,
                metrics_sender,
                persistence_clone,
                &trino_cluster_groups,
            ))
        });

        meter
            .register_callback(
                &[cluster_counts_per_state_metric.as_any()],
                move |observer| {
                    ping_sender.send(()).unwrap();
                    let cluster_counts = std::thread::scope(|s| {
                        s.spawn(|| metrics_receiver.write().unwrap().blocking_recv().unwrap())
                            .join()
                            .unwrap()
                    });

                    for (cluster_group, counts) in cluster_counts {
                        for (state, count) in counts {
                            observer.observe_u64(
                                &cluster_counts_per_state_metric,
                                count,
                                [
                                    KeyValue::new("cluster-group", cluster_group.clone()),
                                    KeyValue::new::<_, &str>("state", state.into()),
                                ]
                                .as_ref(),
                            );
                        }
                    }
                },
            )
            .context(RegisterMetricsCallbackSnafu)?;

        Ok(Self {
            registry,
            http_counter,
            queued_time,
            cluster_infos,
        })
    }
}

// Copied from https://github.com/open-telemetry/opentelemetry-rust/issues/1376#issuecomment-1816813128
async fn queued_query_counts_metrics_handler(
    mut ping_receiver: UnboundedReceiver<()>,
    metrics_sender: UnboundedSender<HashMap<String, u64>>,
    persistence: Arc<PersistenceImplementation>,
    trino_cluster_groups: &HashMap<String, TrinoClusterGroupConfig>,
) {
    loop {
        let Some(()) = ping_receiver.recv().await else {
            break;
        };

        let counts = try_join_all(
            trino_cluster_groups
                .keys()
                .map(|cg| persistence.get_queued_query_count(cg)),
        )
        .await;

        let counts = match counts {
            Ok(counts) => counts,
            Err(e) => {
                error!(
                    ?e,
                    "queued_query_count_metrics_handler: Failed to get_queued_query_count"
                );
                // We need so send *something*, so we don't block the other thread
                if let Err(e) = metrics_sender.send(HashMap::new()) {
                    error!(
                        ?e,
                        "queued_query_count_metrics_handler: Failed to send to metrics_sender"
                    );
                }
                continue;
            }
        };

        let queued_query_counts = trino_cluster_groups.keys().cloned().zip(counts).collect();

        if let Err(e) = metrics_sender.send(queued_query_counts) {
            error!(
                ?e,
                "queued_query_count_metrics_handler: Failed to send to metrics_sender"
            );
        }
    }
}

async fn cluster_counts_per_state_metrics_handler(
    mut ping_receiver: UnboundedReceiver<()>,
    metrics_sender: UnboundedSender<HashMap<String, HashMap<ClusterState, u64>>>,
    persistence: Arc<PersistenceImplementation>,
    trino_cluster_groups: &HashMap<String, TrinoClusterGroupConfig>,
) {
    loop {
        let Some(()) = ping_receiver.recv().await else {
            break;
        };

        let mut cluster_counts_per_state = HashMap::new();
        // TODO: Improve parallelism
        for (cluster_group, clusters) in trino_cluster_groups {
            let states = try_join_all(
                clusters
                    .trino_clusters
                    .iter()
                    .map(|c| persistence.get_cluster_state(&c.name)),
            )
            .await;

            let states = match states {
                Ok(states) => states,
                Err(e) => {
                    error!(
                        ?e,
                        "cluster_counts_per_state_metrics_handler: Failed to get_cluster_state"
                    );
                    // We need so send *something*, so we don't block the other thread
                    if let Err(e) = metrics_sender.send(HashMap::new()) {
                        error!(
                            ?e,
                            "cluster_counts_per_state_metrics_handler: Failed to send to metrics_sender"
                        );
                    }
                    continue;
                }
            };

            let mut count_per_state = HashMap::new();
            for state in states {
                *count_per_state.entry(state).or_default() += 1;
            }

            cluster_counts_per_state.insert(cluster_group.to_owned(), count_per_state);
        }

        if let Err(e) = metrics_sender.send(cluster_counts_per_state) {
            error!(
                ?e,
                "cluster_counts_per_state_metrics_handler: Failed to send to metrics_sender"
            );
        }
    }
}
