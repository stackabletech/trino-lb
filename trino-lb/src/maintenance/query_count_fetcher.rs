use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures::{future::join_all, TryFutureExt};
use snafu::Snafu;
use tokio::time;
use tracing::{error, info, info_span, instrument, Instrument};
use trino_lb_core::{config::TrinoClusterConfig, trino_cluster::ClusterState, TrinoClusterName};
use trino_lb_persistence::{Persistence, PersistenceImplementation};

use crate::{config::TrinoClusterGroupConfig, metrics::Metrics, trino_client::get_cluster_info};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to create HTTP client"))]
    CreateHttpClient { source: reqwest::Error },
}

pub struct QueryCountFetcher {
    persistence: Arc<PersistenceImplementation>,
    clusters: Vec<TrinoClusterConfig>,
    ignore_certs: bool,
    refresh_query_counter_interval: Duration,
    metrics: Arc<Metrics>,
}

impl QueryCountFetcher {
    #[instrument(skip(persistence, metrics))]
    pub fn new(
        persistence: Arc<PersistenceImplementation>,
        config: &HashMap<String, TrinoClusterGroupConfig>,
        ignore_certs: bool,
        refresh_query_counter_interval: &Duration,
        metrics: Arc<Metrics>,
    ) -> Result<Self, Error> {
        // Remove all the duplicated clusters that are part of multiple groups.
        let clusters: HashMap<&TrinoClusterName, &TrinoClusterConfig> = config
            .values()
            .flat_map(|g| &g.trino_clusters)
            .map(|c| (&c.name, c))
            .collect();
        let clusters = clusters.into_values().cloned().collect();

        Ok(Self {
            persistence,
            clusters,
            ignore_certs,
            refresh_query_counter_interval: *refresh_query_counter_interval,
            metrics,
        })
    }

    pub fn start_loop(self) {
        tokio::spawn(async move {
            self.loop_().await;
        });
    }

    async fn loop_(&self) {
        let mut interval = time::interval(self.refresh_query_counter_interval);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            // First tick does not sleep, so let's put it at the start of the loop.
            interval.tick().await;

            async {
                let last_update = self.persistence.get_last_query_count_fetcher_update().await;
                let last_update = match last_update {
                    Ok(last_update) => last_update,
                    Err(err) => {
                        error!(
                            ?err,
                            "QueryCountFetcher: Failed to get last QueryCountFetcher update timestamp"
                        );
                        // This return leaves the current async {} block, not the outer loop!
                        return;
                    }
                };

                if SystemTime::now()
                    < last_update
                + self.refresh_query_counter_interval
                // Safety buffer for varying run times.
                - Duration::from_millis(50)
                {
                    info!("QueryCountFetcher: Did not update query counters, as there already was a update {:?} ago.",
                        last_update.elapsed());

                    if let Ok(mut cluster_infos) = self.metrics.cluster_infos.write() {
                        // As we are not the active leader pulling the cluster info, we should not expose outdated metrics.
                        cluster_infos.clear();
                    }

                    // This return leaves the current async {} block, not the outer loop!
                    return;
                }

                // We store the updated timestamp before actually doing the query update counter.
                // This *could* lead to missing query counter updates, but prevents two trino-lb updating the counters in
                // parallel. Also if the update fails on a trino-lb instances are pretty high it will fails on a different
                // one as well.
                //
                // We also don't swap the timestamp atomically, so there is the theoretic chance of multiple query count
                // fetchers run in parallel. This will probably not cause any issues unless with get dozens of trino-lb
                // instances and can be fixed in the future.
                let result = self
                    .persistence
                    .set_last_query_count_fetcher_update(SystemTime::now())
                    .await;
                if let Err(err) = result {
                    error!(
                        ?err,
                        "QueryCountFetcher: Failed to set last QueryCountFetcher update timestamp"
                    );
                    // This return leaves the current async {} block, not the outer loop!
                    return;
                }

                let cluster_states = join_all(
                    self.clusters
                    .iter()
                    .map(|c| self.persistence.get_cluster_state(&c.name)
                        .unwrap_or_else(|_| ClusterState::Unknown))
                ).await;

                // Just before adding the new metrics remove the old entries (otherwise we have leftovers from shut down
                // clusters)
                if let Ok(mut cluster_infos) = self.metrics.cluster_infos.write() {
                    cluster_infos.clear();
                }
                let result = join_all(
                    self.clusters
                        .iter()
                        .zip(cluster_states)
                        .map(|(cluster, state)| self.process_cluster(cluster, state))
                )
                .await;

                info!(
                    "QueryCountFetcher: Updated query counters from {} remote clusters",
                    result.len()
                );
            }.instrument(info_span!("Fetching current query counters")).await;
        }
    }

    #[instrument(skip(self, cluster), fields(cluster_name = cluster.name))]
    async fn process_cluster(&self, cluster: &TrinoClusterConfig, state: ClusterState) {
        match state {
            ClusterState::Ready | ClusterState::Unhealthy | ClusterState::Draining { .. } => {
                self.fetch_and_store_query_count(cluster).await;
            }
            ClusterState::Unknown
            | ClusterState::Stopped
            | ClusterState::Starting
            | ClusterState::Terminating
            | ClusterState::Deactivated => {
                if let Err(err) = self
                    .persistence
                    .set_cluster_query_count(&cluster.name, 0)
                    .await
                {
                    error!(
                        cluster = cluster.name,
                        ?err,
                        "QueryCountFetcher: Failed to set current cluster query count to zero"
                    );
                }
            }
        }
    }

    #[instrument(skip(self, cluster), fields(cluster_name = cluster.name))]
    async fn fetch_and_store_query_count(&self, cluster: &TrinoClusterConfig) {
        let cluster_info =
            get_cluster_info(&cluster.endpoint, self.ignore_certs, &cluster.credentials).await;

        match cluster_info {
            Ok(cluster_info) => {
                let result = self
                    .persistence
                    .set_cluster_query_count(
                        &cluster.name,
                        cluster_info.running_queries
                            + cluster_info.blocked_queries
                            + cluster_info.queued_queries,
                    )
                    .await;

                if let Ok(mut cluster_infos) = self.metrics.cluster_infos.write() {
                    cluster_infos.insert(cluster.name.clone(), cluster_info);
                }

                if let Err(err) = result {
                    error!(
                        cluster = cluster.name,
                        ?err,
                        "QueryCountFetcher: Failed to set current cluster query count"
                    );
                }
            }
            Err(err) => error!(
                cluster = cluster.name,
                ?err,
                "QueryCountFetcher: Failed to get current cluster info"
            ),
        }
    }
}
