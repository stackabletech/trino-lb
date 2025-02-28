use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, SystemTimeError},
};

use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use futures::future::try_join_all;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable::StackableScaler;
use tokio::{
    join,
    task::{JoinError, JoinSet},
    time,
};
use tracing::{debug, error, info, instrument, warn, Instrument, Span};
use trino_lb_core::{
    config::{Config, ScalerConfig, ScalerConfigImplementation},
    trino_cluster::ClusterState,
    TrinoClusterName,
};
use trino_lb_persistence::{Persistence, PersistenceImplementation};

use crate::cluster_group_manager::TrinoCluster;

use self::config::TrinoClusterGroupAutoscaling;

pub mod config;
pub mod stackable;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Stackable scaling error"), context(false))]
    #[allow(clippy::enum_variant_names)]
    StackableError { source: stackable::Error },

    #[snafu(display("Configuration error: A specific Trino cluster can only be part of a single clusterGroup. Please make sure the Trino cluster {cluster_name:?} only is part of a single clusterGroup."))]
    ConfigErrorTrinoClusterInMultipleClusterGroups { cluster_name: String },

    #[snafu(display("Failed to create Stackable autoscaler"))]
    CreateStackableAutoscaler { source: stackable::Error },

    #[snafu(display("Failed to get the counter of running queries on the cluster {cluster:?}"))]
    GetClusterQueryCounter {
        source: trino_lb_persistence::Error,
        cluster: TrinoClusterName,
    },

    #[snafu(display(
        "Failed to get the query counter on the clusters of the group {cluster_group:?}"
    ))]
    GetQueryCounterForGroup {
        source: trino_lb_persistence::Error,
        cluster_group: String,
    },

    #[snafu(display("Failed to get the queued query counter of the group {cluster_group:?}"))]
    GetQueuedQueryCounterForGroup {
        source: trino_lb_persistence::Error,
        cluster_group: String,
    },

    #[snafu(display(
        "The cluster group {cluster_group:?} has an invalid autoscaler configuration"
    ))]
    InvalidAutoscalerConfiguration {
        source: crate::scaling::config::Error,
        cluster_group: String,
    },

    #[snafu(display(
        "Failed to read current cluster state for cluster {cluster:?} from persistence"
    ))]
    ReadCurrentClusterStateFromPersistence {
        source: trino_lb_persistence::Error,
        cluster: TrinoClusterName,
    },

    #[snafu(display(
        "Failed to read current cluster state for cluster group {cluster_group:?} from persistence"
    ))]
    ReadCurrentClusterStateForClusterGroupFromPersistence {
        source: trino_lb_persistence::Error,
        cluster_group: String,
    },

    #[snafu(display("Failed to set cluster state for cluster {cluster:?} in persistence"))]
    SetCurrentClusterStateInPersistence {
        source: trino_lb_persistence::Error,
        cluster: TrinoClusterName,
    },

    #[snafu(display(
        "Failed to determine how long the cluster {cluster:?} has no queries running (currently draining). Maybe the clocks are out of sync"
    ))]
    DetermineDurationWithoutQueries {
        source: SystemTimeError,
        cluster: TrinoClusterName,
    },

    #[snafu(display("Failed to join reconcile cluster group task"))]
    JoinReconcileClusterGroupTask { source: JoinError },

    #[snafu(display("Failed to join apply cluster target state task"))]
    JoinApplyClusterTargetStateTask { source: JoinError },

    #[snafu(display("Failed to join get current cluster state task"))]
    JoinGetCurrentClusterStateTask { source: JoinError },

    #[snafu(display("The variable \"scaler\" is None. This should never happen, as we only run the reconciliation when a scaler is configured!"))]
    ScalerVariableIsNone,

    #[snafu(display("The scaler config is missing. This is a bug in trino-lb, as it should exist at this particular code path"))]
    ScalerConfigMissing,
}

/// The scaler periodically
/// 1. Checks the state of all clusters. In case scaling is disabled (either entirely or for a given cluster group),
///    the cluster states will always be set Ready, so that the cluster will get queries routed.
/// 2. In case scaling is enabled for a cluster group it supervises the load and scaled the number of clusters
///    accordingly
pub struct Scaler {
    /// The original config passed by the user
    config: Option<ScalerConfig>,
    /// In case this is [`None`], no scaling at all is configured.
    scaler: Option<ScalerImplementation>,
    persistence: Arc<PersistenceImplementation>,
    /// Stores a list of all Trino clusters per cluster group.
    groups: HashMap<String, Vec<TrinoCluster>>,
    /// Stores the scaling config per cluster group. This HashMap only contains entries for the cluster groups that
    /// actually need scaling, non-scaled cluster groups are missing from the HashMap.
    scaling_config: HashMap<String, TrinoClusterGroupAutoscaling>,
}

impl Scaler {
    #[instrument(skip(persistence))]
    pub async fn new(
        config: &Config,
        persistence: Arc<PersistenceImplementation>,
    ) -> Result<Self, Error> {
        let mut scaling_config = HashMap::new();

        let scaler = match &config.cluster_autoscaler {
            None => None,
            Some(scaler) => {
                for (group_name, group) in &config.trino_cluster_groups {
                    if let Some(autoscaling) = &group.autoscaling {
                        scaling_config.insert(
                            group_name.to_owned(),
                            autoscaling.to_owned().try_into().context(
                                InvalidAutoscalerConfigurationSnafu {
                                    cluster_group: group_name,
                                },
                            )?,
                        );
                    }
                    // Cluster groups that don't need scaling are missing from the `scaling_config`.
                }

                let scaler = match &scaler.implementation {
                    ScalerConfigImplementation::Stackable(scaler_config) => {
                        StackableScaler::new(scaler_config, &config.trino_cluster_groups)
                            .await
                            .context(CreateStackableAutoscalerSnafu)?
                            .into()
                    }
                };
                Some(scaler)
            }
        };

        // FIXME: Remove duplicated code (copied from ClusterGroupManager)
        let mut clusters_seen = HashSet::new();
        let mut groups = HashMap::new();
        for (group_name, group_config) in &config.trino_cluster_groups {
            let mut group = Vec::with_capacity(group_config.trino_clusters.len());
            for cluster_config in &group_config.trino_clusters {
                let cluster_name = cluster_config.name.clone();
                if !clusters_seen.insert(cluster_name.clone()) {
                    ConfigErrorTrinoClusterInMultipleClusterGroupsSnafu {
                        cluster_name: cluster_name.clone(),
                    }
                    .fail()?;
                }

                group.push(TrinoCluster {
                    name: cluster_name,
                    max_running_queries: group_config.max_running_queries,
                    endpoint: cluster_config.endpoint.clone(),
                })
            }
            groups.insert(group_name.clone(), group);
        }

        Ok(Scaler {
            scaler,
            persistence,
            groups,
            scaling_config,
            config: config.cluster_autoscaler.clone(),
        })
    }

    pub fn start_loop(self) -> Result<(), Error> {
        if self.scaler.is_some() {
            // As there is a scaler configured, let's start it normally.
            let interval = self
                .config
                .as_ref()
                .context(ScalerConfigMissingSnafu)?
                .reconcile_interval;
            let mut interval = time::interval(interval);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

            let me = Arc::new(self);
            tokio::spawn(async move {
                loop {
                    // First tick does not sleep, so let's put it at the start of the loop.
                    interval.tick().await;

                    match me.clone().reconcile().await {
                        Ok(()) => info!("Scaler: reconciled"),
                        Err(error) => error!(?error, "Scaler: reconciled failed"),
                    }
                }
            });
        } else {
            // There is no scaling configured at all, so we periodically need to set all clusters active. We need to do
            // this repeatedly, as the state would be stuck in Unknown until trino-lb get's restarted once the
            // persistence gets wiped.
            let mut interval = time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

            tokio::spawn(async move {
                loop {
                    // First tick does not sleep, so let's put it at the start of the loop.
                    interval.tick().await;
                    if let Err(error) = self.set_all_clusters_to_ready().await {
                        error!(?error, "Scaler: Failed to set all clusters to ready");
                    }
                }
            });
        }

        Ok(())
    }

    #[instrument(name = "Scaler::reconcile", skip(self))]
    pub async fn reconcile(self: Arc<Self>) -> Result<(), Error> {
        let mut join_set = JoinSet::new();

        for (cluster_group, clusters) in self.groups.clone() {
            let me = Arc::clone(&self);
            join_set.spawn(
                me.reconcile_cluster_group(cluster_group, clusters)
                    .instrument(Span::current()),
            );
        }

        while let Some(res) = join_set.join_next().await {
            res.context(JoinReconcileClusterGroupTaskSnafu)??;
        }
        info!("All cluster groups reconciled");

        Ok(())
    }

    #[instrument(name = "Scaler::reconcile_cluster_group", skip(self, clusters))]
    pub async fn reconcile_cluster_group(
        self: Arc<Self>,
        cluster_group: String,
        clusters: Vec<TrinoCluster>,
    ) -> Result<(), Error> {
        let now = Utc::now();
        let scaling_config = match self.scaling_config.get(&cluster_group) {
            Some(scaling_config) => scaling_config,
            None => {
                // As there is no scaling configured for this cluster group, we periodically need to set all clusters
                // ready. We need to do this repeatedly, as the state would be stuck in Unknown until trino-lb get's
                // restarted once the persistence gets wiped.
                for cluster in clusters {
                    self.persistence
                        .set_cluster_state(&cluster.name, ClusterState::Ready)
                        .await
                        .context(SetCurrentClusterStateInPersistenceSnafu {
                            cluster: &cluster.name,
                        })?;
                }
                return Ok(());
            }
        };

        let mut join_set = JoinSet::new();
        for cluster in &clusters {
            let me = Arc::clone(&self);
            join_set.spawn(
                me.get_current_cluster_state(cluster.name.clone(), scaling_config.to_owned())
                    .instrument(Span::current()),
            );
        }

        let mut target_states = HashMap::new();
        while let Some(res) = join_set.join_next().await {
            let (cluster_name, current_state) =
                res.context(JoinGetCurrentClusterStateTaskSnafu)??;
            target_states.insert(cluster_name, current_state);
        }
        info!(current_states = ?target_states, "Current cluster states");

        // Determine needed clusters
        let queued = self
            .persistence
            .get_queued_query_count(&cluster_group)
            .await
            .context(GetQueuedQueryCounterForGroupSnafu {
                cluster_group: &cluster_group,
            })?;
        if queued >= scaling_config.upscale_queued_queries_threshold {
            // Check if there is already a cluster starting, nothing to do in that case
            let already_starting = target_states.values().any(|s| *s == ClusterState::Starting);

            if !already_starting {
                // Walk list top to bottom and start the first cluster that can be started
                let to_start = clusters
                    .iter()
                    .map(|c| (target_states.get(&c.name).unwrap(), c))
                    .find(|(state, _)| state.can_be_started());

                if let Some((_, to_start)) = to_start {
                    target_states.insert(to_start.name.to_owned(), ClusterState::Starting);
                }
            }
        } else if queued == 0 {
            // Determine excess clusters, this only makes sense when we don't upscale
            let cluster_query_counters = try_join_all(
                clusters
                    .iter()
                    .map(|g| async { self.persistence.get_cluster_query_count(&g.name).await }),
            )
            .await
            .context(GetQueryCounterForGroupSnafu {
                cluster_group: &cluster_group,
            })?;
            let max_running_queries: u64 = clusters
                .iter()
                .map(|c| (c, target_states.get(&c.name).unwrap()))
                .filter(|(_, s)| s.ready_to_accept_queries())
                .map(|(c, _)| c.max_running_queries)
                .sum();
            let current_running_queries: u64 = cluster_query_counters.iter().sum();

            let utilization_percent = match (max_running_queries, current_running_queries) {
                // No cluster is ready to accept queries and no queries running
                (0, 0) => 0,
                // No cluster is ready to accept queries but there are some queries still running
                // This means the clusters are even more utilized than they normally should have (although they e.g. can
                // have some queries running during draining)
                // So we set the utilization to 100%
                (0, _) => 100,
                // We can calculate the percentage normally, it's safe to divide by max_running_queries
                (_, _) => 100 * current_running_queries / max_running_queries,
            };

            debug!(
                current_running_queries,
                max_running_queries, utilization_percent, "Current cluster group query utilization"
            );

            if utilization_percent <= scaling_config.downscale_running_queries_percentage_threshold
            {
                // Check if there is already a cluster shutting down, nothing to do in that case
                let already_shutting_down = clusters
                    .iter()
                    .map(|c| target_states.get(&c.name).unwrap())
                    .any(|s| {
                        *s == ClusterState::Terminating
                            || matches!(s, ClusterState::Draining { .. })
                    });

                if !already_shutting_down {
                    // Walk list bottom to top and find the first cluster that is currently active
                    let shut_down_candidates = clusters
                        .iter()
                        .rev()
                        .map(|c| (target_states.get(&c.name).unwrap(), c))
                        .filter(|(state, _)| **state == ClusterState::Ready)
                        .collect::<Vec<_>>();

                    // We don't want to shut down the last remaining cluster obviously
                    // The only exception is the case no queries were running at all, in that case we shut down
                    // the unneeded cluster.
                    if shut_down_candidates.len() > 1 || current_running_queries == 0 {
                        if let Some((_, to_shut_down)) = shut_down_candidates.first() {
                            target_states.insert(
                                to_shut_down.name.to_owned(),
                                ClusterState::Draining {
                                    last_time_seen_with_queries: SystemTime::now(),
                                },
                            );
                        }
                    }
                }
            }
        }

        // Spin up the minimum amount of required clusters
        let min_clusters = self.get_current_min_cluster_count(scaling_config, &cluster_group, &now);
        debug!(cluster_group, min_clusters, "Current min clusters");
        for cluster in clusters.iter().take(min_clusters as usize) {
            let current_state = target_states.get(&cluster.name).unwrap();
            let target_state = current_state.start();
            target_states.insert(cluster.name.to_owned(), target_state);
        }

        debug!(?target_states, "Target cluster states");

        let mut join_set = JoinSet::new();

        for cluster in clusters {
            // FIXME: unwrap
            let me = Arc::clone(&self);
            let target_state = target_states.get(&cluster.name).unwrap();
            join_set.spawn(
                me.apply_cluster_target_state(cluster, target_state.clone())
                    .instrument(Span::current()),
            );
        }

        while let Some(res) = join_set.join_next().await {
            res.context(JoinApplyClusterTargetStateTaskSnafu)??;
        }
        info!("Applied all target cluster states");

        Ok(())
    }

    #[instrument(name = "Scaler::get_current_state", skip(self, scaling_config))]
    async fn get_current_cluster_state(
        self: Arc<Self>,
        cluster_name: TrinoClusterName,
        scaling_config: TrinoClusterGroupAutoscaling,
    ) -> Result<(TrinoClusterName, ClusterState), Error> {
        let scaler = self.scaler.as_ref().context(ScalerVariableIsNoneSnafu)?;

        let (stored_state, activated, ready) = join!(
            self.persistence.get_cluster_state(&cluster_name),
            scaler.is_activated(&cluster_name),
            scaler.is_ready(&cluster_name)
        );
        let (stored_state, activated, ready) = (
            stored_state.context(ReadCurrentClusterStateFromPersistenceSnafu {
                cluster: &cluster_name,
            })?,
            activated?,
            ready?,
        );

        let current_state = match stored_state {
            ClusterState::Unknown => {
                // State not known in persistence, so let's determine current state
                match (activated, ready) {
                    (true, true) => ClusterState::Ready,
                    // It could also be Terminating or Unhealthy, but in that case it would need to be stored as
                    // Terminating or Unhealthy in the persistence
                    (true, false) => ClusterState::Starting,
                    // This might happen for very short time periods. E.g. for the Stackable scaler, this can be
                    // the case when spec.clusterOperation.stopped was just set to true, but trino-operator did
                    // not have the time yet to update the status of the TrinoCluster to reflect the change.
                    (false, true) => ClusterState::Terminating,
                    (false, false) => ClusterState::Stopped,
                }
            }
            ClusterState::Stopped => ClusterState::Stopped,
            ClusterState::Starting => {
                if ready {
                    ClusterState::Ready
                } else {
                    ClusterState::Starting
                }
            }
            ClusterState::Ready => {
                if ready {
                    ClusterState::Ready
                } else {
                    ClusterState::Unhealthy
                }
            }
            ClusterState::Unhealthy => {
                if ready {
                    ClusterState::Ready
                } else {
                    ClusterState::Unhealthy
                }
            }
            ClusterState::Draining {
                last_time_seen_with_queries,
            } => {
                // There might be the case someone manually "force-killed" to cluster as the draining took to
                // long. We should detect this case.
                if !ready {
                    if activated {
                        ClusterState::Terminating
                    } else {
                        ClusterState::Stopped
                    }
                } else {
                    let current_query_counter = self
                        .persistence
                        .get_cluster_query_count(&cluster_name)
                        .await
                        .context(GetClusterQueryCounterSnafu {
                            cluster: &cluster_name,
                        })?;

                    let duration_with_no_queries = last_time_seen_with_queries.elapsed().context(
                        DetermineDurationWithoutQueriesSnafu {
                            cluster: &cluster_name,
                        },
                    )?;

                    if current_query_counter == 0 {
                        if duration_with_no_queries
                            >= scaling_config.drain_idle_duration_before_shutdown
                        {
                            ClusterState::Terminating
                        } else {
                            ClusterState::Draining {
                                // Don't set it to `SystemTime::now()`, as there is currently no query running
                                last_time_seen_with_queries,
                            }
                        }
                    } else {
                        ClusterState::Draining {
                            last_time_seen_with_queries: SystemTime::now(),
                        }
                    }
                }
            }
            ClusterState::Terminating => {
                if !ready {
                    ClusterState::Stopped
                } else {
                    ClusterState::Terminating
                }
            }
            ClusterState::Deactivated => ClusterState::Deactivated,
        };

        Ok((cluster_name, current_state))
    }

    #[instrument(
        name = "Scaler::apply_cluster_target_state",
        skip(self, cluster),
        fields(%cluster.name)
    )]
    async fn apply_cluster_target_state(
        self: Arc<Self>,
        cluster: TrinoCluster,
        target_state: ClusterState,
    ) -> Result<(), Error> {
        let scaler = self.scaler.as_ref().context(ScalerVariableIsNoneSnafu)?;

        match target_state {
            ClusterState::Unknown => {
                error!(cluster = cluster.name, ?target_state, "After calculating the new target states the state was \"Unknown\", so we did not enabled or disable the cluster. This should not happen!")
            }
            ClusterState::Stopped | ClusterState::Terminating => {
                scaler.deactivate(&cluster.name).await?;
            }
            ClusterState::Starting
            | ClusterState::Unhealthy
            | ClusterState::Ready
            | ClusterState::Draining { .. } => {
                scaler.activate(&cluster.name).await?;
            }
            ClusterState::Deactivated => {
                // We don't do anything here, as it's up to the (possible human) operator to take care of the cluster
            }
        }

        self.persistence
            .set_cluster_state(&cluster.name, target_state.to_owned())
            .await
            .context(SetCurrentClusterStateInPersistenceSnafu {
                cluster: &cluster.name,
            })?;

        Ok(())
    }

    /// Returns an Error in case any of the clusters can not be turned ready. Following clusters will not be tried, as
    /// the assumptions is that they will also fail.
    #[instrument(name = "Scaler::set_all_clusters_to_ready", skip(self))]
    async fn set_all_clusters_to_ready(&self) -> Result<(), Error> {
        for cluster in self.groups.values().flatten() {
            self.persistence
                .set_cluster_state(&cluster.name, ClusterState::Ready)
                .await
                .context(SetCurrentClusterStateInPersistenceSnafu {
                    cluster: &cluster.name,
                })?;
        }

        Ok(())
    }

    #[instrument(skip(self))]
    fn get_current_min_cluster_count(
        &self,
        scaling_config: &TrinoClusterGroupAutoscaling,
        cluster_group: &str,
        date: &DateTime<Utc>,
    ) -> u64 {
        let min_cluster = scaling_config
            .min_clusters
            .iter()
            .rev()
            .find(|c| c.date_is_in_range(date));

        min_cluster
            .map(|m| m.min)
            // In case no time period matches we assume no cluster is needed
            .unwrap_or(0)
    }
}

#[enum_dispatch(ScalerImplementation)]
pub trait ScalerTrait {
    async fn activate(&self, cluster: &TrinoClusterName) -> Result<(), Error>;

    async fn deactivate(&self, cluster: &TrinoClusterName) -> Result<(), Error>;

    async fn is_activated(&self, cluster: &TrinoClusterName) -> Result<bool, Error>;

    async fn is_ready(&self, cluster: &TrinoClusterName) -> Result<bool, Error>;
}

#[enum_dispatch]
pub enum ScalerImplementation {
    Stackable(StackableScaler),
}
