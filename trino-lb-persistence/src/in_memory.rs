use std::{
    collections::HashMap,
    num::TryFromIntError,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::RwLock;
use tracing::{error, info, instrument};
use trino_lb_core::{
    TrinoClusterName, TrinoLbQueryId, TrinoQueryId,
    trino_cluster::ClusterState,
    trino_query::{QueuedQuery, TrinoQuery},
};

use crate::Persistence;

pub struct InMemoryPersistence {
    queued_queries: RwLock<HashMap<TrinoLbQueryId, QueuedQuery>>,
    queries: RwLock<HashMap<TrinoQueryId, TrinoQuery>>,
    cluster_query_counts: RwLock<HashMap<TrinoClusterName, AtomicU64>>,
    cluster_states: RwLock<HashMap<TrinoClusterName, ClusterState>>,
    last_query_count_fetcher_update: AtomicU64,
}

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Queued query with id {queued_query_id:?} not found"))]
    QueuedQueryNotFound { queued_query_id: TrinoLbQueryId },

    #[snafu(display("Failed to determined elapsed time since last queryCountFetcher update"))]
    DetermineElapsedTimeSinceLastUpdate { source: SystemTimeError },

    #[snafu(display(
        "Failed to store determined elapsed time since last queryCountFetcher update as millis in a u64"
    ))]
    ConvertElapsedTimeSinceLastUpdateToMillis { source: TryFromIntError },
}

impl Default for InMemoryPersistence {
    fn default() -> Self {
        info!("Using in-memory persistence");

        Self {
            queued_queries: RwLock::new(HashMap::new()),
            queries: RwLock::new(HashMap::new()),
            cluster_query_counts: RwLock::new(HashMap::new()),
            cluster_states: RwLock::new(HashMap::new()),
            last_query_count_fetcher_update: AtomicU64::from(0),
        }
    }
}

impl Persistence for InMemoryPersistence {
    #[instrument(skip(self, queued_query))]
    async fn store_queued_query(&self, queued_query: QueuedQuery) -> Result<(), super::Error> {
        let mut queued_queries = self.queued_queries.write().await;
        queued_queries.insert(queued_query.id.clone(), queued_query);

        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_queued_query(
        &self,
        queued_query_id: &TrinoLbQueryId,
    ) -> Result<QueuedQuery, super::Error> {
        let queued_queries = self.queued_queries.read().await;
        Ok(queued_queries
            .get(queued_query_id)
            .context(QueuedQueryNotFoundSnafu { queued_query_id })?
            .clone())
    }

    #[instrument(skip(self, queued_query))]
    async fn remove_queued_query(&self, queued_query: &QueuedQuery) -> Result<(), super::Error> {
        let mut queued_queries = self.queued_queries.write().await;
        queued_queries.remove(&queued_query.id);

        Ok(())
    }

    #[instrument(skip(self, query))]
    async fn store_query(&self, query: TrinoQuery) -> Result<(), super::Error> {
        let mut queries = self.queries.write().await;
        queries.insert(query.id.clone(), query);

        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_query(
        &self,
        query_id: &TrinoQueryId,
    ) -> Result<Option<TrinoQuery>, super::Error> {
        let queries = self.queries.read().await;
        Ok(queries.get(query_id).cloned())
    }

    #[instrument(skip(self))]
    async fn remove_query(&self, query_id: &TrinoQueryId) -> Result<(), super::Error> {
        let mut queries = self.queries.write().await;
        queries.remove(query_id);

        Ok(())
    }

    #[instrument(skip(self))]
    async fn inc_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
        max_allowed_count: u64,
    ) -> Result<bool, super::Error> {
        let current_counts = self.cluster_query_counts.read().await;

        match current_counts.get(cluster_name) {
            Some(count) => {
                let mut current = count.load(Ordering::SeqCst);
                loop {
                    if current + 1 > max_allowed_count {
                        return Ok(false);
                    }

                    match count.compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::SeqCst,
                        // [`Ordering::Relaxed`] should be sufficient here, but better safe than sorry
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            return Ok(true);
                        }
                        Err(x) => current = x,
                    }
                }
            }
            _ => {
                // We need to drop `current_counts` here to release the read lock it holds :)
                // Otherwise the [`RwLock::write`] call will block forever.
                drop(current_counts);

                self.cluster_query_counts
                    .write()
                    .await
                    .insert(cluster_name.clone(), AtomicU64::new(1));

                Ok(true)
            }
        }
    }

    #[instrument(skip(self))]
    async fn dec_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<(), super::Error> {
        match self.cluster_query_counts.read().await.get(cluster_name) {
            Some(count) => {
                if count.fetch_sub(1, Ordering::SeqCst) == 0 {
                    error!(
                        cluster_name,
                        "Persistence was asked to decrement the number of queries for the given cluster, but it would result in a negative amount of queries. Setting it to 0 instead."
                    );
                    count.store(0, Ordering::SeqCst);
                }
            }
            _ => {
                error!(
                    "Persistence was asked to decrement the number of queries, but no query count for this cluster was not known. This should not happen."
                )
            }
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn set_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
        count: u64,
    ) -> Result<(), super::Error> {
        let current_counts = self.cluster_query_counts.read().await;
        match current_counts.get(cluster_name) {
            Some(current_count) => {
                current_count.store(count, Ordering::SeqCst);
            }
            _ => {
                drop(current_counts);

                self.cluster_query_counts
                    .write()
                    .await
                    .insert(cluster_name.clone(), AtomicU64::from(count));
            }
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<u64, super::Error> {
        Ok(self
            .cluster_query_counts
            .read()
            .await
            .get(cluster_name)
            .map(|c| c.load(Ordering::SeqCst))
            // There can be the case this function is called before `inc_cluster_queries`, so the number of queries is 0 in this case.
            .unwrap_or_default())
    }

    #[instrument(skip(self))]
    async fn get_queued_query_count(&self, cluster_group: &str) -> Result<u64, super::Error> {
        Ok(self
            .queued_queries
            .read()
            .await
            .values()
            .filter(|q| q.cluster_group == cluster_group)
            // Safety: I think this is a reasonable assumption
            .count() as u64)
    }

    #[instrument(skip(self))]
    async fn delete_queued_queries_not_accessed_after(
        &self,
        not_accessed_after: SystemTime,
    ) -> Result<u64, super::Error> {
        let mut removed = 0;
        self.queued_queries.write().await.retain(|_, q| {
            if q.last_accessed >= not_accessed_after {
                true
            } else {
                removed += 1;
                false
            }
        });

        Ok(removed)
    }

    #[instrument(skip(self))]
    async fn get_last_query_count_fetcher_update(&self) -> Result<SystemTime, super::Error> {
        let ms = self.last_query_count_fetcher_update.load(Ordering::SeqCst);

        // TODO: Check for overflows
        Ok(UNIX_EPOCH + Duration::from_millis(ms))
    }

    #[instrument(skip(self))]
    async fn set_last_query_count_fetcher_update(
        &self,
        update: SystemTime,
    ) -> Result<(), super::Error> {
        let ms = update
            .duration_since(UNIX_EPOCH)
            .context(DetermineElapsedTimeSinceLastUpdateSnafu)?
            .as_millis()
            .try_into()
            .context(ConvertElapsedTimeSinceLastUpdateToMillisSnafu)?;

        self.last_query_count_fetcher_update
            .store(ms, Ordering::SeqCst);

        Ok(())
    }

    #[instrument(skip(self))]
    async fn set_cluster_state(
        &self,
        cluster_name: &TrinoClusterName,
        state: ClusterState,
    ) -> Result<(), super::Error> {
        let mut cluster_states = self.cluster_states.write().await;
        cluster_states.insert(cluster_name.to_owned(), state);

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_cluster_state(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<ClusterState, super::Error> {
        let cluster_states = self.cluster_states.read().await;

        Ok(cluster_states
            .get(cluster_name)
            .cloned()
            .unwrap_or(ClusterState::Unknown))
    }
}
