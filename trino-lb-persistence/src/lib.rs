use std::{fmt::Debug, time::SystemTime};

use enum_dispatch::enum_dispatch;
use snafu::Snafu;
use trino_lb_core::{
    TrinoClusterName, TrinoLbQueryId, TrinoQueryId,
    trino_cluster::ClusterState,
    trino_query::{QueuedQuery, TrinoQuery},
};

pub mod in_memory;
pub mod postgres;
pub mod redis;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("In-memory persistence error"), context(false))]
    InMemoryError { source: in_memory::Error },

    #[snafu(display("Redis persistence error"), context(false))]
    RedisError { source: redis::Error },

    #[snafu(display("Postgres persistence error"), context(false))]
    PostgresError { source: postgres::Error },
}

/// Please note that the following functions *must* be atomic! trino-lb is build on the concept that you can deploy (and scale)
/// multiple replicas of trino-lb and every instance can answer requests for every query correctly. This is especially important
/// for increment and decrement operations to not end up with a wrong query count after multiple trino-lb instances modifying the
/// counter simultaneous.
#[enum_dispatch(PersistenceImplementation)]
// According to https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits.html
#[trait_variant::make(SendPersistence: Send)]
pub trait Persistence {
    async fn store_queued_query(&self, query: QueuedQuery) -> Result<(), Error>;
    async fn load_queued_query(&self, query_id: &TrinoLbQueryId) -> Result<QueuedQuery, Error>;
    async fn remove_queued_query(&self, query: &QueuedQuery) -> Result<(), Error>;

    async fn store_query(&self, query: TrinoQuery) -> Result<(), Error>;
    async fn load_query(&self, query_id: &TrinoQueryId) -> Result<TrinoQuery, Error>;
    async fn remove_query(&self, query_id: &TrinoQueryId) -> Result<(), Error>;

    /// `max_allowed_count` is the (inclusive) maximum count that is allowed *after* the increment.
    /// The returned boolean represents wether the increment has happened or was denied because
    /// the count would get to high.
    async fn inc_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
        max_allowed_count: u64,
    ) -> Result<bool, Error>;

    /// It is in the responsibility of the implementation to make sure the resulting counter is not less than zero.
    async fn dec_cluster_query_count(&self, cluster_name: &TrinoClusterName) -> Result<(), Error>;

    /// This function does not need to check for any transactional guarantees. Just set the passed value as fast as
    /// possible.
    async fn set_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
        count: u64,
    ) -> Result<(), Error>;
    async fn get_cluster_query_count(&self, cluster_name: &TrinoClusterName) -> Result<u64, Error>;

    /// Returns the number of queued queries in trino-lb for every cluster group.
    async fn get_queued_query_count(&self, cluster_group: &str) -> Result<u64, Error>;

    /// Deletes all queued queries that have not been accessed after the given timestamp using
    /// [`QueuedQuery::last_accessed`]. Returns the number of removed queued queries.
    async fn delete_queued_queries_not_accessed_after(
        &self,
        not_accessed_after: SystemTime,
    ) -> Result<u64, Error>;

    async fn get_last_query_count_fetcher_update(&self) -> Result<SystemTime, Error>;
    async fn set_last_query_count_fetcher_update(&self, update: SystemTime) -> Result<(), Error>;

    async fn set_cluster_state(
        &self,
        cluster_name: &TrinoClusterName,
        state: ClusterState,
    ) -> Result<(), Error>;

    async fn get_cluster_state(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<ClusterState, Error>;
}

#[enum_dispatch]
pub enum PersistenceImplementation {
    Redis(redis::RedisPersistence<::redis::aio::ConnectionManager>),
    RedisCluster(
        redis::RedisPersistence<
            ::redis::cluster_async::ClusterConnection<::redis::aio::MultiplexedConnection>,
        >,
    ),
    Postgres(postgres::PostgresPersistence),
    InMemory(in_memory::InMemoryPersistence),
}
