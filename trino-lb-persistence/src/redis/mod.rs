use std::{
    fmt::Debug,
    num::TryFromIntError,
    sync::{Arc, PoisonError, RwLock},
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

use futures::{
    TryFutureExt,
    future::{BoxFuture, try_join_all},
};
use redis::{
    AsyncCommands, Client, IntoConnectionInfo, RedisError, Script,
    aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    cluster::{ClusterClientBuilder, ClusterConfig},
    cluster_async::ClusterConnection,
    io::tcp::{TcpSettings, socket2::TcpKeepalive},
};
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{Instrument, debug, debug_span, info, instrument, warn};
use trino_lb_core::{
    TrinoClusterName, TrinoLbQueryId, TrinoQueryId,
    config::RedisConfig,
    trino_cluster::ClusterState,
    trino_query::{QueuedQuery, TrinoQuery},
};
use url::Url;

use crate::Persistence;

const REDIS_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const REDIS_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

// TCP keepalive / user-timeout settings for the Redis socket.
//
// The redis crate's [`ConnectionManager`] only reconnects when a command fails with a
// dropped-connection error (broken pipe, connection reset, ...). When the Kubernetes node running
// the Redis master is drained, the established TCP connection turns into a black hole: packets are
// silently dropped and no FIN/RST ever arrives, so the only error trino-lb ever sees is a response
// *timeout* - which the redis crate does not treat as a reason to reconnect. Without these socket
// options the connection would stay wedged until the liveness probe restarts the pod (see issue
// #109). Enabling TCP keepalive (and TCP_USER_TIMEOUT) lets the kernel tear the dead socket down on
// its own, shrinking the detection window; the [`Reconnectable`] wrapper then rebuilds it.
const REDIS_TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(5);
const REDIS_TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(2);
const REDIS_TCP_KEEPALIVE_RETRIES: u32 = 3;
const REDIS_TCP_USER_TIMEOUT: Duration = Duration::from_secs(12);

/// How often the background health check pings Redis to detect a black-holed connection (see the
/// note on [`REDIS_TCP_KEEPALIVE_TIME`]) and rebuild it via [`Reconnectable`]. Such a connection
/// only ever produces response *timeouts*, which the redis crate does not treat as a reason to
/// reconnect, so without this proactive check it would stay wedged until the liveness probe
/// restarts the pod (see issue #109).
const REDIS_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// TCP settings applied to every Redis connection (and every reconnect), see the constants above.
fn tcp_settings() -> TcpSettings {
    TcpSettings::default()
        .set_keepalive(
            TcpKeepalive::new()
                .with_time(REDIS_TCP_KEEPALIVE_TIME)
                .with_interval(REDIS_TCP_KEEPALIVE_INTERVAL)
                .with_retries(REDIS_TCP_KEEPALIVE_RETRIES),
        )
        .set_user_timeout(REDIS_TCP_USER_TIMEOUT)
}

const LAST_QUERY_COUNT_FETCHER_UPDATE_KEY: &str = "lastQueryCountFetcherUpdate";

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to extract redis host from endpoint {endpoint}"))]
    ExtractRedisHost { endpoint: Url },

    #[snafu(display("Failed to create redis client"))]
    CreateClient { source: RedisError },

    #[snafu(display("Failed to serialize to binary representation"))]
    SerializeToBinary { source: bincode::error::EncodeError },

    #[snafu(display("Failed to deserialize from binary representation"))]
    DeserializeFromBinary { source: bincode::error::DecodeError },

    #[snafu(display("Failed to write to redis"))]
    WriteToRedis { source: RedisError },

    #[snafu(display("Failed to read from redis"))]
    ReadFromRedis { source: RedisError },

    #[snafu(display("Failed to delete from redis"))]
    DeleteFromRedis { source: RedisError },

    #[snafu(display(
        "Failed to increment cluster query count for cluster {cluster_name:?} in redis"
    ))]
    IncrementClusterQueryCount {
        source: RedisError,
        cluster_name: TrinoClusterName,
    },

    #[snafu(display(
        "Failed to decrement cluster query count for cluster {cluster_name:?} in redis"
    ))]
    DecrementClusterQueryCount {
        source: RedisError,
        cluster_name: TrinoClusterName,
    },

    #[snafu(display("Failed to set cluster query count for cluster {cluster_name:?} in redis"))]
    SetClusterQueryCount {
        source: RedisError,
        cluster_name: TrinoClusterName,
    },

    #[snafu(display("Failed to read cluster query count for cluster {cluster_name:?} in redis"))]
    ReadClusterQueryCount {
        source: RedisError,
        cluster_name: TrinoClusterName,
    },

    #[snafu(display(
        "Failed to convert retrieved cluster query count {retrieved:?} to an u64 for cluster {cluster_name:?}"
    ))]
    ConvertClusterQueryCountToU64 {
        source: TryFromIntError,
        cluster_name: TrinoClusterName,
        retrieved: Option<i64>,
    },

    #[snafu(display("Failed to get last cluster query count fetcher update timestamp"))]
    GetLastQueryCountFetcherUpdate { source: RedisError },

    #[snafu(display("Failed to set last cluster query count fetcher update timestamp"))]
    SetLastQueryCountFetcherUpdate { source: RedisError },

    #[snafu(display("Failed to determined elapsed time since last queryCountFetcher update"))]
    DetermineElapsedTimeSinceLastUpdate { source: SystemTimeError },

    #[snafu(display(
        "Failed to store determined elapsed time since last queryCountFetcher update as millis in a u64"
    ))]
    ConvertElapsedTimeSinceLastUpdateToMillis { source: TryFromIntError },

    #[snafu(display("Failed to set cluster state"))]
    SetClusterState { source: RedisError },

    #[snafu(display("Failed to get cluster state"))]
    GetClusterState { source: RedisError },

    #[snafu(display("Failed to execute compare and set lua script."))]
    ExecuteCASScript { source: RedisError },

    #[snafu(display("Invalid response from compare and set lua script. Expected either 0 or 1"))]
    InvalidCASScriptResponse { response: u64 },

    #[snafu(display("Failed to get queued query count for cluster group {cluster_group:?}"))]
    GetQueuedQueryCount {
        source: RedisError,
        cluster_group: String,
    },

    #[snafu(display("Failed to list queued queries for cluster group {cluster_group:?}"))]
    ListQueuedQueries {
        source: RedisError,
        cluster_group: String,
    },
}

/// Builds a fresh Redis connection. Used by [`Reconnectable`] to rebuild a connection that went bad.
type ConnectionFactory<T> =
    Arc<dyn Fn() -> BoxFuture<'static, Result<T, RedisError>> + Send + Sync>;

/// A Redis connection that can be transparently rebuilt when it goes bad.
///
/// The redis crate's [`ConnectionManager`] only reconnects on dropped-connection errors, not on
/// timeouts, so a black-holed connection (e.g. after a Redis node drain, see issue #109 and the
/// note on [`REDIS_TCP_KEEPALIVE_TIME`]) would otherwise time out forever. This wrapper lets us
/// rebuild the underlying connection ourselves; the background health check in
/// [`RedisPersistence::spawn_health_check`] decides *when* to rebuild.
struct Reconnectable<T> {
    current: RwLock<Arc<T>>,
    factory: ConnectionFactory<T>,
    /// Guarantees only a single rebuild runs at a time.
    reconnecting: tokio::sync::Mutex<()>,
}

impl<T> Reconnectable<T>
where
    T: Send + Sync + 'static,
{
    fn new(initial: T, factory: ConnectionFactory<T>) -> Self {
        Self {
            current: RwLock::new(Arc::new(initial)),
            factory,
            reconnecting: tokio::sync::Mutex::new(()),
        }
    }

    /// Returns the connection currently in use.
    fn current(&self) -> Arc<T> {
        Arc::clone(&self.current.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Rebuilds the connection, replacing the one currently in use.
    ///
    /// `used` is the connection that was found to be bad. This is a no-op if another rebuild is
    /// already in progress, or if the connection has already been replaced since `used` was
    /// obtained (i.e. another rebuild already happened). This keeps overlapping failure reports
    /// from rebuilding the connection over and over.
    async fn reconnect(&self, used: &Arc<T>) {
        let Ok(_guard) = self.reconnecting.try_lock() else {
            // Another task is already rebuilding the connection.
            return;
        };

        if !Arc::ptr_eq(&self.current(), used) {
            // The connection was already replaced while we waited for the lock.
            return;
        }

        match (self.factory)().await {
            Ok(connection) => {
                *self.current.write().unwrap_or_else(PoisonError::into_inner) =
                    Arc::new(connection);
                info!("Successfully rebuilt the Redis connection");
            }
            Err(error) => {
                warn!(
                    ?error,
                    "Failed to rebuild the Redis connection, will retry on the next health check"
                );
            }
        }
    }
}

/// This Redis implementation works against Redis clusters. It uses a single connection that is shared between all
/// operations for best performance. However, this makes atomic operations hard as their are some pitfalls regarding
/// `WATCH` in combination with `MULTI` and `EXEC` documented
/// [in this Stackoverflow answer](https://stackoverflow.com/a/68783183). In a nutshell The first exec unwatches all
/// properties. Therefore, the second multi/exec goes through without watch-guard. One mentioned solution was to use
/// multiple connections (obviously), but we can achieve our goals using LUA scripts that offer e.g. the compare-and-set
/// mechanism we need even when re-using a connection.
pub struct RedisPersistence<R>
where
    R: AsyncCommands + Clone,
{
    connection: Arc<Reconnectable<R>>,
    compare_and_set_script: Script,

    /// Sometimes we need to do stuff for all cluster groups, so we need to store them to iterate over them
    cluster_groups: Vec<String>,
}

impl RedisPersistence<ConnectionManager> {
    pub async fn new(config: &RedisConfig, cluster_groups: Vec<String>) -> Result<Self, Error> {
        let redis_host = config.endpoint.host_str().context(ExtractRedisHostSnafu {
            endpoint: config.endpoint.clone(),
        })?;
        info!(redis_host, "Using redis persistence");

        let redis_config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(REDIS_CONNECTION_TIMEOUT))
            .set_response_timeout(Some(REDIS_RESPONSE_TIMEOUT));

        // The TCP settings live on the `ConnectionInfo` (not on `ConnectionManagerConfig`, which
        // does not expose them), and are re-applied on every reconnect the manager performs.
        let connection_info = config
            .endpoint
            .as_str()
            .into_connection_info()
            .context(CreateClientSnafu)?
            .set_tcp_settings(tcp_settings());
        let client = Client::open(connection_info).context(CreateClientSnafu)?;

        let factory: ConnectionFactory<ConnectionManager> = {
            let client = client.clone();
            Arc::new(move || {
                let client = client.clone();
                let redis_config = redis_config.clone();
                Box::pin(async move {
                    client
                        .get_connection_manager_with_config(redis_config)
                        .await
                })
            })
        };

        let connection = factory().await.context(CreateClientSnafu)?;

        Ok(Self::new_with_connection(
            connection,
            factory,
            cluster_groups,
        ))
    }
}

impl RedisPersistence<ClusterConnection<MultiplexedConnection>> {
    pub async fn new(config: &RedisConfig, cluster_groups: Vec<String>) -> Result<Self, Error> {
        let redis_host = config.endpoint.host_str().context(ExtractRedisHostSnafu {
            endpoint: config.endpoint.clone(),
        })?;
        info!(redis_host, "Using redis cluster persistence");

        let redis_config = ClusterConfig::new()
            .set_connection_timeout(REDIS_CONNECTION_TIMEOUT)
            .set_response_timeout(REDIS_RESPONSE_TIMEOUT);

        let client = ClusterClientBuilder::new([config.endpoint.as_str()])
            .tcp_settings(tcp_settings())
            .build()
            .context(CreateClientSnafu)?;

        let factory: ConnectionFactory<ClusterConnection<MultiplexedConnection>> = {
            let client = client.clone();
            Arc::new(move || {
                let client = client.clone();
                let redis_config = redis_config.clone();
                Box::pin(async move { client.get_async_connection_with_config(redis_config).await })
            })
        };

        let connection = factory().await.context(CreateClientSnafu)?;

        Ok(Self::new_with_connection(
            connection,
            factory,
            cluster_groups,
        ))
    }
}

impl<R> Persistence for RedisPersistence<R>
where
    R: AsyncCommands + Clone + Send + Sync + 'static,
{
    #[instrument(skip(self, queued_query))]
    async fn store_queued_query(&self, queued_query: QueuedQuery) -> Result<(), super::Error> {
        let key = queued_query_key(&queued_query.id);

        let value = bincode::serde::encode_to_vec(&queued_query, BINCODE_CONFIG)
            .context(SerializeToBinarySnafu)?;

        let mut connection_1 = self.connection();
        let mut connection_2 = self.connection();

        // We can't use a pipe here, as we otherwise get "Received crossed slots in pipeline - CrossSlot"
        tokio::try_join!(
            connection_1
                .set::<_, _, ()>(key, value)
                .map_err(|err| Error::WriteToRedis { source: err }),
            connection_2
                .sadd::<_, _, ()>(queued_query_set_name(&queued_query.cluster_group), key)
                .map_err(|err| Error::WriteToRedis { source: err }),
        )?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_queued_query(
        &self,
        queued_query_id: &TrinoLbQueryId,
    ) -> Result<QueuedQuery, super::Error> {
        let key = queued_query_key(queued_query_id);
        let value: Vec<u8> = self
            .connection()
            .get(key)
            .await
            .context(ReadFromRedisSnafu)?;

        Ok(bincode::serde::decode_from_slice(&value, BINCODE_CONFIG)
            .context(DeserializeFromBinarySnafu)?
            .0)
    }

    #[instrument(skip(self, queued_query))]
    async fn remove_queued_query(&self, queued_query: &QueuedQuery) -> Result<(), super::Error> {
        let key = queued_query_key(&queued_query.id);
        let mut connection = self.connection();

        // We can't use a pipe here, as we otherwise get "Received crossed slots in pipeline - CrossSlot"
        let _: () = connection
            .srem(queued_query_set_name(&queued_query.cluster_group), key)
            .await
            .context(WriteToRedisSnafu)?;
        let _: () = connection.del(key).await.context(WriteToRedisSnafu)?;

        Ok(())
    }

    #[instrument(skip(self, query))]
    async fn store_query(&self, query: TrinoQuery) -> Result<(), super::Error> {
        let key = query_key(&query.id);
        let value = bincode::serde::encode_to_vec(&query, BINCODE_CONFIG)
            .context(SerializeToBinarySnafu)?;

        let _: () = self
            .connection()
            .set(key, value)
            .await
            .context(WriteToRedisSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_query(&self, query_id: &TrinoQueryId) -> Result<TrinoQuery, super::Error> {
        let key = query_key(query_id);
        let value: Vec<u8> = self
            .connection()
            .get(key)
            .await
            .context(ReadFromRedisSnafu)?;

        Ok(bincode::serde::decode_from_slice(&value, BINCODE_CONFIG)
            .context(DeserializeFromBinarySnafu)?
            .0)
    }

    #[instrument(skip(self))]
    async fn remove_query(&self, query_id: &TrinoQueryId) -> Result<(), super::Error> {
        let key = query_key(query_id);
        let _: () = self
            .connection()
            .del(key)
            .await
            .context(DeleteFromRedisSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn inc_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
        max_allowed_count: u64,
    ) -> Result<bool, super::Error> {
        let key = cluster_query_counter_key(cluster_name);
        let mut connection = self.connection();

        loop {
            let current = connection
                .get::<_, Option<u64>>(&key)
                .instrument(debug_span!("get current value"))
                .await
                .context(ReadFromRedisSnafu)?
                .unwrap_or_default();

            debug!(current, "Current counter is");

            if current + 1 > max_allowed_count {
                debug!(
                    current,
                    max_allowed_count,
                    "Rejected increasing the cluster query count, as the current count + 1 is bigger than the max allowed count"
                );
                return Ok(false);
            }

            let response: u8 = self
                .compare_and_set_script
                .key(&key)
                .arg(current)
                .arg(current + 1)
                .invoke_async(&mut connection)
                .instrument(debug_span!("invoking compare-and-set lua script"))
                .await
                .context(ExecuteCASScriptSnafu)?;

            match response {
                0 => {
                    continue;
                }
                1 => {
                    return Ok(true);
                }
                _ => InvalidCASScriptResponseSnafu { response }.fail()?,
            }
        }
    }

    /// This function is pretty complicated, as it only decrements the counter in case it is above zero.
    /// This way we ensure we don't end up with a negative counter in redis and the read path failing with
    ///
    /// WARN Error while processing request error=FindBestClusterForClusterGroup { source: GetClusterQueryCounter { source: RedisError { source: ReadClusterQueryCount { source: Response was of incompatible type - TypeError: "Could not convert from string." (response was string-data('"-1"')), cluster_name: "trino-m-1" } }, cluster_group: "m" }, cluster_group: "m" }
    #[instrument(skip(self))]
    async fn dec_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<(), super::Error> {
        let key = cluster_query_counter_key(cluster_name);
        let mut connection = self.connection();

        loop {
            let current = connection
                .get::<_, Option<u64>>(&key)
                .instrument(debug_span!("get current value"))
                .await
                .context(ReadFromRedisSnafu)?
                .unwrap_or_default();

            if current == 0 {
                debug!("Current value was already 0, nothing to do here");
                return Ok(());
            }

            let response: u8 = self
                .compare_and_set_script
                .key(&key)
                .arg(current)
                .arg(current - 1)
                .invoke_async(&mut connection)
                .instrument(debug_span!("invoking compare-and-set lua script"))
                .await
                .context(ExecuteCASScriptSnafu)?;

            match response {
                0 => {
                    continue;
                }
                1 => {
                    return Ok(());
                }
                _ => InvalidCASScriptResponseSnafu { response }.fail()?,
            }
        }
    }

    #[instrument(skip(self))]
    async fn set_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
        count: u64,
    ) -> Result<(), super::Error> {
        let key = cluster_query_counter_key(cluster_name);

        let _: () = self
            .connection()
            .set(key, count)
            .await
            .context(SetClusterQueryCountSnafu { cluster_name })?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<u64, super::Error> {
        let key = cluster_query_counter_key(cluster_name);
        Ok(self
            .connection()
            .get::<_, Option<u64>>(key)
            .await
            .context(ReadClusterQueryCountSnafu { cluster_name })?
            // There can be the case this function is called before `inc_cluster_queries`, so the number of queries is 0 in this case.
            .unwrap_or_default())
    }

    #[instrument(skip(self))]
    async fn get_queued_query_count(&self, cluster_group: &str) -> Result<u64, super::Error> {
        Ok(self
            .connection()
            .scard::<_, Option<u64>>(queued_query_set_name(cluster_group))
            .await
            .context(GetQueuedQueryCountSnafu { cluster_group })?
            // The set might not be there yet, as no queries have been queued for this cluster group so far.
            .unwrap_or_default())
    }

    #[instrument(skip(self))]
    async fn delete_queued_queries_not_accessed_after(
        &self,
        not_accessed_after: SystemTime,
    ) -> Result<u64, super::Error> {
        let counts = try_join_all(self.cluster_groups.iter().map(|cg| {
            self.delete_queued_queries_not_accessed_after_for_cluster_group(cg, &not_accessed_after)
        }))
        .await?;

        Ok(counts.iter().sum())
    }

    #[instrument(skip(self))]
    async fn get_last_query_count_fetcher_update(&self) -> Result<SystemTime, super::Error> {
        let ms = self
            .connection()
            .get::<_, Option<u64>>(LAST_QUERY_COUNT_FETCHER_UPDATE_KEY)
            .await
            .context(GetLastQueryCountFetcherUpdateSnafu)?
            // There can be the case this function is called before `set_last_query_count_fetcher_update`, so we can
            // safely return 1970-01-01 here,
            .unwrap_or_default();

        // TODO: Check for overflows
        Ok(UNIX_EPOCH + Duration::from_millis(ms))
    }

    #[instrument(skip(self))]
    async fn set_last_query_count_fetcher_update(
        &self,
        update: SystemTime,
    ) -> Result<(), super::Error> {
        let ms: u64 = update
            .duration_since(UNIX_EPOCH)
            .context(DetermineElapsedTimeSinceLastUpdateSnafu)?
            .as_millis()
            .try_into()
            .context(ConvertElapsedTimeSinceLastUpdateToMillisSnafu)?;

        let _: () = self
            .connection()
            .set(LAST_QUERY_COUNT_FETCHER_UPDATE_KEY, ms)
            .await
            .context(SetLastQueryCountFetcherUpdateSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn set_cluster_state(
        &self,
        cluster_name: &TrinoClusterName,
        state: ClusterState,
    ) -> Result<(), super::Error> {
        let key = cluster_state_key(cluster_name);
        let value =
            bincode::serde::encode_to_vec(state, BINCODE_CONFIG).context(SerializeToBinarySnafu)?;

        let _: () = self
            .connection()
            .set(key, value)
            .await
            .context(SetClusterStateSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_cluster_state(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<ClusterState, super::Error> {
        let key = cluster_state_key(cluster_name);

        let cluster_state: Option<Vec<u8>> = self
            .connection()
            .get(key)
            .await
            .context(GetClusterStateSnafu)?;

        Ok(match cluster_state {
            Some(cluster_state) => {
                bincode::serde::decode_from_slice(&cluster_state, BINCODE_CONFIG)
                    .context(DeserializeFromBinarySnafu)?
                    .0
            }
            None => ClusterState::Unknown,
        })
    }
}

impl<R> RedisPersistence<R>
where
    R: AsyncCommands + Clone + Send + Sync + 'static,
{
    /// Wraps the initial connection in a [`Reconnectable`] and starts the background health check
    /// that rebuilds it when it goes bad.
    fn new_with_connection(
        connection: R,
        factory: ConnectionFactory<R>,
        cluster_groups: Vec<String>,
    ) -> Self {
        let connection = Arc::new(Reconnectable::new(connection, factory));
        Self::spawn_health_check(Arc::clone(&connection));

        Self {
            connection,
            compare_and_set_script: compare_and_set_script(),
            cluster_groups,
        }
    }

    fn connection(&self) -> R {
        (*self.connection.current()).clone()
    }

    /// Spawns a background task that periodically pings Redis and rebuilds the connection if the
    /// ping fails.
    ///
    /// The redis crate's [`ConnectionManager`] reconnects on dropped-connection errors but not on
    /// timeouts, so a black-holed connection (see the note on [`REDIS_TCP_KEEPALIVE_TIME`]) would
    /// otherwise time out on every command indefinitely. A periodic ping detects this - and heals
    /// it even while there is no other traffic - without having to wrap every individual operation.
    /// [`Reconnectable::reconnect`] makes sure we only rebuild once per dead connection.
    fn spawn_health_check(connection: Arc<Reconnectable<R>>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(REDIS_HEALTH_CHECK_INTERVAL).await;

                let current = connection.current();
                let mut ping_connection = (*current).clone();
                // The connection carries its own response timeout, so a black-holed socket makes
                // this `PING` fail rather than hang forever.
                let ping: Result<(), RedisError> =
                    redis::cmd("PING").query_async(&mut ping_connection).await;

                if let Err(error) = ping {
                    warn!(
                        ?error,
                        "Redis health check failed, rebuilding the connection"
                    );
                    connection.reconnect(&current).await;
                }
            }
        });
    }

    #[instrument(skip(self))]
    async fn delete_queued_queries_not_accessed_after_for_cluster_group(
        &self,
        cluster_group: &str,
        not_accessed_after: &SystemTime,
    ) -> Result<u64, super::Error> {
        let mut connection = self.connection();
        let mut removed = 0;

        if let Ok(mut queued) = connection.sscan(queued_query_set_name(cluster_group)).await {
            // TODO: Await `load_queued_query` in parallel (if possible) or add them to a Vec to bulk-delete afterwards
            while let Some(key) = queued.next_item().await {
                let key = key.with_context(|_| ListQueuedQueriesSnafu { cluster_group })?;
                let queued_query = self.load_queued_query(&key).await?;
                if &queued_query.last_accessed < not_accessed_after {
                    self.remove_queued_query(&queued_query).await?;
                    removed += 1;
                }
            }
        }

        info!(
            cluster_group,
            removed,
            ?not_accessed_after,
            "Deleted all queries that were not accessed after"
        );

        Ok(removed)
    }
}

/// Trino query ids will always start with `20231208` and will therefore be unique.
fn query_key(query_id: &TrinoQueryId) -> &str {
    query_id
}

/// trino-lb query ids will always start with `trino_lb_20231208` and will therefore be unique.
fn queued_query_key(query_id: &TrinoLbQueryId) -> &str {
    query_id
}

fn queued_query_set_name(cluster_group: &str) -> String {
    format!("queued-{cluster_group}")
}

fn cluster_query_counter_key(cluster: &TrinoClusterName) -> String {
    format!("{cluster}_query_count")
}

fn cluster_state_key(cluster: &TrinoClusterName) -> String {
    format!("{cluster}_state")
}

fn compare_and_set_script() -> Script {
    Script::new(
        r"
    local current = redis.call('GET', KEYS[1]);
    if current == ARGV[1] then
        redis.call('SET', KEYS[1], ARGV[2]);
        return 1;
        end;
    -- Special case: The entry did not exist so far, so just set it
    if redis.call('EXISTS', KEYS[1]) == 0 then
        redis.call('SET', KEYS[1], ARGV[2]);
        return 1;
        end;
    return 0;
    ",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    /// Returns a [`Reconnectable`] over a `u32` plus a counter of how many times it was rebuilt.
    /// Every rebuild yields once (so concurrent rebuild attempts interleave deterministically) and
    /// then returns the new rebuild count as the connection value.
    fn counting_reconnectable() -> (Arc<AtomicU32>, Reconnectable<u32>) {
        let rebuilds = Arc::new(AtomicU32::new(0));

        let factory_rebuilds = Arc::clone(&rebuilds);
        let factory: ConnectionFactory<u32> = Arc::new(move || {
            let rebuilds = Arc::clone(&factory_rebuilds);
            Box::pin(async move {
                tokio::task::yield_now().await;
                Ok(rebuilds.fetch_add(1, Ordering::SeqCst) + 1)
            })
        });

        (rebuilds, Reconnectable::new(0, factory))
    }

    #[tokio::test]
    async fn reconnect_replaces_the_connection() {
        let (rebuilds, reconnectable) = counting_reconnectable();

        let used = reconnectable.current();
        assert_eq!(*used, 0);

        reconnectable.reconnect(&used).await;

        assert_eq!(rebuilds.load(Ordering::SeqCst), 1);
        assert_eq!(*reconnectable.current(), 1);
    }

    #[tokio::test]
    async fn reconnect_is_a_noop_for_an_already_replaced_connection() {
        let (rebuilds, reconnectable) = counting_reconnectable();

        let stale = reconnectable.current();
        reconnectable.reconnect(&stale).await;
        assert_eq!(*reconnectable.current(), 1);

        // The handle is now stale (the connection was replaced); reconnecting with it must not
        // rebuild again, otherwise repeated failure reports would rebuild over and over.
        reconnectable.reconnect(&stale).await;
        assert_eq!(rebuilds.load(Ordering::SeqCst), 1);
        assert_eq!(*reconnectable.current(), 1);
    }

    #[tokio::test]
    async fn concurrent_reconnects_rebuild_only_once() {
        let (rebuilds, reconnectable) = counting_reconnectable();
        let reconnectable = Arc::new(reconnectable);

        let used = reconnectable.current();
        let attempts = (0..8).map(|_| {
            let reconnectable = Arc::clone(&reconnectable);
            let used = Arc::clone(&used);
            async move { reconnectable.reconnect(&used).await }
        });
        futures::future::join_all(attempts).await;

        // Single-flight: many concurrent failures on the same connection rebuild it exactly once.
        assert_eq!(rebuilds.load(Ordering::SeqCst), 1);
        assert_eq!(*reconnectable.current(), 1);
    }
}
