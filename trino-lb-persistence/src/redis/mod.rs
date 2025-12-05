use std::{
    fmt::Debug,
    num::TryFromIntError,
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

use futures::{TryFutureExt, future::try_join_all};
use redis::{
    AsyncCommands, Client, RedisError, Script,
    aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    cluster::{ClusterClientBuilder, ClusterConfig},
    cluster_async::ClusterConnection,
};
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{Instrument, debug, debug_span, info, instrument};
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
    connection: R,
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
            .set_connection_timeout(REDIS_CONNECTION_TIMEOUT)
            .set_response_timeout(REDIS_RESPONSE_TIMEOUT);

        let client = Client::open(config.endpoint.as_str()).context(CreateClientSnafu)?;
        let connection = client
            .get_connection_manager_with_config(redis_config)
            .await
            .context(CreateClientSnafu)?;

        Ok(Self {
            connection,
            compare_and_set_script: compare_and_set_script(),
            cluster_groups,
        })
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
            .build()
            .context(CreateClientSnafu)?;
        let connection = client
            .get_async_connection_with_config(redis_config)
            .await
            .context(CreateClientSnafu)?;

        Ok(Self {
            connection,
            compare_and_set_script: compare_and_set_script(),
            cluster_groups,
        })
    }
}

impl<R> Persistence for RedisPersistence<R>
where
    R: AsyncCommands + Clone,
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
            .unwrap()
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
    R: AsyncCommands + Clone,
{
    fn connection(&self) -> R {
        self.connection.clone()
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
