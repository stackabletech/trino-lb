use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{future::try_join_all, TryFutureExt};
use redis::{aio::MultiplexedConnection, AsyncCommands, Client, Script};
use snafu::{OptionExt, ResultExt};
use tracing::{debug, debug_span, info, instrument, Instrument};
use trino_lb_core::{
    config::RedisConfig,
    trino_cluster::ClusterState,
    trino_query::{QueuedQuery, TrinoQuery},
    TrinoClusterName, TrinoLbQueryId, TrinoQueryId,
};

use crate::{
    redis::{
        cluster_query_counter_key, cluster_state_key, compare_and_set_script, query_key,
        queued_query_key, queued_query_set_name, ConvertElapsedTimeSinceLastUpdateToMillisSnafu,
        CreateClientSnafu, DeleteFromRedisSnafu, DeserializeFromBinarySnafu,
        DetermineElapsedTimeSinceLastUpdateSnafu, ExecuteCASScriptSnafu, ExtractRedisHostSnafu,
        GetClusterStateSnafu, GetLastQueryCountFetcherUpdateSnafu, InvalidCASScriptResponseSnafu,
        ReadClusterQueryCountSnafu, ReadFromRedisSnafu, SerializeToBinarySnafu,
        SetClusterQueryCountSnafu, SetClusterStateSnafu, SetLastQueryCountFetcherUpdateSnafu,
        WriteToRedisSnafu, LAST_QUERY_COUNT_FETCHER_UPDATE_KEY,
    },
    Persistence,
};

pub struct RedisPersistence {
    connection: MultiplexedConnection,
    compare_and_set_script: Script,

    /// Sometimes we need to do stuff for all cluster groups, so we need to store them to iterate over them
    cluster_groups: Vec<String>,
}

impl RedisPersistence {
    pub async fn new(
        config: &RedisConfig,
        cluster_groups: Vec<String>,
    ) -> Result<Self, super::Error> {
        // Logging only the host, as the endpoint can contain credentials
        let redis_host = config.endpoint.host_str().context(ExtractRedisHostSnafu {
            endpoint: config.endpoint.clone(),
        })?;
        info!(redis_host, "Using redis cluster persistence");

        let client = Client::open(config.endpoint.as_str()).context(CreateClientSnafu)?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .context(CreateClientSnafu)?;

        Ok(Self {
            connection,
            compare_and_set_script: compare_and_set_script(),
            cluster_groups,
        })
    }
}

impl Persistence for RedisPersistence {
    #[instrument(skip(self))]
    async fn store_queued_query(&self, queued_query: QueuedQuery) -> Result<(), crate::Error> {
        let key = queued_query_key(&queued_query.id);
        let value = bincode::serialize(&queued_query).context(SerializeToBinarySnafu)?;

        let mut connection_1 = self.connection();
        let mut connection_2 = self.connection();

        // We can't use a pipe here, as we otherwise get "Received crossed slots in pipeline - CrossSlot"
        tokio::try_join!(
            connection_1
                .set::<_, _, ()>(key, value)
                .map_err(|err| super::Error::WriteToRedis { source: err }),
            connection_2
                .sadd::<_, _, ()>(queued_query_set_name(&queued_query.cluster_group), key)
                .map_err(|err| super::Error::WriteToRedis { source: err }),
        )?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_queued_query(
        &self,
        queued_query_id: &TrinoLbQueryId,
    ) -> Result<QueuedQuery, crate::Error> {
        let key = queued_query_key(queued_query_id);
        let value: Vec<u8> = self
            .connection()
            .get(key)
            .await
            .context(ReadFromRedisSnafu)?;

        Ok(bincode::deserialize(&value).context(DeserializeFromBinarySnafu)?)
    }

    #[instrument(skip(self))]
    async fn remove_queued_query(&self, queued_query: &QueuedQuery) -> Result<(), crate::Error> {
        let key = queued_query_key(&queued_query.id);
        let mut connection = self.connection();

        // We can't use a pipe here, as we otherwise get "Received crossed slots in pipeline - CrossSlot"
        connection
            .srem(queued_query_set_name(&queued_query.cluster_group), key)
            .await
            .context(WriteToRedisSnafu)?;
        connection.del(key).await.context(WriteToRedisSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn store_query(&self, query: TrinoQuery) -> Result<(), crate::Error> {
        let key = query_key(&query.id);
        let value = bincode::serialize(&query).context(SerializeToBinarySnafu)?;

        self.connection()
            .set(key, value)
            .await
            .context(WriteToRedisSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_query(&self, query_id: &TrinoQueryId) -> Result<TrinoQuery, crate::Error> {
        let key = query_key(query_id);
        let value: Vec<u8> = self
            .connection()
            .get(key)
            .await
            .context(ReadFromRedisSnafu)?;

        Ok(bincode::deserialize(&value).context(DeserializeFromBinarySnafu)?)
    }

    #[instrument(skip(self))]
    async fn remove_query(&self, query_id: &TrinoQueryId) -> Result<(), crate::Error> {
        let key = query_key(query_id);
        self.connection()
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
    ) -> Result<bool, crate::Error> {
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
                debug!(current, max_allowed_count,
                    "Rejected increasing the cluster query count, as the current count + 1 is bigger than the max allowed count");
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
    ) -> Result<(), crate::Error> {
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
    ) -> Result<(), crate::Error> {
        let key = cluster_query_counter_key(cluster_name);

        self.connection()
            .set(key, count)
            .await
            .context(SetClusterQueryCountSnafu { cluster_name })?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<u64, crate::Error> {
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
    async fn get_queued_query_count(&self, cluster_group: &str) -> Result<u64, crate::Error> {
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
    ) -> Result<u64, crate::Error> {
        let counts = try_join_all(self.cluster_groups.iter().map(|cg| {
            self.delete_queued_queries_not_accessed_after_for_cluster_group(cg, &not_accessed_after)
        }))
        .await?;

        Ok(counts.iter().sum())
    }

    #[instrument(skip(self))]
    async fn get_last_query_count_fetcher_update(&self) -> Result<SystemTime, crate::Error> {
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
    ) -> Result<(), crate::Error> {
        let ms: u64 = update
            .duration_since(UNIX_EPOCH)
            .context(DetermineElapsedTimeSinceLastUpdateSnafu)?
            .as_millis()
            .try_into()
            .context(ConvertElapsedTimeSinceLastUpdateToMillisSnafu)?;

        self.connection()
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
    ) -> Result<(), crate::Error> {
        let key = cluster_state_key(cluster_name);
        let value = bincode::serialize(&state).context(SerializeToBinarySnafu)?;

        self.connection()
            .set(key, value)
            .await
            .context(SetClusterStateSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_cluster_state(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<ClusterState, crate::Error> {
        let key = cluster_state_key(cluster_name);

        let cluster_state: Option<Vec<u8>> = self
            .connection()
            .get(key)
            .await
            .context(GetClusterStateSnafu)?;

        Ok(match cluster_state {
            Some(cluster_state) => {
                bincode::deserialize(&cluster_state).context(DeserializeFromBinarySnafu)?
            }
            None => ClusterState::Unknown,
        })
    }
}

impl RedisPersistence {
    fn connection(&self) -> MultiplexedConnection {
        self.connection.clone()
    }

    #[instrument(skip(self))]
    async fn delete_queued_queries_not_accessed_after_for_cluster_group(
        &self,
        cluster_group: &str,
        not_accessed_after: &SystemTime,
    ) -> Result<u64, crate::Error> {
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
