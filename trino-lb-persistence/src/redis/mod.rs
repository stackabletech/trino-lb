//! This Redis implementations work against Redis instances or clusters. It uses a single connection that is shared between all
//! operations for best performance. However, this makes atomic operations hard as their are some pitfalls regarding
//! `WATCH` in combination with `MULTI` and `EXEC` documented
//! [in this Stackoverflow answer](https://stackoverflow.com/a/68783183). In a nutshell the first exec unwatches all
//! properties. Therefore, the second multi/exec goes through without watch-guard. One mentioned solution was to use
//! multiple connections (obviously), but we can achieve our goals using LUA scripts that offer e.g. the compare-and-set
//! mechanism we need even when re-using a connection.
//!
use std::{fmt::Debug, num::TryFromIntError, time::SystemTimeError};

use ::redis::{RedisError, Script};
use snafu::Snafu;
use trino_lb_core::{TrinoClusterName, TrinoLbQueryId, TrinoQueryId};
use url::Url;

pub use redis_cluster::RedisClusterPersistence;
pub use singe_redis::RedisPersistence;

mod redis_cluster;
mod singe_redis;

const LAST_QUERY_COUNT_FETCHER_UPDATE_KEY: &str = "lastQueryCountFetcherUpdate";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to extract redis host from endpoint {endpoint}"))]
    ExtractRedisHost { endpoint: Url },

    #[snafu(display("Failed to create redis client"))]
    CreateClient { source: RedisError },

    #[snafu(display("Failed to serialize to binary representation"))]
    SerializeToBinary { source: bincode::Error },

    #[snafu(display("Failed to deserialize from binary representation"))]
    DeserializeFromBinary { source: bincode::Error },

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

    #[snafu(display("Failed to convert retrieved cluster query count {retrieved:?} to an u64 for cluster {cluster_name:?}"))]
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

    #[snafu(display("Failed to store determined elapsed time since last queryCountFetcher update as millis in a u64"))]
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
