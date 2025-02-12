use std::{fmt::Debug, time::SystemTime};

use chrono::{DateTime, Utc};
use rand::distr::{Alphanumeric, SampleString};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use url::Url;

use crate::{sanitization::Sanitize, TrinoClusterName, TrinoLbQueryId, TrinoQueryId};

pub const QUEUED_QUERY_ID_PREFIX: &str = "trino_lb_";

/// A query that is queued in trino-lb.
/// It does *not* track on which cluster it is queued, as the assignment to an actual.
/// Trino cluster happens as late as possible. Instead, it contains the needed info to
/// make a decision later on.
#[derive(Clone, Serialize, Deserialize)]
pub struct QueuedQuery {
    /// The id of the query trino-lb made up till we get the correct one from Trino.
    pub id: TrinoLbQueryId,

    /// The actual query we need to post to Trino.
    pub query: String,

    /// We need to store the headers, so that we can pass them along the POST
    /// we send to Trino once the query is unqueued.
    #[serde(with = "http_serde::header_map")]
    pub headers: http::HeaderMap,

    /// The time the query was submitted to trino-lb.
    pub creation_time: SystemTime,

    /// Last time the Trino client polled for an update. Please note that this field does not need to be updated every
    /// time the client ask for an update, as this would cause unnecessary calls to `store_queued_query` in the
    /// persistence.
    pub last_accessed: SystemTime,

    /// The target group the `trino_lb::routing::Router` has determined for this query.
    pub cluster_group: String,
}

/// A query that was already submitted to a Trino cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrinoQuery {
    /// id of the query. In case it is queued in trino-lb it's a id trino-lb made up,
    /// which will change to a different id once send to an actual Trino cluster.
    pub id: TrinoQueryId,

    /// Name of the trino cluster the query is running on.
    pub trino_cluster: TrinoClusterName,

    /// Endpoint of the Trino cluster the query is running on.
    pub trino_endpoint: Url,

    /// The time the query was submitted to trino-lb.
    pub creation_time: SystemTime,

    /// The time the query was send to Trino
    pub delivered_time: SystemTime,
}

impl QueuedQuery {
    pub fn new_from(query: String, headers: http::HeaderMap, cluster_group: String) -> Self {
        let query_id = new_query_id();
        let now = SystemTime::now();

        Self {
            id: query_id,
            query,
            headers,
            creation_time: now,
            last_accessed: now,
            cluster_group,
        }
    }
}

impl TrinoQuery {
    pub fn new_from(
        trino_cluster: TrinoClusterName,
        trino_query_id: TrinoQueryId,
        trino_endpoint: Url,
        creation_time: SystemTime,
        delivered_time: SystemTime,
    ) -> Self {
        TrinoQuery {
            id: trino_query_id,
            trino_cluster,
            trino_endpoint,
            creation_time,
            delivered_time,
        }
    }
}

impl Debug for QueuedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuedQuery")
            .field("id", &self.id)
            .field("query", &self.query)
            .field("headers", &self.headers.sanitize())
            .field("creation_time", &self.creation_time)
            .field("cluster_group", &self.cluster_group)
            .finish()
    }
}

/// Produce a [`TrinoLbQueryId`] similar to what Trino does (e.g. `20231125_173754_00083_4sknc`),
/// but with an `trino_lb_` prefix, so that it's clear this is a faked query ID.
#[instrument]
fn new_query_id() -> TrinoLbQueryId {
    let utc: DateTime<Utc> = Utc::now();
    let time_part = utc.format("%Y%m%d_%H%M%S");
    let rand_part = Alphanumeric.sample_string(&mut rand::rng(), 8);

    format!("{QUEUED_QUERY_ID_PREFIX}{time_part}_{rand_part}",)
}
