use std::{
    num::TryFromIntError,
    time::{Duration, SystemTimeError},
};

use prusto::{QueryError, Warning};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tracing::instrument;
use url::Url;

use crate::{trino_query::QueuedQuery, TrinoQueryId};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to join API path onto trino-lb url {trino_lb_addr}"))]
    JoinApiPathToTrinoLbUrl {
        source: url::ParseError,
        trino_lb_addr: Url,
    },

    #[snafu(display("Failed to parse nextUri Trino send us"))]
    ParseNextUriFromTrino { source: url::ParseError },

    #[snafu(display("Failed to determine the elapsed time of a queued query. Are all system clocks of trino-lb instances in sync?"))]
    DetermineElapsedTime { source: SystemTimeError },

    #[snafu(display("The queued time {queued_time:?} is too big to be send to trino, as the trino API only accepts an 64bit number for queued_time_millis"))]
    ElapsedTimeTooBig {
        source: TryFromIntError,
        queued_time: Duration,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoQueryApiResponse {
    pub id: TrinoQueryId,

    /// Normally this will always be set, only the last call will not return a `next_uri`.
    pub next_uri: Option<String>,
    pub info_uri: String,
    pub partial_cancel_uri: Option<String>,

    pub columns: Option<serde_json::Value>,
    pub data: Option<serde_json::Value>,

    pub error: Option<QueryError>,
    pub warnings: Vec<Warning>,

    pub stats: Stat,

    pub update_type: Option<String>,
    pub update_count: Option<u64>,
}

/// Copied from [`prusto::Stat`], but with `root_stage`
#[derive(Deserialize, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Stat {
    pub completed_splits: u32,
    pub cpu_time_millis: u64,
    pub elapsed_time_millis: u64,
    pub nodes: u32,
    pub peak_memory_bytes: u64,
    pub physical_input_bytes: u64,
    pub processed_bytes: u64,
    pub processed_rows: u64,
    pub progress_percentage: Option<f32>,
    pub queued_splits: u32,
    pub queued_time_millis: u64,
    pub queued: bool,
    pub root_stage: Option<serde_json::Value>,
    pub running_percentage: Option<f32>,
    pub running_splits: u32,
    pub scheduled: bool,
    pub spilled_bytes: u64,
    pub state: String,
    pub total_splits: u32,
    pub wall_time_millis: u64,
}

impl TrinoQueryApiResponse {
    #[instrument(
        fields(trino_lb_addr = %trino_lb_addr),
    )]
    pub fn new_from_queued_query(
        query: &QueuedQuery,
        current_sequence_number: u64,
        trino_lb_addr: &Url,
    ) -> Result<Self, Error> {
        let next_sequence_number = current_sequence_number + 1;
        let query_id = &query.id;
        let queued_time = query
            .creation_time
            .elapsed()
            .context(DetermineElapsedTimeSnafu)?;
        let queued_time_ms: u64 = queued_time
            .as_millis()
            .try_into()
            .context(ElapsedTimeTooBigSnafu { queued_time })?;

        Ok(TrinoQueryApiResponse {
            id: query.id.clone(),
            next_uri: Some(
                trino_lb_addr
                    .join(&format!(
                        "v1/statement/queued_in_trino_lb/{query_id}/{next_sequence_number}"
                    ))
                    .context(JoinApiPathToTrinoLbUrlSnafu {
                        trino_lb_addr: trino_lb_addr.clone(),
                    })?
                    .to_string(),
            ),
            info_uri: trino_lb_addr
                .join(&format!("ui/query.html?{query_id}"))
                .context(JoinApiPathToTrinoLbUrlSnafu {
                    trino_lb_addr: trino_lb_addr.clone(),
                })?
                .to_string(),
            partial_cancel_uri: None,
            columns: None,
            data: None,
            error: None,
            stats: Stat {
                completed_splits: 0,
                cpu_time_millis: 0,
                elapsed_time_millis: queued_time_ms,
                nodes: 0,
                peak_memory_bytes: 0,
                physical_input_bytes: 0,
                processed_bytes: 0,
                processed_rows: 0,
                progress_percentage: None,
                queued_splits: 0,
                queued_time_millis: queued_time_ms,
                queued: true,
                root_stage: None,
                running_percentage: None,
                running_splits: 0,
                scheduled: false,
                spilled_bytes: 0,
                state: "QUEUED_IN_TRINO_LB".to_string(),
                total_splits: 0,
                wall_time_millis: 0,
            },
            warnings: vec![],
            update_type: None,
            update_count: None,
        })
    }

    #[instrument(
        fields(trino_lb_addr = %trino_lb_addr),
    )]
    pub fn change_next_uri_to_trino_lb(&mut self, trino_lb_addr: &Url) -> Result<(), Error> {
        if let Some(next_uri) = &self.next_uri {
            let next_uri = Url::parse(next_uri).context(ParseNextUriFromTrinoSnafu)?;
            self.next_uri = Some(change_next_uri_to_trino_lb(&next_uri, trino_lb_addr).to_string());
        }

        Ok(())
    }
}

fn change_next_uri_to_trino_lb(next_uri: &Url, trino_lb_addr: &Url) -> Url {
    let mut result = trino_lb_addr.clone();
    result.set_path(next_uri.path());
    result
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("http://trino", "http://trino-lb", "http://trino-lb/")]
    #[case("http://trino:8080", "http://trino-lb", "http://trino-lb/")]
    #[case("http://trino", "http://trino-lb:8080", "http://trino-lb:8080/")]
    #[case("http://trino:8080", "http://trino-lb:1234", "http://trino-lb:1234/")]
    #[case("https://trino", "http://trino-lb", "http://trino-lb/")]
    #[case("http://trino", "https://trino-lb", "https://trino-lb/")]
    #[case("https://trino", "https://trino-lb", "https://trino-lb/")]
    #[case(
        "https://trino:8443/v1/statement",
        "https://trino-lb:1234",
        "https://trino-lb:1234/v1/statement"
    )]
    #[case(
        "https://trino-m-1-coordinator-default.default.svc.cluster.local:8443/v1/statement/executing/20240112_082858_00000_kggk9/yb3c629e616e7cd9fdef859ce15bd660d26e44d24/0",
        "https://5.250.179.64:1234",
        "https://5.250.179.64:1234/v1/statement/executing/20240112_082858_00000_kggk9/yb3c629e616e7cd9fdef859ce15bd660d26e44d24/0"
    )]
    fn test_change_next_uri_to_trino_lb(
        #[case] next_uri: String,
        #[case] trino_lb_addr: String,
        #[case] expected: String,
    ) {
        let next_uri = Url::parse(&next_uri).unwrap();
        let trino_lb_addr = Url::parse(&trino_lb_addr).unwrap();
        let result = change_next_uri_to_trino_lb(&next_uri, &trino_lb_addr);
        assert_eq!(result.to_string(), expected);
    }
}
