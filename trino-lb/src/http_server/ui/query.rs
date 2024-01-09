use std::sync::Arc;

use axum::{
    extract::{RawQuery, State},
    response::{Html, IntoResponse, Response},
};
use http::StatusCode;
use opentelemetry::KeyValue;
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{instrument, warn};
use trino_lb_core::TrinoLbQueryId;
use trino_lb_persistence::Persistence;

use crate::http_server::AppState;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Query ID missing. It needs to be specified as query parameter such as https://127.0.0.1:8443/ui/query.html?trino_lb_20231227_122313_2JzDa3bT"))]
    QueryIdMissing {},

    #[snafu(display("Query with ID {query_id:?} not found. Maybe the query is not queued any more but was handed over to a Trino cluster."))]
    QueryIdNotFound {
        source: trino_lb_persistence::Error,
        query_id: TrinoLbQueryId,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        warn!(error = ?self, "Error while processing ui query request");
        let status_code = match self {
            Error::QueryIdMissing { .. } => StatusCode::BAD_REQUEST,
            Error::QueryIdNotFound { .. } => StatusCode::NOT_FOUND,
        };
        (status_code, format!("{self}")).into_response()
    }
}

/// Show some information to the user about the query state
#[instrument(name = "POST /ui/query.html", skip(state))]
pub async fn get_ui_query(
    State(state): State<Arc<AppState>>,
    RawQuery(query_id): RawQuery,
) -> Result<Html<String>, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "get_ui_query")]);

    let query_id = query_id.context(QueryIdMissingSnafu)?;
    let queued_query = state
        .persistence
        .load_queued_query(&query_id)
        .await
        .context(QueryIdNotFoundSnafu {
            query_id: &query_id,
        })?;

    Ok(Html(format!(
        "
        <h1>Query is queued in trino-lb</h1>
        <p>Your query with the ID {query_id:?} is currently queued and very important to us! Please hold the line.</p>
        <br>
        <p>It is current queued for {queued_duration:?} in the trino cluster group {cluster_group}</p>",
        cluster_group = queued_query.cluster_group,
        queued_duration = queued_query.creation_time.elapsed().unwrap_or_default(),
    )))
}
