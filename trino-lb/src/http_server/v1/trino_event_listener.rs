//! Listens for Trino HTTP events, which are pushed to this endpoint
//!
//! See <https://trino.io/docs/current/admin/event-listeners-http.html#http-event-listener>

use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    response::{IntoResponse, Response},
};
use http::StatusCode;
use opentelemetry::KeyValue;
use snafu::{ResultExt, Snafu};
use tracing::{instrument, warn};
use trino_lb_core::{
    config::TrinoLbProxyMode,
    trino_api::trino_events::{TrinoEvent, TrinoQueryState},
};
use trino_lb_persistence::Persistence;

use crate::http_server::AppState;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(
        "Failed to decrement the query counter query trino cluster {trino_cluster:?}"
    ))]
    DecClusterQueryCounter {
        source: trino_lb_persistence::Error,
        trino_cluster: String,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        warn!(error = ?self, "Error while processing Trino HTTP event");

        // We use a match here to let authors of new errors decide if Trino should retry the call
        // (which it only does for a >= 500 HTTP return code according to
        // https://trino.io/docs/current/admin/event-listeners-http.html#configuration-properties).
        match self {
            // Could be caused by temporary problems, so we let Trino retry
            Error::DecClusterQueryCounter { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{self:?}")).into_response()
            }
        }
    }
}

#[instrument(
    name = "POST /v1/trino-event-listener",
    skip(state, trino_event),
    fields(
        query_id = trino_event.query_id(),
        query_state = %trino_event.query_state(),
    )
)]
pub async fn post_trino_event_listener(
    State(state): State<Arc<AppState>>,
    trino_event: Json<TrinoEvent>,
) -> Result<(), Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "post_trino_event_listener")]);

    if state.config.trino_lb.proxy_mode != TrinoLbProxyMode::ProxyFirstCall {
        warn!(
            "Received a Trino event, but trino-lb is configured to proxy all calls. \
            Thus it is ignoring the received Trino HTTP events. \
            Either don't send HTTP events to trino-lb or configure it to only proxy the first call."
        );
        return Ok(());
    }

    let query_id = trino_event.query_id();
    let trino_host = trino_event.server_address();
    let uri = trino_event.uri();

    let Some(cluster) = state.cluster_name_for_host.get(trino_host) else {
        warn!(
            query_id,
            %uri, "Got a Trino query event for a cluster that I don't know."
        );
        return Ok(());
    };

    match trino_event.query_state() {
        TrinoQueryState::Queued => {
            // We don't do anything, the query counter is incremented once trino-lb submits a query
            // to an Trino cluster.

            // // As there query is already running we are acting after the fact.
            // // So we can not reject the query, even if too many query are already running
            // let max_allowed_queries = u64::MAX;
            // state
            //     .persistence
            //     .inc_cluster_query_count(cluster, max_allowed_queries)
            //     .await
            //     .with_context(|_| IncClusterQueryCounterSnafu {
            //         trino_cluster: cluster,
            //     })?;
        }
        TrinoQueryState::Finished => {
            state
                .persistence
                .dec_cluster_query_count(cluster)
                .await
                .with_context(|_| DecClusterQueryCounterSnafu {
                    trino_cluster: cluster,
                })?;
        }
        TrinoQueryState::Executing => warn!(
            "Got a Trino query event for an running query. As you should only send query started or \
            finished query events, only queued or finished should be received! Ignoring this event."
        ),
    }

    Ok(())
}
