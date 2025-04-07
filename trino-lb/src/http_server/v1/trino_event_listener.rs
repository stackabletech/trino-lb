//! Listens for Trino HTTP events, which are pushed to this endpoint
//!
//! See <https://trino.io/docs/current/admin/event-listeners-http.html#http-event-listener>

use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    response::{IntoResponse, Response},
};
use futures::TryFutureExt;
use http::StatusCode;
use opentelemetry::KeyValue;
use snafu::{ResultExt, Snafu};
use tracing::{debug, instrument, warn};
use trino_lb_core::{
    TrinoQueryId,
    config::TrinoLbProxyMode,
    trino_api::trino_events::{TrinoEvent, TrinoQueryState},
};
use trino_lb_persistence::Persistence;

use crate::http_server::AppState;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to load query with id {query_id:?} from persistence"))]
    LoadQueryFromPersistence {
        source: trino_lb_persistence::Error,
        query_id: TrinoQueryId,
    },

    #[snafu(display("Failed to delete query from persistence"))]
    DeleteQueryFromPersistence {
        source: trino_lb_persistence::Error,
        query_id: TrinoQueryId,
    },

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
            Error::LoadQueryFromPersistence { .. }
            | Error::DeleteQueryFromPersistence { .. }
            | Error::DecClusterQueryCounter { .. } => {
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

    if state.config.trino_lb.proxy_mode == TrinoLbProxyMode::ProxyAllCalls {
        warn!(
            "Received a Trino event, but trino-lb is configured to proxy all calls. \
            Thus it is ignoring the received Trino HTTP events. \
            Either don't send HTTP events to trino-lb or configure it to only proxy the first call."
        );
        return Ok(());
    }

    match trino_event.query_state() {
        TrinoQueryState::Queued => handle_queued_query(state, trino_event.0).await?,
        TrinoQueryState::Finished => handle_finished_query(state, trino_event.0).await?,
        TrinoQueryState::Executing => warn!(
            "Got a Trino query event for an running query. As you should only send query started or \
            finished query events, only queued or finished should be received! Ignoring this event."
        ),
    }

    Ok(())
}

/// This function is called once a query on Trino finishes.
#[instrument(
    skip(state, trino_event),
    fields(query_id = trino_event.query_id())
)]
async fn handle_finished_query(state: Arc<AppState>, trino_event: TrinoEvent) -> Result<(), Error> {
    let query_id = trino_event.query_id();
    let query = state
        .persistence
        .load_query(trino_event.query_id())
        .await
        .with_context(|_| LoadQueryFromPersistenceSnafu {
            query_id: trino_event.query_id(),
        })?;

    match query {
        Some(query) => {
            debug!(
                query_id,
                trino_cluster = query.trino_cluster,
                "Query on Trino finished"
            );
            tokio::try_join!(
                state.persistence.remove_query(query_id).map_err(|err| {
                    Error::DeleteQueryFromPersistence {
                        source: err,
                        query_id: query_id.to_owned(),
                    }
                }),
                state
                    .persistence
                    .dec_cluster_query_count(&query.trino_cluster)
                    .map_err(|err| {
                        Error::DecClusterQueryCounter {
                            source: err,
                            trino_cluster: query.trino_cluster.to_owned(),
                        }
                    }),
            )?;
        }
        None => warn!(
            query_id,
            uri = %trino_event.metadata.uri,
            "A query I didn't know about finished on some Trino cluster. \
            This probably means it was submitted to it directly instead via trino-lb!",
        ),
    }

    todo!()
}

/// This function *should* be called for every new query on Trino.
#[instrument(
    skip(state, trino_event),
    fields(query_id = trino_event.query_id())
)]
async fn handle_queued_query(state: Arc<AppState>, trino_event: TrinoEvent) -> Result<(), Error> {
    let query_id = trino_event.query_id();
    let query = state
        .persistence
        .load_query(query_id)
        .await
        .with_context(|_| LoadQueryFromPersistenceSnafu {
            query_id: trino_event.query_id(),
        })?;

    match query {
        Some(query) => {
            debug!(
                query_id,
                trino_cluster = query.trino_cluster,
                "Query on Trino started"
            );
        }
        None => {
            warn!(
                query_id = trino_event.query_id(),
                uri = %trino_event.metadata.uri,
                "A query I didn't know about was started on some Trino cluster. \
                This probably means it was submitted to it directly instead via trino-lb!",
            );

            // TODO: We might be able to determine the Trino cluster from the URL and increment it's
            // query count. However, what should we do in case it was a Trino cluster not known by
            // trino-lb?
        }
    }

    Ok(())
}
