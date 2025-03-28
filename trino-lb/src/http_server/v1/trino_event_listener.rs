//! Listens for Trino HTTP events, which are pushed to this endpoint
//!
//! See <https://trino.io/docs/current/admin/event-listeners-http.html#http-event-listener>

use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    response::{IntoResponse, Response},
};
use http::{HeaderMap, StatusCode};
use opentelemetry::KeyValue;
use snafu::Snafu;
use tracing::{debug, instrument, warn};
use trino_lb_core::{sanitization::Sanitize, trino_api::trino_events::OriginalTrinoEvent};

use crate::http_server::AppState;

#[derive(Snafu, Debug)]
pub enum Error {}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        warn!(error = ?self, "Error while processing Trino HTTP event");
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{self:?}")).into_response()
    }
}

#[instrument(
    name = "POST /v1/trino-event-listener",
    skip(state, trino_event),
    fields(headers = ?headers.sanitize()),
)]
pub async fn post_trino_event_listener(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    trino_event: Json<OriginalTrinoEvent>,
) -> Result<(), Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "post_trino_event_listener")]);

    debug!(?trino_event, "Got Trino event");

    Ok(())
}
