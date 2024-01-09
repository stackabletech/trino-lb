use std::{fmt::Debug, string::FromUtf8Error, sync::Arc};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use prometheus::{Encoder, TextEncoder};
use snafu::{ResultExt, Snafu};
use tracing::{instrument, warn};

use crate::http_server::AppState;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to encode Prometheus metrics as text"))]
    EncodePrometheusMetrics { source: prometheus::Error },

    #[snafu(display("Failed to create utf-8 string from text-encoded prometheus metrics"))]
    StringFromPrometheusMetrics { source: FromUtf8Error },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        warn!(error = ?self, "Error while processing metrics request");
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{self:?}")).into_response()
    }
}

#[instrument(skip(state))]
pub async fn get(State(state): State<Arc<AppState>>) -> Result<String, Error> {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = state.metrics.registry.gather();
    encoder
        .encode(&metric_families, &mut buffer)
        .context(EncodePrometheusMetricsSnafu)?;

    String::from_utf8(buffer).context(StringFromPrometheusMetricsSnafu)
}
