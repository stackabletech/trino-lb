use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Body,
    extract::{Query, State},
    response::{IntoResponse, Response},
};
use http::{HeaderMap, StatusCode};
use opentelemetry::KeyValue;
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{instrument, warn};

use crate::{error_formatting::snafu_error_to_string, http_server::AppState};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("no URI to call passed as query parameter \"url\"."))]
    NoUrlGiven {},

    #[snafu(display("failed to GET the url {url:?}"))]
    GetUrl { source: reqwest::Error, url: String },

    #[snafu(display("failed to build response"))]
    BuildResponse { source: http::Error },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        warn!(error = ?self, "Error while processing request");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            snafu_error_to_string(&self),
        )
            .into_response()
    }
}

/// A general-purpose proxy
#[instrument(name = "GET /proxy", skip(state))]
pub async fn get(
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Response<Body>, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "get_proxy")]);

    let url = params.get("url").context(NoUrlGivenSnafu)?;
    let resp = reqwest::get(url).await.context(GetUrlSnafu { url })?;

    Response::builder()
        .status(resp.status())
        .body(Body::from_stream(resp.bytes_stream()))
        .context(BuildResponseSnafu)
}
