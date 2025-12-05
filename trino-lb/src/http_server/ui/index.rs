use std::{collections::BTreeMap, sync::Arc};

use askama::Template;
use axum::{
    extract::State,
    response::{Html, IntoResponse, Response},
};
use http::StatusCode;
use opentelemetry::KeyValue;
use snafu::{ResultExt, Snafu};
use tracing::{instrument, warn};

use crate::{
    cluster_group_manager::{self, ClusterStats},
    error_formatting::snafu_error_to_string,
    http_server::AppState,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to get all cluster states"))]
    GetAllClusterStates {
        source: cluster_group_manager::Error,
    },

    #[snafu(display("Failed to render template"))]
    RenderTemplate { source: askama::Error },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        warn!(error = ?self, "Error while processing ui query request");
        let status_code = match self {
            Error::GetAllClusterStates { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RenderTemplate { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status_code, snafu_error_to_string(&self)).into_response()
    }
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate<'a> {
    cluster_stats: &'a BTreeMap<&'a String, ClusterStats>,
}

/// Show some information to the user about the query state
#[instrument(name = "GET /ui/index.html", skip(state))]
pub async fn get_ui_index(State(state): State<Arc<AppState>>) -> Result<Html<String>, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "get_ui_index")]);

    let cluster_stats = state
        .cluster_group_manager
        .get_all_cluster_stats()
        .await
        .context(GetAllClusterStatesSnafu)?;

    // Sort the clusters alphabetically
    let cluster_stats: BTreeMap<_, _> = cluster_stats
        .into_iter()
        .map(|(cluster, stats)| (&cluster.name, stats))
        .collect();

    let index = IndexTemplate {
        cluster_stats: &cluster_stats,
    };
    index.render().context(RenderTemplateSnafu).map(Html)
}
