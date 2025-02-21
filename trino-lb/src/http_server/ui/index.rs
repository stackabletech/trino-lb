use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::State,
    response::{Html, IntoResponse, Response},
};
use http::StatusCode;
use indoc::{formatdoc, indoc};
use opentelemetry::KeyValue;
use snafu::{ResultExt, Snafu};
use tracing::{instrument, warn};

use crate::{cluster_group_manager, http_server::AppState};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to get all cluster states"))]
    GetAllClusterStates {
        source: cluster_group_manager::Error,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        warn!(error = ?self, "Error while processing ui query request");
        let status_code = match self {
            Error::GetAllClusterStates { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status_code, format!("{self}")).into_response()
    }
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

    let mut html: String = indoc! {"
        <h1>Cluster stats</h1>
        <br>
        <table>
        <tr>
            <th>Cluster</th>
            <th>State</th>
            <th>Query counter</th>
        </tr>
    "}
    .to_owned();

    for (cluster, stats) in cluster_stats {
        html.push_str(&formatdoc! {"
            <tr>
                <td>{cluster}</td>
                <td>{state}</td>
                <td>{query_counter}</td>
            </tr>
            ",
            state = stats.state,
            query_counter = stats.query_counter,
        });
    }

    html.push_str(indoc! {"
        </table>
    "});

    Ok(Html(html))
}
