use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Basic},
};
use http::StatusCode;
use opentelemetry::KeyValue;
use serde::Serialize;
use snafu::{ResultExt, Snafu, ensure};
use tracing::{info, instrument, warn};
use trino_lb_core::{
    TrinoClusterName, config::TrinoLbAdminAuthenticationConfig, trino_cluster::ClusterState,
};
use trino_lb_persistence::Persistence;

use crate::{
    cluster_group_manager::{self, ClusterStats},
    error_formatting::snafu_error_to_string,
    http_server::AppState,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("No admin authentication method defined"))]
    NoAdminAuthenticationMethodDefined,

    #[snafu(display("Invalid admin credentials"))]
    InvalidAdminCredentials,

    #[snafu(display("Unknown Trino cluster {cluster:?}"))]
    UnknownCluster { cluster: TrinoClusterName },

    #[snafu(display("Failed to get cluster state for cluster {cluster:?} from persistence"))]
    GetClusterStateFromPersistence {
        source: trino_lb_persistence::Error,
        cluster: TrinoClusterName,
    },

    #[snafu(display("Failed to set cluster state for cluster {cluster:?} in persistence"))]
    SetClusterStateInPersistence {
        source: trino_lb_persistence::Error,
        cluster: TrinoClusterName,
    },

    #[snafu(display("Failed to get the query counter for cluster {cluster:?} from persistence"))]
    GetQueryCounterForGroup {
        source: trino_lb_persistence::Error,
        cluster: TrinoClusterName,
    },

    #[snafu(display("Failed to get all cluster states"))]
    GetAllClusterStates {
        source: cluster_group_manager::Error,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        warn!(error = ?self, "Error while processing admin request");
        let status_code = match self {
            Error::NoAdminAuthenticationMethodDefined => StatusCode::UNAUTHORIZED,
            Error::InvalidAdminCredentials => StatusCode::UNAUTHORIZED,
            Error::UnknownCluster { .. } => StatusCode::NOT_FOUND,
            Error::GetClusterStateFromPersistence { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::SetClusterStateInPersistence { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetQueryCounterForGroup { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetAllClusterStates { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status_code, snafu_error_to_string(&self)).into_response()
    }
}

/// (Re)-Activates a Trino Cluster, so that it may receive new incoming queries.
///
/// This is useful for maintenance actions (in combination with deactivation).
#[instrument(name = "POST /admin/clusters/{cluster_name}/activate", skip(state))]
pub async fn post_activate_cluster(
    TypedHeader(Authorization(basic_auth)): TypedHeader<Authorization<Basic>>,
    State(state): State<Arc<AppState>>,
    Path(cluster_name): Path<TrinoClusterName>,
) -> Result<Json<ClusterActivationResponse>, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "post_activate_cluster")]);

    set_cluster_activation(state, basic_auth, &cluster_name, true).await
}

/// Deactivates a Trino Cluster, so that no new incoming queries are handed over to this cluster.
///
/// This will not abort any already running queries. Use `GET /admin/cluster-status` to retrieve the
/// number of active queries before doing any maintenance work on this cluster.
///
/// This is useful for maintenance actions (in combination with activation).
#[instrument(name = "POST /admin/clusters/{cluster_name}/deactivate", skip(state))]
pub async fn post_deactivate_cluster(
    TypedHeader(Authorization(basic_auth)): TypedHeader<Authorization<Basic>>,
    State(state): State<Arc<AppState>>,
    Path(cluster_name): Path<TrinoClusterName>,
) -> Result<Json<ClusterActivationResponse>, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "post_deactivate_cluster")]);

    set_cluster_activation(state, basic_auth, &cluster_name, false).await
}

/// Get the status of a single Trino cluster.
#[instrument(name = "/admin/clusters/{cluster_name}/status", skip(state))]
pub async fn get_cluster_status(
    State(state): State<Arc<AppState>>,
    Path(cluster_name): Path<TrinoClusterName>,
) -> Result<Json<ClusterStats>, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "get_cluster_status")]);

    ensure!(
        state.config.is_cluster_in_config(&cluster_name),
        UnknownClusterSnafu {
            cluster: cluster_name
        }
    );

    let cluster_state = state
        .persistence
        .get_cluster_state(&cluster_name)
        .await
        .context(GetClusterStateFromPersistenceSnafu {
            cluster: &cluster_name,
        })?;
    let cluster_query_counter = state
        .persistence
        .get_cluster_query_count(&cluster_name)
        .await
        .context(GetQueryCounterForGroupSnafu {
            cluster: &cluster_name,
        })?;

    Ok(Json(ClusterStats {
        state: cluster_state,
        query_counter: cluster_query_counter,
    }))
}

/// Get the status of all Trino clusters.
#[instrument(name = "/admin/clusters/status", skip(state))]
pub async fn get_all_cluster_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<BTreeMap<TrinoClusterName, ClusterStats>>, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "get_all_cluster_status")]);

    let cluster_stats = state
        .cluster_group_manager
        .get_all_cluster_stats()
        .await
        .context(GetAllClusterStatesSnafu)?;

    Ok(Json(
        cluster_stats
            .into_iter()
            .map(|(cluster, stats)| (cluster.name.clone(), stats))
            .collect(),
    ))
}

#[derive(Debug, Serialize)]
pub struct ClusterActivationResponse {
    state: ClusterState,
}

#[instrument(skip(state))]
async fn set_cluster_activation(
    state: Arc<AppState>,
    basic_auth: Basic,
    cluster_name: &TrinoClusterName,
    activation: bool,
) -> Result<Json<ClusterActivationResponse>, Error> {
    match &state.config.trino_lb.admin_authentication {
        Some(TrinoLbAdminAuthenticationConfig::BasicAuth { username, password }) => {
            ensure!(
                basic_auth.username() == username && basic_auth.password() == password,
                InvalidAdminCredentialsSnafu {}
            );
        }
        None => return Err(Error::NoAdminAuthenticationMethodDefined),
    }

    ensure!(
        state.config.is_cluster_in_config(cluster_name),
        UnknownClusterSnafu {
            cluster: cluster_name
        }
    );

    let desired_state = if activation {
        info!(cluster = cluster_name, "Re-activating Trino cluster");
        ClusterState::Unknown
    } else {
        info!(cluster = cluster_name, "Deactivating Trino cluster");
        ClusterState::Deactivated
    };

    state
        .persistence
        .set_cluster_state(cluster_name, desired_state)
        .await
        .context(SetClusterStateInPersistenceSnafu {
            cluster: cluster_name,
        })?;

    Ok(Json(ClusterActivationResponse {
        state: desired_state,
    }))
}
