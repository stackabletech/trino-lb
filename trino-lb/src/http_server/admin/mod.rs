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
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu, ensure};
use tracing::{info, instrument, warn};
use trino_lb_core::{
    TrinoClusterName, config::TrinoLbAdminAuthenticationConfig, trino_cluster::ClusterState,
};
use trino_lb_persistence::Persistence;

use crate::{
    cluster_group_manager::{self, ClusterStats},
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

    #[snafu(display("Failed to set cluster state for cluster {cluster:?} in persistence"))]
    SetClusterStateInPersistence {
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
            Error::SetClusterStateInPersistence { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetAllClusterStates { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status_code, format!("{self}")).into_response()
    }
}

/// (Re)-Activates a Trino Cluster, so that it receives new queries.
///
/// This is useful for maintenance actions (in combination with deactivation).
#[instrument(name = "POST /admin/activate-cluster/{cluster_name}", skip(state))]
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

/// Deactivate a Trino Cluster, so that it doesn't receive any new queries.
///
/// This is useful for maintenance actions (in combination with activation).
#[instrument(name = "POST /admin/deactivate-cluster/{cluster_name}", skip(state))]
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

/// Get the status of the Trino clusters
#[instrument(name = "GET /admin/cluster-status", skip(state))]
pub async fn get_cluster_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<BTreeMap<TrinoClusterName, ClusterStats>>, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "get_cluster_status")]);

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

#[derive(Debug, Deserialize, Serialize)]
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
