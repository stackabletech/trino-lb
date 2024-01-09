use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use axum::{body::Body, response::IntoResponse, Json};
use futures::future::try_join_all;
use http::{HeaderMap, StatusCode};
use reqwest::Client;
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{debug, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use trino_lb_core::{
    config::Config,
    sanitization::Sanitize,
    trino_api::TrinoQueryApiResponse,
    trino_query::{hack_http_to_reqwest_headers, hack_reqwest_to_http_headers, TrinoQuery},
};
use trino_lb_persistence::{Persistence, PersistenceImplementation};
use url::Url;

use crate::tracing::add_current_context_to_client_request;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to create HTTP client"))]
    CreateHttpClient { source: reqwest::Error },

    #[snafu(display("Cluster group {group:?} not found"))]
    ClusterGroupNotFound { group: String },

    #[snafu(display("Failed to construct Trino API path"))]
    ConstructTrinoApiPath { source: url::ParseError },

    #[snafu(display("Failed to contact Trino API to post query"))]
    ContactTrinoPostQuery { source: reqwest::Error },

    #[snafu(display("Failed to decode Trino API response"))]
    DecodeTrinoResponse { source: reqwest::Error },

    #[snafu(display("Configuration error: A specific Trino cluster can only be part of a single clusterGroup. Please make sure the Trino cluster {cluster_name:?} only is part of a single clusterGroup."))]
    ConfigErrorTrinoClusterInMultipleClusterGroups { cluster_name: String },

    #[snafu(display(
        "Failed to get the query counter on the clusters of the group {cluster_group:?}"
    ))]
    GetQueryCounterForGroup {
        source: trino_lb_persistence::Error,
        cluster_group: String,
    },

    #[snafu(display(
        "Failed to join the path of the current request {requested_path:?} to the Trino endpoint {trino_endpoint}"
    ))]
    JoinRequestPathToTrinoEndpoint {
        source: url::ParseError,
        requested_path: String,
        trino_endpoint: Url,
    },

    #[snafu(display(
        "Failed to read current cluster state for cluster group {cluster_group:?} from persistence"
    ))]
    ReadCurrentClusterStateForClusterGroupFromPersistence {
        source: trino_lb_persistence::Error,
        cluster_group: String,
    },
}

pub struct ClusterGroupManager {
    groups: HashMap<String, Vec<TrinoCluster>>,
    persistence: Arc<PersistenceImplementation>,
    http_client: Client,
}

#[derive(Clone, Debug)]
pub struct TrinoCluster {
    pub name: String,
    pub max_running_queries: u64,
    pub endpoint: Url,
}

pub enum SendToTrinoResponse {
    HandedOver {
        trino_query_api_response: TrinoQueryApiResponse,
        headers: http::HeaderMap,
    },
    Unauthorized {
        headers: http::HeaderMap,
        body: Body,
    },
}

impl IntoResponse for SendToTrinoResponse {
    fn into_response(self) -> axum::response::Response {
        match self {
            SendToTrinoResponse::HandedOver {
                trino_query_api_response,
                headers,
            } => (headers, Json(trino_query_api_response)).into_response(),
            SendToTrinoResponse::Unauthorized { headers, body } => {
                (StatusCode::UNAUTHORIZED, headers, body).into_response()
            }
        }
    }
}

impl ClusterGroupManager {
    #[instrument(skip(persistence))]
    pub fn new(
        persistence: Arc<PersistenceImplementation>,
        config: &Config,
        ignore_certs: bool,
    ) -> Result<Self, Error> {
        let mut clusters_seen = HashSet::new();

        let mut groups = HashMap::new();
        for (group_name, group_config) in &config.trino_cluster_groups {
            let mut group = Vec::with_capacity(group_config.trino_clusters.len());
            for cluster_config in &group_config.trino_clusters {
                let cluster_name = cluster_config.name.clone();
                if !clusters_seen.insert(cluster_name.clone()) {
                    ConfigErrorTrinoClusterInMultipleClusterGroupsSnafu {
                        cluster_name: cluster_name.clone(),
                    }
                    .fail()?;
                }

                group.push(TrinoCluster {
                    name: cluster_name,
                    max_running_queries: group_config.max_running_queries,
                    endpoint: cluster_config.endpoint.clone(),
                })
            }
            groups.insert(group_name.clone(), group);
        }

        let http_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(ignore_certs)
            .build()
            .context(CreateHttpClientSnafu)?;

        Ok(Self {
            groups,
            persistence,
            http_client,
        })
    }

    #[instrument(skip(self))]
    pub async fn send_query_to_cluster(
        &self,
        query: String,
        headers: &http::HeaderMap,
        cluster: &TrinoCluster,
    ) -> Result<SendToTrinoResponse, Error> {
        let headers = hack_http_to_reqwest_headers(headers);
        // TODO: Enable propagation again. This is disabled, as the POST /v1/statement span runs for the whole
        // query lifetime and let it look like the initial POST takes multiple minutes.
        // add_current_context_to_client_request(tracing::Span::current().context(), &mut r_headers);

        let response = self
            .http_client
            .post(
                cluster
                    .endpoint
                    .join("v1/statement")
                    .context(ConstructTrinoApiPathSnafu)?,
            )
            .headers(headers)
            .body(query)
            .send()
            .await
            .context(ContactTrinoPostQuerySnafu)?;
        let headers = hack_reqwest_to_http_headers(response.headers());

        // In case OpenId connect is used, a 401 will be returned instead of the actual response.
        // Additionally, the following two headers will be set:
        //
        // WWW-Authenticate:  Basic realm="Trino"
        // WWW-Authenticate:  Bearer x_redirect_server="https://5.250.182.203:8443/oauth2/token/initiate/80a5152ecfd179618c5ba55d49513a7aec2787212a07c3b2d80c9624b3b9007f", x_token_server="https://5.250.182.203:8443/oauth2/token/abcf2e93-ac90-424e-972b-f00bc1c4e5db"
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            let headers = filter_to_www_authenticate_headers(&headers);
            let body = response
                .bytes()
                .await
                .context(DecodeTrinoResponseSnafu)?
                .into();
            return Ok(SendToTrinoResponse::Unauthorized { headers, body });
        }

        let headers = filter_to_trino_headers(&headers);
        let trino_query_api_response = response.json().await.context(DecodeTrinoResponseSnafu)?;

        Ok(SendToTrinoResponse::HandedOver {
            trino_query_api_response,
            headers,
        })
    }

    #[instrument(
        skip(self),
        fields(next_uri = %next_uri, headers = ?headers.sanitize())
    )]
    pub async fn ask_for_query_state(
        &self,
        next_uri: Url,
        headers: HeaderMap,
    ) -> Result<(TrinoQueryApiResponse, HeaderMap), Error> {
        let mut headers = hack_http_to_reqwest_headers(&headers);
        add_current_context_to_client_request(tracing::Span::current().context(), &mut headers);
        let response = self
            .http_client
            .get(next_uri)
            .headers(headers)
            .send()
            .await
            .context(ContactTrinoPostQuerySnafu)?;
        let headers = hack_reqwest_to_http_headers(response.headers());

        let headers = filter_to_trino_headers(&headers);
        let trino_query_api_response = response.json().await.context(DecodeTrinoResponseSnafu)?;

        Ok((trino_query_api_response, headers))
    }

    #[instrument(
        skip(self),
        fields(request_headers = ?request_headers.sanitize())
    )]
    pub async fn cancel_query_on_trino(
        &self,
        request_headers: &http::HeaderMap,
        query: &TrinoQuery,
        requested_path: &str,
    ) -> Result<(), Error> {
        let mut headers = hack_http_to_reqwest_headers(request_headers);
        add_current_context_to_client_request(tracing::Span::current().context(), &mut headers);

        self.http_client
            .delete(query.trino_endpoint.join(requested_path).context(
                JoinRequestPathToTrinoEndpointSnafu {
                    requested_path,
                    trino_endpoint: query.trino_endpoint.clone(),
                },
            )?)
            .headers(headers)
            .send()
            .await
            .context(ContactTrinoPostQuerySnafu)?;

        Ok(())
    }

    /// Tries to find the best cluster from the specified `cluster_group`. If all clusters of the requested group have reached their
    /// configured query limit, this function returns [`None`].
    #[instrument(skip(self))]
    pub async fn try_find_best_cluster_for_group(
        &self,
        cluster_group: &str,
    ) -> Result<Option<&TrinoCluster>, Error> {
        let clusters = self
            .groups
            .get(cluster_group)
            .context(ClusterGroupNotFoundSnafu {
                group: cluster_group.to_string(),
            })?;

        let cluster_states = try_join_all(
            clusters
                .iter()
                .map(|c| self.persistence.get_cluster_state(&c.name)),
        )
        .await
        .context(ReadCurrentClusterStateForClusterGroupFromPersistenceSnafu { cluster_group })?;

        let clusters = clusters
            .iter()
            .zip(cluster_states)
            .filter(|(_, state)| state.ready_to_accept_queries())
            .map(|(c, _)| c)
            .collect::<Vec<_>>();

        let cluster_query_counters = try_join_all(
            clusters
                .iter()
                .map(|g| async { self.persistence.get_cluster_query_count(&g.name).await }),
        )
        .await
        .context(GetQueryCounterForGroupSnafu { cluster_group })?;

        let debug_output = clusters
            .iter()
            .map(|c| &c.name)
            .zip(cluster_query_counters.iter())
            .collect::<Vec<_>>();
        debug!(query_counters = ?debug_output, "Clusters had the following query counters");

        let cluster_with_min_queries = clusters
            .into_iter()
            .zip(cluster_query_counters)
            .filter(|(cluster, counter)| *counter < cluster.max_running_queries)
            .min_by_key(|(_, counter)| *counter)
            .map(|(c, _)| c);

        Ok(cluster_with_min_queries)
    }
}

fn filter_to_trino_headers(headers: &HeaderMap) -> HeaderMap {
    let mut trino_headers = HeaderMap::new();
    for (name, value) in headers.into_iter() {
        if name.as_str().to_lowercase().starts_with("x-trino") {
            trino_headers.append(name, value.clone());
        }
    }

    trino_headers
}

fn filter_to_www_authenticate_headers(headers: &HeaderMap) -> HeaderMap {
    let mut www_headers = HeaderMap::new();
    for (name, value) in headers.into_iter() {
        if &name.as_str().to_lowercase() == "www-authenticate" {
            www_headers.append(name, value.clone());
        }
    }

    www_headers
}
