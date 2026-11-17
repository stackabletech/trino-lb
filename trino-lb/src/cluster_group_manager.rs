use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use axum::{Json, body::Body, response::IntoResponse};
use futures::future::try_join_all;
use http::{
    HeaderMap, HeaderName, HeaderValue, StatusCode,
    header::{FORWARDED, HOST},
};
use reqwest::Client;
use serde::Serialize;
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{Instrument, debug, info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use trino_lb_core::{
    config::Config, sanitization::Sanitize, trino_api::TrinoQueryApiResponse,
    trino_cluster::ClusterState, trino_query::TrinoQuery,
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

    #[snafu(display("Failed to call Trino HEAD URL {url:?}"))]
    CallTrinoHeadUrl { source: reqwest::Error, url: Url },

    #[snafu(display("Failed to decode Trino API response"))]
    DecodeTrinoResponse { source: reqwest::Error },

    #[snafu(display("Failed to get the bytes of the Trino API response"))]
    GetTrinoResponseBytes { source: reqwest::Error },

    #[snafu(display("Failed to parse Trino API response as JSON"))]
    ParseTrinoResponse { source: serde_json::Error },

    #[snafu(display(
        "Configuration error: A specific Trino cluster can only be part of a single clusterGroup. Please make sure the Trino cluster {cluster_name:?} only is part of a single clusterGroup."
    ))]
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

    #[snafu(display(
        "Failed to determine the host of the external Trino endpoint {external_endpoint}"
    ))]
    ExternalTrinoEndpointWithoutHost { external_endpoint: Url },

    #[snafu(display(
        "Failed to turn {value:?} of the external Trino endpoint {external_endpoint} into a HTTP header value"
    ))]
    ConvertExternalTrinoEndpointToHeaderValue {
        source: http::header::InvalidHeaderValue,
        value: String,
        external_endpoint: Url,
    },
}

/// Not part of [`http::header`], so it needs to be defined here.
const X_FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");

/// Not part of [`http::header`], so it needs to be defined here.
const X_FORWARDED_PORT: HeaderName = HeaderName::from_static("x-forwarded-port");

/// Not part of [`http::header`], so it needs to be defined here.
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

pub struct ClusterGroupManager {
    groups: HashMap<String, Vec<TrinoCluster>>,
    persistence: Arc<PersistenceImplementation>,
    http_client: Client,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct TrinoCluster {
    pub name: String,
    pub max_running_queries: u64,
    pub endpoint: Url,
    pub external_endpoint: Option<Url>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterStats {
    pub state: ClusterState,
    pub query_counter: u64,
}

pub enum SendToTrinoResponse {
    HandedOver {
        trino_query_api_response: Box<TrinoQueryApiResponse>,
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
    // Intentionally including the config here, this is only logged on startup
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
                    external_endpoint: cluster_config.external_endpoint.clone(),
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

    #[instrument(
        skip(self, cluster),
        fields(cluster.name, headers = ?headers.sanitize())
    )]
    pub async fn send_query_to_cluster(
        &self,
        query: String,
        mut headers: http::HeaderMap,
        cluster: &TrinoCluster,
    ) -> Result<SendToTrinoResponse, Error> {
        point_forwarded_headers_to_trino(&mut headers, cluster.external_endpoint.as_ref())?;

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
        let headers = response.headers();

        // In case OpenId connect is used, a 401 will be returned instead of the actual response.
        // Additionally, the following two headers will be set:
        //
        // WWW-Authenticate:  Basic realm="Trino"
        // WWW-Authenticate:  Bearer x_redirect_server="https://5.250.182.203:8443/oauth2/token/initiate/80a5152ecfd179618c5ba55d49513a7aec2787212a07c3b2d80c9624b3b9007f", x_token_server="https://5.250.182.203:8443/oauth2/token/abcf2e93-ac90-424e-972b-f00bc1c4e5db"
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            let headers = filter_to_www_authenticate_headers(headers);
            let body = response
                .bytes()
                .await
                .context(GetTrinoResponseBytesSnafu)?
                .into();
            return Ok(SendToTrinoResponse::Unauthorized { headers, body });
        }

        let headers = filter_to_trino_headers(headers);
        let trino_query_api_response = response.json().await.context(DecodeTrinoResponseSnafu)?;

        Ok(SendToTrinoResponse::HandedOver {
            trino_query_api_response,
            headers,
        })
    }

    #[instrument(
        skip(self, external_endpoint),
        fields(next_uri = %next_uri, headers = ?headers.sanitize())
    )]
    pub async fn ask_for_query_state(
        &self,
        next_uri: Url,
        external_endpoint: Option<&Url>,
        mut headers: HeaderMap,
    ) -> Result<(TrinoQueryApiResponse, HeaderMap), Error> {
        point_forwarded_headers_to_trino(&mut headers, external_endpoint)?;
        add_current_context_to_client_request(tracing::Span::current().context(), &mut headers);
        let response = self
            .http_client
            .get(next_uri)
            .headers(headers)
            .send()
            .instrument(info_span!("Send HTTP GET to Trino"))
            .await
            .context(ContactTrinoPostQuerySnafu)?;
        let headers = response.headers();
        let headers = filter_to_trino_headers(headers);

        let bytes = response
            .bytes()
            .instrument(info_span!("Get response bytes"))
            .await
            .context(GetTrinoResponseBytesSnafu)?;
        let trino_query_api_response = info_span!("Parse JSON response", bytes = bytes.len())
            .in_scope(|| serde_json::from_slice(&bytes).context(ParseTrinoResponseSnafu))?;

        Ok((trino_query_api_response, headers))
    }

    /// Sometimes the trino-client HEADs a /executing/xxx endpoint instead of GETing it.
    /// We need to proxy this as a HEAD request as well.
    #[instrument(
        skip(self, external_endpoint),
        fields(head_uri = %head_uri, headers = ?headers.sanitize())
    )]
    pub async fn send_head_to_trino(
        &self,
        head_uri: Url,
        external_endpoint: Option<&Url>,
        mut headers: HeaderMap,
    ) -> Result<HeaderMap, Error> {
        point_forwarded_headers_to_trino(&mut headers, external_endpoint)?;
        add_current_context_to_client_request(tracing::Span::current().context(), &mut headers);

        let response = self
            .http_client
            .head(head_uri.clone())
            .headers(headers)
            .send()
            .instrument(info_span!("Send HTTP HEAD to Trino"))
            .await
            .with_context(|_| CallTrinoHeadUrlSnafu { url: head_uri })?;
        let headers = response.headers();
        let headers = filter_to_trino_headers(headers);

        Ok(headers)
    }

    #[instrument(
        skip(self),
        fields(request_headers = ?request_headers.sanitize())
    )]
    pub async fn cancel_query_on_trino(
        &self,
        mut request_headers: http::HeaderMap,
        query: &TrinoQuery,
        requested_path: &str,
    ) -> Result<(), Error> {
        point_forwarded_headers_to_trino(
            &mut request_headers,
            query.trino_external_endpoint.as_ref(),
        )?;
        add_current_context_to_client_request(
            tracing::Span::current().context(),
            &mut request_headers,
        );

        self.http_client
            .delete(query.trino_endpoint.join(requested_path).context(
                JoinRequestPathToTrinoEndpointSnafu {
                    requested_path,
                    trino_endpoint: query.trino_endpoint.clone(),
                },
            )?)
            .headers(request_headers)
            .send()
            .await
            .context(ContactTrinoPostQuerySnafu)?;

        Ok(())
    }

    /// Tries to find the best cluster from the specified `cluster_group`. If all clusters of the requested group have
    /// reached their configured query limit, this function returns [`None`].
    #[instrument(skip(self))]
    pub async fn try_find_best_cluster_for_group(
        &self,
        cluster_group: &str,
    ) -> Result<Option<&TrinoCluster>, Error> {
        let cluster_stats = self
            .get_cluster_stats_for_cluster_group(cluster_group)
            .await?;

        let cluster_with_min_queries = cluster_stats
            .into_iter()
            // Only send queries to clusters that are actually able to accept them
            .filter(|(_, stats)| stats.state.ready_to_accept_queries())
            // Only send queries to clusters that are not already full
            .filter(|(cluster, stats)| stats.query_counter < cluster.max_running_queries)
            // Pick the emptiest cluster
            .min_by_key(|(_, stats)| stats.query_counter)
            .map(|(cluster, _)| cluster);

        Ok(cluster_with_min_queries)
    }

    /// Collect statistics (such as state and query counter) for all Trino clusters in a given clusterGroup
    #[instrument(skip(self))]
    pub async fn get_cluster_stats_for_cluster_group(
        &self,
        cluster_group: &str,
    ) -> Result<HashMap<&TrinoCluster, ClusterStats>, Error> {
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

        let cluster_query_counters = try_join_all(
            clusters
                .iter()
                .map(|g| async { self.persistence.get_cluster_query_count(&g.name).await }),
        )
        .await
        .context(GetQueryCounterForGroupSnafu { cluster_group })?;

        let cluster_stats = clusters
            .iter()
            .zip(cluster_states)
            .zip(cluster_query_counters)
            .map(|((trino_cluster, state), query_counter)| {
                (
                    trino_cluster,
                    ClusterStats {
                        state,
                        query_counter,
                    },
                )
            })
            .collect();

        debug!(?cluster_stats, "Clusters had the following stats");

        Ok(cluster_stats)
    }

    /// Get the stats for all clusters, regardless the cluster group membership
    pub async fn get_all_cluster_stats(
        &self,
    ) -> Result<HashMap<&TrinoCluster, ClusterStats>, Error> {
        let cluster_stats = try_join_all(
            self.groups
                .keys()
                .map(|cluster_group| self.get_cluster_stats_for_cluster_group(cluster_group)),
        )
        .await?;

        let mut all_cluster_stats = HashMap::new();
        for cluster_stat in cluster_stats {
            all_cluster_stats.extend(cluster_stat);
        }
        Ok(all_cluster_stats)
    }
}

/// Trino builds the absolute URLs it hands out to clients from the host and protocol of the incoming
/// request. Among others this affects
///
/// 1. the `x_redirect_server` and `x_token_server` of the OAuth 2.0 `WWW-Authenticate` challenge,
/// 2. the `infoUri` and `partialCancelUri` of a query and
/// 3. the `ackUri` of spooled segments.
///
/// As trino-lb proxies the requests of the clients, all of the relevant headers point at trino-lb,
/// which makes Trino hand out URLs that don't exist on trino-lb. The OAuth 2.0 endpoints are the
/// worst offender here: Clients get sent to `https://<trino-lb>/oauth2/token/{id}`, which trino-lb
/// answers with a `404`, breaking the entire authentication flow.
///
/// To prevent this, the forwarding related headers are overwritten with the `externalEndpoint` of
/// the Trino cluster, so that Trino hands out URLs pointing at itself, which clients can actually
/// reach. The `nextUri` is unaffected by this, as it is always rewritten to point back at trino-lb
/// afterwards (see [`TrinoQueryApiResponse::update_trino_references`]).
///
/// `X-Forwarded-For` and `X-Real-Ip` are intentionally left alone, as they describe the client
/// itself and not the address the client asked for.
///
/// This is a no-op in case no `externalEndpoint` is configured for the Trino cluster, as trino-lb
/// has no clue which address clients can reach Trino at in that case.
#[instrument(skip_all)]
fn point_forwarded_headers_to_trino(
    headers: &mut HeaderMap,
    external_endpoint: Option<&Url>,
) -> Result<(), Error> {
    let Some(external_endpoint) = external_endpoint else {
        return Ok(());
    };

    let host = external_endpoint
        .host_str()
        .context(ExternalTrinoEndpointWithoutHostSnafu {
            external_endpoint: external_endpoint.clone(),
        })?;

    // The port is only added in case it is not the default one of the scheme, so that Trino hands
    // out `https://trino.example.com/...` instead of `https://trino.example.com:443/...`.
    let host = match external_endpoint.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    };
    let host = to_header_value(&host, external_endpoint)?;

    headers.insert(HOST, host.clone());
    headers.insert(X_FORWARDED_HOST, host);
    headers.insert(
        X_FORWARDED_PROTO,
        to_header_value(external_endpoint.scheme(), external_endpoint)?,
    );

    match external_endpoint.port() {
        Some(port) => {
            headers.insert(
                X_FORWARDED_PORT,
                to_header_value(&port.to_string(), external_endpoint)?,
            );
        }
        // Trino derives the port from the protocol in this case, which is better than leaving the
        // port of an upstream proxy (pointing at trino-lb) in place.
        None => {
            headers.remove(X_FORWARDED_PORT);
        }
    }

    // The `Forwarded` header (RFC 7239) takes precedence over the `X-Forwarded-*` headers in Jetty,
    // which Trino is built upon. It therefore needs to be removed, as it would otherwise override
    // everything set above. Every proxy we are aware of sets `X-Forwarded-For` alongside
    // `Forwarded`, so the address of the client is not lost by doing so.
    headers.remove(FORWARDED);

    debug!(
        headers = ?headers.sanitize(),
        "Pointed the forwarding related headers at the external Trino endpoint"
    );

    Ok(())
}

fn to_header_value(value: &str, external_endpoint: &Url) -> Result<HeaderValue, Error> {
    HeaderValue::from_str(value).context(ConvertExternalTrinoEndpointToHeaderValueSnafu {
        value,
        external_endpoint: external_endpoint.clone(),
    })
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// The headers a Gateway or Ingress in front of trino-lb typically sets, all of them pointing at
    /// trino-lb instead of Trino.
    fn headers_pointing_at_trino_lb() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("trino-lb.example.com"));
        headers.insert(
            FORWARDED,
            HeaderValue::from_static("host=trino-lb.example.com;proto=https"),
        );
        headers.insert(
            X_FORWARDED_HOST,
            HeaderValue::from_static("trino-lb.example.com"),
        );
        headers.insert(X_FORWARDED_PORT, HeaderValue::from_static("443"));
        headers.insert(X_FORWARDED_PROTO, HeaderValue::from_static("https"));
        headers.insert("x-forwarded-for", HeaderValue::from_static("172.30.10.61"));
        headers.insert("x-real-ip", HeaderValue::from_static("172.30.10.61"));
        headers
    }

    #[rstest]
    #[case("https://trino.example.com", "trino.example.com", "https", None)]
    #[case(
        "https://trino.example.com:8443",
        "trino.example.com:8443",
        "https",
        Some("8443")
    )]
    #[case(
        "http://trino.example.com:8080",
        "trino.example.com:8080",
        "http",
        Some("8080")
    )]
    // Default ports of the scheme are not added, so that Trino hands out the nicer looking
    // `https://trino.example.com/...` instead of `https://trino.example.com:443/...`.
    #[case("https://trino.example.com:443", "trino.example.com", "https", None)]
    #[case("http://trino.example.com:80", "trino.example.com", "http", None)]
    #[case(
        "https://5.250.182.203:8443",
        "5.250.182.203:8443",
        "https",
        Some("8443")
    )]
    fn point_forwarded_headers_to_external_trino_endpoint(
        #[case] external_endpoint: &str,
        #[case] expected_host: &str,
        #[case] expected_proto: &str,
        #[case] expected_port: Option<&str>,
    ) {
        let external_endpoint: Url = external_endpoint
            .parse()
            .expect("test case URL is always valid");
        let mut headers = headers_pointing_at_trino_lb();

        point_forwarded_headers_to_trino(&mut headers, Some(&external_endpoint))
            .expect("headers built from a valid URL are always valid");

        assert_eq!(
            headers.get(HOST).expect("host is always set"),
            expected_host
        );
        assert_eq!(
            headers
                .get(X_FORWARDED_HOST)
                .expect("x-forwarded-host is always set"),
            expected_host
        );
        assert_eq!(
            headers
                .get(X_FORWARDED_PROTO)
                .expect("x-forwarded-proto is always set"),
            expected_proto
        );
        assert_eq!(
            headers
                .get(X_FORWARDED_PORT)
                .map(|port| port.to_str().expect("the port is always valid ASCII")),
            expected_port
        );

        // The `Forwarded` header takes precedence over the `X-Forwarded-*` headers, so it must be gone.
        assert_eq!(headers.get(FORWARDED), None);

        // The client address must survive, as Trino uses it for e.g. auditing.
        assert_eq!(
            headers.get("x-forwarded-for").expect("must be kept"),
            "172.30.10.61"
        );
        assert_eq!(
            headers.get("x-real-ip").expect("must be kept"),
            "172.30.10.61"
        );
    }

    /// Without an `externalEndpoint` trino-lb has no clue which address clients can reach Trino at,
    /// so the headers need to be passed through unchanged.
    #[test]
    fn keep_forwarded_headers_without_external_trino_endpoint() {
        let mut headers = headers_pointing_at_trino_lb();

        point_forwarded_headers_to_trino(&mut headers, None)
            .expect("not touching any headers never fails");

        assert_eq!(headers, headers_pointing_at_trino_lb());
    }
}
