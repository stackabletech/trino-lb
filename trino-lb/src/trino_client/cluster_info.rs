use reqwest::header;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use tracing::instrument;
use trino_lb_core::config::TrinoClusterCredentialsConfig;
use url::Url;
use urlencoding::encode;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to construct http client"))]
    ConstructHttpClient { source: reqwest::Error },

    #[snafu(display("Failed to join UI login path onto trino endpoint {trino_endpoint}"))]
    JoinUiLoginPathToTrinoEndpoint {
        source: url::ParseError,
        trino_endpoint: Url,
    },

    #[snafu(display("Failed to parse clusterInfo json response"))]
    ParseClusterInfoResponse { source: reqwest::Error },

    #[snafu(display("Failed to log into Trino cluster using endpoint {login_endpoint}"))]
    LogIntoTrinoCluster {
        source: reqwest::Error,
        login_endpoint: Url,
    },

    #[snafu(display(
        "Failed to retrieve stats from Trino cluster using endpoint {stats_endpoint}"
    ))]
    RetrieveStatsFromTrinoCluster {
        source: reqwest::Error,
        stats_endpoint: Url,
    },
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfo {
    pub running_queries: u64,
    pub blocked_queries: u64,
    pub queued_queries: u64,
    pub active_coordinators: u64,
    pub active_workers: u64,
    pub running_drivers: u64,
    pub total_available_processors: u64,
    pub reserved_memory: f32,
    pub total_input_rows: u64,
    pub total_input_bytes: u64,
    pub total_cpu_time_secs: u64,
}

#[instrument(skip(credentials))]
pub async fn get_cluster_info(
    endpoint: &Url,
    ignore_certs: bool,
    credentials: &TrinoClusterCredentialsConfig,
) -> Result<ClusterInfo, Error> {
    // We create a new client here every time just to be sure we don't accidentally leak the cookie store to a different
    // connection.
    let client = reqwest::Client::builder()
        .cookie_store(true)
        .danger_accept_invalid_certs(ignore_certs)
        .build()
        .context(ConstructHttpClientSnafu)?;

    let login_endpoint =
        endpoint
            .join("ui/login")
            .context(JoinUiLoginPathToTrinoEndpointSnafu {
                trino_endpoint: endpoint.clone(),
            })?;
    client
        .post(login_endpoint.clone())
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(login_body(credentials))
        .send()
        .await
        .context(LogIntoTrinoClusterSnafu { login_endpoint })?;

    let stats_endpoint =
        endpoint
            .join("ui/api/stats")
            .context(JoinUiLoginPathToTrinoEndpointSnafu {
                trino_endpoint: endpoint.clone(),
            })?;
    let response = client
        .get(stats_endpoint.clone())
        .send()
        .await
        .context(RetrieveStatsFromTrinoClusterSnafu { stats_endpoint })?;

    response.json().await.context(ParseClusterInfoResponseSnafu)
}

fn login_body(credentials: &TrinoClusterCredentialsConfig) -> String {
    format!(
        "username={}&password={}&redirectPath=",
        encode(&credentials.username),
        encode(&credentials.password),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    #[rstest]
    #[case("admin", "admin", "username=admin&password=admin&redirectPath=")]
    #[case(
        "foo",
        "superSecure!1&2;3",
        "username=foo&password=superSecure%211%262%3B3&redirectPath="
    )]
    fn test_login_body(
        #[case] username: String,
        #[case] password: String,
        #[case] expected: String,
    ) {
        let credentials = TrinoClusterCredentialsConfig { username, password };

        assert_eq!(login_body(&credentials), expected);
    }
}
