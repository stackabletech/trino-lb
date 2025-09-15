use http::HeaderMap;
use prusto::{Client, ClientBuilder, DataSet, auth::Auth};
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::instrument;
use trino_lb_core::{
    sanitization::Sanitize,
    trino_query_plan::{ExplainQueryResult, QueryPlan, QueryPlanEstimation},
};
use url::Url;

pub use crate::trino_client::cluster_info::{ClusterInfo, get_cluster_info};
use crate::{config::TrinoClientConfig, trino_client::workarounds::query_estimation_workarounds};

mod cluster_info;
mod workarounds;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to extract Trino host from URL {url}"))]
    ExtractTrinoHost { url: Url },

    #[snafu(display("Failed to extract Trino port from URL {url}"))]
    ExtractTrinoPort { url: Url },

    #[snafu(display("Failed to create Trino client"))]
    CreateTrinoClient {
        #[snafu(source(from(prusto::error::Error, Box::new)))]
        source: Box<prusto::error::Error>,
    },

    #[snafu(display("Failed to create HTTP client"))]
    CreateHttpClient { source: reqwest::Error },

    #[snafu(display("Failed to execute explain query {explain_query:?}"))]
    ExecuteExplainQuery {
        #[snafu(source(from(prusto::error::Error, Box::new)))]
        source: Box<prusto::error::Error>,
        explain_query: String,
    },

    #[snafu(display("Failed to extract query plan from query_plan {query_plan:?}"))]
    ExtractQueryPlan {
        query_plan: DataSet<ExplainQueryResult>,
    },

    #[snafu(display("Failed to parse query plan {query_plan:?}"))]
    ParseQueryPlan {
        source: serde_json::Error,
        query_plan: String,
    },

    #[snafu(display("Failed to construct Trino API path"))]
    ConstructTrinoApiPath { source: url::ParseError },

    #[snafu(display("Failed to contact Trino API to post query"))]
    ContactTrinoPostQuery { source: reqwest::Error },

    #[snafu(display("Failed to decode Trino API response"))]
    DecodeTrinoResponse { source: reqwest::Error },
}

pub struct TrinoClient {
    config: TrinoClientConfig,
    client: Client,
}

impl TrinoClient {
    pub fn new(config: &TrinoClientConfig) -> Result<Self, Error> {
        let client = trino_client_builder_from_config(config)?
            .build()
            .context(CreateTrinoClientSnafu)?;

        Ok(Self {
            config: config.clone(),
            client,
        })
    }
}

impl TrinoClient {
    // The user can issues queries such as "show tables" or "select * from foo", which need catalog and schema information
    // We need to grab these from the headers and pass them to the "explain <user-query>" query as well
    // to not run into errors.
    #[instrument(
        skip(self),
        fields(client_headers = ?client_headers.sanitize())
    )]
    pub async fn query_estimation(
        &self,
        query: &str,
        client_headers: &HeaderMap,
    ) -> Result<QueryPlanEstimation, Error> {
        let explain_query = format!("explain (format json) {query}");
        if let Some(query_estimation_workarounds) = query_estimation_workarounds(query) {
            // We found a workaround, let's return that
            return Ok(query_estimation_workarounds);
        }

        let trino_catalog = client_headers
            .get("x-trino-catalog")
            .and_then(|header| header.to_str().ok());
        let trino_schema = client_headers
            .get("x-trino-schema")
            .and_then(|header| header.to_str().ok());

        let query_plan: DataSet<ExplainQueryResult> =
            if trino_catalog.is_none() && trino_schema.is_none() {
                self.client
                    .get_all(explain_query.clone())
                    .await
                    .context(ExecuteExplainQuerySnafu { explain_query })?
            } else {
                // In this case we sadly need to build a custom client that is set up to use the specific
                // catalog and schema :/

                let mut client_builder = trino_client_builder_from_config(&self.config)?;
                if let Some(trino_catalog) = trino_catalog {
                    client_builder = client_builder.catalog(trino_catalog);
                }
                if let Some(trino_schema) = trino_schema {
                    client_builder = client_builder.schema(trino_schema);
                }
                let client = client_builder.build().context(CreateTrinoClientSnafu)?;

                client
                    .get_all(explain_query.clone())
                    .await
                    .context(ExecuteExplainQuerySnafu { explain_query })?
            };

        let query_plan = &query_plan
            .as_slice()
            .first()
            .context(ExtractQueryPlanSnafu {
                query_plan: query_plan.clone(),
            })?
            .query_plan;
        let query_plan: QueryPlan =
            serde_json::from_str(query_plan).context(ParseQueryPlanSnafu { query_plan })?;

        Ok(query_plan.total_estimates())
    }
}

#[instrument(skip(config))]
fn trino_client_builder_from_config(config: &TrinoClientConfig) -> Result<ClientBuilder, Error> {
    Ok(ClientBuilder::new(
        &config.username,
        config.endpoint.host().context(ExtractTrinoHostSnafu {
            url: config.endpoint.clone(),
        })?,
    )
    .port(
        config
            .endpoint
            .port_or_known_default()
            .context(ExtractTrinoPortSnafu {
                url: config.endpoint.clone(),
            })?,
    )
    .secure(config.endpoint.scheme() == "https")
    .no_verify(config.ignore_cert)
    .auth(Auth::Basic(
        config.username.to_owned(),
        Some(config.password.to_owned()),
    )))
}
