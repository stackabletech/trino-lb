use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use tracing::{instrument, warn};
use trino_lb_core::sanitization::Sanitize;

use crate::{
    config::{ExplainCostTargetConfig, ExplainCostsRouterConfig},
    routing::RouterImplementationTrait,
    trino_client::{self, TrinoClient},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to create Trino client"))]
    ExtractTrinoHost { source: trino_client::Error },
}

pub struct ExplainCostsRouter {
    config: ExplainCostsRouterConfig,
    trino_client: TrinoClient,
}

impl ExplainCostsRouter {
    #[instrument(name = "ExplainCostsRouter::new")]
    pub fn new(config: &ExplainCostsRouterConfig) -> Result<Self, Error> {
        let trino_client = TrinoClient::new(&config.trino_cluster_to_run_explain_query)
            .context(ExtractTrinoHostSnafu)?;

        Ok(Self {
            config: config.clone(),
            trino_client,
        })
    }
}

#[async_trait]
impl RouterImplementationTrait for ExplainCostsRouter {
    #[instrument(
        name = "ExplainCostsRouter::route"
        skip(self),
        fields(headers = ?headers.sanitize()),
    )]
    async fn route(&self, query: &str, headers: &http::HeaderMap) -> Option<String> {
        let query_estimation = match self.trino_client.query_estimation(query, headers).await {
            Ok(query_estimation) => query_estimation,
            Err(error) => {
                warn!(query, ?error, "Query estimation failed, skipped routing");
                return None;
            }
        };

        for ExplainCostTargetConfig {
            cluster_max_query_plan_estimation,
            trino_cluster_group,
        } in &self.config.targets
        {
            if query_estimation.smaller_in_all_measurements(cluster_max_query_plan_estimation) {
                return Some(trino_cluster_group.clone());
            }
        }

        warn!(
            %query_estimation,
            "The query estimates where bigger than any clusterGroup can handle"
        );

        None
    }
}
