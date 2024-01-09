use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use snafu::{ResultExt, Snafu};
use tracing::instrument;
use trino_lb_core::sanitization::Sanitize;

use crate::config::{Config, RoutingConfig};

mod explain_costs;
mod python_script;
mod trino_routing_group_header;

pub use explain_costs::ExplainCostsRouter;
pub use python_script::PythonScriptRouter;
pub use trino_routing_group_header::TrinoRoutingGroupHeaderRouter;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to create explain costs router"))]
    CreateExplainCostsRouter { source: explain_costs::Error },

    #[snafu(display("Failed to create python script router"))]
    CreatePythonScriptRouter { source: python_script::Error },

    #[snafu(display("Configuration error: The router {router:?} is configured to route to trinoClusterGroup {trino_cluster_group:?} which does not exist"))]
    ConfigErrorClusterGroupDoesNotExist {
        router: String,
        trino_cluster_group: String,
    },

    #[snafu(display("Configuration error: The routingFallback is configured to route to trinoClusterGroup {routing_fallback:?} which does not exist"))]
    ConfigErrorRoutingFallbackDoesNotExist { routing_fallback: String },
}

pub struct Router {
    routers: Vec<RoutingImplementation>,
    routing_fallback: String,
}

impl Router {
    #[instrument]
    pub fn new(config: &Config) -> Result<Self, Error> {
        let mut routers = Vec::with_capacity(config.routers.len());
        let cluster_groups = &config.trino_cluster_groups.keys().collect::<Vec<_>>();

        for router in &config.routers {
            let router = match router {
                RoutingConfig::ExplainCosts(router_config) => {
                    let targets = router_config.targets.iter().map(|t| &t.trino_cluster_group);
                    check_every_target_group_exists(targets, cluster_groups, "ExplainCostsRouter")?;

                    ExplainCostsRouter::new(router_config)
                        .context(CreateExplainCostsRouterSnafu)?
                        .into()
                }
                RoutingConfig::TrinoRoutingGroupHeader(router_config) => {
                    TrinoRoutingGroupHeaderRouter::new(
                        router_config,
                        config.trino_cluster_groups.keys().cloned().collect(),
                    )
                    .into()
                }
                RoutingConfig::PythonScript(router_config) => PythonScriptRouter::new(
                    router_config,
                    config.trino_cluster_groups.keys().cloned().collect(),
                )
                .context(CreatePythonScriptRouterSnafu)?
                .into(),
            };
            routers.push(router);
        }

        if !cluster_groups.contains(&&config.routing_fallback) {
            ConfigErrorRoutingFallbackDoesNotExistSnafu {
                routing_fallback: config.routing_fallback.clone(),
            }
            .fail()?;
        }

        Ok(Self {
            routers,
            routing_fallback: config.routing_fallback.clone(),
        })
    }

    #[instrument(
        skip(self),
        fields(headers = ?headers.sanitize()),
    )]
    pub async fn get_target_cluster_group(
        &self,
        query: &String,
        headers: &http::HeaderMap,
    ) -> String {
        for router in &self.routers {
            if let Some(target_cluster_group) = router.route(query, headers).await {
                return target_cluster_group;
            }
        }

        self.routing_fallback.clone()
    }
}

#[async_trait]
#[enum_dispatch(RoutingImplementation)]
pub trait RouterImplementationTrait {
    /// The router will be asked to make a decision for the queued query. It can either return
    /// the target clusterGroup the query should be places on or [`None`] in case it does not
    /// have an opinion.
    async fn route(&self, query: &str, headers: &http::HeaderMap) -> Option<String>;
}

#[enum_dispatch]
pub enum RoutingImplementation {
    ExplainCosts(ExplainCostsRouter),
    TrinoRoutingGroupHeader(TrinoRoutingGroupHeaderRouter),
    PythonScript(PythonScriptRouter),
}

#[instrument(skip(targets))]
fn check_every_target_group_exists<'a>(
    mut targets: impl Iterator<Item = &'a String>,
    cluster_groups: &[&String],
    router: &str,
) -> Result<(), Error> {
    let target_without_group = targets.find(|group| !cluster_groups.contains(group));

    if let Some(target_without_group) = target_without_group {
        ConfigErrorClusterGroupDoesNotExistSnafu {
            router,
            trino_cluster_group: target_without_group,
        }
        .fail()?;
    }

    Ok(())
}
