use std::collections::HashSet;

use async_trait::async_trait;
use tracing::{instrument, warn};
use trino_lb_core::{config::TrinoRoutingGroupHeaderRouterConfig, sanitization::Sanitize};

use crate::routing::RouterImplementationTrait;

pub struct TrinoRoutingGroupHeaderRouter {
    config: TrinoRoutingGroupHeaderRouterConfig,
    valid_target_groups: HashSet<String>,
}

impl TrinoRoutingGroupHeaderRouter {
    #[instrument(name = "TrinoRoutingGroupHeaderRouter::new")]
    pub fn new(
        config: &TrinoRoutingGroupHeaderRouterConfig,
        valid_target_groups: HashSet<String>,
    ) -> Self {
        Self {
            config: config.clone(),
            valid_target_groups,
        }
    }
}

#[async_trait]
impl RouterImplementationTrait for TrinoRoutingGroupHeaderRouter {
    #[instrument(
        name = "TrinoRoutingGroupHeaderRouter::route"
        skip(self),
        fields(headers = ?headers.sanitize()),
    )]
    async fn route(&self, query: &str, headers: &http::HeaderMap) -> Option<String> {
        let target_group = headers.get(&self.config.header_name);
        if let Some(target_group) = target_group {
            if let Ok(target_group) = target_group.to_str() {
                if self.valid_target_groups.contains(target_group) {
                    return Some(target_group.to_string());
                } else {
                    // TODO: Maybe let the routers return client errors to the clients in case of user errors.
                    warn!(
                        target_group,
                        "The client requested a target group that does not exist, skipped routing"
                    );
                }
            }
        }

        None
    }
}
