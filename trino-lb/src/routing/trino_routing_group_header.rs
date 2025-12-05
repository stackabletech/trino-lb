use std::collections::HashSet;

use tracing::{instrument, warn};
use trino_lb_core::{config::TrinoRoutingGroupHeaderRouterConfig, sanitization::Sanitize};

use crate::routing::RouterImplementationTrait;

pub struct TrinoRoutingGroupHeaderRouter {
    config: TrinoRoutingGroupHeaderRouterConfig,
    valid_target_groups: HashSet<String>,
}

impl TrinoRoutingGroupHeaderRouter {
    // Intentionally including the config here, this is only logged on startup
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

impl RouterImplementationTrait for TrinoRoutingGroupHeaderRouter {
    #[instrument(
        name = "TrinoRoutingGroupHeaderRouter::route"
        skip(self),
        fields(headers = ?headers.sanitize()),
    )]
    async fn route(&self, query: &str, headers: &http::HeaderMap) -> Option<String> {
        let target_group = headers.get(&self.config.header_name);
        if let Some(target_group) = target_group
            && let Ok(target_group) = target_group.to_str()
        {
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

        None
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderName, HeaderValue};
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(None)]
    #[case(Some("foo"))]
    #[case(Some("bar,bak"))]
    #[tokio::test]
    async fn test_standard_header(#[case] x_trino_routing_group: Option<&str>) {
        let config = serde_yaml::from_str("").unwrap();
        let valid_target_groups = HashSet::from(["foo".to_string(), "bar,bak".to_string()]);
        let router = TrinoRoutingGroupHeaderRouter::new(&config, valid_target_groups);

        let mut headers = HeaderMap::new();
        if let Some(x_trino_routing_group) = x_trino_routing_group {
            headers.insert(
                HeaderName::from_static("x-trino-routing-group"),
                HeaderValue::from_str(x_trino_routing_group).unwrap(),
            );
        }

        assert_eq!(
            router.route("", &headers).await.as_deref(),
            x_trino_routing_group
        );
    }

    #[rstest]
    #[case("x-trino-routing-group", None)]
    #[case("x-trino-routing-group", Some("foo"))]
    #[case("x-trino-routing-group", Some("bar,bak"))]
    #[case("custom-header", None)]
    #[case("custom-header", Some("foo"))]
    #[case("custom-header", Some("bar,bak"))]
    #[tokio::test]
    async fn test_custom_header(
        #[case] header_name: String,
        #[case] x_trino_routing_group: Option<&str>,
    ) {
        let config = TrinoRoutingGroupHeaderRouterConfig {
            header_name: header_name.clone(),
        };
        let valid_target_groups = HashSet::from(["foo".to_string(), "bar,bak".to_string()]);
        let router = TrinoRoutingGroupHeaderRouter::new(&config, valid_target_groups);

        let mut headers = HeaderMap::new();
        if let Some(x_trino_routing_group) = x_trino_routing_group {
            headers.insert(
                HeaderName::from_bytes(header_name.as_bytes()).unwrap(),
                HeaderValue::from_str(x_trino_routing_group).unwrap(),
            );
        }

        assert_eq!(
            router.route("", &headers).await.as_deref(),
            x_trino_routing_group
        );
    }

    #[tokio::test]
    async fn test_target_group_does_not_exist() {
        let config = serde_yaml::from_str("").unwrap();
        let valid_target_groups = HashSet::from(["foo".to_string()]);
        let router = TrinoRoutingGroupHeaderRouter::new(&config, valid_target_groups);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-trino-routing-group"),
            HeaderValue::from_str("does not exist").unwrap(),
        );

        // Currently we don't raise any error to the user and just ignore this request. This might change in the future.
        assert_eq!(router.route("", &headers).await, None);
    }
}
