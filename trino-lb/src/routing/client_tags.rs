use std::collections::HashSet;

use http::HeaderValue;
use snafu::Snafu;
use tracing::{instrument, warn};
use trino_lb_core::{
    config::{ClientTagsRouterConfig, TagMatchingStrategy},
    sanitization::Sanitize,
};

use crate::routing::RouterImplementationTrait;

const TRINO_CLIENT_TAGS_HEADER: &str = "x-trino-client-tags";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(
        "Configuration error: The configured target cluster group {cluster_group} does not exist"
    ))]
    TargetClusterGroupNotFound { cluster_group: String },
}

pub struct ClientTagsRouter {
    config: ClientTagsRouterConfig,
}

impl ClientTagsRouter {
    // Intentionally including the config here, this is only logged on startup
    #[instrument(name = "ClientTagsRouter::new")]
    pub fn new(
        config: &ClientTagsRouterConfig,
        valid_target_groups: HashSet<String>,
    ) -> Result<Self, Error> {
        if !valid_target_groups.contains(&config.trino_cluster_group) {
            TargetClusterGroupNotFoundSnafu {
                cluster_group: &config.trino_cluster_group,
            }
            .fail()?;
        }

        Ok(Self {
            config: config.clone(),
        })
    }
}

impl RouterImplementationTrait for ClientTagsRouter {
    #[instrument(
        name = "ClientTagHeadersRouter::route"
        skip(self),
        fields(headers = ?headers.sanitize()),
    )]
    async fn route(&self, query: &str, headers: &http::HeaderMap) -> Option<String> {
        if let Some(Ok(client_tags)) = headers
            .get(TRINO_CLIENT_TAGS_HEADER)
            .map(HeaderValue::to_str)
        {
            let client_tags = client_tags
                .split(',')
                .map(String::from)
                .collect::<HashSet<_>>();
            match &self.config.tag_matching_strategy {
                TagMatchingStrategy::OneOf(one_of) => {
                    if !one_of.is_disjoint(&client_tags) {
                        return Some(self.config.trino_cluster_group.clone());
                    }
                }
                TagMatchingStrategy::AllOf(all_of) => {
                    if all_of.is_subset(&client_tags) {
                        return Some(self.config.trino_cluster_group.clone());
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use http::{HeaderMap, HeaderName};
    use rstest::rstest;

    #[rstest]
    #[case(None, None)]
    #[case(Some("foo"), Some("my-target"))]
    #[case(Some("bar"), Some("my-target"))]
    #[case(Some("bak"), Some("my-target"))]
    #[case(Some("system=airflow"), Some("my-target"))]
    #[case(Some("foo,bar,bak,system=airflow"), Some("my-target"))]
    #[case(Some("bak,foo,bar,system=airflow"), Some("my-target"))]
    #[case(Some("foo,bar,bak,something-else"), Some("my-target"))]
    #[tokio::test]
    async fn test_routing_with_one_of(
        #[case] x_trino_client_tags: Option<&str>,
        #[case] expected: Option<&str>,
    ) {
        let config = serde_yaml::from_str(
            r#"
            trinoClusterGroup: my-target
            oneOf: ["foo", "bar", "bak", "system=airflow"]
        "#,
        )
        .unwrap();
        let router = ClientTagsRouter::new(&config, HashSet::from(["my-target".to_string()]))
            .expect("Failed to create ClientTagsRouter");
        let mut headers = HeaderMap::new();

        if let Some(x_trino_client_tags) = x_trino_client_tags {
            headers.insert(
                HeaderName::from_static("x-trino-client-tags"),
                x_trino_client_tags
                    .parse()
                    .expect("Failed to create x-trino-client-tags header"),
            );
        }

        assert_eq!(router.route("", &headers).await.as_deref(), expected);
    }

    #[rstest]
    #[case(None, None)]
    #[case(Some("foo"), None)]
    #[case(Some("bar"), None)]
    #[case(Some("bak"), None)]
    #[case(Some("foo,bar,bak"), None)]
    #[case(Some("foo,bar,bak,system=airflow"), Some("my-target"))]
    #[case(Some("bak,foo,bar,system=airflow"), Some("my-target"))]
    #[case(Some("foo,bar,bak,system=airflow,something-else"), Some("my-target"))]
    #[tokio::test]
    async fn test_routing_with_all_of(
        #[case] x_trino_client_tags: Option<&str>,
        #[case] expected: Option<&str>,
    ) {
        let config = serde_yaml::from_str(
            r#"
            trinoClusterGroup: my-target
            allOf: ["foo", "bar", "bak", "system=airflow"]
        "#,
        )
        .unwrap();
        let router = ClientTagsRouter::new(&config, HashSet::from(["my-target".to_string()]))
            .expect("Failed to create ClientTagsRouter");
        let mut headers = HeaderMap::new();

        if let Some(x_trino_client_tags) = x_trino_client_tags {
            headers.insert(
                HeaderName::from_static("x-trino-client-tags"),
                x_trino_client_tags
                    .parse()
                    .expect("Failed to create x-trino-client-tags header"),
            );
        }

        assert_eq!(router.route("", &headers).await.as_deref(), expected);
    }

    /// Seems like whatever comes first takes precedence
    #[test]
    fn test_configuring_one_of_and_all_of() {
        let config: ClientTagsRouterConfig = serde_yaml::from_str(
            r#"
            trinoClusterGroup: my-target
            allOf: ["allOf"]
            oneOf: ["oneOf"]
        "#,
        )
        .unwrap();
        assert!(matches!(
            config.tag_matching_strategy,
            TagMatchingStrategy::AllOf(_)
        ));

        let config: ClientTagsRouterConfig = serde_yaml::from_str(
            r#"
            trinoClusterGroup: my-target
            oneOf: ["oneOf"]
            allOf: ["allOf"]
        "#,
        )
        .unwrap();
        assert!(matches!(
            config.tag_matching_strategy,
            TagMatchingStrategy::OneOf(_)
        ));
    }
}
