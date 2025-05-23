use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    fs::File,
    path::PathBuf,
    time::Duration,
};

use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use url::Url;

use crate::{TrinoClusterName, trino_query_plan::QueryPlanEstimation};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to read configuration file at {config_file:?}"))]
    ReadConfigFile {
        source: std::io::Error,
        config_file: PathBuf,
    },

    #[snafu(display("Failed to parse configuration file at {config_file:?}"))]
    ParseConfigFile {
        source: serde_yaml::Error,
        config_file: PathBuf,
    },
}

#[derive(Clone, Debug, Deserialize)]
// We want to fail on unknown config properties (as Trino is doing as well) to make the user aware that what he tried to
// configure is not a valid configuration.
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Config {
    pub trino_lb: TrinoLbConfig,

    pub trino_cluster_groups: HashMap<String, TrinoClusterGroupConfig>,

    #[serde(default)]
    pub trino_cluster_groups_ignore_cert: bool,

    pub routers: Vec<RoutingConfig>,

    pub routing_fallback: String,

    pub cluster_autoscaler: Option<ScalerConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoLbConfig {
    pub external_address: Url,

    pub persistence: PersistenceConfig,

    #[serde(default)]
    pub tls: TrinoLbTlsConfig,

    #[serde(
        default = "default_refresh_query_counter_interval",
        with = "humantime_serde"
    )]
    pub refresh_query_counter_interval: Duration,

    pub tracing: Option<TrinoLbTracingConfig>,

    #[serde(default)]
    pub ports: TrinoLbPortsConfig,
}

fn default_refresh_query_counter_interval() -> Duration {
    Duration::from_secs(60)
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoLbTlsConfig {
    #[serde(default)]
    pub enabled: bool,

    pub cert_pem_file: Option<PathBuf>,
    pub key_pem_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoLbTracingConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(rename = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<Url>,

    #[serde(rename = "OTEL_EXPORTER_OTLP_PROTOCOL")]
    pub otlp_protocol: Option<opentelemetry_otlp::Protocol>,

    /// TODO: Ideally [`opentelemetry_otlp::Compression`] would serialize and deserialize to
    /// `gzip`. However, they currently de/ser to `Gzip`, which differs from `opentelemetry_otlp::Compression::from_str`
    /// We should raise an upstream issue
    #[serde(rename = "OTEL_EXPORTER_OTLP_COMPRESSION")]
    pub otlp_compression: Option<opentelemetry_otlp::Compression>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoLbPortsConfig {
    #[serde(default = "TrinoLbPortsConfig::default_http_port")]
    pub http: u16,

    #[serde(default = "TrinoLbPortsConfig::default_https_port")]
    pub https: u16,

    #[serde(default = "TrinoLbPortsConfig::default_metrics_port")]
    pub metrics: u16,
}

impl TrinoLbPortsConfig {
    /// Same port Trino is using by default
    fn default_http_port() -> u16 {
        8080
    }

    /// Same port Trino is using by default
    fn default_https_port() -> u16 {
        8443
    }

    fn default_metrics_port() -> u16 {
        9090
    }
}

impl Default for TrinoLbPortsConfig {
    fn default() -> Self {
        Self {
            http: Self::default_http_port(),
            https: Self::default_https_port(),
            metrics: Self::default_metrics_port(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum PersistenceConfig {
    InMemory {},
    Redis(RedisConfig),
    Postgres(PostgresConfig),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RedisConfig {
    pub endpoint: Url,

    #[serde(default)]
    pub cluster_mode: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PostgresConfig {
    pub url: Url,

    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_max_connections() -> u32 {
    10
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoClusterGroupConfig {
    pub max_running_queries: u64,
    pub autoscaling: Option<TrinoClusterGroupAutoscalingConfig>,
    pub trino_clusters: Vec<TrinoClusterConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoClusterConfig {
    pub name: String,
    pub endpoint: Url,
    pub credentials: TrinoClusterCredentialsConfig,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoClusterCredentialsConfig {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoClusterGroupAutoscalingConfig {
    pub upscale_queued_queries_threshold: u64,
    pub downscale_running_queries_percentage_threshold: u64,
    #[serde(with = "humantime_serde")]
    pub drain_idle_duration_before_shutdown: Duration,
    pub min_clusters: Vec<MinClustersConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MinClustersConfig {
    pub time_utc: String,
    pub weekdays: String,
    pub min: u64,
}

impl Debug for TrinoClusterCredentialsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoClusterCredentialsConfig")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum RoutingConfig {
    ExplainCosts(ExplainCostsRouterConfig),
    TrinoRoutingGroupHeader(TrinoRoutingGroupHeaderRouterConfig),
    PythonScript(PythonScriptRouterConfig),
    ClientTags(ClientTagsRouterConfig),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExplainCostsRouterConfig {
    pub trino_cluster_to_run_explain_query: TrinoClientConfig,

    pub targets: Vec<ExplainCostTargetConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoClientConfig {
    pub endpoint: Url,
    #[serde(default)]
    pub ignore_cert: bool,
    pub username: String,
    pub password: String,
}

impl Debug for TrinoClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoClientConfig")
            .field("endpoint", &self.endpoint)
            .field("ignore_cert", &self.ignore_cert)
            .field("username", &self.username)
            .field("password", &"<redacted>>")
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize)]
// #[serde(flatten)] is not supported in combination with structs that use deny_unknown_fields. Neither the outer nor
// inner flattened struct should use that attribute.
#[serde(rename_all = "camelCase")]
pub struct ExplainCostTargetConfig {
    #[serde(flatten)]
    pub cluster_max_query_plan_estimation: QueryPlanEstimation,
    pub trino_cluster_group: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TrinoRoutingGroupHeaderRouterConfig {
    #[serde(default = "default_trino_routing_group_header")]
    pub header_name: String,
}

fn default_trino_routing_group_header() -> String {
    "X-Trino-Routing-Group".to_string()
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PythonScriptRouterConfig {
    pub script: String,
}

#[derive(Clone, Debug, Deserialize)]
// #[serde(flatten)] is not supported in combination with structs that use deny_unknown_fields. Neither the outer nor
// inner flattened struct should use that attribute.
#[serde(rename_all = "camelCase")]
pub struct ClientTagsRouterConfig {
    #[serde(flatten)]
    pub tag_matching_strategy: TagMatchingStrategy,
    pub trino_cluster_group: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TagMatchingStrategy {
    AllOf(HashSet<String>),
    OneOf(HashSet<String>),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ScalerConfig {
    #[serde(
        with = "humantime_serde",
        default = "default_scaler_reconcile_interval"
    )]
    pub reconcile_interval: Duration,
    #[serde(flatten)]
    pub implementation: ScalerConfigImplementation,
}

fn default_scaler_reconcile_interval() -> Duration {
    Duration::from_secs(5)
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum ScalerConfigImplementation {
    Stackable(StackableScalerConfig),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StackableScalerConfig {
    pub clusters: HashMap<TrinoClusterName, StackableCluster>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StackableCluster {
    pub name: String,
    pub namespace: String,
}

impl Config {
    /// Using [`std::fs::File`] over `tokio::fs::File`, as [`serde_yaml::from_reader`] does not support
    /// async yet (?). Should not matter, as we only read the config once during startup.
    pub async fn read_from_file(config_file: &PathBuf) -> Result<Self, Error> {
        let config_file_content =
            File::open(config_file).context(ReadConfigFileSnafu { config_file })?;

        let deserializer = serde_yaml::Deserializer::from_reader(config_file_content);
        serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
            .context(ParseConfigFileSnafu { config_file })
    }
}
