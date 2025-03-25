use std::collections::HashMap;

use chrono::{DateTime, Utc};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::{
    Api, Client, Discovery,
    api::{Patch, PatchParams},
    core::{DynamicObject, GroupVersionKind},
};
use serde_json::Value;
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{Instrument, debug_span, instrument};
use trino_lb_core::{
    TrinoClusterName,
    config::{StackableScalerConfig, TrinoClusterGroupConfig},
};

use super::ScalerTrait;

const K8S_FIELD_MANAGER: &str = "trino-lb";
const MIN_READY_SECONDS_SINCE_LAST_TRANSITION: i64 = 5;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to create Kubernetes client"))]
    CreateClient { source: kube::Error },

    #[snafu(display("Failed to run Kubernetes discovery"))]
    RunDiscovery { source: kube::Error },

    #[snafu(display("Failed to resolve Kubernetes GroupVersionKind {gvk:?}"))]
    ResolveGvk { gvk: GroupVersionKind },

    #[snafu(display("Failed to list Trino clusters"))]
    ListTrinoClusters { source: kube::Error },

    #[snafu(display("Failed to read Trino cluster {cluster:?} in namespace {namespace:?}"))]
    ReadTrinoCluster {
        source: kube::Error,
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display("Failed to patch Trino cluster {cluster:?} in namespace {namespace:?}"))]
    PatchTrinoCluster {
        source: kube::Error,
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display(
        "Failed to get status of Trino cluster {cluster:?} in namespace {namespace:?}"
    ))]
    GetTrinoClusterStatus {
        source: kube::Error,
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display(
        "The status field is missing for the Trino cluster {cluster:?} in namespace {namespace:?}"
    ))]
    StatusFieldMissingInTrinoCluster {
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display(
        "The status.conditions field is missing for the Trino cluster {cluster:?} in namespace {namespace:?}"
    ))]
    StatusConditionsFieldMissingInTrinoCluster {
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display(
        "The status.conditions field is not an array for the Trino cluster {cluster:?} in namespace {namespace:?}"
    ))]
    StatusConditionsFieldIsNotArray {
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display(
        "No \"Available\" entry in the status.conditions list for the Trino cluster {cluster:?} in namespace {namespace:?}"
    ))]
    NoAvailableEntryInStatusConditionsList {
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display(
        "The \"Available\" entry in the status.conditions list has no \"status\" field for the Trino cluster {cluster:?} in namespace {namespace:?}"
    ))]
    NoStatusInAvailableEntryInStatusConditionsList {
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display(
        "The \"status\" field of \"Available\" entry in the status.conditions list can not be parsed, as it must be either \"True\" or \"False\" for the Trino cluster {cluster:?} in namespace {namespace:?}"
    ))]
    StatusNotParsableInAvailableEntryInStatusConditionsList {
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display(
        "The Trino cluster {cluster:?} has no information on how to be scaled, as it is missing from the Stackable clusterAutoscaler list"
    ))]
    ClusterWithNoScalingInformation { cluster: TrinoClusterName },

    #[snafu(display("The Trino cluster {cluster:?} in namespace {namespace:?} was not found"))]
    TrinoClusterNotFound {
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display("Cluster {cluster:?} not found in the autoscaling configuration"))]
    ClusterNotFound { cluster: TrinoClusterName },

    #[snafu(display(
        "Failed to read the \"stopped\" field of Trino cluster {cluster:?} in namespace {namespace:?}"
    ))]
    ReadTrinoClusterStoppedField {
        cluster: TrinoClusterName,
        namespace: String,
    },

    #[snafu(display(
        "Could not parse the lastTransitionTime {last_transition_time:?} for the Trino cluster {cluster:?} in namespace {namespace:?}"
    ))]
    ParseLastTransitionTime {
        source: serde_json::Error,
        last_transition_time: Value,
        cluster: TrinoClusterName,
        namespace: String,
    },
}

pub struct StackableScaler {
    clusters: HashMap<String, StackableTrinoCluster>,
}

struct StackableTrinoCluster {
    /// This name does not necessarily need to be the same name the cluster has in trino-lb!
    name: String,

    namespace: String,

    /// [`Api`] with the correct namespace
    api: Api<DynamicObject>,
}

impl StackableScaler {
    // Intentionally including the config here, this is only logged on startup
    #[instrument(name = "StackableScaler::new")]
    pub async fn new(
        config: &StackableScalerConfig,
        trino_cluster_groups: &HashMap<String, TrinoClusterGroupConfig>,
    ) -> Result<Self, Error> {
        let client = Client::try_default().await.context(CreateClientSnafu)?;
        let discovery = Discovery::new(client.clone())
            .filter(&["trino.stackable.tech"])
            .run()
            .await
            .context(RunDiscoverySnafu)?;

        let trino_gvk = GroupVersionKind {
            group: "trino.stackable.tech".to_owned(),
            version: "v1alpha1".to_owned(),
            kind: "TrinoCluster".to_owned(),
        };
        let (trino_resource, _) = discovery
            .resolve_gvk(&trino_gvk)
            .with_context(|| ResolveGvkSnafu { gvk: trino_gvk })?;

        for cluster in trino_cluster_groups
            .values()
            .filter(|c| c.autoscaling.is_some())
            .flat_map(|g| &g.trino_clusters)
        {
            if !config.clusters.contains_key(&cluster.name) {
                ClusterWithNoScalingInformationSnafu {
                    cluster: &cluster.name,
                }
                .fail()?;
            }
        }

        let mut clusters = HashMap::with_capacity(config.clusters.len());

        // TODO: Await in parallel to reduce startup times
        #[allow(clippy::for_kv_map)]
        for (cluster_name, cluster) in &config.clusters {
            // TODO check that _cluster_name exists in trino_cluster_groups

            let api: Api<DynamicObject> =
                Api::namespaced_with(client.clone(), &cluster.namespace, &trino_resource);

            let trino =
                api.get_opt(&cluster.name)
                    .await
                    .with_context(|_| ReadTrinoClusterSnafu {
                        cluster: &cluster.name,
                        namespace: &cluster.namespace,
                    })?;

            if trino.is_none() {
                TrinoClusterNotFoundSnafu {
                    cluster: &cluster.name,
                    namespace: &cluster.namespace,
                }
                .fail()?;
            }

            clusters.insert(
                cluster_name.to_owned(),
                StackableTrinoCluster {
                    name: cluster.name.to_owned(),
                    namespace: cluster.namespace.to_owned(),
                    api,
                },
            );
        }

        Ok(StackableScaler { clusters })
    }

    #[instrument(skip(self))]
    async fn set_activation(&self, cluster: &TrinoClusterName, active: bool) -> Result<(), Error> {
        let cluster = self
            .clusters
            .get(cluster)
            .with_context(|| ClusterNotFoundSnafu { cluster })?;

        let patch = serde_json::json!({
            "apiVersion": "trino.stackable.tech/v1alpha1",
            "kind": "TrinoCluster",
            "spec": {
                "clusterOperation": {
                    "stopped": !active,
                }
            }
        });
        let params = PatchParams::apply(K8S_FIELD_MANAGER)
            // Sorry, we need to force here, as we otherwise get
            // StackableError { source: PatchTrinoCluster { source: Api(ErrorResponse { status: "Failure", message: "Apply failed with 1 conflict: conflict with \"kubectl-client-side-apply\" using trino.stackable.tech/v1alpha1: .spec.clusterOperation.stopped", reason: "Conflict", code: 409 }), cluster: "trino-s-1", namespace: "default" } }
            .force();
        let patch = Patch::Apply(&patch);

        cluster
            .api
            .patch(&cluster.name, &params, &patch)
            .instrument(debug_span!("Patching Trino cluster"))
            .await
            .with_context(|_| PatchTrinoClusterSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?;

        Ok(())
    }
}

impl ScalerTrait for StackableScaler {
    #[instrument(name = "StackableScaler::activate", skip(self))]
    async fn activate(&self, cluster: &TrinoClusterName) -> Result<(), super::Error> {
        Ok(self.set_activation(cluster, true).await?)
    }

    #[instrument(name = "StackableScaler::deactivate", skip(self))]
    async fn deactivate(&self, cluster: &TrinoClusterName) -> Result<(), super::Error> {
        Ok(self.set_activation(cluster, false).await?)
    }

    #[instrument(name = "StackableScaler::is_ready", skip(self))]
    async fn is_ready(&self, cluster: &TrinoClusterName) -> Result<bool, super::Error> {
        let cluster = self
            .clusters
            .get(cluster)
            .with_context(|| ClusterNotFoundSnafu { cluster })?;

        let status = cluster
            .api
            .get_status(&cluster.name)
            .instrument(debug_span!("Get Trino cluster status"))
            .await
            .with_context(|_| GetTrinoClusterStatusSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?;

        // I would prefer switching to using https://docs.rs/k8s-openapi/latest/k8s_openapi/apimachinery/pkg/apis/meta/v1/struct.Condition.html
        // for parsing. Sadly the Stackable Condition is not compatible with the k8s-openapi Condition struct, so I
        // created https://github.com/stackabletech/issues/issues/489. The branch refactor/conditions-parsing has an
        // (non-working) example code, which can be used after the Issues is resolved

        let conditions = status
            .data
            .get("status")
            .with_context(|| StatusFieldMissingInTrinoClusterSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?
            .get("conditions")
            .with_context(|| StatusConditionsFieldMissingInTrinoClusterSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?
            .as_array()
            .with_context(|| StatusConditionsFieldIsNotArraySnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?;

        let available = conditions
            .iter()
            .find(|c| c.get("type") == Some(&Value::String("Available".to_string())))
            .with_context(|| NoAvailableEntryInStatusConditionsListSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?;

        let status = available.get("status").with_context(|| {
            NoStatusInAvailableEntryInStatusConditionsListSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            }
        })?;

        let is_available = match status {
            Value::String(status) if status == "True" => true,
            Value::String(status) if status == "False" => false,
            _ => StatusNotParsableInAvailableEntryInStatusConditionsListSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            }
            .fail()?,
        };

        // Return early if the cluster is not available
        if !is_available {
            return Ok(false);
        }

        // Careful investigation has shown that trino-lb can quickly react to TrinoClusters coming
        // available. When trying to immediately send queries to Trino we encountered errors such as:
        //
        // WARN trino_lb::http_server::v1::statement: Error while processing request
        // error=SendQueryToTrino { source: ContactTrinoPostQuery { source: reqwest::Error { kind: Request,
        // url: "https://trino-m-1-coordinator-default.default.svc.cluster.local:8443/v1/statement",
        // source: hyper_util::client::legacy::Error(Connect, ConnectError("dns error", Custom { kind: Uncategorized,
        // error: "failed to lookup address information: Name or service not known" })) } } }
        //
        // This is because the coordinator is ready but it might take some additional time for the
        // DNS record of the Service to propagate.
        // To prevent that we only consider TrinoClusters healthy if a minimum amount of seconds
        // passed after it was marked as healthy.

        // It's valid for the lastTransitionTime to be not set, we assume the cluster is old in this
        // case
        if let Some(last_transition_time) = available.get("lastTransitionTime") {
            let last_transition_time: Time = serde_json::from_value(last_transition_time.clone())
                .with_context(|_| ParseLastTransitionTimeSnafu {
                last_transition_time: last_transition_time.clone(),
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?;

            let seconds_since_last_transition = elapsed_seconds_since(last_transition_time.0);
            if seconds_since_last_transition < MIN_READY_SECONDS_SINCE_LAST_TRANSITION {
                tracing::debug!(
                    seconds_since_last_transition,
                    min_ready_seconds_since_last_transition =
                        MIN_READY_SECONDS_SINCE_LAST_TRANSITION,
                    "The trino cluster recently turned ready, not marking as ready yet"
                );

                return Ok(false);
            }
        }

        // All checks succeeded, TrinoCluster is ready
        Ok(true)
    }

    #[instrument(name = "StackableScaler::is_activated", skip(self))]
    async fn is_activated(&self, cluster: &TrinoClusterName) -> Result<bool, super::Error> {
        let cluster = self
            .clusters
            .get(cluster)
            .context(ClusterNotFoundSnafu { cluster })?;

        let stackable_cluster = cluster
            .api
            .get(&cluster.name)
            .instrument(debug_span!("Getting Trino cluster"))
            .await
            .context(ReadTrinoClusterSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?;

        Ok(!stackable_cluster
            .data
            .get("spec")
            .and_then(|status| status.get("clusterOperation"))
            .and_then(|cluster_config| cluster_config.get("stopped"))
            .and_then(|stopped| stopped.as_bool())
            .context(ReadTrinoClusterStoppedFieldSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?)
    }
}

fn elapsed_seconds_since(datetime: DateTime<Utc>) -> i64 {
    let now = Utc::now();
    (now - datetime).num_seconds()
}
