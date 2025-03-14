use std::collections::HashMap;

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
            .context(ResolveGvkSnafu { gvk: trino_gvk })?;

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

            let trino = api
                .get_opt(&cluster.name)
                .await
                .context(ReadTrinoClusterSnafu {
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
            .context(ClusterNotFoundSnafu { cluster })?;

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
            .context(PatchTrinoClusterSnafu {
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
            .context(ClusterNotFoundSnafu { cluster })?;

        let status = cluster
            .api
            .get_status(&cluster.name)
            .instrument(debug_span!("Get Trino cluster status"))
            .await
            .context(GetTrinoClusterStatusSnafu {
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
            .context(StatusFieldMissingInTrinoClusterSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?
            .get("conditions")
            .context(StatusConditionsFieldMissingInTrinoClusterSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?
            .as_array()
            .context(StatusConditionsFieldIsNotArraySnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?;

        let available = conditions
            .iter()
            .find(|c| c.get("type") == Some(&Value::String("Available".to_string())))
            .context(NoAvailableEntryInStatusConditionsListSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            })?;

        let available = available.get("status").context(
            NoStatusInAvailableEntryInStatusConditionsListSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            },
        )?;

        let available = match available {
            Value::String(available) if available == "True" => true,
            Value::String(available) if available == "False" => false,
            _ => StatusNotParsableInAvailableEntryInStatusConditionsListSnafu {
                cluster: &cluster.name,
                namespace: &cluster.namespace,
            }
            .fail()?,
        };

        Ok(available)
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
