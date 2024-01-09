# Stackable autoscaler

This autoscaler auto-scales [Stackable TrinoClusters](https://docs.stackable.tech/home/stable/trino/), which are part of the Stackable Data Platform (SDP).

The SDP is an Open source data platform running on a Kubernetes cluster and offers Infrastructure as Code for Trino and related tools (such as Hive metastore, Superset or Open policy agent).

The autoscaler uses the [`stopped` field on a Stackable TrinoCluster](https://docs.stackable.tech/home/stable/concepts/operations/cluster_operations) to turn the cluster on or off.
A turned off cluster shuts down all Pods entirely (Coordinator + Workers), resulting in no computing costs at all.
