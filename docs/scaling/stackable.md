# Stackable autoscaler

This autoscaler auto-scales [Stackable TrinoClusters](https://docs.stackable.tech/home/stable/trino/), which are part of the Stackable Data Platform (SDP).

The SDP is an Open source data platform running on a Kubernetes cluster and offers Infrastructure as Code for Trino and related tools (such as Hive metastore, Superset or Open policy agent).

The autoscaler uses the [`stopped` field on a Stackable TrinoCluster](https://docs.stackable.tech/home/stable/concepts/operations/cluster_operations) to turn the cluster on or off.
A turned off cluster shuts down all Pods entirely (Coordinator + Workers), resulting in no computing costs at all.

## Example config
Please have a look at the example config in `example-configs/ha-redis-autoscaling-stackable.yaml` on a working example.
You need to provide at least the following two things:

### Autoscaling config for cluster groups
For every cluster group that should be scaled a `autoscaling` configuration is needed.
For cluster groups that are static (and should not be scaled) you can emit the `autoscaling` configuration.

```yaml
trinoClusterGroups:
  s:
    maxRunningQueries: 3
    autoscaling:
      upscaleQueuedQueriesThreshold: 1
      downscaleRunningQueriesPercentageThreshold: 70
      drainIdleDurationBeforeShutdown: 60s
      minClusters:
        - timeUtc: 00:00:00 - 23:59:59
          weekdays: Mon - Son
          min: 0
    trinoClusters:
      - name: trino-s-1
        endpoint: https://trino-s-1-coordinator-default.default.svc.cluster.local:8443
        credentials: *common-credentials
```

In this case the cluster-group `s` will be started on-demand as it has a minimum cluster count of `0`.

### Stackable autoscaler config
The Stackable autoscaler needs to know for each TrinoCluster the Kubernetes name and namespace of the CustomResource.
By having this information it can enable, disable and check Stackable TrinoClusters.

You need to add the following top-level config:

```yaml
clusterAutoscaler:
  stackable:
    clusters:
      trino-s-1:
        name: trino-s-1
        namespace: default
      trino-m-1:
        name: trino-m-1
        namespace: default
      trino-m-2:
        name: trino-m-2
        namespace: default
```