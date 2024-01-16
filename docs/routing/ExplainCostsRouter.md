# ExplainCostsRouter

This router executes an `explain {query}` [EXPLAIN](https://trino.io/docs/current/sql/explain.html?highlight=explain) query for every incoming query.
Trino will respond with an resource estimation the query will consume.
Please note that this functional heavily depends on [Table statistics](https://trino.io/docs/current/optimizer/statistics.html) being present for the access tables to get meaningful estimations.

For this to work trino-lb executes `explain (format json) {query}` and sums up all the resource estimations of the children stages.
This is a very simplistic model and will likely be improved in the future - we are happy about any suggestions on how the estimates should be calculated the best [in the tracking issue](https://github.com/stackabletech/trino-lb/issues/11).

After trino-lb got the query estimation, it walks a list of resource buckets you can specify top to bottom and picks the first one that fulfills all resource requirements (CPU, memory, Network traffic etc.).
If no bucket matches the router will not a make a decision and let the routers further down the chain decide.

> [!WARNING]
> Please keep in mind that trino-lb will determine the target cluster group for every incoming query instantly (otherwise it does not know if it should queued or hand over the query).
> In case a Trino client submits many queries at once this will result in the same number of `explain` queries on the Trino used for the query estimations.
> There is [a issue to address this](https://github.com/stackabletech/trino-lb/issues/10), however until this is resolved it is recommended to have the `ExplainCostsRouter` near the end of the chain and try to classify the queries with a different router.
> However, this is only important if you have more queries/s than a Trino cluster can run `explain` queries for (which hopefully is pretty much).

# Configuration

With this words of caution, lets jump into the configuration.

You need to specify the cluster that executes the `explain` queries. Currently only password based authentication is supported.

```yaml
routers:
  - explainCosts:
      trinoClusterToRunExplainQuery:
        endpoint: https://trino-coordinator-default.default.svc.cluster.local:8443
        ignoreCert: true # optional, defaults to false
        username: admin
        password: adminadmin
      targets:
        - cpuCost: 5E+9
          memoryCost: 5E9 # 5GB
          networkCost: 10E9 # 10GB
          outputRowCount: 1E6
          outputSizeInBytes: 1E9 # 1GB
          trinoClusterGroup: s
        - cpuCost: 5E+12
          memoryCost: 5E12 # 5TB
          networkCost: 5E12 # 5TB
          outputRowCount: 1E9
          outputSizeInBytes: 5E12 # 5TB
          trinoClusterGroup: m
```
