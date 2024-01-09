# Example setups

## Simple in-memory load-balancer

This setup is pretty simple and distributes incoming queries across a fixed number of identical Trino clusters.
It can tolerate the failure of queries due to trino-lb restarts.
It is only recommended for development and testing purposes.
When using the in-memory persistence only a single trino-lb instance can be used.

![Simple in-memory load-balancer](./assets/example-setups/simple-in-memory-load-balancer.drawio.svg)

* Distribute queries across all trino cluster evenly
* Queue queries in case all clusters are busy
* No high availability
* In case trino-lb gets restarted all queued and running queries will fail

## HA load-balancer with persistence

This setup is pretty simple and distributes incoming queries across a fixed number of identical Trino clusters.
Unlike the in-memory use-case it is highly available and can tolerate restarts.
It can also be scaled horizontally.

![HA load-balancer with persistence](./assets/example-setups/ha-load-balancer-with-persistence.drawio.svg)

* Distribute queries across all trino cluster evenly
* Queue queries in case all clusters are busy
* High availability both for trino-lb and the underlying persistence (Redis)
* In case trino-lb gets restarted all queued and running queries will continue to run (when at least one trino-lb keeps running)

## HA router with persistence

Fully-fledged setup similar to [HA load-balancer with persistence](#ha-load-balancer-with-persistence), but it also classifies the incoming queries by e.g. interactive/scheduled jobs or the size of the query.

![HA router with persistence](./assets/example-setups/ha-router-with-persistence.drawio.svg)

* Route queries based on certain criteria to a cluster group, distribute the load evenly between the clusters that are part of a cluster group
* Queue queries in case all clusters in the cluster group are busy
* High availability and scaling as well as persistence
