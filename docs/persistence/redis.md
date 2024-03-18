# Redis persistence

The Redis persistence currently supports either a single-instance Redis instance or a [Redis Cluster](https://redis.io/docs/management/scaling/) as a distributed key-value store.

The Redis persistence is the most tested implementation and is currently the recommended choice for production systems.

## Configuration

You only need to provide the endpoint the Redis cluster is available at.
Authentication is supported by providing the password in the endpoint (the username is set to an empty string).

The following configuration connects to the Redis cluster running at `trino-lb-redis-cluster.trino-lb.svc.cluster.local` on Port `6379` secured with the password `redis`.

```yaml
trinoLb:
  persistence:
    redis:
      endpoint: redis://:redis@trino-lb-redis-cluster.trino-lb.svc.cluster.local:6379/
```

### Redis clusters

To turn on Redis cluster mode, you need to enable it as follows:

```yaml
trinoLb:
  persistence:
    redis:
      clusterMode: true
      endpoint: redis://:redis@trino-lb-redis-cluster.trino-lb.svc.cluster.local:6379/
```
