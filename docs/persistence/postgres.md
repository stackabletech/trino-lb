# Postgres persistence

## Configuration

You only need to provide the url the Postgres is available at.
Authentication is supported by providing username and password in the url.

```yaml
trinoLb:
  persistence:
    postgres:
      url: postgres://trino-lb:trino-lb@postgres-postgresql.default.svc.cluster.local/trino_lb
      maxConnections: 10 # optional, defaults to 10
```

## Example installation

The above configuration works with a Postgres installed with the following command:

`helm install postgres bitnami/postgresql --version 13.2.18 --set auth.username=trino-lb,auth.password=trino-lb,auth.database=trino_lb,image.repository=bitnamilegacy/postgresql,volumePermissions.image.repository=bitnamilegacy/os-shell,metrics.image.repository=bitnamilegacy/postgres-exporter,global.security.allowInsecureImages=true`
