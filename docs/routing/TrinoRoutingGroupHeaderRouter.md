# TrinoRoutingGroupHeaderRouter

This very simple router looks at the `X-Trino-Routing-Group` header (name of the header is configurable) and send the query to whatever cluster group was put in the header (e.g. `X-Trino-Routing-Group: foo` would go to the cluster group `foo`).
In case the specified cluster group does not exist the router will not make a decision and therefore let the next router in the chain decide.

## Configuration

The configuration of the Router is very simple, as it does not need any properties:

```yaml
routers:
  - trinoRoutingGroupHeader: {}
```

Additionally you can configure the name of the HTTP header in case it differs from `X-Trino-Routing-Group`:

```yaml
routers:
  - trinoRoutingGroupHeader:
      headerName: X-My-Custom-Routing-Header # optional, defaults to X-Trino-Routing-Group
```
