# Proxy mode

trino-lb can be configured to either proxy all calls to the underlying Trino clusters or only the initial `POST` request and instruct the client to connect directly to the Trino cluster for the subsequent polling requests.
It also needs to keep track of all started and finished queries on the Trino clusters, so that it can correctly calculate the number of running queries.

You can configure the proxy mode using

```yaml
trinoLb:
  proxyMode: ProxyAllCalls # or ProxyFirstCall, defaults to ProxyAllCalls
```

## Proxy all calls (default)

In this mode, the client will make all requests through trino-lb, not only the initial `POST`.

Benefits:

- Counting queries can be achieved by inspecting the traffic
- Trino clients do not require network access to coordinator

Downsides:

- Increased query run times when a lot of data is transferred from the Trino coordinator to the Trino client due to network delay added by trino-lb (see the [performance research task](https://github.com/stackabletech/trino-lb/issues/72) for details)

## Proxy first call

In this mode, the client only sends the initial `POST` request to trino-lb. All following requests will be send to the Trino cluster directly.

As trino-lb cannot inspect the traffic in the subsequent calls, it would have no knowledge of the started and finished queries. However, an [HTTP event listener](https://trino.io/docs/current/admin/event-listeners-http.html) can be configured in Trino to inform trino-lb about all query starts and completions.

Benefits:

- Better performance, as there is no network delay added by trino-lb
- In the future more advanced features can be built based on information from the Trino events

Downsides:

- It requires active configuration on the Trino side, namely setting up the HTTP event listener
- Trino clients require network access to the coordinator

A sample configuration in Trino can look something like the following.
Please have a look at the [kuttl tests](https://github.com/stackabletech/trino-lb/tree/main/tests/templates/kuttl/) for complete examples.

Please note that you cannot disable the TLS certificate check. In the example below, the secret-operator from the Stackable Data Platform is used to provision valid TLS certificates automatically.

```yaml
apiVersion: trino.stackable.tech/v1alpha1
kind: TrinoCluster
metadata:
  name: my-trino
spec:
  coordinators:
    configOverrides: &configOverrides
      config.properties:
        event-listener.config-files: /tmp/http-event-listener.properties
    podOverrides: &podOverrides
      spec:
        containers:
          - name: trino
            volumeMounts:
              - name: http-event-listener-config
                mountPath: /tmp/http-event-listener.properties
                subPath: http-event-listener.properties
        volumes:
          - name: http-event-listener-config
            configMap:
              name: trino-http-event-listener-config
  workers:
    configOverrides: *configOverrides
    podOverrides: *podOverrides
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: trino-http-event-listener-config
data:
  http-event-listener.properties: |
    event-listener.name=http

    http-event-listener.connect-ingest-uri=https://trino-lb.trino-lb.svc.cluster.local:8443/v1/trino-event-listener
    http-event-listener.connect-retry-count=10
    http-event-listener.http-client.trust-store-path=/stackable/server_tls/truststore.p12
    http-event-listener.http-client.trust-store-password=changeit

    http-event-listener.log-created=true
    http-event-listener.log-completed=true
    http-event-listener.log-split=false
```
