# Proxy mode

trino-lb can either proxy all calls to the underlying Trino clusters or only the initial `POST` and than hand of to the Trino cluster.
It also needs to keep track of all started and finished queries on the Trino clusters, so that it can correctly calculate the number of running queries.

You can configure the proxy mode using

```yaml
trinoLb:
  proxyMode: ProxyAllCalls # or ProxyFirstCall, defaults to ProxyAllCalls
```

## Proxy all calls (default)

All calls will be proxied, not only the initial `POST`.

Benefits:

* Counting queries can be achieved by "sniffing" the traffic
* Trino clients don't need network access to coordinator

Downsides:

* Query runtimes can be increased in case many data is transferred from the Trino coordinator to the Trino client due to network delay added by trino-lb (have a look at [a performance research task](https://github.com/stackabletech/trino-lb/issues/72) for details)

## Proxy first call

Only the initial `POST` is proxied, all following requests will be send to the Trino cluster directly.

As trino-lb can not "sniff" the traffic to get informed about started and finished queries we need to hook it up as [HTTP event listener](https://trino.io/docs/current/admin/event-listeners-http.html) in Trino.
This way trino-lb will get informed about all query starts and completions.

Benefits:

* Better performance, as there is no network delay added by trino-lb
* In the future more advanced features can be build based on information from the Trino events

Downsides:

* It requires active configuration on the Trino side, namely setting up the HTTP event listener
* Trino clients need to have network access to the coordinators

A sample configuration in Trino can look something like the following.
Please have a look at the [kuttl tests](https://github.com/stackabletech/trino-lb/tree/feat/trino-query-events/tests/templates/kuttl/) for complete examples.

Please note that you can not disable the TLS certificate check, in this case secret-operator from the Stackable Data Platform is used to automatically provision valid TLS certificates.

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
