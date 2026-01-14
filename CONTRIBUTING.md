# Contributor guide

## Build docker image

```bash
docker build . -f docker/Dockerfile -t foo:bar
```

## Test docker image

Update `tests/test-definition.yaml` by setting the `trino-lb` dimension to `foo:bar`.

Afterwards load the image into kind

```bash
kind load docker-image foo:bar
```

Last but not least run

```bash
./scripts/run_tests.sh
```

## mitm proxy debugging

First, port-forward trino-lb to localhost:

```bash
kubectl -n <namespace> port-forward svc/trino-lb 8443:8443
```

Please make sure that `trinoLb.externalAddress` in your trino-lb config points to `https://127.0.0.1:8443`, so that it populates the nextUri correctly.

Then start `mitmproxy`:

```bash
# nix-shell -p mitmproxy
mitmproxy --listen-port 8080 --ssl-insecure
```

Afterwards connect with trino-cli:

```bash
~/Downloads/trino-cli-478 --server https://127.0.0.1:8443 --insecure --user admin --password --http-proxy=127.0.0.1:8080

export TRINO_USER="admin"
export TRINO_PASSWORD="adminadmin"
echo 'SELECT * FROM tpch.sf100.customer' | ~/Downloads/trino-cli-478 --server https://127.0.0.1:8443 --insecure --password --http-proxy 127.0.0.1:8080
```
