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

## mitm Proxy debugging

First, port-forward trino-lb to localhost:

```bash
kubectl -n kuttl-test-more-titmouse port-forward svc/trino-lb 8443:8443
```

Than start `mitmproxy`:

```bash
# nix-shell -p mitmproxy
mitmproxy --listen-port 8080 --ssl-insecure
```

Afterwards connect with trino-cli:

```bash
~/Downloads/trino-cli-478 --server https://127.0.0.1:8443 --insecure --user admin --password --http-proxy=127.0.0.1:8080

echo 'SELECT * FROM tpch.sf100.customer' | ~/Downloads/trino-cli-478 --server https://127.0.0.1:8443 --insecure --user admin --password --http-proxy=127.0.0.1:8080
```




!!!!!!!!!!TEMP!!!!!!!!!!
k -n kuttl-test-prime-lemur port-forward svc/minio 9000:9000
