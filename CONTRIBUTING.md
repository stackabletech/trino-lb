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
