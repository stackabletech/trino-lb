# docker build -f docker/Dockerfile . -t oci.stackable.tech/stackable/trino-lb:0.0.0-dev

FROM debian:trixie AS builder

# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install --no-install-recommends -y ca-certificates curl gcc python3 libpython3-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# hadolint ignore=SC1091
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.91.1 && . "${HOME}/.cargo/env"

WORKDIR /trino-lb
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY trino-lb trino-lb
COPY trino-lb-bench trino-lb-bench
COPY trino-lb-core trino-lb-core
COPY trino-lb-persistence trino-lb-persistence

# hadolint ignore=SC1091
RUN . "$HOME/.cargo/env" && cargo build --locked --release --all-features

# Build final image
FROM debian:trixie-slim

# Install python, needed for PythonScriptRouter
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install --no-install-recommends -y python3 python3-pip libpython3-dev && \
    rm -rf /var/lib/apt/lists/*

# There are probably some users that want to use this libs in their PythonScriptRouter to e.g. make external HTTP calls
# hadolint ignore=DL3013
RUN python3 -m pip install --no-cache-dir --break-system-packages requests

COPY --from=builder /trino-lb/target/release/trino-lb /trino-lb/target/release/trino-lb

ENV PATH="${PATH}:/trino-lb/target/release/"
ENTRYPOINT ["trino-lb", "--config-file", "/etc/trino-lb-config.yaml"]
