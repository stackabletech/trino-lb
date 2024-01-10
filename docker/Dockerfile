FROM rust:1.74.0-slim

# Install python, needed for PythonScriptRouter
RUN apt update && \
    apt install -y python3 python3-pip libpython3-dev && \
    rm -rf /var/lib/apt/lists/*

# There are propably some users that want to use this libs in their PythonScriptRouter to e.g. make external HTTP calls
RUN pip install --break-system-packages requests

WORKDIR /trino-lb

COPY Cargo.toml Cargo.toml
COPY trino-lb trino-lb
# TODO: Think about if we really want to ship trino-lb-bench
COPY trino-lb-bench trino-lb-bench
COPY trino-lb-core trino-lb-core
COPY trino-lb-persistence trino-lb-persistence

RUN cargo build --release --all-features
ENV PATH="${PATH}:/trino-lb/target/release/"

ENTRYPOINT trino-lb --config-file /etc/trino-lb-config.yaml
