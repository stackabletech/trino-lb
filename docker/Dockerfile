FROM debian:bookworm as builder

RUN apt update && \
    apt install -y curl gcc python3 libpython3-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.79.0 && . "${HOME}/.cargo/env"

WORKDIR /trino-lb
COPY Cargo.toml Cargo.toml
COPY trino-lb trino-lb
COPY trino-lb-bench trino-lb-bench
COPY trino-lb-core trino-lb-core
COPY trino-lb-persistence trino-lb-persistence

RUN . "$HOME/.cargo/env" && cargo build --release --all-features

# Build final image
FROM debian:bookworm-slim

# Install python, needed for PythonScriptRouter
RUN apt update && \
    apt install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# There are propably some users that want to use this libs in their PythonScriptRouter to e.g. make external HTTP calls
RUN python3 -m pip install --break-system-packages requests

COPY --from=builder /trino-lb/target/release/trino-lb /trino-lb/target/release/trino-lb

ENV PATH="${PATH}:/trino-lb/target/release/"
ENTRYPOINT ["trino-lb", "--config-file", "/etc/trino-lb-config.yaml"]
