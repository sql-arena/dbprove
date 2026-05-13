FROM rust:1-bookworm AS builder

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN cargo install datafusion-cli --locked --root /opt/datafusion-cli

WORKDIR /tmp/datafusion-plan-json
COPY plan_json/Cargo.toml Cargo.toml
COPY plan_json/src src
RUN cargo install --path . --root /opt/datafusion-plan-json

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/datafusion-cli /opt/datafusion-cli
COPY --from=builder /opt/datafusion-plan-json /opt/datafusion-plan-json
COPY tpch /opt/tpch
COPY bootstrap.sql /opt/datafusion/bootstrap.sql
COPY entrypoint.sh /usr/local/bin/datafusion-entrypoint.sh

RUN chmod +x /usr/local/bin/datafusion-entrypoint.sh \
    && mkdir -p /workspace /var/lib/datafusion

ENV PATH="/opt/datafusion-cli/bin:/opt/datafusion-plan-json/bin:${PATH}"
ENV DATAFUSION_EXECUTION_COLLECT_STATISTICS=true

WORKDIR /workspace

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/datafusion-entrypoint.sh"]
