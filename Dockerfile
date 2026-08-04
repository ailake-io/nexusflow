# Single-binary deployment (Marco 11, CLAUDE.md §7): `docker run` starts one
# process serving both the REST/WebSocket API and the embedded frontend at
# http://localhost:8080 — no external dependency beyond the container.
#
# `docker build --build-arg RUNTIME_IMAGE=nvidia/cuda:12.4.1-runtime-ubuntu22.04 .`
# selects the "cuda" profile's base image (`docker run --gpus all` at
# runtime). NOTE: nexus-ai's `cuda` feature isn't implemented yet (see
# crates/nexus-ai/Cargo.toml — only `cpu` exists so far; `cuda`/`metal` are
# still future work per IMPLEMENTATION_PLAN.md Marco 5), and nexus-server
# doesn't link nexus-ai at all yet. This runtime stage is genuinely
# functional today (same binary, same behavior as the default profile) —
# it just isn't accelerating anything yet. It exists so the multi-arch/GPU
# runtime plumbing (base image, --gpus all, driver libraries) is ready the
# day nexus-ai's ONNX-via-ort CUDA execution provider lands, without a
# second Dockerfile to maintain in sync.

ARG RUNTIME_IMAGE=debian:bookworm-slim
# `docker build --build-arg FEATURES=embed-ui,connectors-all .` links every
# optional connector (nexus-server/Cargo.toml) into the binary — default
# stays embed-ui only, same size/behavior as before this arg existed. NOT
# validated as a full docker build in this repo yet (each of the 14 extra
# connectors was validated via plain `cargo build`, not through this
# Dockerfile specifically) — kafka's rdkafka may need `zlib1g` added to the
# runtime stage's apt-get line below if you hit a missing-.so at container
# start.
ARG FEATURES=embed-ui

FROM node:22-slim AS frontend
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# There's no crates.io distribution of the ADBC C++ drivers (see
# crates/nexus-connectors/nexus-connector-postgres/src/driver.rs) — built
# here from apache/arrow-adbc source, same as scripts/build-adbc-*.sh do for
# local dev and CI (.github/workflows/ci.yml's `test` job).
FROM debian:bookworm-slim AS adbc
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates cmake make g++ pkg-config libpq-dev libsqlite3-dev libfmt-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY scripts/build-adbc-postgresql-driver.sh scripts/build-adbc-sqlite-driver.sh scripts/
RUN scripts/build-adbc-postgresql-driver.sh /out \
 && scripts/build-adbc-sqlite-driver.sh /out

FROM rust:1-slim-bookworm AS builder
ARG FEATURES
# pkg-config/libssl-dev cover the default (embed-ui) build. The rest only
# matter if FEATURES pulls in the heavier connectors (see nexus-server/
# Cargo.toml's per-connector features): cmake+make for odbc's vendored
# unixODBC build, protobuf-compiler + libprotobuf-dev for milvus/lancedb's
# tonic/prost codegen (libprotobuf-dev ships the google/protobuf/*.proto
# well-known types that protoc needs to resolve `import "google/protobuf/
# empty.proto"` — --no-install-recommends drops it otherwise since it's
# only a Recommends of protobuf-compiler, not a Depends), libsqlite3-dev
# for iceberg's sqlx "sqlite" feature — kept unconditional since this whole
# stage is discarded after the build (see the `runtime` stage below), so it
# costs build time, not final image size.
RUN apt-get update && apt-get install -y --no-install-recommends \
      pkg-config libssl-dev cmake make protobuf-compiler libprotobuf-dev libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# rust-embed's #[derive] (crates/nexus-server/src/embedded_ui.rs) reads this
# directory at *compile* time — must exist before `cargo build` runs, same
# requirement CI's clippy/test jobs have (see ci.yml).
COPY --from=frontend /src/frontend/dist ./frontend/dist
RUN cargo build --release -p nexusflow --features "${FEATURES}"

FROM ${RUNTIME_IMAGE} AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 libsqlite3-0 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r -g 1000 nexusflow \
    && useradd -r -u 1000 -g nexusflow nexusflow

COPY --from=builder /src/target/release/nexusflow /usr/lib/nexusflow/nexusflow-bin
COPY --from=adbc /out/libadbc_driver_postgresql.so /out/libadbc_driver_sqlite.so /usr/lib/nexusflow/
COPY packaging/linux/nexusflow-wrapper.sh /usr/bin/nexusflow
RUN chmod +x /usr/bin/nexusflow \
    && chown -R nexusflow:nexusflow /usr/lib/nexusflow

USER nexusflow
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://localhost:8080/health || exit 1

# NEXUS_JWT_SECRET / NEXUS_ENCRYPTION_KEY have no defaults on purpose (see
# nexus-server/src/lib.rs's run()) — ARCHITECTURE.md §10 requires the
# operator to supply both via `docker run -e`, never a baked-in default.
ENTRYPOINT ["/usr/bin/nexusflow"]
