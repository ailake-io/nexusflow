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
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# rust-embed's #[derive] (crates/nexus-server/src/embedded_ui.rs) reads this
# directory at *compile* time — must exist before `cargo build` runs, same
# requirement CI's clippy/test jobs have (see ci.yml).
COPY --from=frontend /src/frontend/dist ./frontend/dist
RUN cargo build --release -p nexusflow --features embed-ui

FROM ${RUNTIME_IMAGE} AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 libsqlite3-0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/nexusflow /usr/lib/nexusflow/nexusflow-bin
COPY --from=adbc /out/libadbc_driver_postgresql.so /out/libadbc_driver_sqlite.so /usr/lib/nexusflow/
COPY packaging/linux/nexusflow-wrapper.sh /usr/bin/nexusflow
RUN chmod +x /usr/bin/nexusflow

EXPOSE 8080
# NEXUS_JWT_SECRET / NEXUS_ENCRYPTION_KEY have no defaults on purpose (see
# nexus-server/src/lib.rs's run()) — ARCHITECTURE.md §10 requires the
# operator to supply both via `docker run -e`, never a baked-in default.
ENTRYPOINT ["/usr/bin/nexusflow"]
