# Single-binary deployment (Marco 11, CLAUDE.md §7): `docker run` starts one
# process serving both the REST/WebSocket API and the embedded frontend at
# http://localhost:8080 — no external dependency beyond the container.
#
# `docker build --build-arg RUNTIME_IMAGE=nvidia/cuda:12.4.1-runtime-ubuntu22.04 .`
# selects the "cuda" profile's base image (`docker run --gpus all` at
# runtime). NOTE: nexus-ai's `cuda`/`metal` execution providers register
# correctly at compile time but have not been validated on real GPU/Apple
# Silicon hardware (sandbox is Linux without a GPU) — runtime falls back
# silently to CPU if the driver/hardware is missing. The `embeddings`
# feature links `nexus-ai` into `nexus-server`; the default `embed-ui`
# profile does not, keeping the base image small. This runtime stage is
# functional today and ready for the day the CUDA EP is validated.

ARG RUNTIME_IMAGE=ubuntu:24.04@sha256:561618e2c15bf2397621dd04f96926663a3b5616c189cf7e38db7e82f5c538ea
# `docker build --build-arg FEATURES=embed-ui,connectors-all .` links every
# optional connector (nexus-server/Cargo.toml) into the binary — default
# stays embed-ui only, same size/behavior as before this arg existed. When
# building with `kafka`/`connectors-all`, the builder stage needs g++ because
# rdkafka-sys compiles librdkafka statically; the runtime stage may also need
# `zlib1g` if you hit a missing-.so at container start.
ARG FEATURES=embed-ui

FROM node:22-slim@sha256:d649c27dae7ba0137b3cef5dd75baa422c08dc3d9e3fc0c23dfb172dc3cc6436 AS frontend
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# There's no crates.io distribution of the ADBC C++ drivers (see
# crates/nexus-connectors/nexus-connector-postgres/src/driver.rs) — built
# here from apache/arrow-adbc source, same as scripts/build-adbc-*.sh do for
# local dev and CI (.github/workflows/ci.yml's `test` job).
FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241 AS adbc
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates cmake make g++ pkg-config libpq-dev libsqlite3-dev libfmt-dev unzip curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY scripts/build-adbc-postgresql-driver.sh scripts/build-adbc-sqlite-driver.sh scripts/build-adbc-duckdb-driver.sh scripts/
ARG DUCKDB_VERSION=1.5.5
RUN scripts/build-adbc-postgresql-driver.sh /out \
 && scripts/build-adbc-sqlite-driver.sh /out \
 && DUCKDB_VERSION="${DUCKDB_VERSION}" scripts/build-adbc-duckdb-driver.sh /out

FROM rust:1-slim-trixie@sha256:8e8cf8f7fd54a2d23d5a743b3a03f56e26b6c774276c33fa0595111704ebb15c AS clickhouse-adbc
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY scripts/build-adbc-clickhouse-driver.sh scripts/
RUN scripts/build-adbc-clickhouse-driver.sh /out

FROM rust:1-slim-trixie@sha256:8e8cf8f7fd54a2d23d5a743b3a03f56e26b6c774276c33fa0595111704ebb15c AS builder
ARG FEATURES
# Trixie gives us glibc/libstdc++ new enough to link the prebuilt ONNX
# Runtime that ort-sys pulls in when the `embeddings` feature is enabled.
# pkg-config/libssl-dev cover the default (embed-ui) build. The rest only
# matter if FEATURES pulls in the heavier connectors (see nexus-server/
# Cargo.toml's per-connector features): cmake+make for odbc's vendored
# unixODBC build, protobuf-compiler + libprotobuf-dev for milvus/lancedb's
# tonic/prost codegen (libprotobuf-dev ships the google/protobuf/*.proto
# well-known types that protoc needs to resolve `import "google/protobuf/
# empty.proto"` — --no-install-recommends drops it otherwise since it's
# only a Recommends of protobuf-compiler, not a Depends), libsqlite3-dev
# for iceberg's sqlx "sqlite" feature, libcurl4-openssl-dev for kafka's
# rdkafka-sys vendored librdkafka CMake build (unconditionally probes for
# libcurl at build time — CI's `connectors` job never hits this because its
# self-hosted host has libcurl-dev installed system-wide already, outside
# anything the workflow itself installs; a fresh container image has no
# such thing) — kept unconditional since this whole stage is discarded
# after the build (see the `runtime` stage below), so it costs build time,
# not final image size.
RUN apt-get update && apt-get install -y --no-install-recommends \
      pkg-config libssl-dev cmake make g++ zlib1g-dev protobuf-compiler libprotobuf-dev libsqlite3-dev libcurl4-openssl-dev python3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# rust-embed's #[derive] (crates/nexus-server/src/embedded_ui.rs) reads this
# directory at *compile* time — must exist before `cargo build` runs, same
# requirement CI's clippy/test jobs have (see ci.yml).
COPY --from=frontend /src/frontend/dist ./frontend/dist
# BuildKit cache mounts for cargo registry + target/ make rebuilds *much*
# faster once the first full compile has happened. Because the binary ends up
# inside the target cache mount, copy it out to /tmp before the mount is
# unmounted so the runtime stage can COPY it normally.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release -p nexusflow --features "${FEATURES}" && \
    cp /src/target/release/nexusflow /tmp/nexusflow-bin

FROM ${RUNTIME_IMAGE} AS runtime
# `unixodbc` here is NOT a runtime dependency of the binary itself — the
# `odbc` connector statically vendors and links unixODBC's driver-manager
# code in (odbc-api's `vendored-unix-odbc` feature, see
# nexus-connector-odbc/Cargo.toml), so nexusflow-bin has no dynamic
# dependency on libodbc.so.2. What's installed here is the package's
# `/etc/odbcinst.ini` + `odbcinst` tooling, so an operator can
# `apt-get install`/register a vendor ODBC driver (e.g. `odbc-postgresql`)
# against a standard location instead of building that infrastructure
# themselves. No vendor driver is bundled: the `odbc` connector is a
# generic bridge for legacy/unsupported systems (SQL Server, Oracle, SAP
# HANA, Teradata, ...) that have no native NexusFlow connector, so there's
# no single "right" vendor driver to pick, and most require accepting a
# vendor EULA (e.g. Microsoft's msodbcsql18) or aren't freely
# redistributable (Oracle Instant Client, IBM DB2 CLI). Postgres/MySQL/
# SQLite/DuckDB/ClickHouse already have a native ADBC or bridging connector
# (see ARCHITECTURE.md §4.1) and don't go through ODBC at all, so their
# ODBC drivers are deliberately not installed here either.
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 libsqlite3-0 ca-certificates curl python3 python3-pip unixodbc \
    # Pinned (not floating "latest") so a build can't silently pull in a
    # compromised/broken release of any of these — same reproducibility
    # posture as nexus-ai's ONNX model revision pin and
    # scripts/build-python-runtime.sh's python-build-standalone tag pin.
    && pip3 install --break-system-packages --no-cache-dir \
      pandas==2.2.3 numpy==1.26.4 pyarrow==17.0.0 polars==1.8.2 python-dateutil==2.9.0 \
      dbt-core==1.12.3 dbt-postgres==1.11.0 \
    # dbt-postgres is the baseline adapter (Postgres is the primary ADBC
    # warehouse this repo documents dbt against, CLAUDE.md §4.4) — a user
    # targeting a different warehouse (dbt-duckdb, dbt-clickhouse, etc.)
    # still needs to install that adapter into the running container
    # themselves; add it here if a real need for it shows up.
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r -g 1001 nexusflow \
    && useradd -r -u 1001 -g nexusflow nexusflow \
    # Embedding pipelines download the ONNX model to $HOME/.cache at
    # runtime (nexus-ai via hf-hub) — `useradd -r` creates no home dir, so
    # the cache creation fails with EACCES for the runtime user and every
    # embedding pipeline errors on a fresh container ("hugging face hub
    # error: I/O error Permission denied").
    && mkdir -p /home/nexusflow && chown 1001:1001 /home/nexusflow

COPY --from=builder /tmp/nexusflow-bin /usr/lib/nexusflow/nexusflow-bin
COPY --from=adbc /out/libadbc_driver_postgresql.so /out/libadbc_driver_sqlite.so /out/libadbc_driver_duckdb.so /usr/lib/nexusflow/
COPY --from=clickhouse-adbc /out/libadbc_clickhouse.so /usr/lib/nexusflow/
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
