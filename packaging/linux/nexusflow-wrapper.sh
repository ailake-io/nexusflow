#!/bin/sh
# Installed as /usr/bin/nexusflow (deb/rpm + Docker image) — points the ADBC
# connectors at the driver .so's this package ships alongside the binary in
# /usr/lib/nexusflow/. There's no crates.io distribution of these drivers to
# statically link (crates/nexus-connectors/nexus-connector-postgres/
# src/driver.rs — ADBC_DRIVER_POSTGRESQL_PATH has no built-in default), so
# something has to set the env var before exec; a caller's own env wins if
# already set (e.g. pointing at a newer driver build for testing).
#
# All four drivers ship in the Docker image (Dockerfile `adbc`/`clickhouse-adbc`
# stages); the deb/rpm packages ship at least postgres/sqlite — the export is
# harmless when the file is absent since the connector only loads the driver
# when a pipeline actually uses it.
export ADBC_DRIVER_POSTGRESQL_PATH="${ADBC_DRIVER_POSTGRESQL_PATH:-/usr/lib/nexusflow/libadbc_driver_postgresql.so}"
export ADBC_DRIVER_SQLITE_PATH="${ADBC_DRIVER_SQLITE_PATH:-/usr/lib/nexusflow/libadbc_driver_sqlite.so}"
export ADBC_DRIVER_CLICKHOUSE_PATH="${ADBC_DRIVER_CLICKHOUSE_PATH:-/usr/lib/nexusflow/libadbc_clickhouse.so}"
export ADBC_DRIVER_DUCKDB_PATH="${ADBC_DRIVER_DUCKDB_PATH:-/usr/lib/nexusflow/libadbc_driver_duckdb.so}"
exec /usr/lib/nexusflow/nexusflow-bin "$@"
