#!/bin/sh
# Installed as /usr/bin/nexusflow (deb/rpm) — points the ADBC connectors at
# the driver .so's this package ships alongside the binary in
# /usr/lib/nexusflow/. There's no crates.io distribution of these C++
# drivers to statically link (crates/nexus-connectors/nexus-connector-postgres/
# src/driver.rs — ADBC_DRIVER_POSTGRESQL_PATH has no built-in default), so
# something has to set the env var before exec; a caller's own env wins if
# already set (e.g. pointing at a newer driver build for testing).
export ADBC_DRIVER_POSTGRESQL_PATH="${ADBC_DRIVER_POSTGRESQL_PATH:-/usr/lib/nexusflow/libadbc_driver_postgresql.so}"
export ADBC_DRIVER_SQLITE_PATH="${ADBC_DRIVER_SQLITE_PATH:-/usr/lib/nexusflow/libadbc_driver_sqlite.so}"
exec /usr/lib/nexusflow/nexusflow-bin "$@"
