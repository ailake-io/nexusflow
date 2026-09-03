#!/usr/bin/env bash
# Fetches the official prebuilt libduckdb release and stages it as
# libadbc_driver_duckdb.so. DuckDB's own build already implements the ADBC
# interface (src/common/adbc/adbc.cpp — exports `duckdb_adbc_init`, which
# nexus-connector-duckdb falls back to when the standard `AdbcDriverInit`
# symbol is absent, see its driver.rs), so there's no separate ADBC driver
# to build from source here, just a pinned download + rename to the name
# ADBC_DRIVER_DUCKDB_PATH points at.
#
# Output: $OUT_DIR/libadbc_driver_duckdb.so
#
# Usage:
#   ./scripts/build-adbc-duckdb-driver.sh [OUT_DIR]
#
# Env:
#   DUCKDB_VERSION   pinned DuckDB release to fetch (default below)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/target/adbc}"
DUCKDB_VERSION="${DUCKDB_VERSION:-1.5.5}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

mkdir -p "$OUT_DIR"

echo "==> fetching libduckdb v${DUCKDB_VERSION} (linux-amd64)"
curl -fsSL -o "$WORK_DIR/libduckdb.zip" \
  "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-amd64.zip"
unzip -p "$WORK_DIR/libduckdb.zip" libduckdb.so > "$OUT_DIR/libadbc_driver_duckdb.so"
chmod +x "$OUT_DIR/libadbc_driver_duckdb.so"

echo "==> done: $OUT_DIR/libadbc_driver_duckdb.so"
