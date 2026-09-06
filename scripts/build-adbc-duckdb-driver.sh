#!/usr/bin/env bash
# Fetches the official prebuilt libduckdb release and stages it as
# libadbc_driver_duckdb.{so,dylib}. DuckDB's own build already implements
# the ADBC interface (src/common/adbc/adbc.cpp — exports `duckdb_adbc_init`,
# which nexus-connector-duckdb falls back to when the standard
# `AdbcDriverInit` symbol is absent, see its driver.rs), so there's no
# separate ADBC driver to build from source here, just a pinned download +
# rename to the name ADBC_DRIVER_DUCKDB_PATH points at.
#
# Output: $OUT_DIR/libadbc_driver_duckdb.so (Linux) or .dylib (macOS)
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

# Real bug caught on a first real macOS run (2026-09-06): this script used
# to fetch `libduckdb-linux-amd64.zip` unconditionally — `unzip` doesn't
# validate ELF vs. Mach-O, so it "succeeded" on macOS too, silently
# staging a non-functional Linux .so under a macOS build's target/adbc.
# DuckDB does publish a real macOS asset (`libduckdb-osx-universal.zip`,
# containing `libduckdb.dylib`, confirmed by inspecting the zip contents),
# just under a different name/extension.
if [[ "$(uname)" == "Darwin" ]]; then
  DUCKDB_ASSET="libduckdb-osx-universal.zip"
  INNER_NAME="libduckdb.dylib"
  LIB_EXT="dylib"
else
  DUCKDB_ASSET="libduckdb-linux-amd64.zip"
  INNER_NAME="libduckdb.so"
  LIB_EXT="so"
fi

mkdir -p "$OUT_DIR"

echo "==> fetching libduckdb v${DUCKDB_VERSION} (${DUCKDB_ASSET})"
curl -fsSL -o "$WORK_DIR/libduckdb.zip" \
  "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/${DUCKDB_ASSET}"
unzip -p "$WORK_DIR/libduckdb.zip" "$INNER_NAME" > "$OUT_DIR/libadbc_driver_duckdb.$LIB_EXT"
chmod +x "$OUT_DIR/libadbc_driver_duckdb.$LIB_EXT"

echo "==> done: $OUT_DIR/libadbc_driver_duckdb.$LIB_EXT"
