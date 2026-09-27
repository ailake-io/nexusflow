#!/usr/bin/env bash
# Fetches the official Apache Arrow ADBC driver for BigQuery and extracts
# its prebuilt shared library — same rationale and mechanism as
# scripts/fetch-adbc-snowflake-driver.sh (Go binary, shipped prebuilt in
# the `adbc-driver-bigquery` PyPI wheel, no Go toolchain needed).
# Confirmed by inspecting the wheel directly (2026-08-18): the .so lives
# at adbc_driver_bigquery/libadbc_driver_bigquery.so inside the wheel.
#
# Usage: scripts/fetch-adbc-bigquery-driver.sh [output_dir]
# Writes: <output_dir>/libadbc_driver_bigquery.so (default output_dir: ./target/adbc)
set -euo pipefail

OUT_DIR="${1:-./target/adbc}"
mkdir -p "$OUT_DIR"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

pip download adbc-driver-bigquery --no-deps \
  --platform manylinux2014_x86_64 --only-binary=:all: \
  -d "$WORK_DIR" >/dev/null

WHEEL="$(find "$WORK_DIR" -maxdepth 1 -name '*.whl' | head -n1)"
if [ -z "$WHEEL" ]; then
  echo "fetch-adbc-bigquery-driver: no wheel downloaded" >&2
  exit 1
fi

unzip -o -q "$WHEEL" -d "$WORK_DIR/extracted"
SO_PATH="$WORK_DIR/extracted/adbc_driver_bigquery/libadbc_driver_bigquery.so"
if [ ! -f "$SO_PATH" ]; then
  echo "fetch-adbc-bigquery-driver: expected .so not found at $SO_PATH — wheel layout may have changed, re-inspect it" >&2
  exit 1
fi

cp "$SO_PATH" "$OUT_DIR/libadbc_driver_bigquery.so"
echo "fetch-adbc-bigquery-driver: wrote $OUT_DIR/libadbc_driver_bigquery.so"
