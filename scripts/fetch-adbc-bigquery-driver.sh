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
#
# Env:
#   BIGQUERY_ADBC_VERSION   pinned adbc-driver-bigquery PyPI release
#                           (default below) — unpinned `pip download` would
#                           silently pull whatever's newest on PyPI at build
#                           time (found in a security review, 2026-09-27:
#                           same reproducibility gap `DUCKDB_VERSION` already
#                           closes for the duckdb fetch).
#
# macOS support added 2026-09-30 (install-driver-coverage audit): PyPI does
# publish a `macosx_11_0_arm64` wheel for this package too — confirmed by
# downloading it directly and checking the extracted .so with `file`
# (real Mach-O arm64 dylib, just kept the `.so` extension like the Linux
# one; `nexus-connector-bigquery` locates it purely via the
# `ADBC_DRIVER_BIGQUERY_PATH` env var, so the extension never matters).
# `manylinux2014_x86_64` stays the Linux target — matches the x86_64-only
# scope of every other Linux package script in this repo.
set -euo pipefail

OUT_DIR="${1:-./target/adbc}"
BIGQUERY_ADBC_VERSION="${BIGQUERY_ADBC_VERSION:-1.11.0}"
mkdir -p "$OUT_DIR"

case "$(uname -s)" in
  Darwin) PLATFORM_TAG="macosx_11_0_arm64" ;;
  *) PLATFORM_TAG="manylinux2014_x86_64" ;;
esac

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

pip download "adbc-driver-bigquery==${BIGQUERY_ADBC_VERSION}" --no-deps \
  --platform "$PLATFORM_TAG" --only-binary=:all: \
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
