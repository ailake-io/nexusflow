#!/usr/bin/env bash
# Fetches the official Apache Arrow ADBC driver for Snowflake and extracts
# its prebuilt shared library. Unlike the public nexusflow repo's
# scripts/build-adbc-postgresql-driver.sh (C++/libpq, built from source via
# cmake), this driver is written in Go but shipped as a *prebuilt* binary
# inside the `adbc-driver-snowflake` PyPI wheel — no Go toolchain needed,
# just download + unzip. Confirmed by inspecting the wheel directly
# (2026-08-18): the .so lives at
# adbc_driver_snowflake/libadbc_driver_snowflake.so inside the wheel, same
# filename regardless of platform (see the package's own __init__.py
# `_driver_path()`).
#
# Usage: scripts/fetch-adbc-snowflake-driver.sh [output_dir]
# Writes: <output_dir>/libadbc_driver_snowflake.so (default output_dir: ./target/adbc)
set -euo pipefail

OUT_DIR="${1:-./target/adbc}"
mkdir -p "$OUT_DIR"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

pip download adbc-driver-snowflake --no-deps \
  --platform manylinux2014_x86_64 --only-binary=:all: \
  -d "$WORK_DIR" >/dev/null

WHEEL="$(find "$WORK_DIR" -maxdepth 1 -name '*.whl' | head -n1)"
if [ -z "$WHEEL" ]; then
  echo "fetch-adbc-snowflake-driver: no wheel downloaded" >&2
  exit 1
fi

unzip -o -q "$WHEEL" -d "$WORK_DIR/extracted"
SO_PATH="$WORK_DIR/extracted/adbc_driver_snowflake/libadbc_driver_snowflake.so"
if [ ! -f "$SO_PATH" ]; then
  echo "fetch-adbc-snowflake-driver: expected .so not found at $SO_PATH — wheel layout may have changed, re-inspect it" >&2
  exit 1
fi

cp "$SO_PATH" "$OUT_DIR/libadbc_driver_snowflake.so"
echo "fetch-adbc-snowflake-driver: wrote $OUT_DIR/libadbc_driver_snowflake.so"
