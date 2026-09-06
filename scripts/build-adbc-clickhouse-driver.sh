#!/usr/bin/env bash
# Builds libadbc_clickhouse.{so,dylib} from the ClickHouse/adbc_clickhouse
# Rust sources. The ClickHouse ADBC driver is not distributed on crates.io
# and must be built as a dynamic library for loading through the ADBC
# driver manager.
#
# Prerequisites:
#   - Rust/Cargo (same toolchain used for nexusflow)
#   - OpenSSL/LibreSSL/BoringSSL headers when using the native-tls feature,
#     or a ring/aws-lc compatible setup when using rustls-tls
#
# Output: $OUT_DIR/libadbc_clickhouse.so (Linux) or .dylib (macOS)
#
# Usage:
#   ./scripts/build-adbc-clickhouse-driver.sh [OUT_DIR]
#
# Env:
#   ADBC_CLICKHOUSE_REF   git ref of ClickHouse/adbc_clickhouse (default pinned below)
#   ADBC_CLICKHOUSE_FEATURES  cargo features (default: ffi,native-tls)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/target/adbc}"
ADBC_CLICKHOUSE_REF="${ADBC_CLICKHOUSE_REF:-v0.1.0-alpha.1}"
ADBC_CLICKHOUSE_FEATURES="${ADBC_CLICKHOUSE_FEATURES:-ffi,native-tls}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

# Real failure caught on a first real macOS run (2026-09-06): cargo names
# the shared-library output with the platform-correct extension on its
# own (.dylib on macOS), but this script's `cp` hardcoded `.so` — same
# class of bug as the postgresql/sqlite ADBC scripts already fixed
# earlier this session, just missed on this one.
if [[ "$(uname)" == "Darwin" ]]; then
  LIB_EXT="dylib"
else
  LIB_EXT="so"
fi

mkdir -p "$OUT_DIR"

echo "==> cloning ClickHouse/adbc_clickhouse@${ADBC_CLICKHOUSE_REF} (shallow)"
git clone --depth 1 --branch "$ADBC_CLICKHOUSE_REF" https://github.com/ClickHouse/adbc_clickhouse.git "$WORK_DIR/adbc_clickhouse"

echo "==> building libadbc_clickhouse.$LIB_EXT with features=${ADBC_CLICKHOUSE_FEATURES}"
cargo build --manifest-path "$WORK_DIR/adbc_clickhouse/Cargo.toml" \
  --release --features "$ADBC_CLICKHOUSE_FEATURES"

cp "$WORK_DIR/adbc_clickhouse/target/release/libadbc_clickhouse.$LIB_EXT" "$OUT_DIR/"

echo "==> done: $OUT_DIR/libadbc_clickhouse.$LIB_EXT"
