#!/usr/bin/env bash
# Builds libadbc_driver_manager.so and libadbc_driver_sqlite.so from the
# apache/arrow-adbc C/C++ sources (there is no Rust or prebuilt-crates.io
# distribution of the ADBC SQLite driver — see ARCHITECTURE.md §3 and
# IMPLEMENTATION_PLAN.md Marco 2).
#
# Simpler than the PostgreSQL driver: SQLite itself just needs dev headers
# (`sqlite3.h`), no runtime RPATH juggling — the C library is ubiquitous.
#
# Prerequisites (no sudo required if sqlite3 dev headers are available via a
# package manager):
#   - cmake >= 3.18, a C++17 compiler
#   - libsqlite3-dev (or equivalent — provides sqlite3.h, found via
#     find_package(SQLite3))
#   - libfmt-dev (or any fmt install find_package(fmt) can locate)
#
# Output: $OUT_DIR/libadbc_driver_manager.so and
#         $OUT_DIR/libadbc_driver_sqlite.so
#
# Usage:
#   ./scripts/build-adbc-sqlite-driver.sh [OUT_DIR]
#
# Env:
#   ADBC_ADBC_REF   git ref of apache/arrow-adbc to build (default: main)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/target/adbc}"
ADBC_REF="${ADBC_ADBC_REF:-main}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

mkdir -p "$OUT_DIR"

echo "==> cloning apache/arrow-adbc@${ADBC_REF} (shallow)"
git clone --depth 1 --branch "$ADBC_REF" https://github.com/apache/arrow-adbc.git "$WORK_DIR/arrow-adbc" \
  || git clone --depth 1 https://github.com/apache/arrow-adbc.git "$WORK_DIR/arrow-adbc"

echo "==> configuring cmake"
mkdir -p "$WORK_DIR/build"
cmake -S "$WORK_DIR/arrow-adbc/c" -B "$WORK_DIR/build" \
  -DADBC_DRIVER_SQLITE=ON \
  -DADBC_DRIVER_MANAGER=ON \
  -DADBC_WITH_VENDORED_NANOARROW=ON \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_SKIP_RPATH=ON

echo "==> building"
cmake --build "$WORK_DIR/build" -j"$(nproc)" \
  --target adbc_driver_manager_shared adbc_driver_sqlite_shared

cp "$WORK_DIR/build/driver_manager/libadbc_driver_manager.so" "$OUT_DIR/"
cp "$WORK_DIR/build/driver/sqlite/libadbc_driver_sqlite.so" "$OUT_DIR/"

echo "==> done: $OUT_DIR/libadbc_driver_manager.so"
echo "==> done: $OUT_DIR/libadbc_driver_sqlite.so"
