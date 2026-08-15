#!/usr/bin/env bash
# Builds libadbc_driver_manager.so and libadbc_driver_postgresql.so from the
# apache/arrow-adbc C/C++ sources (there is no Rust or prebuilt-crates.io
# distribution of the ADBC PostgreSQL driver — see ARCHITECTURE.md §3 and
# IMPLEMENTATION_PLAN.md Marco 1).
#
# Prerequisites (no sudo required if libpq is available via a package manager
# such as conda-forge, which ships pkg-config files):
#   - cmake >= 3.18, a C++17 compiler
#   - pkg-config + libpq (dev headers + .pc file) discoverable via PKG_CONFIG_PATH,
#     for BUILD time only
#   - libfmt-dev (or any fmt install find_package(fmt) can locate)
#   - at RUNTIME, libpq.so.5 and libstdc++.so.6 must be resolvable through the
#     system's normal library search path (ldconfig) — e.g. `apt install
#     libpq5` on Debian/Ubuntu. This is deliberate: see the CMAKE_SKIP_RPATH
#     note below.
#
# Output: $OUT_DIR/libadbc_driver_manager.so and
#         $OUT_DIR/libadbc_driver_postgresql.so
#
# Usage:
#   ./scripts/build-adbc-postgresql-driver.sh [OUT_DIR]
#
# Env:
#   ADBC_ADBC_REF        git ref of apache/arrow-adbc to build (default pinned below)
#   PKG_CONFIG_PATH       must expose libpq.pc if libpq isn't a system package

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/target/adbc}"
ADBC_REF="${ADBC_ADBC_REF:-apache-arrow-adbc-24}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

mkdir -p "$OUT_DIR"

echo "==> cloning apache/arrow-adbc@${ADBC_REF} (shallow)"
git clone --depth 1 --branch "$ADBC_REF" https://github.com/apache/arrow-adbc.git "$WORK_DIR/arrow-adbc"

echo "==> configuring cmake"
mkdir -p "$WORK_DIR/build"
# CMAKE_SKIP_RPATH: if libpq was found via a non-system location (e.g. a conda
# env's PKG_CONFIG_PATH), CMake would otherwise bake that path into the
# driver's RUNPATH — and that same directory usually also ships its own
# (older) libstdc++, which then shadows the system one at load time and
# breaks with a GLIBCXX version mismatch. Skipping RPATH forces the loader to
# resolve libpq.so.5 / libstdc++.so.6 from the system's ldconfig cache
# instead, which is what we actually want at runtime.
cmake -S "$WORK_DIR/arrow-adbc/c" -B "$WORK_DIR/build" \
  -DADBC_DRIVER_POSTGRESQL=ON \
  -DADBC_DRIVER_MANAGER=ON \
  -DADBC_WITH_VENDORED_NANOARROW=ON \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_SKIP_RPATH=ON

echo "==> building"
cmake --build "$WORK_DIR/build" -j"$(nproc)"

cp "$WORK_DIR/build/driver_manager/libadbc_driver_manager.so" "$OUT_DIR/"
cp "$WORK_DIR/build/driver/postgresql/libadbc_driver_postgresql.so" "$OUT_DIR/"

echo "==> done: $OUT_DIR/libadbc_driver_manager.so"
echo "==> done: $OUT_DIR/libadbc_driver_postgresql.so"
