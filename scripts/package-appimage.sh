#!/usr/bin/env bash
# Builds NexusFlow-<version>-x86_64.AppImage. Same prerequisites as
# package-deb.sh (release binary w/ embed-ui,connectors-all + all four ADBC
# driver .so's already built) plus `appimagetool` on PATH or at
# $APPIMAGETOOL — get it from
# https://github.com/AppImage/appimagetool/releases (continuous build).
#
# Nothing to bundle for the `odbc` connector itself: its driver *manager*
# (unixODBC) is built from source and statically linked in via odbc-api's
# `vendored-unix-odbc` feature (see nexus-connector-odbc/Cargo.toml) — the
# binary has no runtime dependency on libodbc.so.2. A vendor driver (e.g.
# psqlodbc for PostgreSQL) is still the operator's own responsibility to
# install on the host and register in /etc/odbcinst.ini, same as for the
# deb/rpm packages.
#
# Usage: APPIMAGETOOL=/path/to/appimagetool ./scripts/package-appimage.sh [OUT_DIR]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(grep -A3 '^\[workspace.package\]' "$REPO_ROOT/Cargo.toml" | grep '^version' | head -1 | cut -d '"' -f2)"
OUT_DIR="${1:-$REPO_ROOT/target/package}"

# APPIMAGETOOL may be either a bare binary ("appimagetool" or "/tmp/appimagetool")
# or a binary followed by flags ("/tmp/appimagetool --appimage-extract-and-run").
# Split it so command -v only checks the binary and flags are passed separately.
APPIMAGETOOL="${APPIMAGETOOL:-appimagetool}"
read -ra APPIMAGETOOL_ARGS <<< "$APPIMAGETOOL"
APPIMAGETOOL_BIN="${APPIMAGETOOL_ARGS[0]}"
APPIMAGETOOL_FLAGS=("${APPIMAGETOOL_ARGS[@]:1}")

WORK_DIR="$(mktemp -d)"
APPDIR="$WORK_DIR/NexusFlow.AppDir"
trap 'rm -rf "$WORK_DIR"' EXIT

BIN="$REPO_ROOT/target/release/nexusflow"
ADBC_DIR="$REPO_ROOT/target/adbc"
PYTHON_RUNTIME_DIR="$REPO_ROOT/target/python-runtime/python"
for f in "$BIN" "$ADBC_DIR/libadbc_driver_postgresql.so" "$ADBC_DIR/libadbc_driver_sqlite.so" \
         "$ADBC_DIR/libadbc_driver_duckdb.so" "$ADBC_DIR/libadbc_clickhouse.so"; do
  [ -f "$f" ] || { echo "missing $f — build it first (see this script's header)" >&2; exit 1; }
done
[ -x "$PYTHON_RUNTIME_DIR/bin/python3" ] || {
  echo "missing $PYTHON_RUNTIME_DIR — run ./scripts/build-python-runtime.sh first" >&2
  exit 1
}
command -v "$APPIMAGETOOL_BIN" >/dev/null || {
  echo "appimagetool not found — set \$APPIMAGETOOL or put it on PATH" >&2
  exit 1
}

mkdir -p "$APPDIR/usr/bin" "$APPDIR/usr/lib" "$APPDIR/usr/lib/nexusflow"
install -m 755 "$BIN" "$APPDIR/usr/bin/nexusflow-bin"
install -m 755 "$ADBC_DIR/libadbc_driver_postgresql.so" "$APPDIR/usr/lib/"
install -m 755 "$ADBC_DIR/libadbc_driver_sqlite.so" "$APPDIR/usr/lib/"
install -m 755 "$ADBC_DIR/libadbc_driver_duckdb.so" "$APPDIR/usr/lib/"
install -m 755 "$ADBC_DIR/libadbc_clickhouse.so" "$APPDIR/usr/lib/"
# Self-contained CPython + pandas/pyarrow/dbt-core/dbt-postgres (see
# scripts/build-python-runtime.sh) — `cp -a` preserves the symlinks a
# python-build-standalone tree relies on (e.g. bin/python3 -> python3.12).
cp -a "$PYTHON_RUNTIME_DIR" "$APPDIR/usr/lib/nexusflow/python"
install -m 755 "$REPO_ROOT/packaging/linux/AppRun" "$APPDIR/AppRun"
install -m 644 "$REPO_ROOT/packaging/linux/nexusflow.desktop" "$APPDIR/nexusflow.desktop"
install -m 644 "$REPO_ROOT/frontend/public/favicon.svg" "$APPDIR/nexusflow.svg"

mkdir -p "$OUT_DIR"
ARCH=x86_64 "$APPIMAGETOOL_BIN" "${APPIMAGETOOL_FLAGS[@]}" "$APPDIR" "$OUT_DIR/NexusFlow-${VERSION}-x86_64.AppImage"
echo "==> built $OUT_DIR/NexusFlow-${VERSION}-x86_64.AppImage"
