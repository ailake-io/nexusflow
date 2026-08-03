#!/usr/bin/env bash
# Builds NexusFlow-<version>-x86_64.AppImage. Same prerequisites as
# package-deb.sh (release binary w/ embed-ui,connectors-all + both ADBC driver .so's
# already built) plus `appimagetool` on PATH or at $APPIMAGETOOL — get it
# from https://github.com/AppImage/appimagetool/releases (continuous build).
#
# Usage: APPIMAGETOOL=/path/to/appimagetool ./scripts/package-appimage.sh [OUT_DIR]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(grep -A3 '^\[workspace.package\]' "$REPO_ROOT/Cargo.toml" | grep '^version' | head -1 | cut -d '"' -f2)"
OUT_DIR="${1:-$REPO_ROOT/target/package}"
APPIMAGETOOL="${APPIMAGETOOL:-appimagetool}"
WORK_DIR="$(mktemp -d)"
APPDIR="$WORK_DIR/NexusFlow.AppDir"
trap 'rm -rf "$WORK_DIR"' EXIT

BIN="$REPO_ROOT/target/release/nexusflow"
ADBC_DIR="$REPO_ROOT/target/adbc"
for f in "$BIN" "$ADBC_DIR/libadbc_driver_postgresql.so" "$ADBC_DIR/libadbc_driver_sqlite.so"; do
  [ -f "$f" ] || { echo "missing $f — build it first (see this script's header)" >&2; exit 1; }
done
command -v "$APPIMAGETOOL" >/dev/null || {
  echo "appimagetool not found — set \$APPIMAGETOOL or put it on PATH" >&2
  exit 1
}

mkdir -p "$APPDIR/usr/bin" "$APPDIR/usr/lib"
install -m 755 "$BIN" "$APPDIR/usr/bin/nexusflow-bin"
install -m 755 "$ADBC_DIR/libadbc_driver_postgresql.so" "$APPDIR/usr/lib/"
install -m 755 "$ADBC_DIR/libadbc_driver_sqlite.so" "$APPDIR/usr/lib/"
install -m 755 "$REPO_ROOT/packaging/linux/AppRun" "$APPDIR/AppRun"
install -m 644 "$REPO_ROOT/packaging/linux/nexusflow.desktop" "$APPDIR/nexusflow.desktop"
install -m 644 "$REPO_ROOT/frontend/public/favicon.svg" "$APPDIR/nexusflow.svg"

mkdir -p "$OUT_DIR"
ARCH=x86_64 "$APPIMAGETOOL" "$APPDIR" "$OUT_DIR/NexusFlow-${VERSION}-x86_64.AppImage"
echo "==> built $OUT_DIR/NexusFlow-${VERSION}-x86_64.AppImage"
