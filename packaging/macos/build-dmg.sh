#!/usr/bin/env bash
# Builds NexusFlow-<version>.dmg — NOT validated (no macOS host in this dev
# sandbox, see IMPLEMENTATION_PLAN.md Marco 11; `hdiutil`/`codesign` only
# exist on macOS). Mirrors scripts/package-appimage.sh's AppDir approach: a
# self-contained bundle wrapping the release binary + ADBC driver .dylib's,
# with the same "no built-in default path" problem those drivers have (see
# nexusflow-wrapper.sh here and nexus-connector-postgres/src/driver.rs).
#
# Same known gap as ../macos/nexusflow.rb: scripts/build-adbc-*-driver.sh
# hardcode a `.so` output extension, which won't match CMake's `.dylib`
# output on macOS as-is.
#
# Prerequisites (already built, this script only assembles + packages):
#   npm --prefix frontend ci && npm --prefix frontend run build
#   cargo build --release --features embed-ui
#   ./scripts/build-adbc-postgresql-driver.sh target/adbc
#   ./scripts/build-adbc-sqlite-driver.sh target/adbc
#
# Usage: ./packaging/macos/build-dmg.sh [OUT_DIR]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VERSION="$(grep -A3 '^\[workspace.package\]' "$REPO_ROOT/Cargo.toml" | grep '^version' | head -1 | cut -d '"' -f2)"
OUT_DIR="${1:-$REPO_ROOT/target/package}"
WORK_DIR="$(mktemp -d)"
APP="$WORK_DIR/NexusFlow.app"
trap 'rm -rf "$WORK_DIR"' EXIT

BIN="$REPO_ROOT/target/release/nexusflow"
ADBC_DIR="$REPO_ROOT/target/adbc"
for f in "$BIN" "$ADBC_DIR/libadbc_driver_postgresql.dylib" "$ADBC_DIR/libadbc_driver_sqlite.dylib"; do
  [ -f "$f" ] || { echo "missing $f — build it first (see this script's header)" >&2; exit 1; }
done

mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
install -m 755 "$BIN" "$APP/Contents/MacOS/nexusflow-bin"
install -m 755 "$REPO_ROOT/packaging/macos/nexusflow-wrapper.sh" "$APP/Contents/MacOS/nexusflow"
install -m 644 "$ADBC_DIR/libadbc_driver_postgresql.dylib" "$APP/Contents/Resources/"
install -m 644 "$ADBC_DIR/libadbc_driver_sqlite.dylib" "$APP/Contents/Resources/"

cat > "$APP/Contents/Info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>NexusFlow</string>
  <key>CFBundleIdentifier</key><string>io.ailake.nexusflow</string>
  <key>CFBundleVersion</key><string>$VERSION</string>
  <key>CFBundleShortVersionString</key><string>$VERSION</string>
  <key>CFBundleExecutable</key><string>nexusflow</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>LSBackgroundOnly</key><false/>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
EOF

mkdir -p "$OUT_DIR"
hdiutil create -volname "NexusFlow" -srcfolder "$APP" -ov -format UDZO \
  "$OUT_DIR/NexusFlow-${VERSION}.dmg"
echo "==> built $OUT_DIR/NexusFlow-${VERSION}.dmg"
