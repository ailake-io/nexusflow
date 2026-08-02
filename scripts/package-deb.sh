#!/usr/bin/env bash
# Builds nexusflow_<version>_amd64.deb — single-binary deploy (Marco 11,
# CLAUDE.md §7) plus the two ADBC driver .so's it dlopens at runtime (no
# crates.io distribution of those exists — see
# nexus-connector-postgres/src/driver.rs, ARCHITECTURE.md §3). This script
# only stages and packages already-built artifacts, it doesn't build Rust,
# C++ or the frontend itself:
#   npm --prefix frontend ci && npm --prefix frontend run build
#   cargo build --release -p nexusflow --features embed-ui
#   ./scripts/build-adbc-postgresql-driver.sh && ./scripts/build-adbc-sqlite-driver.sh
#
# Usage: ./scripts/package-deb.sh [OUT_DIR]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(grep -A3 '^\[workspace.package\]' "$REPO_ROOT/Cargo.toml" | grep '^version' | head -1 | cut -d '"' -f2)"
ARCH="amd64"
OUT_DIR="${1:-$REPO_ROOT/target/package}"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

BIN="$REPO_ROOT/target/release/nexusflow"
ADBC_DIR="$REPO_ROOT/target/adbc"
for f in "$BIN" "$ADBC_DIR/libadbc_driver_postgresql.so" "$ADBC_DIR/libadbc_driver_sqlite.so"; do
  [ -f "$f" ] || { echo "missing $f — build it first (see this script's header)" >&2; exit 1; }
done

mkdir -p "$STAGE/usr/lib/nexusflow" "$STAGE/usr/bin" \
  "$STAGE/usr/share/applications" "$STAGE/usr/share/icons/hicolor/scalable/apps" \
  "$STAGE/DEBIAN"

install -m 755 "$BIN" "$STAGE/usr/lib/nexusflow/nexusflow-bin"
install -m 755 "$ADBC_DIR/libadbc_driver_postgresql.so" "$STAGE/usr/lib/nexusflow/"
install -m 755 "$ADBC_DIR/libadbc_driver_sqlite.so" "$STAGE/usr/lib/nexusflow/"
install -m 755 "$REPO_ROOT/packaging/linux/nexusflow-wrapper.sh" "$STAGE/usr/bin/nexusflow"
install -m 644 "$REPO_ROOT/packaging/linux/nexusflow.desktop" "$STAGE/usr/share/applications/"
install -m 644 "$REPO_ROOT/frontend/public/favicon.svg" \
  "$STAGE/usr/share/icons/hicolor/scalable/apps/nexusflow.svg"

# libpq5/libsqlite3-0 pull in the rest of libadbc_driver_postgresql.so's
# transitive deps (libssl3, libgssapi-krb5-2, libldap-2.5-0, ...)
# automatically via their own Depends — nothing else needs listing here
# (confirmed via `ldd target/adbc/libadbc_driver_*.so`).
cat > "$STAGE/DEBIAN/control" <<EOF
Package: nexusflow
Version: $VERSION
Architecture: $ARCH
Maintainer: NexusFlow <noreply@ailake.io>
Depends: libpq5, libsqlite3-0
Section: database
Priority: optional
Homepage: https://github.com/ailake-io/nexusflow
Description: Universal Rust data & vector ETL/lakehouse framework
 Single-binary NexusFlow server: REST/WebSocket API, embedded web UI,
 connector router (ADBC/Arrow Flight/bridging) and AI embedding pipeline.
EOF

mkdir -p "$OUT_DIR"
dpkg-deb --build --root-owner-group "$STAGE" "$OUT_DIR/nexusflow_${VERSION}_${ARCH}.deb"
echo "==> built $OUT_DIR/nexusflow_${VERSION}_${ARCH}.deb"
