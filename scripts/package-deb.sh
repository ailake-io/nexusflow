#!/usr/bin/env bash
# Builds nexusflow_<version>_amd64.deb — single-binary deploy (Marco 11,
# CLAUDE.md §7) plus the four ADBC driver .so's it dlopens at runtime (no
# crates.io distribution of those exists — see
# nexus-connector-postgres/src/driver.rs, ARCHITECTURE.md §3). This script
# only stages and packages already-built artifacts, it doesn't build Rust,
# C++ or the frontend itself:
#   npm --prefix frontend ci && npm --prefix frontend run build
#   cargo build --release -p nexusflow --features embed-ui,connectors-all
#   ./scripts/build-adbc-postgresql-driver.sh && ./scripts/build-adbc-sqlite-driver.sh \
#     && ./scripts/build-adbc-duckdb-driver.sh && ./scripts/build-adbc-clickhouse-driver.sh
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
PYTHON_RUNTIME_DIR="$REPO_ROOT/target/python-runtime/python"
for f in "$BIN" "$ADBC_DIR/libadbc_driver_postgresql.so" "$ADBC_DIR/libadbc_driver_sqlite.so" \
         "$ADBC_DIR/libadbc_driver_duckdb.so" "$ADBC_DIR/libadbc_clickhouse.so"; do
  [ -f "$f" ] || { echo "missing $f — build it first (see this script's header)" >&2; exit 1; }
done
[ -x "$PYTHON_RUNTIME_DIR/bin/python3" ] || {
  echo "missing $PYTHON_RUNTIME_DIR — run ./scripts/build-python-runtime.sh first" >&2
  exit 1
}

mkdir -p "$STAGE/usr/lib/nexusflow" "$STAGE/usr/bin" \
  "$STAGE/usr/share/applications" "$STAGE/usr/share/icons/hicolor/scalable/apps" \
  "$STAGE/lib/systemd/system" "$STAGE/var/lib/nexusflow" "$STAGE/etc/nexusflow" \
  "$STAGE/DEBIAN"

install -m 755 "$BIN" "$STAGE/usr/lib/nexusflow/nexusflow-bin"
install -m 755 "$ADBC_DIR/libadbc_driver_postgresql.so" "$STAGE/usr/lib/nexusflow/"
install -m 755 "$ADBC_DIR/libadbc_driver_sqlite.so" "$STAGE/usr/lib/nexusflow/"
install -m 755 "$ADBC_DIR/libadbc_driver_duckdb.so" "$STAGE/usr/lib/nexusflow/"
install -m 755 "$ADBC_DIR/libadbc_clickhouse.so" "$STAGE/usr/lib/nexusflow/"
# Self-contained CPython + pandas/pyarrow/dbt-core/dbt-postgres (see
# scripts/build-python-runtime.sh) — makes the `python-transform`/`dbt`
# features work with zero manual install on the target machine.
cp -a "$PYTHON_RUNTIME_DIR" "$STAGE/usr/lib/nexusflow/python"
install -m 755 "$REPO_ROOT/packaging/linux/nexusflow-wrapper.sh" "$STAGE/usr/bin/nexusflow"
install -m 644 "$REPO_ROOT/packaging/linux/nexusflow.desktop" "$STAGE/usr/share/applications/"
install -m 644 "$REPO_ROOT/packaging/linux/nexusflow.service" "$STAGE/lib/systemd/system/"
install -m 644 "$REPO_ROOT/frontend/public/favicon.svg" \
  "$STAGE/usr/share/icons/hicolor/scalable/apps/nexusflow.svg"

# libpq5/libsqlite3-0 pull in the rest of libadbc_driver_postgresql.so's
# transitive deps (libssl3, libgssapi-krb5-2, libldap-2.5-0, ...)
# automatically via their own Depends (confirmed via
# `ldd target/adbc/libadbc_driver_*.so`). unixodbc is nexus-connector-odbc's
# runtime driver manager (odbc-api dlopens it, doesn't bundle it — connectors-all
# is now compiled in, see release.yml); libsasl2-2 covers rdkafka's SASL auth
# path, which its bundled librdkafka build links against dynamically even
# though librdkafka itself is statically linked in.
cat > "$STAGE/DEBIAN/control" <<EOF
Package: nexusflow
Version: $VERSION
Architecture: $ARCH
Maintainer: NexusFlow <noreply@ailake.io>
Depends: libpq5, libsqlite3-0, unixodbc, libsasl2-2
Section: database
Priority: optional
Homepage: https://github.com/ailake-io/nexusflow
Description: Universal Rust data & vector ETL/lakehouse framework
 Single-binary NexusFlow server: REST/WebSocket API, embedded web UI,
 connector router (ADBC/Arrow Flight/bridging) and AI embedding pipeline.
EOF

cat > "$STAGE/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e

if ! id -u nexusflow >/dev/null 2>&1; then
  useradd -r -m -d /var/lib/nexusflow -s /usr/sbin/nologin nexusflow
fi

mkdir -p /var/lib/nexusflow /etc/nexusflow
chown nexusflow:nexusflow /var/lib/nexusflow

if [ ! -f /etc/nexusflow/nexusflow.env ]; then
  cat > /etc/nexusflow/nexusflow.env <<'ENV'
# NexusFlow environment configuration
# NEXUS_JWT_SECRET=change-me
# NEXUS_ENCRYPTION_KEY=change-me
ENV
  chmod 640 /etc/nexusflow/nexusflow.env
  chown root:nexusflow /etc/nexusflow/nexusflow.env
fi

if [ -d /run/systemd/system ]; then
  systemctl daemon-reload >/dev/null 2>&1 || true
  systemctl enable nexusflow.service >/dev/null 2>&1 || true
fi

#DEBHELPER#
EOF
chmod 755 "$STAGE/DEBIAN/postinst"

mkdir -p "$OUT_DIR"
dpkg-deb --build --root-owner-group "$STAGE" "$OUT_DIR/nexusflow_${VERSION}_${ARCH}.deb"
echo "==> built $OUT_DIR/nexusflow_${VERSION}_${ARCH}.deb"
