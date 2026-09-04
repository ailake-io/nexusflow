#!/usr/bin/env bash
# Builds an nexusflow-<version>-1.*.x86_64.rpm via rpmbuild. Same
# prerequisites as package-deb.sh. Needs `rpmbuild` on PATH — on
# Debian/Ubuntu that's `apt install rpm`; Fedora/RHEL/openSUSE ship it by
# default.
#
# Usage: ./scripts/package-rpm.sh [OUT_DIR]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(grep -A3 '^\[workspace.package\]' "$REPO_ROOT/Cargo.toml" | grep '^version' | head -1 | cut -d '"' -f2)"
OUT_DIR="${1:-$REPO_ROOT/target/package}"

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
command -v rpmbuild >/dev/null || { echo "rpmbuild not found (apt install rpm on Debian/Ubuntu)" >&2; exit 1; }

RPMBUILD_ROOT="$(mktemp -d)"
trap 'rm -rf "$RPMBUILD_ROOT"' EXIT
mkdir -p "$RPMBUILD_ROOT"/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

# No %build section — this stages already-built artifacts (see package-deb.sh's
# header for the exact build commands), it doesn't recompile Rust/C++/the
# frontend. libpq/sqlite-libs are the RPM-side names of deb's libpq5/
# libsqlite3-0 (same rationale: pulls in the rest of the driver .so's
# transitive deps automatically, see package-deb.sh). unixODBC/cyrus-sasl-lib
# are the Fedora/RHEL names of deb's unixodbc/libsasl2-2 — same rationale,
# connectors-all is compiled in (nexus-connector-odbc dlopens unixODBC,
# rdkafka's SASL auth path links libsasl2 dynamically even though librdkafka
# itself is static). openSUSE names this package `libsasl2-3` instead of
# `cyrus-sasl-lib`; not handled here since this spec targets Fedora/RHEL.
cat > "$RPMBUILD_ROOT/SPECS/nexusflow.spec" <<EOF
Name: nexusflow
Version: $VERSION
Release: 1%{?dist}
Summary: Universal Rust data & vector ETL/lakehouse framework
License: Apache-2.0
URL: https://github.com/ailake-io/nexusflow
Requires: libpq, sqlite-libs, unixODBC, cyrus-sasl-lib
BuildArch: x86_64

%description
Single-binary NexusFlow server: REST/WebSocket API, embedded web UI,
connector router (ADBC/Arrow Flight/bridging) and AI embedding pipeline.

%install
mkdir -p %{buildroot}/usr/lib/nexusflow %{buildroot}/usr/bin %{buildroot}/usr/share/applications %{buildroot}/usr/share/icons/hicolor/scalable/apps %{buildroot}/usr/lib/systemd/system %{buildroot}/var/lib/nexusflow %{buildroot}/etc/nexusflow
install -m 755 $BIN %{buildroot}/usr/lib/nexusflow/nexusflow-bin
install -m 755 $ADBC_DIR/libadbc_driver_postgresql.so %{buildroot}/usr/lib/nexusflow/
install -m 755 $ADBC_DIR/libadbc_driver_sqlite.so %{buildroot}/usr/lib/nexusflow/
install -m 755 $ADBC_DIR/libadbc_driver_duckdb.so %{buildroot}/usr/lib/nexusflow/
install -m 755 $ADBC_DIR/libadbc_clickhouse.so %{buildroot}/usr/lib/nexusflow/
cp -a $PYTHON_RUNTIME_DIR %{buildroot}/usr/lib/nexusflow/python
install -m 755 $REPO_ROOT/packaging/linux/nexusflow-wrapper.sh %{buildroot}/usr/bin/nexusflow
install -m 644 $REPO_ROOT/packaging/linux/nexusflow.desktop %{buildroot}/usr/share/applications/
install -m 644 $REPO_ROOT/packaging/linux/nexusflow.service %{buildroot}/usr/lib/systemd/system/
install -m 644 $REPO_ROOT/frontend/public/favicon.svg %{buildroot}/usr/share/icons/hicolor/scalable/apps/nexusflow.svg

%post
if ! id -u nexusflow >/dev/null 2>&1; then
  useradd -r -m -d /var/lib/nexusflow -s /sbin/nologin nexusflow
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
  systemctl preset nexusflow.service >/dev/null 2>&1 || true
fi

%files
%attr(755, root, root) /usr/lib/nexusflow/nexusflow-bin
%attr(755, root, root) /usr/lib/nexusflow/libadbc_driver_postgresql.so
%attr(755, root, root) /usr/lib/nexusflow/libadbc_driver_sqlite.so
%attr(755, root, root) /usr/lib/nexusflow/libadbc_driver_duckdb.so
%attr(755, root, root) /usr/lib/nexusflow/libadbc_clickhouse.so
/usr/lib/nexusflow/python
%attr(755, root, root) /usr/bin/nexusflow
%attr(644, root, root) /usr/share/applications/nexusflow.desktop
%attr(644, root, root) /usr/lib/systemd/system/nexusflow.service
%attr(644, root, root) /usr/share/icons/hicolor/scalable/apps/nexusflow.svg
%attr(750, nexusflow, nexusflow) %dir /var/lib/nexusflow
%attr(750, root, nexusflow) %dir /etc/nexusflow
EOF

rpmbuild --define "_topdir $RPMBUILD_ROOT" -bb "$RPMBUILD_ROOT/SPECS/nexusflow.spec"
mkdir -p "$OUT_DIR"
cp "$RPMBUILD_ROOT"/RPMS/x86_64/*.rpm "$OUT_DIR/"
echo "==> built rpm(s) in $OUT_DIR"
