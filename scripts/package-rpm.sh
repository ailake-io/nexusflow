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
for f in "$BIN" "$ADBC_DIR/libadbc_driver_postgresql.so" "$ADBC_DIR/libadbc_driver_sqlite.so"; do
  [ -f "$f" ] || { echo "missing $f — build it first (see this script's header)" >&2; exit 1; }
done
command -v rpmbuild >/dev/null || { echo "rpmbuild not found (apt install rpm on Debian/Ubuntu)" >&2; exit 1; }

RPMBUILD_ROOT="$(mktemp -d)"
trap 'rm -rf "$RPMBUILD_ROOT"' EXIT
mkdir -p "$RPMBUILD_ROOT"/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

# No %build section — this stages already-built artifacts (see package-deb.sh's
# header for the exact build commands), it doesn't recompile Rust/C++/the
# frontend. libpq/sqlite-libs are the RPM-side names of deb's libpq5/
# libsqlite3-0 (same rationale: pulls in the rest of the driver .so's
# transitive deps automatically, see package-deb.sh).
cat > "$RPMBUILD_ROOT/SPECS/nexusflow.spec" <<EOF
Name: nexusflow
Version: $VERSION
Release: 1%{?dist}
Summary: Universal Rust data & vector ETL/lakehouse framework
License: Apache-2.0
URL: https://github.com/ailake-io/nexusflow
Requires: libpq, sqlite-libs
BuildArch: x86_64

%description
Single-binary NexusFlow server: REST/WebSocket API, embedded web UI,
connector router (ADBC/Arrow Flight/bridging) and AI embedding pipeline.

%install
mkdir -p %{buildroot}/usr/lib/nexusflow %{buildroot}/usr/bin %{buildroot}/usr/share/applications %{buildroot}/usr/share/icons/hicolor/scalable/apps
install -m 755 $BIN %{buildroot}/usr/lib/nexusflow/nexusflow-bin
install -m 755 $ADBC_DIR/libadbc_driver_postgresql.so %{buildroot}/usr/lib/nexusflow/
install -m 755 $ADBC_DIR/libadbc_driver_sqlite.so %{buildroot}/usr/lib/nexusflow/
install -m 755 $REPO_ROOT/packaging/linux/nexusflow-wrapper.sh %{buildroot}/usr/bin/nexusflow
install -m 644 $REPO_ROOT/packaging/linux/nexusflow.desktop %{buildroot}/usr/share/applications/
install -m 644 $REPO_ROOT/frontend/public/favicon.svg %{buildroot}/usr/share/icons/hicolor/scalable/apps/nexusflow.svg

%files
/usr/lib/nexusflow/nexusflow-bin
/usr/lib/nexusflow/libadbc_driver_postgresql.so
/usr/lib/nexusflow/libadbc_driver_sqlite.so
/usr/bin/nexusflow
/usr/share/applications/nexusflow.desktop
/usr/share/icons/hicolor/scalable/apps/nexusflow.svg
EOF

rpmbuild --define "_topdir $RPMBUILD_ROOT" -bb "$RPMBUILD_ROOT/SPECS/nexusflow.spec"
mkdir -p "$OUT_DIR"
cp "$RPMBUILD_ROOT"/RPMS/x86_64/*.rpm "$OUT_DIR/"
echo "==> built rpm(s) in $OUT_DIR"
