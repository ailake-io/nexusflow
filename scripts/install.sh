#!/bin/sh
# Installs nexusflow (Linux x86_64 only) without a package manager — same
# pattern as rustup/dbt-fusion's installers.
#
#   curl -fsSL https://raw.githubusercontent.com/ailake-io/nexusflow/develop/scripts/install.sh | sh
#
# Downloads a release tarball built by .github/workflows/release.yml
# (nexusflow-linux-x86_64.tar.gz: the release binary built with
# --features embed-ui,connectors-all, plus the two ADBC driver libraries
# nexus-connector-postgres/-sqlite dlopen at runtime — see
# nexus-connector-postgres/src/driver.rs, ARCHITECTURE.md §3), then writes a
# thin wrapper at $NEXUSFLOW_BIN_DIR/nexusflow that points
# ADBC_DRIVER_POSTGRESQL_PATH/ADBC_DRIVER_SQLITE_PATH at the two libraries
# it just installed — same wiring as packaging/linux/nexusflow-wrapper.sh,
# just resolved to this install location instead of a fixed /usr/lib path.
#
# Env overrides:
#   NEXUSFLOW_VERSION   git tag to install (default: latest release)
#   NEXUSFLOW_INSTALL_DIR   where the binary + drivers live (default: ~/.local/share/nexusflow)
#   NEXUSFLOW_BIN_DIR       where the `nexusflow` wrapper is linked (default: ~/.local/bin)

set -eu

REPO="ailake-io/nexusflow"
INSTALL_DIR="${NEXUSFLOW_INSTALL_DIR:-$HOME/.local/share/nexusflow}"
BIN_DIR="${NEXUSFLOW_BIN_DIR:-$HOME/.local/bin}"

os="$(uname -s)"
arch="$(uname -m)"

[ "$os" = "Linux" ] || { echo "error: unsupported OS: $os (only Linux has prebuilt binaries)" >&2; exit 1; }

case "$arch" in
  x86_64|amd64) arch="x86_64" ;;
  *) echo "error: unsupported architecture: $arch (only x86_64 has prebuilt binaries)" >&2; exit 1 ;;
esac

if [ -n "${NEXUSFLOW_VERSION:-}" ]; then
  version="$NEXUSFLOW_VERSION"
else
  echo "==> looking up latest release"
  version="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
    | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')"
  [ -n "$version" ] || { echo "error: could not determine latest release version" >&2; exit 1; }
fi

asset="nexusflow-${platform}-${arch}.tar.gz"
url="https://github.com/$REPO/releases/download/$version/$asset"

echo "==> installing nexusflow $version ($platform/$arch)"
echo "==> downloading $url"

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

curl -fsSL "$url" -o "$work_dir/$asset"

# Verify tarball integrity against the published SHA256SUMS before extracting.
sums_url="https://github.com/$REPO/releases/download/$version/SHA256SUMS"
if ! curl -fsSL "$sums_url" -o "$work_dir/SHA256SUMS"; then
  echo "error: could not download SHA256SUMS for $version" >&2
  exit 1
fi
(
  cd "$work_dir"
  sha256sum -c "SHA256SUMS" --ignore-missing || {
    echo "error: checksum verification failed for $asset" >&2
    exit 1
  }
)

# Verify GPG signature if SHA256SUMS.asc is published and gpg is available.
asc_url="https://github.com/$REPO/releases/download/$version/SHA256SUMS.asc"
if command -v gpg >/dev/null 2>&1 && curl -fsSL "$asc_url" -o "$work_dir/SHA256SUMS.asc"; then
  if ! gpg --verify "$work_dir/SHA256SUMS.asc" "$work_dir/SHA256SUMS" >/dev/null 2>&1; then
    echo "error: GPG signature verification failed for SHA256SUMS" >&2
    exit 1
  fi
fi

tar -xzf "$work_dir/$asset" -C "$work_dir"

mkdir -p "$INSTALL_DIR" "$BIN_DIR"
install -m 755 "$work_dir/nexusflow-bin" "$INSTALL_DIR/nexusflow-bin"

lib_ext="so"
install -m 755 "$work_dir/libadbc_driver_postgresql.$lib_ext" "$INSTALL_DIR/"
install -m 755 "$work_dir/libadbc_driver_sqlite.$lib_ext" "$INSTALL_DIR/"

cat > "$BIN_DIR/nexusflow" <<EOF
#!/bin/sh
export ADBC_DRIVER_POSTGRESQL_PATH="\${ADBC_DRIVER_POSTGRESQL_PATH:-$INSTALL_DIR/libadbc_driver_postgresql.$lib_ext}"
export ADBC_DRIVER_SQLITE_PATH="\${ADBC_DRIVER_SQLITE_PATH:-$INSTALL_DIR/libadbc_driver_sqlite.$lib_ext}"
exec "$INSTALL_DIR/nexusflow-bin" "\$@"
EOF
chmod 755 "$BIN_DIR/nexusflow"

echo "==> installed to $BIN_DIR/nexusflow"
echo "==> note: the odbc/kafka connectors need unixODBC + libsasl2 on this machine"
echo "    (Debian/Ubuntu: sudo apt-get install unixodbc libsasl2-2)"
case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) echo "==> $BIN_DIR is not on your PATH — add it, e.g.: export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac
echo "==> run: NEXUS_JWT_SECRET=... NEXUS_ENCRYPTION_KEY=\$(openssl rand -hex 32) nexusflow"
