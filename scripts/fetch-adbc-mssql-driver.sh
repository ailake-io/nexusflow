#!/usr/bin/env bash
# Fetches libadbc_driver_mssql.so via the ADBC Driver Foundry's `dbc`
# CLI (Columnar Technologies) — free, no account/login needed for this
# specific driver (confirmed this session: only Oracle/Teradata/SAP
# HANA are gated behind `dbc auth login`; mssql is not). License is
# "Permissive Binary License" — redistribution in binary form is
# explicitly allowed, unlike SAP HANA's client.
#
# Tested end-to-end in this session:
#   curl -LsSf https://dbc.columnar.tech/install.sh | sh   # installs `dbc`
#   dbc install mssql                                       # no auth prompt
#   -> installed to <prefix>/etc/adbc/drivers/mssql_linux_amd64_<ver>/libadbc_driver_mssql.so
#
# Usage:
#   ./scripts/fetch-adbc-mssql-driver.sh [OUT_DIR]
#
# Output: $OUT_DIR/libadbc_driver_mssql.so
#
# Env (both pinned below — found in a security review, 2026-09-27:
# unpinned `dbc install mssql` would silently pull whatever's newest at
# build time, same reproducibility gap `DUCKDB_VERSION` already closes
# for the duckdb fetch):
#   DBC_CLI_VERSION      pinned `dbc` CLI release (`dbc install --version`
#                        confirms `dbc install DRIVER=X.Y.Z` constraint
#                        syntax; the CLI itself still needs its own pin)
#   MSSQL_ADBC_VERSION   pinned mssql driver release. `dbc install` verifies
#                        the driver's signature+checksum by default (no
#                        `--no-verify`/`--insecure-no-checksum` flag used
#                        here) — pinning the version is about reproducible
#                        builds, not integrity (integrity is already covered).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/target/adbc}"
DBC_CLI_VERSION="${DBC_CLI_VERSION:-0.3.0}"
MSSQL_ADBC_VERSION="${MSSQL_ADBC_VERSION:-1.6.2}"
mkdir -p "$OUT_DIR"

if ! command -v dbc >/dev/null 2>&1; then
    echo "==> installing dbc CLI ${DBC_CLI_VERSION} (Columnar Technologies, MIT-licensed installer)"
    curl -LsSf https://dbc.columnar.tech/install.sh | sh -s -- --version "$DBC_CLI_VERSION"
    export PATH="$HOME/.local/bin:$PATH"
fi

echo "==> dbc install mssql=${MSSQL_ADBC_VERSION} (no auth needed, unlike oracle/hana/teradata)"
dbc install "mssql=${MSSQL_ADBC_VERSION}"

# `dbc` places the driver under a manifest directory that varies by
# environment (conda prefix, ~/.local, XDG config dir, etc.) — read the
# real path back out of its own manifest instead of guessing a fixed
# location. A fixed candidate list was tried first and proved wrong in a
# real run (2026-09-27, plain root-in-container: `dbc` 1.6.2 actually
# wrote to `$HOME/.config/adbc/drivers/mssql.toml`, XDG_CONFIG_HOME's
# default, not `.local/etc/` or `/usr/local/etc/` like every other
# candidate this script tried) — a `find` under `$HOME` is what actually
# locates it regardless of which layout this dbc version/environment
# picks.
MANIFEST="$(find "$HOME" -maxdepth 6 -iname 'mssql.toml' -path '*adbc*drivers*' 2>/dev/null | head -n1)"

if [ -z "$MANIFEST" ]; then
    echo "error: could not locate mssql.toml manifest under \$HOME after 'dbc install mssql' — check dbc's own output above for the install path" >&2
    exit 1
fi

DRIVER_PATH=$(grep -oP "(?<=linux_amd64 = ')[^']+" "$MANIFEST")
if [ -z "$DRIVER_PATH" ] || [ ! -f "$DRIVER_PATH" ]; then
    echo "error: mssql.toml at $MANIFEST didn't yield a valid driver path ($DRIVER_PATH)" >&2
    exit 1
fi

cp "$DRIVER_PATH" "$OUT_DIR/libadbc_driver_mssql.so"
echo "==> done: $OUT_DIR/libadbc_driver_mssql.so"
