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

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/target/adbc}"
mkdir -p "$OUT_DIR"

if ! command -v dbc >/dev/null 2>&1; then
    echo "==> installing dbc CLI (Columnar Technologies, MIT-licensed installer)"
    curl -LsSf https://dbc.columnar.tech/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

echo "==> dbc install mssql (no auth needed, unlike oracle/hana/teradata)"
dbc install mssql

# `dbc` places the driver under a manifest directory that varies by
# environment (conda prefix, ~/.local, etc.) — read the real path back
# out of its own manifest instead of guessing a fixed location.
MANIFEST=""
for candidate in \
    "$HOME"/*/etc/adbc/drivers/mssql.toml \
    "$HOME"/.local/etc/adbc/drivers/mssql.toml \
    /usr/local/etc/adbc/drivers/mssql.toml
do
    if [ -f "$candidate" ]; then
        MANIFEST="$candidate"
        break
    fi
done

if [ -z "$MANIFEST" ]; then
    echo "error: could not locate mssql.toml manifest after 'dbc install mssql' — check dbc's own output above for the install path" >&2
    exit 1
fi

DRIVER_PATH=$(grep -oP "(?<=linux_amd64 = ')[^']+" "$MANIFEST")
if [ -z "$DRIVER_PATH" ] || [ ! -f "$DRIVER_PATH" ]; then
    echo "error: mssql.toml at $MANIFEST didn't yield a valid driver path ($DRIVER_PATH)" >&2
    exit 1
fi

cp "$DRIVER_PATH" "$OUT_DIR/libadbc_driver_mssql.so"
echo "==> done: $OUT_DIR/libadbc_driver_mssql.so"
