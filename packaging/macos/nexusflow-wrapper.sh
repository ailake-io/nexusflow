#!/bin/bash
# Bundled inside NexusFlow.app/Contents/MacOS/ — same ADBC driver-path
# wiring as packaging/linux/nexusflow-wrapper.sh, but .dylib instead of .so
# and relative to the .app bundle's own Resources dir (a macOS .app can be
# moved/renamed by the user, so no fixed absolute path like the Linux
# deb/rpm's /usr/lib/nexusflow works here).
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESOURCES="$HERE/../Resources"
export ADBC_DRIVER_POSTGRESQL_PATH="$RESOURCES/libadbc_driver_postgresql.dylib"
export ADBC_DRIVER_SQLITE_PATH="$RESOURCES/libadbc_driver_sqlite.dylib"
exec "$HERE/nexusflow-bin" "$@"
