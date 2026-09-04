#!/usr/bin/env bash
# Downloads a relocatable, self-contained CPython build (python-build-
# standalone's "install_only" variant — built by design for exactly this
# "drop it anywhere, it just works" bundling use case, no absolute-prefix
# baked into the interpreter) and pre-installs every package the
# `python-transform` and `dbt` Cargo features need at *build* time. This is
# what lets the Linux packages (AppImage/.deb/.rpm) ship those two features
# fully self-contained — CLAUDE.md §7's single-binary-deploy story
# previously assumed the operator's own system already had python3 +
# pandas/pyarrow/... and a `dbt` CLI on PATH, same gap the Docker image
# closed for itself via the Dockerfile's own `pip3 install` (see its
# comment) but the native packages never did.
#
# Usage: ./scripts/build-python-runtime.sh [OUT_DIR]
# Output: $OUT_DIR/python/  (bin/python3, bin/dbt, lib/... — fully
# self-contained, no system python involved at install time or runtime).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/target/python-runtime}"

# Pinned to a specific python-build-standalone release for reproducibility
# (same rationale as nexus-ai's ONNX model revision pin, EmbeddingModelSpec::
# Onnx::revision) — bump both together when a newer release is actually
# needed, don't float on "latest" and risk a build silently picking up a
# different Python patch version. SHA256 is the release's own published
# digest (GitHub release API `assets[].digest`, confirmed against a real
# download, not guessed) — verified below so a compromised/tampered
# download fails the build instead of silently shipping.
PBS_TAG="${PBS_TAG:-20260901}"
PBS_PYTHON_VERSION="${PBS_PYTHON_VERSION:-3.12.14}"
PBS_SHA256="${PBS_SHA256:-72748da13197c1fb161e3afeef20a6a385ff24f2165e6e2758e47008e7faba4c}"
PBS_URL="https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_TAG}/cpython-${PBS_PYTHON_VERSION}%2B${PBS_TAG}-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "==> downloading $PBS_URL"
curl -LsSf "$PBS_URL" -o "$OUT_DIR/python-runtime.tar.gz"
echo "$PBS_SHA256  $OUT_DIR/python-runtime.tar.gz" | sha256sum -c -
# The install_only(_stripped) tarball's own top-level directory is named
# "python/" — extracting into $OUT_DIR yields $OUT_DIR/python directly, no
# renaming needed.
tar -xzf "$OUT_DIR/python-runtime.tar.gz" -C "$OUT_DIR"
rm "$OUT_DIR/python-runtime.tar.gz"

PYTHON_BIN="$OUT_DIR/python/bin/python3"
echo "==> installing packages the python-transform/dbt features need"
# Pinned (not floating "latest") — matches this same tightening in the
# Dockerfile's own pip3 install, so both distribution paths install
# identical, reproducible versions.
"$PYTHON_BIN" -m pip install --no-cache-dir \
  pandas==2.2.3 numpy==1.26.4 pyarrow==17.0.0 polars==1.8.2 python-dateutil==2.9.0 \
  dbt-core==1.12.3 dbt-postgres==1.11.0

# pip bakes an *absolute* shebang into every console script it installs,
# pointing at this exact build machine's python3 path — harmless for a .deb/
# .rpm (always extracted to the same fixed /usr/lib/nexusflow/python), but
# breaks the AppImage, which re-mounts $APPDIR at a fresh temp path on every
# launch. Rewriting to `#!/usr/bin/env python3` makes it resolve via PATH
# instead — safe because every packaging script's AppRun/wrapper already
# prepends this tree's own bin/ to PATH ahead of any system python3, so the
# lookup always finds *this* interpreter, never a stray system one.
for script in "$OUT_DIR"/python/bin/dbt; do
  [ -f "$script" ] && sed -i '1s|^#!.*|#!/usr/bin/env python3|' "$script"
done

echo "==> done: $OUT_DIR/python (bin/python3, bin/dbt)"
