# Homebrew formula — NOT validated (no macOS host in this dev sandbox, see
# IMPLEMENTATION_PLAN.md Marco 11). Builds from source rather than a
# prebuilt bottle, since the release binary and frontend/dist both need to
# be assembled per-platform (CLAUDE.md §7).
#
# KNOWN GAP: scripts/build-adbc-postgresql-driver.sh and
# build-adbc-sqlite-driver.sh hardcode a `.so` output extension — CMake
# produces `.dylib` on macOS instead, so the two `cp` lines in those scripts
# will not find their source files as-is on a Mac. They need a small
# platform-aware fix (glob or `uname`-based extension) before this formula's
# `install` block below can actually succeed — tracked here rather than
# fixed blind, since there's no macOS host to verify the fix against.
#
# Install with: brew install --build-from-source ./nexusflow.rb
class Nexusflow < Formula
  desc "Universal Rust data & vector ETL/lakehouse framework"
  homepage "https://github.com/ailake-io/nexusflow"
  url "https://github.com/ailake-io/nexusflow/archive/refs/tags/v0.1.0.tar.gz"
  # sha256 must be updated by the release workflow or maintainer:
  # curl -L <url> | shasum -a 256
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  license "Apache-2.0"

  depends_on "rust" => :build
  depends_on "node" => :build
  depends_on "cmake" => :build
  depends_on "pkg-config" => :build
  depends_on "libpq"
  depends_on "sqlite"

  def install
    system "npm", "--prefix", "frontend", "ci"
    system "npm", "--prefix", "frontend", "run", "build"

    adbc_dir = buildpath/"target/adbc"
    system "./scripts/build-adbc-postgresql-driver.sh", adbc_dir
    system "./scripts/build-adbc-sqlite-driver.sh", adbc_dir

    system "cargo", "build", "--release", "--features", "embed-ui"

    libexec.install "target/release/nexusflow" => "nexusflow-bin"
    libexec.install Dir[adbc_dir/"libadbc_driver_*.dylib"]

    # Same env-var wiring as packaging/linux/nexusflow-wrapper.sh — points
    # the ADBC connectors at the drivers just built into libexec, since
    # there's still no crates.io distribution of them to link statically
    # (nexus-connector-postgres/src/driver.rs, ARCHITECTURE.md §3).
    (bin/"nexusflow").write <<~SH
      #!/bin/bash
      export ADBC_DRIVER_POSTGRESQL_PATH="${ADBC_DRIVER_POSTGRESQL_PATH:-#{libexec}/libadbc_driver_postgresql.dylib}"
      export ADBC_DRIVER_SQLITE_PATH="${ADBC_DRIVER_SQLITE_PATH:-#{libexec}/libadbc_driver_sqlite.dylib}"
      exec "#{libexec}/nexusflow-bin" "$@"
    SH
    (bin/"nexusflow").chmod 0755
  end

  test do
    # Real smoke test once buildable: start the server and check /health,
    # mirroring the Linux .deb/AppImage validation in scripts/package-deb.sh.
    # run() (nexus-server/src/lib.rs) requires these two env vars to boot at
    # all — NEXUS_JWT_SECRET/NEXUS_ENCRYPTION_KEY, see ARCHITECTURE.md §10.
    ENV["NEXUS_JWT_SECRET"] = "test-secret"
    ENV["NEXUS_ENCRYPTION_KEY"] = "ab" * 32
    ENV["NEXUS_CHECKPOINT_DB"] = "sqlite::memory:"
    ENV["NEXUS_AUTH_DB"] = "sqlite::memory:"
    ENV["NEXUS_PIPELINES_DB"] = "sqlite::memory:"
    pid = fork { exec bin/"nexusflow" }
    sleep 1
    begin
      assert_match "ok", shell_output("curl -s http://localhost:8080/health")
    ensure
      Process.kill("TERM", pid)
    end
  end
end
