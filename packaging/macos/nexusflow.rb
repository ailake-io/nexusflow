# Homebrew formula for NexusFlow — installs the release tarball
# (`nexusflow-macos-arm64.tar.gz`) built by `.github/workflows/release.yml`'s
# `build` job (macOS leg, added 2026-09-05).
#
# NOT YET PUBLISHED / NOT YET VALIDATED — no macOS release has actually run
# end-to-end yet as of this commit (the release pipeline's macOS leg is new).
# `sha256` below is a placeholder ("REPLACE_ME") — fill it in with the real
# checksum from that release's `SHA256SUMS` asset once a real
# `nexusflow-macos-arm64.tar.gz` exists, or this formula will fail to
# install with a checksum mismatch by design (Homebrew always verifies).
#
# Install without a tap (works today, once the checksum above is real):
#   brew install --formula https://raw.githubusercontent.com/ailake-io/nexusflow/main/packaging/macos/nexusflow.rb
#
# A dedicated tap (`ailake-io/homebrew-nexusflow`, so `brew install
# ailake-io/nexusflow/nexusflow` works) is a separate follow-up — not
# created yet; would just mirror this same formula file into that repo's
# `Formula/` directory, ideally kept in sync automatically per release
# rather than by hand (e.g. `brew bump-formula-pr` in a follow-up CI step).
#
# Apple Silicon only (arm64) — matches `release.yml`'s matrix, which only
# builds on `macos-latest` (Apple Silicon). No Intel (x86_64) build exists.
#
# The tarball this formula installs comes from release.yml's automatic
# `macos`/`arm64` leg by default (OSS connectors only), but running
# .github/workflows/build-macos-installer.yml (workflow_dispatch, same
# non-Docker Cargo [patch] trick build-windows-installer.yml uses)
# overwrites that same release asset with an enterprise-bundled build —
# same end state as Linux/Windows, this formula doesn't need to know
# which one is currently published under the tag it points at.
class Nexusflow < Formula
  desc "Universal Rust data & vector framework — ETL/ELT/streaming + AI Lakehouse Builder"
  homepage "https://github.com/ailake-io/nexusflow"
  url "https://github.com/ailake-io/nexusflow/releases/download/v0.1.3/nexusflow-macos-arm64.tar.gz"
  sha256 "REPLACE_ME_WITH_REAL_SHA256SUMS_ENTRY"
  license "Apache-2.0"

  depends_on macos: :ventura

  def install
    bin.install "nexusflow-bin" => "nexusflow"
    lib.install Dir["libadbc_driver_*"]
  end

  def caveats
    <<~EOS
      NexusFlow needs two required environment variables to start:
        export NEXUS_JWT_SECRET="$(openssl rand -hex 32)"
        export NEXUS_ENCRYPTION_KEY="$(openssl rand -hex 32)"

      And the two ADBC driver paths, installed alongside this formula's lib/:
        export ADBC_DRIVER_POSTGRESQL_PATH="#{lib}/libadbc_driver_postgresql.dylib"
        export ADBC_DRIVER_SQLITE_PATH="#{lib}/libadbc_driver_sqlite.dylib"

      See https://github.com/ailake-io/nexusflow/blob/main/docs/GETTING_STARTED.md
      for the full list of environment variables and how to run your first
      pipeline.
    EOS
  end

  test do
    system "#{bin}/nexusflow", "--version"
  end
end
