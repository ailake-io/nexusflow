#!/usr/bin/env python3
"""
Compares the two Cargo.lock files this repo builds from: the root workspace
(nexus-core/nexus-ai/nexus-server) and crates/nexus-connectors' own nested
workspace (CLAUDE.md §3/§8.5). Both independently resolve the same connector
crates' dependencies (see .github/workflows/ci.yml's `connectors` job
comment) — if a shared crate drifts to a different pin in each, the
`connectors` CI job is testing a dependency graph that isn't what actually
ships in the nexusflow release binary (built from the root lockfile).

Only flags a name when BOTH lockfiles pin it to exactly one version and
those versions differ. A name resolved to multiple versions on either side
(a diamond dependency) is a structural difference in which feature sets
each workspace pulls in, not drift, and is intentionally not flagged.
"""
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCKFILES = {
    "root": ROOT / "Cargo.lock",
    "nexus-connectors": ROOT / "crates/nexus-connectors/Cargo.lock",
}

# Crates that cannot be aligned today because an upstream dependency pins
# them with an exact (`=`) requirement. Keep this list to cases you've
# actually traced back to a hard constraint — anything else drifting should
# fail the check, not get silently added here.
KNOWN_EXCEPTIONS = {
    # nexus-connector-ailake -> ailake-catalog 0.1.10 -> apache-avro 0.16.0 ->
    # digest 0.10.7 -> crypto-common 0.1.7, which requires
    # `generic-array = "=0.14.7"` exactly. Root's dependency tree reaches
    # crypto-common 0.1.6 instead (no exact pin), so it floats to 0.14.9.
    # Bumping this needs an ailake-catalog release, not a local `cargo
    # update`.
    "generic-array",
}


def versions_by_name(path: Path) -> dict[str, set[str]]:
    data = tomllib.loads(path.read_text())
    versions: dict[str, set[str]] = {}
    for pkg in data["package"]:
        versions.setdefault(pkg["name"], set()).add(pkg["version"])
    return versions


def main() -> int:
    root_versions = versions_by_name(LOCKFILES["root"])
    connectors_versions = versions_by_name(LOCKFILES["nexus-connectors"])

    shared_names = sorted(set(root_versions) & set(connectors_versions))
    drifted = [
        name
        for name in shared_names
        if name not in KNOWN_EXCEPTIONS
        and len(root_versions[name]) == 1
        and len(connectors_versions[name]) == 1
        and root_versions[name] != connectors_versions[name]
    ]

    if not drifted:
        print(f"OK: {len(shared_names)} shared crates, no version drift")
        return 0

    print(f"Cargo.lock drift between root and crates/nexus-connectors ({len(drifted)} crate(s)):")
    for name in drifted:
        root_v = next(iter(root_versions[name]))
        conn_v = next(iter(connectors_versions[name]))
        print(f"  {name}: root={root_v} nexus-connectors={conn_v}")
    print()
    print(
        "Run `cargo update -p <crate> --precise <version>` in whichever "
        "workspace is behind (or both) so they agree, then commit both "
        "Cargo.lock files together. If a crate can't be aligned due to an "
        "upstream exact-version pin, add it to KNOWN_EXCEPTIONS with a "
        "comment explaining why."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
