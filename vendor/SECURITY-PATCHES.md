# Dependency compatibility patches

These packages retain their upstream versions and licenses. The patches update
vulnerable transitive dependencies without changing the connector-facing APIs.
Both workspace manifests apply the same patches. Audit the two workspace
lockfiles; packaged upstream lockfiles are omitted because patched dependencies
are resolved by the workspace.

| Package / crates.io source | Local changes | Remove when |
| --- | --- | --- |
| milvus-sdk-rust 3.0.4 | lru 0.18.2 to fix iterator aliasing and panic safety | Milvus releases with lru >=0.18.2 |
| object_store 0.10.2 | quick-xml 0.42; separate GCP serialization error (`SeError`) from response decoding (`DeError`) | AI Lake uses an object_store release with quick-xml >=0.41 |
| object_store 0.13.2 | quick-xml 0.42 | DataFusion, Delta and Lance accept object_store with quick-xml >=0.41 |
| iceberg-catalog-sql 0.10.1 | SQLx 0.9.1; mark internal constant SQL templates as safe, retaining parameter binding | Iceberg SQL catalog releases with SQLx >=0.9 |

quick-xml >=0.41 fixes RUSTSEC-2026-0194 and RUSTSEC-2026-0195.
SQLx 0.9 permits libsqlite3-sys 0.39.1 (SQLite 3.54.1), fixing
RUSTSEC-2026-0459, and removes the optional SQLx 0.8 MySQL dependency on RSA 0.9
(RUSTSEC-2023-0071). The ODBC test dependency uses rusqlite 0.41 for the same
SQLite library. NATS uses async-nats 0.53 with current rustls-webpki.

The original packaged `Cargo.toml.orig` files are retained as provenance.
