# nexus-connectors

Workspace aninhado — cada conector é um crate próprio aqui dentro (`nexus-connector-postgres`, `nexus-connector-mongodb`, ...), nunca um módulo dentro de um crate único. Ver `ARCHITECTURE.md §3`.

24 crates hoje, contagem via `Cargo.toml` (2026-09-05; o primeiro foi
`nexus-connector-postgres`, Marco 1):
`ailake` (+ modo `ailake-cdc`), `chromadb`, `clickhouse` (ADBC, sink
append-only), `csv`, `deltalake` (+ `deltalake-cdc`), `duckdb` (ADBC,
upsert real), `iceberg` (+ `iceberg-cdc`), `kafka` (source+sink),
`lancedb`, `milvus`, `mongodb` (+ `mongodb-cdc`), `mqtt` (source
apenas), `mysql` (batch via bridging `mysql_async`, sem driver ADBC
oficial — CDC via binlog vive em `nexus-connector-mysql` também,
mesmo crate), `nats` (core pub/sub), `odbc`, `parquet`, `pgvector`,
`pinecone`, `postgres` (+ `postgres-cdc`), `qdrant`, `rabbitmq`
(AMQP 0-9-1), `redis` (Streams), `rest`, `sqlite`. `webhook` (sink
genérico) vive dentro do crate `rest`, não é crate próprio — confirme
com `ls`/`grep members Cargo.toml` antes de tratar esta lista como
definitiva, ela desatualiza rápido.
