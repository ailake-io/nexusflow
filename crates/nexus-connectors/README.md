# nexus-connectors

Workspace aninhado — cada conector é um crate próprio aqui dentro (`nexus-connector-postgres`, `nexus-connector-mongodb`, ...), nunca um módulo dentro de um crate único. Ver `ARCHITECTURE.md §3`.

18 crates hoje (o primeiro foi `nexus-connector-postgres`, Marco 1):
`ailake` (+ modo `ailake-cdc`), `chromadb`, `csv`, `deltalake` (+
`deltalake-cdc`), `iceberg` (+ `iceberg-cdc`), `kafka`, `lancedb`,
`milvus`, `mongodb` (+ `mongodb-cdc`), `mysql` (CDC-only por enquanto —
batch via ADBC ainda não implementado, ver `CLAUDE.md`), `odbc`,
`parquet`, `pgvector`, `pinecone`, `postgres` (+ `postgres-cdc`),
`qdrant`, `rest`, `sqlite`. `webhook` (sink genérico) vive em
`nexus-server` diretamente, não nesse workspace — confirme com `ls`
antes de tratar esta lista como definitiva, ela desatualiza rápido.
