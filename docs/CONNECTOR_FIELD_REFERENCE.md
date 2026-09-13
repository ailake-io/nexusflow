# Referência de campos — Conectores OSS

Gerado a partir do schema real (`GET /connectors`) de uma imagem `nexusflow-enterprise:full` rodando localmente (2026-09-01) — nomes de campo, obrigatoriedade e descrição vêm direto do `config_schema` de cada conector (doc comment Rust real), não inventados. Coluna "Exemplo" é um valor plausível pra preencher o formulário, não necessariamente o default real de cada conector (que pode divergir quando o schema não expõe o `default` explicitamente).

Descrições ficam em inglês de propósito — são o doc comment original do Rust, traduzir risca introduzir imprecisão técnica. Cobre só os 31 conectores OSS; os 40 conectores enterprise (license-gated) estão no repo privado `nexus-connectors-enterprise`, em `docs/CONNECTOR_FIELD_REFERENCE.md`.

## `ailake`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `warehouse` | **sim** | string | `valor-exemplo` | Local filesystem root for the AI-Lake warehouse — created if it doesn't exist yet, no server/container required. This is the legacy field; `warehouse_path` is preferred for new canvas nodes. |
| `warehouse_path` | não | string | `null` | Optional override for `warehouse`. If set and `warehouse` is empty, this path is used as the warehouse root instead. Kept as the primary identifier for SchemaForm/FieldHint tooltip discovery. |
| `namespace` | **sim** | string | `default` | Namespace (like a database/schema) within the warehouse. This is the legacy field; `namespace_name` is preferred for new canvas nodes. |
| `namespace_name` | não | string | `null` | Optional override for `namespace`. If set and `namespace` is empty, this name is used as the namespace instead. |
| `table` | **sim** | string | `events` | Table name within `namespace` — created automatically on first write if it doesn't exist yet. This is the legacy field; `table_name` is preferred for new canvas nodes. |
| `table_name` | não | string | `null` | Optional override for `table`. If set and `table` is empty, this name is used as the table name instead. |
| `primary_key` | **sim** | string | `id` | Column used to upsert on write. |
| `embedding_column` | **sim** | string | `embedding` | Name of the `FixedSizeList<Float32>` column the embedding is written to — indexed with HNSW automatically. |
| `dimension` | **sim** | integer | `384` | Vector size — must match the embedding column's actual length. |
| `storage_options` | não | object | `"{ ... }"` | Storage options for AI-Lake backends that are backed by object storage rather than the default embedded local filesystem. Currently the connector uses `LocalStore`, so these values are collected and exposed for future S3-compatible backends and are ignored by the local implementation. |
| `append_only` | não | boolean | `false` | When true, the sink appends batches without issuing the equality delete that masks pre-existing rows sharing the primary key. This avoids the extra commit and delete scan for large append-only loads. CDC (`__opcode`) batches still honor deletes. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each catalog/store call — the warehouse is a local filesystem today, but this still guards against a locked catalog file or a slow disk stalling the pipeline indefinitely (C15). |
| `flush_threshold_rows` | não | integer | `50000` | Number of rows to accumulate before committing an AI-Lake write session. Larger values reduce the number of create-or-open/write/commit cycles; the remaining rows are flushed when the pipeline finishes (`commit_checkpoint`). |

## `ailake-cdc`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `warehouse` | **sim** | string | `valor-exemplo` | Local filesystem root for the AI-Lake warehouse. This is the legacy field; `warehouse_path` is preferred for new canvas nodes. |
| `warehouse_path` | não | string | `null` | Optional override for `warehouse`. Used only when `warehouse` is empty. |
| `namespace` | **sim** | string | `default` | Namespace (like a database/schema) within the warehouse. This is the legacy field; `namespace_name` is preferred for new canvas nodes. |
| `namespace_name` | não | string | `null` | Optional override for `namespace`. Used only when `namespace` is empty. |
| `table` | **sim** | string | `events` | Table name within `namespace`. This is the legacy field; `table_name` is preferred for new canvas nodes. |
| `table_name` | não | string | `null` | Optional override for `table`. Used only when `table` is empty. |
| `primary_key` | **sim** | string | `id` | Column used to identify rows for CDC deletes and synthetic delete rows. |
| `embedding_column` | **sim** | string | `embedding` | Name of the `FixedSizeList<Float32>` embedding column — used when reading committed data files back. |
| `dimension` | **sim** | integer | `384` | Vector size — must match the embedding column's actual length. |
| `storage_options` | não | object | `"{ ... }"` | Storage options for AI-Lake backends that are backed by object storage rather than the default embedded local filesystem. Currently the connector uses `LocalStore`, so these values are collected and exposed for future S3-compatible backends and are ignored by the local implementation. |
| `starting_snapshot_id` | não | integer | `null` | Snapshot id to read changes after (exclusive) — omit to read the table's entire history. Static field, not auto-advanced between runs (same precedent as Kafka's `start_offsets`). |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each AI-Lake CDC call (connect to catalog, read snapshots, scan changed files) — a stalled catalog or filesystem call would otherwise block the pipeline indefinitely (C15). |

## `chromadb`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `host` | **sim** | string | `localhost` | ChromaDB server address. This field accepts either a complete base URL such as `"http://localhost:8000"` or just a hostname such as `"localhost"`. When a bare hostname is provided, [`ChromaConnectorConfig::base_url`] combines it with [`Self::port`] to build the final URL. For backwards compatibility, this field takes priority over the separated `host`/`port` pair: if it looks like a URL, it is... |
| `port` | não | integer | `5432` | TCP port of the ChromaDB HTTP server. Only used when [`Self::host`] is a bare hostname. Defaults to `8000`. |
| `api_key` | não | string | `SUA_API_KEY` | Optional API key for authenticated ChromaDB instances. When set, every request is sent with a `Authorization: Bearer <key>` header. |
| `tenant` | não | string | `default_tenant` | Tenant name. Leave unset to use ChromaDB's default tenant (`default_tenant`). |
| `database` | não | string | `default_database` | Database name within the tenant. Leave unset to use ChromaDB's default database (`default_database`). |
| `collection` | **sim** | string | `events` | Name of an existing collection. The collection must already be created on the ChromaDB server; this sink only writes rows. |
| `primary_key` | **sim** | string | `id` | Column used as the Chroma document ID. |
| `embedding_column` | **sim** | string | `embedding` | Name of the `FixedSizeList<Float32>` column the embedding is written to. |
| `dimension` | **sim** | integer | `384` | Vector size. Must match the collection's configured dimension. |
| `timeout_seconds` | não | integer | `30` | Per-request timeout in seconds. `reqwest::Client` has no timeout by default, so a stalled connection to ChromaDB would otherwise block the pipeline indefinitely (C15). |
| `max_concurrent_requests` | não | integer | `8` | Maximum concurrent ChromaDB upsert/delete requests issued per batch. ChromaDB's REST API handles small sequential chunks slowly on large loads; parallelizing chunk submission while capping concurrency avoids overwhelming the server. Defaults to 8. |

## `clickhouse`

Capability: `adbc_native`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Full `http://user:pass@host:port/` connection URI. When provided, this value is used exactly as-is and all other connection fields are ignored. |
| `host` | não | string | `localhost` | ClickHouse server host name or IP address. |
| `port` | não | integer | `8123` | HTTP port the ClickHouse server is listening on (the ADBC driver speaks ClickHouse's HTTP interface, not the native TCP protocol). |
| `database` | não | string | `default` | Database name. |
| `username` | não | string | `nexusflow` | User name to authenticate with. |
| `password` | não | string | `s3nhaForte123` | Password for the provided user name. |
| `table` | **sim** | string | `events` | Table name to read from (source) or write to (sink). |
| `partition_column` | não | string | `null` | Column used to partition reads by range for parallelism — any orderable column, not necessarily unique (ClickHouse doesn't enforce a primary key the way Postgres does; this is typically a column from the table's `ORDER BY`). `None` reads the whole table with no `WHERE` clause and no partitioning. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each ADBC call (connect, query, insert) — the driver is a blocking FFI call run via `spawn_blocking`, so a stalled connection would otherwise block that call forever. |

## `csv`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Legacy single-field URI. Local path (e.g. `/data/events.csv`) or cloud URL (`s3://bucket/key`, `gs://bucket/key`, `az://container/key`). When set, it overrides the split `storage`/`path`/`bucket` fields. |
| `storage` | não | string | `valor-exemplo` | Storage backend used to reach the delimited text file. |
| `path` | não | string | `/data/events.csv` | Local file path or cloud object key. Required when `uri` is not used. For cloud backends this is the key inside the bucket/container. |
| `bucket` | não | string | `meu-bucket` | Bucket (S3/GCS) or container (Azure) name. Required for cloud backends when `uri` is not used. |
| `region` | não | string | `us-east-1` | Cloud region, mainly for S3 (e.g. `us-east-1`). |
| `access_key_id` | não | string | `SEU_ACCESS_KEY_ID` | Access key / service account / storage account name, depending on the backend. Mapped to the object_store option names by `storage_options()`. |
| `secret_access_key` | não | string | `SEU_SECRET_ACCESS_KEY` | Secret key / service account key / storage account key, depending on the backend. Mapped to the object_store option names by `storage_options()`. |
| `endpoint` | não | string | `http://localhost:4566` | Custom endpoint URL (e.g. for MinIO or localstack). Mapped to the backend-specific object_store option. |
| `delimiter` | não | string | `,` | Field separator — `,` for CSV, `\t` for TSV, `;`/`\|` or anything else for a custom-delimited TXT file. Defaults to `,`. |
| `has_header` | não | boolean | `true` | Whether the file's first line is a header row naming each column (by `fields`' order) rather than data. Defaults to `true`. |
| `quote` | não | string | `"` | Character used to quote fields. Defaults to `"`. |
| `escape` | não | string | `null` | Optional escape character for quotes inside quoted fields. When unset, the reader/writer use their default behaviour (doubled quotes by default for the writer). |
| `fields` | não | array<object> | `"[ ... ]"` | Explicit target schema, in file-column order. Delimited text has no type information of its own, so if this is left empty **on the source side**, the connector samples the first `schema_sample_rows` rows of the first resolved file and infers one (see `schema::infer_schema`) — same approach `arrow-csv`'s own `Format::infer_schema` uses elsewhere in the ecosystem, narrowed to this connector's 4... |
| `schema_sample_rows` | não | integer | `1000` | How many rows to sample when inferring `fields` (source side only, only used when `fields` is empty). Defaults to 1000 — enough to catch a column that's `int64` for many rows then turns out to need `utf8` (a stray non-numeric value), without reading a huge file twice just to guess its schema. |
| `primary_key` | não | string | `id` | Column used to upsert/delete on write — required for the sink side unless `append_only` is true; ignored by the source. |
| `append_only` | não | boolean | `false` | When true, the sink appends batches to the existing file instead of reading it back, filtering by primary key, and rewriting it. This avoids the I/O cost of the read-filter-rewrite cycle for large append-only loads. CDC (`__opcode`) batches still use the rewrite path so deletes/updates are honored. |
| `batch_size` | não | integer | `50000` | How many rows to fold into a single `RecordBatch` while scanning. Defaults to `50000`. |
| `storage_options` | não | object | `{}` | Extra key/value options forwarded to `object_store`'s cloud builders. The connector also injects backend-specific credentials from `access_key_id`, `secret_access_key`, `region` and `endpoint` when available, so this map can be left empty for the common case. Ignored for local paths. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each call to the object store — matters most for cloud URLs, the only case where a call can actually stall on the network (C15). Defaults to `30`. |

## `deltalake`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `table_uri` | não | string | `` | Full table URI — a local directory, a `file://` URI, or an object-store URI such as `s3://bucket/namespace/table`. Kept for backward compatibility: when this field is non-empty it takes precedence over `path`/`table_name`/`storage_options`. |
| `path` | não | string | `/data/events.csv` | Base directory path or object-store prefix where the Delta table lives. Only used when `table_uri` is empty. The effective table URI is built by appending `table_name` to this path. |
| `table_name` | não | string | `null` | Table name within `path`. Only used when `table_uri` is empty. |
| `storage_options` | não | object | `"{ ... }"` | Object-store credentials and settings used when the Delta table lives on S3, GCS, MinIO or another S3-compatible store. These values are translated into the key/value format expected by `deltalake`/`object_store`. |
| `primary_key` | **sim** | string | `id` | Column used to upsert on write. |
| `append_only` | não | boolean | `false` | When true, the sink appends batches without deleting pre-existing rows that share the primary key. This avoids the delete-then-append cost for large append-only loads. CDC (`__opcode`) batches still honor deletes. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each call to the table's storage backend (open, create, write, delete) — matters most for ADLS/S3/GCS URIs, the only case where a call can actually stall on the network (C15). |
| `flush_threshold_rows` | não | integer | `50000` | Number of rows to accumulate before committing a Delta transaction. Larger values reduce transaction overhead; the remaining rows are flushed when the pipeline finishes (`commit_checkpoint`). |

## `deltalake-cdc`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `table_uri` | não | string | `` | Full table URI for the CDC source — a local directory, a `file://` URI, or an object-store URI. Kept for backward compatibility: when this field is non-empty it takes precedence over `path`/ `table_name`/`storage_options`. |
| `path` | não | string | `/data/events.csv` | Base directory path or object-store prefix where the Delta table lives. Only used when `table_uri` is empty. |
| `table_name` | não | string | `null` | Table name within `path`. Only used when `table_uri` is empty. |
| `storage_options` | não | object | `"{ ... }"` | Object-store credentials and settings used when the Delta table lives on S3, GCS, MinIO or another S3-compatible store. These values are translated into the key/value format expected by `deltalake`/`object_store`. |
| `starting_version` | não | integer | `null` | Delta commit version to read changes from (inclusive) — omit to read from version 0, i.e. every change since `delta.enableChangeDataFeed` was turned on. Static field, not auto-advanced between runs (same precedent as Kafka's `start_offsets`) — the destination sink's idempotent upsert makes re-reading old versions safe, just wasteful on a large table. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each Delta Lake CDC call (connect, read commit history, scan changed files) — a stalled object-store or filesystem call would otherwise block the pipeline indefinitely (C15). |

## `duckdb`

Capability: `adbc_native`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Full DuckDB connection URI. When provided, this value is used exactly as-is and `path` is ignored. Keeps backward compatibility with older pipelines that store a complete path or `:memory:` string. |
| `path` | não | string | `:memory:` | File path to the `.duckdb` file, or `:memory:` for an ephemeral database that only exists for this process's lifetime. Use an absolute path if the DuckDB file lives outside the nexus-server working directory. Ignored when `uri` is set. |
| `table` | **sim** | string | `events` | Table name to read from (source) or write to (sink) — created automatically on the sink side if it doesn't exist yet. |
| `primary_key` | **sim** | string | `id` | Column used to upsert on write — should be an indexed, unique column (integer or text primary key). |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each ADBC call (connect, query, insert) — a concurrent writer holding the DuckDB file lock can otherwise stall a call indefinitely (though the underlying blocking thread keeps running regardless — no cancellation for in-flight ADBC calls). |

## `iceberg`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `catalog_uri` | não | string | `null` | Legacy combined catalog URI. Keep using this if you already have a full URI such as `sqlite:///abs/path/catalog.db?mode=rwc` — it takes priority over `catalog_path` when both are present. For new pipelines prefer `catalog_path` and let the connector build the SQLite URI automatically. |
| `catalog_path` | não | string | `null` | Filesystem path to the SQLite catalog backing metadata. Example: `/var/lib/nexus/iceberg/catalog.db`. Used only when `catalog_uri` is empty or omitted. The connector normalizes this into a `sqlite://...?mode=rwc` URI internally. |
| `warehouse_location` | não | string | `null` | Legacy combined warehouse location. Keep using this if you already have a full URI such as `file:///abs/path/warehouse` — it takes priority over `warehouse_path` when both are present. |
| `warehouse_path` | não | string | `null` | Filesystem path to the Iceberg warehouse root. Example: `/var/lib/nexus/iceberg/warehouse`. Used only when `warehouse_location` is empty or omitted. The connector normalizes this into a `file://` URI via `warehouse_location()`. |
| `namespace` | não | string | `default` | Legacy namespace (database/schema) identifier. Keep using this if you already have a value here — it takes priority over `namespace_name` when both are present. |
| `namespace_name` | não | string | `null` | Namespace (like a database/schema) within the Iceberg catalog. Created automatically on first write if it does not exist yet. Used only when `namespace` is empty or omitted. |
| `table` | não | string | `events` | Legacy table name. Keep using this if you already have a value here — it takes priority over `table_name` when both are present. |
| `table_name` | não | string | `null` | Table name within `namespace`. Created automatically on first write if it does not exist yet, using `format_version`. Used only when `table` is empty or omitted. |
| `storage_options` | não | object | `"{ ... }"` | S3-compatible object-storage options for lakehouse connectors. These fields map to the standard `s3.*` properties used by Iceberg's object-store integrations (e.g. `s3.bucket`, `s3.region`, `s3.access-key-id`, `s3.secret-access-key`, `s3.endpoint`). They are only required when the warehouse or catalog lives on S3, MinIO, R2, Wasabi, or another S3-compatible store; for the default... |
| `format_version` | não | enum(v2/v3) | `v2` | Iceberg table format version to create new tables with. Only applies at table creation time — an already-existing table keeps whatever version it was created with (`ensure_table`'s `load_table` branch never touches it). Defaults to V2, the still-most-widely-supported spec version; pick V3 explicitly to get V3-only features as they land upstream (row lineage, deletion vectors, etc.). |
| `primary_key` | não | string | `id` | Optional primary-key column used by the sink to make appends idempotent. When set, rows whose key already exists in the current table snapshot are silently dropped before the append. This prevents duplicate lines on retry/resume (A01). Leave unset for pure append. |
| `append_only` | não | boolean | `false` | When true, the sink appends batches without scanning the current snapshot for existing primary keys. This avoids the full-snapshot read for large append-only loads. CDC (`__opcode`) batches still honor the existing semantics. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each catalog/table call — both the SQLite catalog and local warehouse are embedded today, but this still guards against a locked catalog file or a future remote storage backend stalling the pipeline indefinitely (C15). |
| `flush_threshold_rows` | não | integer | `50000` | Number of rows to accumulate before committing an Iceberg transaction. Larger values reduce catalog-commit overhead; the remaining rows are flushed when the pipeline finishes (`commit_checkpoint`). |

## `iceberg-cdc`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `catalog_uri` | não | string | `null` | Legacy combined catalog URI. Keep using this if you already have a full URI such as `sqlite:///abs/path/catalog.db?mode=rwc` — it takes priority over `catalog_path` when both are present. |
| `catalog_path` | não | string | `null` | Filesystem path to the SQLite catalog backing metadata. Used only when `catalog_uri` is empty or omitted. |
| `warehouse_location` | não | string | `null` | Legacy combined warehouse location. Keep using this if you already have a full URI such as `file:///abs/path/warehouse` — it takes priority over `warehouse_path` when both are present. |
| `warehouse_path` | não | string | `null` | Filesystem path to the Iceberg warehouse root. Used only when `warehouse_location` is empty or omitted. |
| `namespace` | não | string | `default` | Legacy namespace identifier. Takes priority over `namespace_name` when both are present. |
| `namespace_name` | não | string | `null` | Namespace (database/schema) within the Iceberg catalog. Used only when `namespace` is empty or omitted. |
| `table` | não | string | `events` | Legacy table name. Takes priority over `table_name` when both are present. |
| `table_name` | não | string | `null` | Table name within `namespace`. Used only when `table` is empty or omitted. |
| `storage_options` | não | object | `"{ ... }"` | S3-compatible object-storage options for lakehouse connectors. These fields map to the standard `s3.*` properties used by Iceberg's object-store integrations (e.g. `s3.bucket`, `s3.region`, `s3.access-key-id`, `s3.secret-access-key`, `s3.endpoint`). They are only required when the warehouse or catalog lives on S3, MinIO, R2, Wasabi, or another S3-compatible store; for the default... |
| `starting_snapshot_id` | não | integer | `null` | Snapshot id to read changes after (exclusive) — omit to read every snapshot in the table's history. Static field, not auto-advanced between runs (same precedent as Kafka's `start_offsets`). |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each Iceberg CDC call (connect to catalog, read snapshots, scan changed files) — a stalled catalog or filesystem call would otherwise block the pipeline indefinitely (C15). |

## `kafka`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `bootstrap_servers` | não | string | `localhost:9092` | Comma-separated `host:port` list of Kafka brokers to bootstrap from, e.g. `"broker1:9092,broker2:9092"`. When provided, this overrides any separate `brokers`/`port` entries and is used verbatim in the Kafka client configuration. |
| `brokers` | não | array<string> | `[]` | List of Kafka broker hosts or `host:port` pairs. Used only when `bootstrap_servers` is empty. Entries without an explicit port get `:9092` appended automatically. Example: `["kafka-1", "kafka-2:9093"]`. |
| `port` | não | integer | `9092` | Default port appended to `brokers` entries that do not already contain a colon. Ignored when `bootstrap_servers` is provided or when every broker entry already includes a port. |
| `topic` | **sim** | string | `events` | Topic to consume from. |
| `group_id` | **sim** | string | `nexus-consumer` | Consumer group id — controls offset tracking on the broker side; reuse the same id across runs of the same pipeline to resume from where the group last committed. |
| `client_id` | não | string | `SEU_CLIENT_ID` | Optional client id sent to Kafka in the client metadata. Useful for observability and broker-side logging. When omitted, the Kafka driver generates an id automatically. |
| `security_protocol` | não | string | `valor-exemplo` | Security protocol used to communicate with the brokers. Defaults to plaintext when not set. `SaslSsl` and `SaslPlaintext` enable SASL authentication; `Ssl` enables TLS without SASL. |
| `sasl_mechanism` | não | string | `valor-exemplo` | SASL mechanism for broker authentication. Required when `security_protocol` is `sasl_plaintext` or `sasl_ssl`. Ignored for plaintext or SSL-only connections. |
| `sasl_username` | não | string | `null` | SASL username for broker authentication. Required together with `sasl_password` when SASL is enabled. |
| `sasl_password` | não | string | `null` | SASL password or secret for broker authentication. Stored in plain text in the node config; the frontend masks it as a secret input field. |
| `fields` | não | array<object> | `"[ ... ]"` | Explicit target schema — a JSON message payload carries no fixed schema of its own. Left empty, the connector samples up to `schema_sample_rows` messages via a throwaway consumer group (never committed, doesn't touch `group_id`'s offsets) and infers one — union of keys across the sample, typed by first non-null value, see `nexus_core::RecordBatchBuilder::infer_schema`. |
| `schema_sample_rows` | não | integer | `1000` | How many messages to sample when inferring `fields` (only used when `fields` is empty). Defaults to 1000. |
| `batch_size` | não | integer | `500` | How many decoded messages to fold into a single `RecordBatch`. |
| `poll_timeout_ms` | não | integer | `2000` | How long to wait for a new message before treating the topic as drained for this read — a Kafka topic has no natural end, so a bridging/bounded read needs an idle cutoff. |
| `max_messages` | não | integer | `100000` | Hard cap on messages consumed per `read_batches` call. |
| `start_offsets` | não | object | `{}` | Explicit per-partition start offsets for resume (checkpoint replay) — see `checkpoint_store` (already generic per `(pipeline_id, partition_id)` since Marco 1). Absent partitions fall back to `auto.offset.reset = earliest`. |

## `lancedb`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Deprecated legacy field. Local directory path or object-store URI where LanceDB stores its data — created if it does not exist yet. Kept for backward compatibility; if filled it takes precedence over `path` and `storage_options`. Prefer `path` + `storage_options` for new pipelines. |
| `path` | não | string | `/data/events.csv` | Local directory path where LanceDB stores its data. Created if it does not exist yet. Use this for local deployments, or combine it with `storage_options.s3_bucket` to build an S3-backed URI. |
| `storage_options` | não | object | `"{ ... }"` | Object-store storage options for LanceDB. Only required when the database lives on S3, GCS or Azure rather than on local disk. For local deployments leave every field empty and set `path` instead. |
| `table` | não | string | `events` | Deprecated legacy field. Table name within the database. Kept for backward compatibility; if filled it takes precedence over `table_name`. Prefer `table_name` for new pipelines. |
| `table_name` | não | string | `null` | Table name within the database — created automatically on first write if it does not exist yet. |
| `primary_key` | **sim** | string | `id` | Column used to upsert on write. |
| `embedding_column` | **sim** | string | `embedding` | Name of the `FixedSizeList<Float32>` column the embedding is written to. |
| `dimension` | **sim** | integer | `384` | Vector size — must match the embedding column's actual length. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each call to LanceDB — matters most when the connection URI points at an object store rather than local disk, the only case where a call can actually stall on the network (C15). |

## `milvus`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `url` | não | string | `https://api.example.com` | **Legacy field.** Full Milvus server URL, e.g. `"http://localhost:19530"`. If this field is provided it takes precedence over the separate `host`/`port` fields, so existing pipelines keep working unchanged. For new canvas sources/sinks prefer `host` and `port`. |
| `host` | não | string | `localhost` | Milvus server hostname or IP address, e.g. `"localhost"` or `"milvus.example.com"`. Used only when the legacy `url` field is not set. |
| `port` | não | integer | `5432` | Milvus server port. Defaults to `19530` when `host` is used and `port` is omitted. |
| `api_key` | não | string | `SUA_API_KEY` | API key or token for authenticated Milvus instances (e.g. Zilliz Cloud). This field is kept in the config so the frontend can collect it; the underlying `milvus-sdk-rust` 0.1.0 client currently accepts only a username/password pair, so the sink does not wire it automatically yet. |
| `collection` | não | string | `events` | **Legacy field.** Name of an existing collection — must already be created (with schema and index) on the Milvus server; this sink only writes rows. If this field is provided it takes precedence over `collection_name`, so existing pipelines keep working unchanged. For new canvas sources/sinks prefer `collection_name`. |
| `collection_name` | não | string | `events` | Name of an existing collection — must already be created (with schema and index) on the Milvus server; this sink only writes rows. Used only when the legacy `collection` field is not set. |
| `primary_key` | **sim** | string | `id` | Must be an `Int64` column — matches the primary key type this connector supports (Milvus also allows `VarChar` primary keys, not implemented here). |
| `embedding_column` | **sim** | string | `embedding` | Name of the vector field in the collection the embedding is written to. |
| `dimension` | **sim** | integer | `384` | Must match the vector field's declared dimension in the collection schema. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each call to Milvus (connect, insert, delete, collection lookup) — the SDK exposes no timeout of its own, so a stalled connection would otherwise block the pipeline indefinitely (C15). |

## `mongodb`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `connection_string` | não | string | `postgresql://user:pass@localhost:5432/db` | Full MongoDB connection URI. When provided, it overrides any separate host, port, credential or option fields. Use this for Atlas `mongodb+srv://` strings, replica-set URIs with multiple seed hosts, or any connection string that already encodes authentication, TLS and read-preference options. Example: `mongodb://user:pass@host1:27017,host2:27017/db?replicaSet=rs0`. |
| `hosts` | não | array<string> | `[]` | List of MongoDB server endpoints as `host:port` strings. Used only when `connection_string` is empty. For a standalone server use a single entry such as `["localhost:27017"]`. For a replica set, list the seed members. The entries are joined into a comma-separated host list in the generated URI. Example: `["mongo1:27017", "mongo2:27017"]`. |
| `username` | não | string | `nexusflow` | MongoDB user name for authentication. Leave empty for unauthenticated connections. When supplied together with `password`, the credentials are embedded in the generated URI before the host list. |
| `password` | não | string | `s3nhaForte123` | Password matching `username`. Stored in plain text in the node config; the frontend masks it as a secret input field. Only used when `connection_string` is empty. |
| `auth_database` | não | string | `null` | Authentication database against which the user credentials are verified. Common values are `admin` or the database itself. When omitted, the driver falls back to the value of `database`. |
| `database` | **sim** | string | `nexusflow_test` | Default database name for the connector to read from or write to. This becomes the path component of the generated URI and is used to resolve `collection`. |
| `collection` | **sim** | string | `events` | Collection name within `database` that the source scans or the sink writes to. |
| `primary_key` | **sim** | string | `id` | Document field used as the upsert key on the sink side — see ARCHITECTURE.md §5 (idempotency is a `Sink` contract, not optional). |
| `read_preference` | não | string | `null` | MongoDB read preference for the source cursor, e.g. `primary`, `primaryPreferred`, `secondary`, `secondaryPreferred` or `nearest`. Only affects reads; sinks always write to the primary. Applied as a URI option when `connection_string` is not provided. |
| `tls` | não | boolean | `null` | Whether to require TLS for the generated connection URI. When `connection_string` is provided, this field is ignored. Equivalent to adding `tls=true` to the URI options. |
| `fields` | não | array<object> | `"[ ... ]"` | Explicit target schema — a MongoDB collection carries no fixed schema of its own. Left empty **on the source side**, the connector samples the first `schema_sample_rows` documents and infers one (union of keys across the sample, typed by first non-null value — `nexus_core::RecordBatchBuilder::infer_schema`). The sink side still requires this explicitly. |
| `schema_sample_rows` | não | integer | `1000` | How many documents to sample when inferring `fields` (source side only, only used when `fields` is empty). Defaults to 1000. |
| `batch_size` | não | integer | `1000` | How many documents to fold into a single `RecordBatch` while scanning. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each call to MongoDB (connect, buildInfo, bulk_write/replace_one/delete_one) — a stalled connection would otherwise block the pipeline indefinitely (C15). |

## `mongodb-cdc`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `connection_string` | não | string | `postgresql://user:pass@localhost:5432/db` | Full MongoDB connection URI. When provided, it overrides any separate host, port, credential or option fields. Replica-set URIs are common here because Change Streams require a replica set. |
| `hosts` | não | array<string> | `[]` | List of MongoDB server endpoints as `host:port` strings. Used only when `connection_string` is empty. For a replica set, list the seed members. |
| `username` | não | string | `nexusflow` | MongoDB user name for authentication. Leave empty for unauthenticated connections. |
| `password` | não | string | `s3nhaForte123` | Password matching `username`. Only used when `connection_string` is empty. |
| `auth_database` | não | string | `null` | Authentication database against which the user credentials are verified. When omitted, the driver falls back to the value of `database`. |
| `database` | **sim** | string | `nexusflow_test` | Database name that contains the watched collection. |
| `collection` | **sim** | string | `events` | Collection name to watch with the Change Stream. |
| `fields` | **sim** | array<object> | `"[ ... ]"` | Same projection contract as the batch connector — a change event's full document is projected onto these fields, see `MongoConnectorConfig::fields`. |
| `resume_token` | não | string | `null` | Resume token from a previous run's last processed event (as returned by `MongoCdcSource`'s checkpoint), so a restart picks up where it left off instead of only seeing events from now on. Stored as the token's extended-JSON form. `None` starts watching from the current moment — same "static config field, not server-injected" resume model as Kafka's `start_offsets` (ARCHITECTURE.md §7 doesn't... |
| `batch_size` | não | integer | `1000` | Maximum number of change events to fold into a single `RecordBatch` before yielding downstream. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each MongoDB Change Stream call (connect, watch, resume) — a stalled connection would otherwise block the pipeline indefinitely (C15). |
| `max_batch_events` | não | integer | `1000` | Maximum number of change events to collect in a single run. After this many events the source ends cleanly, letting the run finish and the scheduler start the next micro-batch. The MongoDB resume token persists the last processed event, so the next run picks up where this one left off. |

## `mqtt`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `broker_url` | **sim** | string | `valor-exemplo` | Broker address, e.g. `"mqtt://broker.example.com:1883"` or `"mqtts://broker.example.com:8883"` for TLS. Scheme decides plaintext vs TLS; a missing port defaults to 1883 (plaintext) or 8883 (TLS). |
| `client_id` | **sim** | string | `SEU_CLIENT_ID` | MQTT client id. Not generated randomly on purpose: reusing the same id across runs (together with `clean_session: false`, always set) is what lets the broker's persistent session redeliver QoS 1/2 messages published while this connector was offline — no checkpoint/cursor needed on the NexusFlow side. See ARCHITECTURE.md §7. |
| `topic_filter` | **sim** | string | `valor-exemplo` | Topic filter to subscribe to. Supports MQTT wildcards: `+` for one level (`sensors/+/temperature`), `#` for the remaining levels (`sensors/#`). |
| `qos` | não | enum(at_most_once/at_least_once/exactly_once) | `at_most_once` | Subscription QoS level. |
| `username` | não | string | `nexusflow` | Username for broker authentication, if required. |
| `password` | não | string | `s3nhaForte123` | Password for broker authentication, if required. Stored in plain text in the node config; the frontend masks it as a secret input field. |
| `ca_cert_path` | não | string | `null` | PEM-encoded CA certificate path, for TLS brokers using a private CA (self-signed or internal). Omit to trust the platform's default root store. |
| `client_cert_path` | não | string | `null` | PEM-encoded client certificate path, for brokers requiring mutual TLS (e.g. AWS IoT Core, which always requires client-cert auth). Must be set together with `client_key_path`. |
| `client_key_path` | não | string | `null` | PEM-encoded client private key path, paired with `client_cert_path`. |
| `fields` | não | array<object> | `"[ ... ]"` | Explicit target schema — a JSON message payload carries no fixed schema of its own. Left empty, the connector samples up to `schema_sample_rows` messages via a throwaway, clean-session client (never touches `client_id`'s persistent session) and infers one — see `nexus_core::RecordBatchBuilder::infer_schema`. |
| `schema_sample_rows` | não | integer | `1000` | How many messages to sample when inferring `fields` (only used when `fields` is empty). Defaults to 1000. |
| `batch_size` | não | integer | `500` | How many decoded messages to fold into a single `RecordBatch`. |
| `poll_timeout_ms` | não | integer | `2000` | How long to wait for a new message before treating the subscription as drained for this read — MQTT telemetry has no natural end, so a bridging/bounded read needs an idle cutoff. |
| `max_messages` | não | integer | `100000` | Hard cap on messages consumed per `read_batches` call. |

## `mysql`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Full `mysql://user:pass@host:port/db` connection URI. When provided, this value is used exactly as-is and all other connection fields are ignored. Keeps backward compatibility with older pipelines that already store a complete connection string. |
| `host` | **sim** | string | `localhost` | Database server host name or IP address (e.g. `localhost` or `db.example.com`). |
| `port` | não | integer | `3306` | TCP port the MySQL server is listening on. |
| `username` | **sim** | string | `nexusflow` | User name used to connect to the MySQL server. |
| `password` | **sim** | string | `s3nhaForte123` | Password for `username`. |
| `database` | **sim** | string | `nexusflow_test` | Database (schema) name that contains `table`. |
| `table` | **sim** | string | `events` | Table name this connector reads from or writes to. |
| `primary_key` | **sim** | string | `id` | Column used as the upsert key on the sink side — see ARCHITECTURE.md §5 (idempotency is a `Sink` contract, not optional). Also used to build the `DELETE ... WHERE` clause for rows carrying the `__opcode = "D"` marker (same convention as `mongodb`/CDC sinks). |
| `fields` | não | array<object> | `"[ ... ]"` | Target schema, matched **by name** to `table`'s actual columns (unlike `mysql-cdc`'s positional matching — a plain `SELECT`/`INSERT` here names its columns explicitly, so there's no binlog ambiguity to work around). Same 4-primitive-type ceiling as every other bridging connector. Left empty **on the source side**, the connector runs `SHOW COLUMNS FROM table` and maps each MySQL type onto the... |
| `batch_size` | não | integer | `1000` | How many rows to fold into a single `RecordBatch` while scanning, and the batch size for `exec_batch` writes on the sink side. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each call to MySQL (connect, query, exec) — a stalled connection would otherwise block the pipeline indefinitely (C15). |

## `mysql-cdc`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Full `mysql://user:pass@host:port/db` connection URI. When provided, this value is used exactly as-is and all other connection fields are ignored. Keeps backward compatibility with older pipelines that already store a complete connection string. |
| `host` | **sim** | string | `localhost` | Database server host name or IP address (e.g. `localhost` or `db.example.com`). |
| `port` | não | integer | `3306` | TCP port the MySQL server is listening on. |
| `username` | **sim** | string | `nexusflow` | User name used to connect to the MySQL server for replication. This account needs the `REPLICATION SLAVE` and `REPLICATION CLIENT` privileges on the source server. |
| `password` | **sim** | string | `s3nhaForte123` | Password for the replication user. |
| `database` | **sim** | string | `nexusflow_test` | Database (schema) name that contains the table being replicated. This is used client-side to filter binlog events by database; the replication user itself typically does not need per-database grants. |
| `table` | **sim** | string | `events` | Table name this connector reads changes for. |
| `server_id` | não | integer | `65535` | Fake replica server id registered with the MySQL master — must be unique among every server (real or replica) connected to it. |
| `fields` | **sim** | array<object> | `"[ ... ]"` | Target schema for each row — matched **positionally** to the table's actual column order, not by name: MySQL's binlog protocol doesn't carry column names unless the server has `binlog_row_metadata=FULL` set (off by default), so this connector doesn't depend on it. Same 4-primitive-type ceiling as every other bridging connector. |
| `binlog_filename` | não | string | `null` | Resume position — set both to continue from a specific point, otherwise streaming starts from the current end of the binlog (same "static config field, not server-injected" resume model as Kafka's `start_offsets`). |
| `binlog_position` | não | integer | `null` | Resume position within `binlog_filename`. Must be paired with `binlog_filename`; if either is missing the connector starts from the current end of the binlog. |
| `max_batch_events` | não | integer | `1000` | Maximum number of change events to collect in a single run. After this many events the source ends cleanly, letting the run finish and the scheduler start the next micro-batch. The MySQL replication stream resumes from the last binlog position, so the next run picks up where this one left off. |

## `nats`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `server_url` | **sim** | string | `valor-exemplo` | Server URL, e.g. `"nats://localhost:4222"` or `"tls://localhost:4222"`. |
| `subject` | **sim** | string | `valor-exemplo` | Subject to subscribe to (source) or publish to (sink). Supports NATS wildcards on the source side (`*` for one token, `>` for the rest) — a wildcard subscription blends many logical subjects into one read, so every output row also carries the concrete subject it arrived on, same precedent as MQTT's `MQTT_TOPIC_COLUMN`. |
| `queue_group` | não | string | `null` | Optional queue group — when set, only one subscriber in the group receives each message (load-balanced fan-out), same semantic as a Kafka consumer group but without offset tracking. Ignored by the sink. |
| `auth_token` | não | string | `null` | Optional bearer token for authentication. |
| `username` | não | string | `nexusflow` | Optional username/password authentication — ignored if `auth_token` is set. |
| `password` | não | string | `s3nhaForte123` |  |
| `fields` | **sim** | array<object> | `"[ ... ]"` | Explicit column projection — a NATS message payload is an opaque byte blob (assumed JSON), same contract as `kafka`/`mqtt`'s `fields`. |
| `batch_size` | não | integer | `500` | How many decoded messages to fold into a single `RecordBatch`. |
| `idle_timeout_ms` | não | integer | `2000` | How long to wait for a new message before returning what's been buffered so far — a subject has no natural end, same "idle means try again" contract as Kafka/MQTT. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for connecting. |
| `retries` | não | integer | `3` | Number of retries on transient failures (5xx, timeouts, connect errors). |
| `retry_backoff_seconds` | não | integer | `1` | Base delay between retries in seconds (exponential backoff). |

## `odbc`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `connection_string` | não | string | `postgresql://user:pass@localhost:5432/db` | Full ODBC connection string (`Driver={...};Server=...;...`). When provided, this value is used exactly as-is and all other connection fields are ignored. Keeps backward compatibility with older pipelines that already store a complete connection string. |
| `driver` | **sim** | string | `valor-exemplo` | ODBC driver name, including the curly braces used by the ODBC driver manager (e.g. `{PostgreSQL Unicode}` or `{ODBC Driver 18 for SQL Server}`). The driver must already be registered with unixODBC (or the platform's ODBC driver manager) on the machine running this connector. |
| `server` | **sim** | string | `valor-exemplo` | Database server host name or IP address. |
| `port` | não | integer | `5432` | TCP port the database server is listening on. Optional: many ODBC drivers can resolve the default port from the `Server` value or use the driver's own default. |
| `database` | não | string | `nexusflow_test` | Database/catalog name to connect to. Optional for some drivers (e.g. when the database is selected by a subsequent `USE` statement or when the DSN already encodes it). |
| `username` | **sim** | string | `nexusflow` | User name to authenticate with. |
| `password` | não | string | `s3nhaForte123` | Password for the provided user name. |
| `encrypt` | não | boolean | `null` | Whether the connection should be encrypted. Maps to driver-specific attributes such as `Encrypt` (SQL Server) or `SSLmode` (PostgreSQL). When `None`, the driver's default is used. |
| `trust_server_certificate` | não | boolean | `null` | Whether to trust the server's certificate when encryption is enabled. Common attribute names: `TrustServerCertificate` (SQL Server), `SSLmode=disable/require` (PostgreSQL). When `None`, the driver's default is used. |
| `login_timeout_seconds` | não | integer | `null` | Login timeout in seconds. Optional: when set, adds a `LoginTimeout` attribute to the connection string. |
| `table` | **sim** | string | `events` | Table name to read from (source) or write to (sink). |
| `primary_key` | **sim** | string | `id` | Column used to partition reads and upsert on write — should be indexed on the source database. |
| `fields` | não | array<object> | `"[ ... ]"` | Explicit target schema. Left empty, the connector runs `SELECT *` against `table` and asks the driver for each column's name/type via `SQLDescribeCol` (`ResultSetMetadata::describe_col`) instead of reading any rows — best-effort: ODBC driver quality varies a lot across legacy databases, and a driver that misreports a column's SQL type produces a wrong (though never silently-dropped — unknown... |
| `batch_size` | não | integer | `1000` | How many rows to fold into a single `RecordBatch` while scanning. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each batch write to fail if the ODBC worker thread doesn't respond in time (a stalled driver call would otherwise block the pipeline indefinitely — C15). Only unblocks the async side: the blocking ODBC call itself, and the OS thread running it, keeps running regardless (no cross-thread cancellation for raw ODBC handles). |

## `parquet`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Legacy location: a local path or a cloud URL (`s3://bucket/key`, `gs://bucket/key`, `az://container/key`). Takes precedence over `storage` + `bucket` + `path` when present, for backward compatibility. |
| `path` | não | string | `/data/events.csv` | Path to the Parquet file. For local storage this is a filesystem path; for cloud storage it is the key inside the bucket/container. |
| `storage` | não | string | `valor-exemplo` | Where the target Parquet file lives. |
| `bucket` | não | string | `meu-bucket` | Bucket (S3/GCS) or container (Azure) name. Required for cloud storage unless a full `uri` is provided. |
| `region` | não | string | `us-east-1` | Cloud region — used for S3 as `aws_region` when `storage_options` is built. |
| `access_key_id` | não | string | `SEU_ACCESS_KEY_ID` | Cloud access key / account name — mapped to `aws_access_key_id` for S3, `azure_storage_account_name` for Azure, and ignored for GCS (use a service account key through other means or extend `storage_options` manually in the future). |
| `secret_access_key` | não | string | `SEU_SECRET_ACCESS_KEY` | Cloud secret / account key — mapped to `aws_secret_access_key` for S3, `azure_storage_account_key` for Azure, and to `google_service_account` for GCS. |
| `endpoint` | não | string | `http://localhost:4566` | Custom endpoint — mapped to `aws_endpoint` for S3 and `azure_storage_endpoint` for Azure. |
| `compression` | não | string | `snappy` | Compression codec used when writing Parquet files. |
| `row_group_size` | não | integer | `null` | Maximum number of rows per row group. `None` leaves the writer default unchanged. |
| `primary_key` | **sim** | string | `id` | Column used to identify a row for upsert/delete on write. |
| `append_only` | não | boolean | `false` | When true, each non-CDC batch is written as a new numbered Parquet file inside the target directory instead of reading back and rewriting the single target file. This avoids the read-filter-rewrite cycle for large append-only loads. CDC batches still use the rewrite path so deletes/updates are honored. |

## `pgvector`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Full `postgresql://user:pass@host:port/db` URI — the `pgvector` extension must already be enabled on this database (`CREATE EXTENSION vector`). When provided, this value is used exactly as-is and all other connection fields are ignored. Keeps backward compatibility with older pipelines that already store a complete connection string. |
| `host` | não | string | `localhost` | Database server host name or IP address. |
| `port` | não | integer | `5432` | TCP port the PostgreSQL server is listening on. |
| `username` | não | string | `nexusflow` | User name to authenticate with. |
| `password` | não | string | `s3nhaForte123` | Password for the provided user name. |
| `database` | não | string | `nexusflow_test` | Database name to connect to. |
| `schema` | não | string | `public` | Schema / `search_path` to use for the target table. Defaults to the user's default search path (usually `public`). Only used when building the connection string from individual fields. |
| `ssl_mode` | não | string | `prefer` | SSL/TLS mode for the PostgreSQL connection used by the pgvector sink. Maps to the `sslmode` parameter in PostgreSQL connection strings. |
| `table` | **sim** | string | `events` | Table name — must already exist with a `vector(N)` column matching `embedding_column`/`dimension`; this sink only writes rows. |
| `primary_key` | **sim** | string | `id` | Column used to upsert on write. |
| `embedding_column` | **sim** | string | `embedding` | Name of the `vector(N)` column the embedding is written to. |
| `dimension` | **sim** | integer | `384` | Must match the `vector(N)` column's declared width. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for connecting and for each batch write (begin + upsert/delete + commit) — `tokio_postgres` has no timeout of its own, so a stalled connection would otherwise block the pipeline indefinitely (C15). |

## `pinecone`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `host` | não | string | `localhost` | Index-specific data-plane host, e.g. `https://my-index-xxxx.svc.us-east1-aws.pinecone.io` (from Pinecone's `describe_index` control-plane response — not built from `index` + `environment` here, that addressing scheme is deprecated). Kept for backward compatibility with existing canvas nodes. For new configurations prefer `index_name` plus `grpc_url`/`port`. |
| `api_key` | **sim** | string | `SUA_API_KEY` | Pinecone API key with write access to this index. |
| `primary_key` | **sim** | string | `id` | Column used as the Pinecone vector ID. |
| `embedding_column` | **sim** | string | `embedding` | Name of the `FixedSizeList<Float32>` column the embedding is written to. |
| `dimension` | **sim** | integer | `384` | Vector size — must match the index's configured dimension. |
| `namespace` | não | string | `default` | Pinecone namespace to write into within the index — omit to use the default (unnamed) namespace. |
| `timeout_seconds` | não | integer | `30` | Per-request timeout in seconds — `reqwest::Client` has no timeout by default, so a stalled connection to Pinecone would otherwise block the pipeline indefinitely (C15). |
| `port` | não | integer | `5432` | Optional port to use when `host` is provided as a bare hostname instead of a full URL. Defaults to 443 for HTTPS data-plane traffic. Example: with `host = "my-index.pinecone.io"` and `port = 443`, the connector targets `https://my-index.pinecone.io:443`. |
| `grpc_url` | não | string | `null` | Optional gRPC endpoint for this index, e.g. `https://my-index-xxxx.svc.us-east1-aws.pinecone.io:443`. Used as the fallback data-plane URL when the legacy `host` field is empty, and reserved for future gRPC-based implementations. |
| `index_name` | não | string | `events` | Name of the Pinecone index. Useful for UI validation, logging, and as a human-readable reference when the actual data-plane `host` is provisioned later by the Pinecone control plane. |

## `postgres`

Capability: `adbc_native`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Full `postgresql://user:pass@host:port/db` connection URI. When provided, this value is used exactly as-is and all other connection fields are ignored. Keeps backward compatibility with older pipelines that already store a complete connection string. |
| `host` | não | string | `localhost` | Database server host name or IP address (e.g. `localhost` or `db.example.com`). |
| `port` | não | integer | `5432` | TCP port the PostgreSQL server is listening on. |
| `username` | não | string | `nexusflow` | User name to authenticate with. |
| `password` | não | string | `s3nhaForte123` | Password for the provided user name. |
| `database` | não | string | `nexusflow_test` | Database name to connect to. |
| `schema` | não | string | `public` | Schema / `search_path` to use for the target table. Defaults to the user's default search path (usually `public`). Only used when building the connection string from individual fields. |
| `ssl_mode` | não | string | `prefer` | SSL/TLS mode for the PostgreSQL connection. Maps to the `sslmode` parameter in PostgreSQL connection strings. |
| `table` | **sim** | string | `events` | Table name to read from (source) or write to (sink) — no schema prefix needed unless the table isn't in the connection's default `search_path`. |
| `primary_key` | **sim** | string | `id` | Column used to partition reads by range and to upsert on write — must be an indexed, orderable column (integer/UUID/timestamp). |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each ADBC call (connect, query, insert) — the driver is a blocking FFI call run via `spawn_blocking`, so a stalled connection would otherwise block that call forever (though the underlying blocking thread keeps running regardless — no cancellation for in-flight libpq/ADBC calls) (C15). |

## `postgres-cdc`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Full `postgres://user:pass@host:port/db` connection URI. When provided, this value is used exactly as-is and all other connection fields are ignored. The `?replication=database` parameter is appended automatically at connect time, so you do not need to include it here. |
| `host` | não | string | `localhost` | Database server host name or IP address. |
| `port` | não | integer | `5432` | TCP port the PostgreSQL server is listening on. |
| `username` | não | string | `nexusflow` | User name to authenticate with. |
| `password` | não | string | `s3nhaForte123` | Password for the provided user name. |
| `database` | não | string | `nexusflow_test` | Database name to connect to. |
| `schema` | não | string | `public` | Schema used to qualify the target table when matching replication events. Defaults to the user's default search path (usually `public`). |
| `ssl_mode` | não | string | `prefer` | SSL/TLS mode for the PostgreSQL connection. Maps to the `sslmode` parameter in PostgreSQL connection strings. |
| `table` | **sim** | string | `events` | Table this connector reads changes for. The publication (see `publication_name`) must already cover this table — run `CREATE PUBLICATION <publication_name> FOR TABLE <table>` by hand once before starting this connector; it isn't created automatically. |
| `publication_name` | **sim** | string | `nexus_pub` | Replication publication name that covers `table`. |
| `slot_name` | **sim** | string | `nexus_slot` | Replication slot name — created automatically on first connect if it doesn't exist yet. Reconnecting later with the same name resumes from where this connector last left off: Postgres tracks the confirmed position server-side (via `update_applied_lsn`, called in `cdc.rs` after each event is read), so there's no separate LSN/offset to persist on the nexus-server side for this to work. One... |
| `fields` | não | array<object> | `"[ ... ]"` | Target schema for each change event's row — same 4-primitive-type ceiling as every other bridging connector (Kafka/MongoDB); Postgres column types beyond these aren't supported yet. Left empty, the connector introspects `table`'s real Arrow schema via the same ADBC path the batch `postgres` connector uses (`introspect::cdc_fields`) and narrows it to these 4 types — real catalog metadata, not a... |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each replication connection call (connect, read slot, drop slot) — a stalled connection would otherwise block the pipeline indefinitely (C15). |
| `max_batch_events` | não | integer | `1000` | Maximum number of change events to collect in a single run. After this many events the source ends cleanly, letting the run finish and the scheduler start the next micro-batch. The Postgres replication slot keeps the unconsumed position server-side, so the next run resumes from where this one stopped. |

## `qdrant`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `url` | não | string | `https://api.example.com` | Full Qdrant gRPC URL. Example: `"http://localhost:6334"`. When provided, this value takes precedence over the separate `host`/`port`/`grpc_url` fields and is used exactly as-is. Kept for backward compatibility with existing canvas configurations. |
| `host` | não | string | `localhost` | Qdrant server host or IP address. Example: `"localhost"` or `"127.0.0.1"`. Used only when `url` is not set. If the value already contains a scheme such as `http://` or `https://`, it is used directly; otherwise `http://` is assumed. |
| `port` | não | integer | `6334` | Qdrant gRPC port. The Qdrant gRPC interface listens on port `6334` by default. This port is used together with `host` to build the connection URL when `url` is not provided. |
| `grpc_url` | não | string | `` | Optional explicit gRPC URL. When `url` is empty and `grpc_url` is set, this value is used as the connection URL. It overrides any `host`/`port` combination and is useful when the gRPC endpoint differs from the default location (for example, behind a TLS-terminating proxy). |
| `api_key` | não | string | `SUA_API_KEY` | API key for authenticated Qdrant clusters. Qdrant Cloud and on-premise deployments with authentication enabled require an API key. Leave empty for unauthenticated local instances. When set, the key is passed to the Qdrant client on connection. |
| `collection` | não | string | `events` | Name of an existing Qdrant collection. The collection must already be created (with the right vector size) on the Qdrant server; this sink only writes points. When provided, it takes precedence over `collection_name`. Kept for backward compatibility with existing canvas configurations. |
| `collection_name` | não | string | `events` | Name of an existing Qdrant collection. Alternative, more explicit field for the collection name. Used only when `collection` is empty. |
| `primary_key` | **sim** | string | `id` | Must be an `Int64` column. Qdrant point IDs are unsigned integers or UUIDs; arbitrary string keys are not supported. The values in this column are converted to `u64` point IDs. |
| `embedding_column` | **sim** | string | `embedding` | Name of the `FixedSizeList<Float32>` column the embedding is written to. |
| `dimension` | **sim** | integer | `384` | Vector size. Must match the embedding column's actual length and the vector configuration of the target Qdrant collection. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each gRPC call to Qdrant. The client library exposes no connection/request timeout of its own, so a stalled connection would otherwise block the pipeline indefinitely (C15). |

## `rabbitmq`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `url` | **sim** | string | `https://api.example.com` | AMQP URI, e.g. `"amqp://user:pass@host:5672/%2f"`. |
| `queue` | **sim** | string | `valor-exemplo` | Queue name — declared durable if it doesn't already exist (both source and sink do this independently, idempotently). |
| `exchange` | não | string | `nexus.events` | Exchange to publish to (sink) / that routes to `queue` (source declares the queue directly, exchange routing is the operator's own responsibility via broker config). Empty string (the default/"direct" exchange) publishes straight to `queue` by name, no exchange setup needed — the common case for a simple point-to-point pipeline. |
| `routing_key` | não | string | `null` | Routing key for publishes — defaults to `queue`'s name, which is what routes correctly against the default exchange. |
| `fields` | **sim** | array<object> | `"[ ... ]"` | Explicit column projection — a message payload is an opaque byte blob (assumed JSON), same contract as `kafka`/`nats`'s `fields`. |
| `batch_size` | não | integer | `500` | How many decoded messages to fold into a single `RecordBatch`. |
| `idle_timeout_ms` | não | integer | `2000` | How long to wait for a new message before returning what's been buffered so far — same "idle means try again" contract as Kafka/NATS. |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for connecting. |
| `retries` | não | integer | `3` | Number of retries on transient failures (5xx, timeouts, connect errors). |
| `retry_backoff_seconds` | não | integer | `1` | Base delay between retries in seconds (exponential backoff). |

## `redis`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `url` | **sim** | string | `https://api.example.com` | Connection URL, e.g. `redis://[:password@]host:port[/db]` or `rediss://...` for TLS. |
| `stream_key` | **sim** | string | `valor-exemplo` | Stream key (`XADD`/`XREAD` target). |
| `starting_position` | não | enum(latest/earliest) | `latest` |  |
| `fields` | não | array<object> | `"[ ... ]"` | Column projection for the source — ignored by the sink, which just writes every column of the incoming `RecordBatch` as its own stream-entry field. |
| `idle_timeout_ms` | não | integer | `5000` | How long `XREAD BLOCK` waits for a new entry before returning empty — a timeout here just means "no new entry yet, try again", not an error, same contract MQTT's `idle_timeout_ms` documents. |
| `batch_size` | não | integer | `500` | Max entries read per `XREAD` call (`COUNT`). |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for connecting. |
| `retries` | não | integer | `3` | Number of retries on transient failures (5xx, timeouts, connect errors). |
| `retry_backoff_seconds` | não | integer | `1` | Base delay between retries in seconds (exponential backoff). |

## `rest`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Legacy full URL field. If present, it takes precedence over `url`, `base_url`, and `path`, preserving old configs that stored the complete endpoint address in a single `uri` key. |
| `url` | não | string | `https://api.example.com` | Legacy full URL field. Used when `uri` is absent and either replaces `base_url`/`path` entirely or fills in for a missing `base_url`. |
| `base_url` | não | string | `https://api.example.com` | Scheme + host of the API, e.g. `"https://api.example.com"` — no trailing slash needed. Empty when the full URL is supplied via `uri` or `url`. |
| `path` | não | string | `/data/events.csv` | Path appended to `base_url` for this request, e.g. `"/v1/items"`. Leading slashes are normalized automatically. |
| `method` | não | string | `valor-exemplo` | HTTP method used by the REST source when fetching pages. Serialized as an uppercase verb to match HTTP conventions. |
| `headers` | não | object | `{}` | Extra HTTP headers sent with every request — this is where an API key/bearer token goes (e.g. `{"Authorization": "Bearer ..."}`). |
| `fields` | não | array<object> | `"[ ... ]"` | Explicit target schema — REST responses carry no schema of their own. Left empty, the connector fetches the first page (same request the real read would make first) and infers one from its rows — see `nexus_core::RecordBatchBuilder::infer_schema`. |
| `rows_path` | não | string | `null` | Dot-separated path to the array of row objects in the response body (e.g. `"data.items"`). `None` means the response body itself is the array. |
| `pagination` | não | string | `valor-exemplo` | Pagination strategy for paginated REST sources. |
| `max_pages` | não | integer | `1000` | Hard cap on pages fetched, regardless of pagination signals — guards against a misbehaving API looping forever. |
| `timeout_seconds` | não | integer | `30` | Per-request timeout in seconds. |
| `retries` | não | integer | `3` | Number of retries on transient failures (5xx, timeouts, connect errors). |
| `retry_backoff_seconds` | não | integer | `1` | Base delay between retries in seconds (exponential backoff). |
| `requests_per_second` | não | integer | `0` | Maximum requests per second across this source (0 = unlimited). |

## `sqlite`

Capability: `adbc_native`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Full SQLite connection URI. When provided, this value is used exactly as-is and `file_path` is ignored. Keeps backward compatibility with older pipelines that store a complete path or `:memory:` string. |
| `file_path` | não | string | `:memory:` | File path to the `.db` file, or `:memory:` for an ephemeral database that only exists for this process's lifetime. Use an absolute path if the SQLite file lives outside the nexus-server working directory. Ignored when `uri` is set. |
| `table` | **sim** | string | `events` | Table name to read from (source) or write to (sink) — created automatically on the sink side if it doesn't exist yet. |
| `primary_key` | **sim** | string | `id` | Column used to upsert on write — should be an indexed, unique column (integer or text primary key). |
| `timeout_seconds` | não | integer | `30` | Timeout in seconds for each ADBC call (connect, query, insert) — a concurrent writer holding the SQLite file lock can otherwise stall a call indefinitely (though the underlying blocking thread keeps running regardless — no cancellation for in-flight ADBC calls) (C15). |

## `webhook`

Capability: `bridged`

| Campo | Obrigatório | Tipo | Exemplo | Descrição |
|---|---|---|---|---|
| `uri` | não | string | `postgresql://user:pass@localhost:5432/db` | Legacy full URL field. If present, it takes precedence over `url`, `base_url`, and `path`, preserving old configs that stored the complete endpoint address in a single `uri` key. |
| `url` | não | string | `https://api.example.com` | Legacy full URL field. Used when `uri` is absent and either replaces `base_url`/`path` entirely or fills in for a missing `base_url`. |
| `base_url` | não | string | `https://api.example.com` | Scheme + host of the target API, e.g. `"https://api.example.com"` — no trailing slash needed. Empty when the full URL is supplied via `uri` or `url`. |
| `path` | não | string | `/data/events.csv` | Path appended to `base_url`, e.g. `"/v1/events"`. Leading slashes are normalized automatically. |
| `method` | não | string | `valor-exemplo` | HTTP method used by the webhook sink. |
| `headers` | não | object | `{}` | Extra HTTP headers sent with every request — this is where an API key/bearer token goes (e.g. `{"Authorization": "Bearer ..."}`). |
| `body_mode` | não | string | `valor-exemplo` |  |
| `timeout_seconds` | não | integer | `30` | Per-request timeout in seconds. |
| `retries` | não | integer | `3` | Number of retries on transient failures (5xx, timeouts, connect errors). |
| `retry_backoff_seconds` | não | integer | `1` | Base delay between retries in seconds (exponential backoff). |
| `requests_per_second` | não | integer | `0` | Maximum requests per second — only meaningful with `body_mode: "per_row"`, where a large batch means many requests (0 = unlimited). |
