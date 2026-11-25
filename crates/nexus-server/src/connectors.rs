#[cfg(feature = "postgres-cdc")]
use nexus_connector_postgres::{PostgresCdcConfig, PostgresCdcSource};
use nexus_connector_postgres::{PostgresConnectorConfig, PostgresSink, PostgresSource};
use nexus_connector_sqlite::{SqliteConnectorConfig, SqliteSink, SqliteSource};
use nexus_core::{ConnectorRegistry, NodeSpec, PipelineSpec, Sink, Source};

#[cfg(feature = "ailake-cdc")]
use nexus_connector_ailake::{AilakeCdcConfig, AilakeCdcSource};
#[cfg(feature = "ailake")]
use nexus_connector_ailake::{AilakeConnectorConfig, AilakeSink, AilakeSource};
#[cfg(feature = "chromadb")]
use nexus_connector_chromadb::{ChromaConnectorConfig, ChromaSink};
#[cfg(feature = "clickhouse")]
use nexus_connector_clickhouse::{ClickHouseConnectorConfig, ClickHouseSink, ClickHouseSource};
#[cfg(feature = "csv")]
use nexus_connector_csv::{CsvConnectorConfig, CsvSink, CsvSource};
#[cfg(feature = "deltalake-cdc")]
use nexus_connector_deltalake::{DeltaCdcConfig, DeltaCdcSource};
#[cfg(feature = "deltalake")]
use nexus_connector_deltalake::{DeltaConnectorConfig, DeltaSink, DeltaSource};
#[cfg(feature = "duckdb")]
use nexus_connector_duckdb::{DuckdbConnectorConfig, DuckdbSink, DuckdbSource};
#[cfg(feature = "iceberg-cdc")]
use nexus_connector_iceberg::{IcebergCdcConfig, IcebergCdcSource};
#[cfg(feature = "iceberg")]
use nexus_connector_iceberg::{IcebergConnectorConfig, IcebergSink, IcebergSource};
#[cfg(feature = "kafka")]
use nexus_connector_kafka::{KafkaConnectorConfig, KafkaSink, KafkaSource};
#[cfg(feature = "lancedb")]
use nexus_connector_lancedb::{LanceDbConnectorConfig, LanceDbSink};
#[cfg(feature = "milvus")]
use nexus_connector_milvus::{MilvusConnectorConfig, MilvusSink};
#[cfg(feature = "mongodb")]
use nexus_connector_mongodb::{
    MongoCdcConfig, MongoCdcSource, MongoConnectorConfig, MongoSink, MongoSource,
};
#[cfg(feature = "mqtt")]
use nexus_connector_mqtt::{MqttConnectorConfig, MqttSource};
#[cfg(feature = "mysql-cdc")]
use nexus_connector_mysql::{MySqlCdcConfig, MySqlCdcSource};
#[cfg(feature = "mysql")]
use nexus_connector_mysql::{MySqlConnectorConfig, MySqlSink, MySqlSource};
#[cfg(feature = "nats")]
use nexus_connector_nats::{NatsConnectorConfig, NatsSink, NatsSource};
#[cfg(feature = "odbc")]
use nexus_connector_odbc::{OdbcConnectorConfig, OdbcSink, OdbcSource};
#[cfg(feature = "parquet")]
use nexus_connector_parquet::{ParquetConnectorConfig, ParquetSink, ParquetSource};
#[cfg(feature = "pgvector")]
use nexus_connector_pgvector::{PgVectorConnectorConfig, PgVectorSink};
#[cfg(feature = "pinecone")]
use nexus_connector_pinecone::{PineconeConnectorConfig, PineconeSink};
#[cfg(feature = "qdrant")]
use nexus_connector_qdrant::{QdrantConnectorConfig, QdrantSink};
#[cfg(feature = "rabbitmq")]
use nexus_connector_rabbitmq::{RabbitmqConnectorConfig, RabbitmqSink, RabbitmqSource};
#[cfg(feature = "redis")]
use nexus_connector_redis::{RedisConnectorConfig, RedisSink, RedisSource};
#[cfg(feature = "rest")]
use nexus_connector_rest::{RestConnectorConfig, RestSource, WebhookSink, WebhookSinkConfig};

// Bound work before cloning or deserializing configuration, including configs
// loaded from storage and configs handed to dynamically registered connectors.
fn validate_config_limits(config: &serde_json::Value) -> anyhow::Result<()> {
    anyhow::ensure!(config.is_object(), "connector config must be an object");
    fn visit(value: &serde_json::Value, depth: usize, budget: &mut usize) -> anyhow::Result<()> {
        anyhow::ensure!(depth <= 32, "connector config nesting exceeds 32 levels");
        let cost = match value {
            serde_json::Value::String(s) => s.len().saturating_add(1),
            _ => 1,
        };
        *budget = budget
            .checked_sub(cost)
            .ok_or_else(|| anyhow::anyhow!("connector config exceeds resource limit"))?;
        match value {
            serde_json::Value::Object(fields) => {
                for (key, value) in fields {
                    *budget = budget.checked_sub(key.len()).ok_or_else(|| {
                        anyhow::anyhow!("connector config exceeds resource limit")
                    })?;
                    visit(value, depth + 1, budget)?;
                }
            }
            serde_json::Value::Array(values) => {
                anyhow::ensure!(
                    values.len() <= 4096,
                    "connector config array exceeds 4096 items"
                );
                for value in values {
                    visit(value, depth + 1, budget)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    visit(config, 0, &mut (256 * 1024))
}

/// Deserialize only a bounded JSON object into a statically selected config type.
/// Serializing after the structural checks also gives an exact byte-size limit
/// before the typed parser can allocate connector-specific containers.
fn deserialize_config<T: serde::de::DeserializeOwned>(
    config: &serde_json::Value,
) -> anyhow::Result<T> {
    validate_config_limits(config)?;
    let bytes = serde_json::to_vec(config)?;
    if bytes.len() > 256 * 1024 {
        anyhow::bail!("serialized connector config exceeds 256 KiB");
    }
    Ok(serde_json::from_slice(&bytes)?)
}

/// The only place that knows which connector names exist and how to build
/// them — `nexus-core`/`PipelineEngine` never hardcode a connector list, see
/// `ConnectorRegistry` (ARCHITECTURE.md §3). Adding an OSS connector means
/// adding a match arm here (behind that connector's own Cargo feature — see
/// nexus-server/Cargo.toml), nowhere else in nexus-server. A connector that
/// can't add a match arm here — anything living in a private out-of-tree
/// repo (`ROADMAP.md` Fase 12, Bloco 3) — registers a
/// `SourceBuilder`/`SinkBuilder` instead (`nexus-core/registry.rs`); every
/// `other` arm below falls back to that registry before giving up. Real OSS
/// connectors are unaffected — the hardcoded match arms always win first.
///
/// The six vector-DB sinks (milvus/qdrant/lancedb/pgvector/pinecone/
/// chromadb) and kafka/rest have no `Sink`/`Source` counterpart at all —
/// they're AI Lakehouse destinations or read-only bridging sources by
/// design (see each crate's own src/lib.rs doc comment), not an oversight
/// here.
/// Validates that a source node's config can be deserialized into the
/// connector's strongly-typed config struct. This catches typos, missing
/// required fields, and wrong types at pipeline create/update time, before
/// the invalid config is persisted.
pub fn validate_source_config(node: &NodeSpec) -> anyhow::Result<()> {
    validate_config_limits(&node.config)?;
    match node.connector.as_str() {
        "postgres" => {
            let _: PostgresConnectorConfig = deserialize_config(&node.config)?;
        }
        "sqlite" => {
            let _: SqliteConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let _: MongoConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let _: MySqlConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let _: KafkaConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "mqtt")]
        "mqtt" => {
            let _: MqttConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "rest")]
        "rest" => {
            let _: RestConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "odbc")]
        "odbc" => {
            let _: OdbcConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "deltalake")]
        "deltalake" => {
            let _: DeltaConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => {
            let _: IcebergConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "parquet")]
        "parquet" => {
            let _: ParquetConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "ailake")]
        "ailake" => {
            let _: AilakeConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "csv")]
        "csv" => {
            let _: CsvConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            let _: ClickHouseConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            let _: DuckdbConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let cfg: RedisConnectorConfig = deserialize_config(&node.config)?;
            cfg.validate()?;
        }
        #[cfg(feature = "nats")]
        "nats" => {
            let cfg: NatsConnectorConfig = deserialize_config(&node.config)?;
            cfg.validate()?;
        }
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let cfg: RabbitmqConnectorConfig = deserialize_config(&node.config)?;
            cfg.validate()?;
        }
        #[cfg(feature = "postgres-cdc")]
        "postgres-cdc" => {
            let _: PostgresCdcConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "mongodb")]
        "mongodb-cdc" => {
            let _: MongoCdcConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "mysql-cdc")]
        "mysql-cdc" => {
            let _: MySqlCdcConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "deltalake-cdc")]
        "deltalake-cdc" => {
            let _: DeltaCdcConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "iceberg-cdc")]
        "iceberg-cdc" => {
            let _: IcebergCdcConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "ailake-cdc")]
        "ailake-cdc" => {
            let _: AilakeCdcConfig = deserialize_config(&node.config)?;
        }
        other => match ConnectorRegistry::find_source_builder(other) {
            Some(builder) => (builder.validate)(&node.config)?,
            None => anyhow::bail!("unsupported source connector: {other:?}"),
        },
    }
    Ok(())
}

pub async fn build_source(
    node: &NodeSpec,
    index: usize,
) -> anyhow::Result<(String, Box<dyn Source>)> {
    validate_config_limits(&node.config)?;
    let name = node.resolved_name(index, "source")?;
    let source: Box<dyn Source> = match node.connector.as_str() {
        "postgres" => {
            let cfg: PostgresConnectorConfig = deserialize_config(&node.config)?;
            Box::new(PostgresSource::connect(&cfg, None).await?)
        }
        "sqlite" => {
            let cfg: SqliteConnectorConfig = deserialize_config(&node.config)?;
            Box::new(SqliteSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let cfg: MongoConnectorConfig = deserialize_config(&node.config)?;
            Box::new(MongoSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let cfg: MySqlConnectorConfig = deserialize_config(&node.config)?;
            Box::new(MySqlSource::connect(&cfg).await?)
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let cfg: KafkaConnectorConfig = deserialize_config(&node.config)?;
            Box::new(KafkaSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mqtt")]
        "mqtt" => {
            let cfg: MqttConnectorConfig = deserialize_config(&node.config)?;
            Box::new(MqttSource::connect(&cfg).await?)
        }
        #[cfg(feature = "rest")]
        "rest" => {
            let cfg: RestConnectorConfig = deserialize_config(&node.config)?;
            Box::new(RestSource::connect(&cfg).await?)
        }
        #[cfg(feature = "odbc")]
        "odbc" => {
            let cfg: OdbcConnectorConfig = deserialize_config(&node.config)?;
            Box::new(OdbcSource::connect(&cfg)?)
        }
        #[cfg(feature = "deltalake")]
        "deltalake" => {
            let cfg: DeltaConnectorConfig = deserialize_config(&node.config)?;
            Box::new(DeltaSource::connect(&cfg).await?)
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => {
            let cfg: IcebergConnectorConfig = deserialize_config(&node.config)?;
            Box::new(IcebergSource::connect(&cfg).await?)
        }
        #[cfg(feature = "parquet")]
        "parquet" => {
            let cfg: ParquetConnectorConfig = deserialize_config(&node.config)?;
            Box::new(ParquetSource::connect(&cfg).await?)
        }
        #[cfg(feature = "ailake")]
        "ailake" => {
            let cfg: AilakeConnectorConfig = deserialize_config(&node.config)?;
            Box::new(AilakeSource::connect(&cfg).await?)
        }
        #[cfg(feature = "csv")]
        "csv" => {
            let cfg: CsvConnectorConfig = deserialize_config(&node.config)?;
            Box::new(CsvSource::connect(&cfg).await?)
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            let cfg: ClickHouseConnectorConfig = deserialize_config(&node.config)?;
            Box::new(ClickHouseSource::connect(&cfg, None).await?)
        }
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            let cfg: DuckdbConnectorConfig = deserialize_config(&node.config)?;
            Box::new(DuckdbSource::connect(&cfg).await?)
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let cfg: RedisConnectorConfig = deserialize_config(&node.config)?;
            Box::new(RedisSource::connect(&cfg).await?)
        }
        #[cfg(feature = "nats")]
        "nats" => {
            let cfg: NatsConnectorConfig = deserialize_config(&node.config)?;
            Box::new(NatsSource::connect(&cfg).await?)
        }
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let cfg: RabbitmqConnectorConfig = deserialize_config(&node.config)?;
            Box::new(RabbitmqSource::connect(&cfg).await?)
        }
        #[cfg(feature = "postgres-cdc")]
        "postgres-cdc" => {
            let cfg: PostgresCdcConfig = deserialize_config(&node.config)?;
            Box::new(PostgresCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mongodb")]
        "mongodb-cdc" => {
            let cfg: MongoCdcConfig = deserialize_config(&node.config)?;
            Box::new(MongoCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mysql-cdc")]
        "mysql-cdc" => {
            let cfg: MySqlCdcConfig = deserialize_config(&node.config)?;
            Box::new(MySqlCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "deltalake-cdc")]
        "deltalake-cdc" => {
            let cfg: DeltaCdcConfig = deserialize_config(&node.config)?;
            Box::new(DeltaCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "iceberg-cdc")]
        "iceberg-cdc" => {
            let cfg: IcebergCdcConfig = deserialize_config(&node.config)?;
            Box::new(IcebergCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "ailake-cdc")]
        "ailake-cdc" => {
            let cfg: AilakeCdcConfig = deserialize_config(&node.config)?;
            Box::new(AilakeCdcSource::connect(&cfg).await?)
        }
        other => match ConnectorRegistry::find_source_builder(other) {
            Some(builder) => (builder.build)(node.config.clone()).await?,
            None => anyhow::bail!("unsupported source connector: {other:?}"),
        },
    };
    Ok((name, source))
}

/// Validates that a sink node's config can be deserialized into the
/// connector's strongly-typed config struct. See `validate_source_config`.
pub fn validate_sink_config(node: &NodeSpec) -> anyhow::Result<()> {
    validate_config_limits(&node.config)?;
    match node.connector.as_str() {
        "postgres" => {
            let _: PostgresConnectorConfig = deserialize_config(&node.config)?;
        }
        "sqlite" => {
            let _: SqliteConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let _: MongoConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let _: MySqlConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "odbc")]
        "odbc" => {
            let _: OdbcConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "milvus")]
        "milvus" => {
            let _: MilvusConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "qdrant")]
        "qdrant" => {
            let _: QdrantConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "lancedb")]
        "lancedb" => {
            let _: LanceDbConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "pgvector")]
        "pgvector" => {
            let _: PgVectorConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "pinecone")]
        "pinecone" => {
            let _: PineconeConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "chromadb")]
        "chromadb" => {
            let _: ChromaConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "rest")]
        "webhook" => {
            let _: WebhookSinkConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "deltalake")]
        "deltalake" => {
            let _: DeltaConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => {
            let _: IcebergConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "parquet")]
        "parquet" => {
            let _: ParquetConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "ailake")]
        "ailake" => {
            let _: AilakeConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "csv")]
        "csv" => {
            let _: CsvConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            let _: ClickHouseConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            let _: DuckdbConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let _: KafkaConnectorConfig = deserialize_config(&node.config)?;
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let cfg: RedisConnectorConfig = deserialize_config(&node.config)?;
            cfg.validate()?;
        }
        #[cfg(feature = "nats")]
        "nats" => {
            let cfg: NatsConnectorConfig = deserialize_config(&node.config)?;
            cfg.validate()?;
        }
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let cfg: RabbitmqConnectorConfig = deserialize_config(&node.config)?;
            cfg.validate()?;
        }
        other => match ConnectorRegistry::find_sink_builder(other) {
            Some(builder) => (builder.validate)(&node.config)?,
            None => anyhow::bail!("unsupported sink connector: {other:?}"),
        },
    }
    Ok(())
}

/// Validates that every source/sink config in the spec deserializes into the
/// connector's typed config struct. This catches structural config errors at
/// pipeline create/update time, before persistence.
pub fn validate_pipeline_configs(spec: &PipelineSpec) -> anyhow::Result<()> {
    for (i, node) in spec.sources.iter().enumerate() {
        validate_source_config(node)
            .map_err(|e| anyhow::anyhow!("source[{i}] ({}): {e}", node.connector))?;
    }
    for (i, node) in spec.sinks.iter().enumerate() {
        validate_sink_config(node)
            .map_err(|e| anyhow::anyhow!("sink[{i}] ({}): {e}", node.connector))?;
    }
    if let Some(dbt) = &spec.dbt {
        if let Some(output) = &dbt.output {
            validate_source_config(output)
                .map_err(|e| anyhow::anyhow!("dbt.output ({}): {e}", output.connector))?;
        }
    }
    for (i, node) in spec.post_dbt_sinks.iter().enumerate() {
        validate_sink_config(node)
            .map_err(|e| anyhow::anyhow!("post_dbt_sinks[{i}] ({}): {e}", node.connector))?;
    }
    Ok(())
}

pub async fn build_sink(
    node: &NodeSpec,
    index: usize,
    schema: &arrow_schema::SchemaRef,
) -> anyhow::Result<(String, Box<dyn Sink>)> {
    validate_config_limits(&node.config)?;
    let name = node.resolved_name(index, "sink")?;
    // Most connectors below only ever needed the column *names* — kept as
    // `columns` so their call sites don't change. postgres/sqlite now take
    // `schema` directly (types included) to drive `CREATE TABLE IF NOT
    // EXISTS`; see their own `connect()` doc comments for why.
    #[cfg(any(feature = "pgvector", feature = "clickhouse"))]
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let sink: Box<dyn Sink> = match node.connector.as_str() {
        "postgres" => {
            let cfg: PostgresConnectorConfig = deserialize_config(&node.config)?;
            Box::new(PostgresSink::connect(&cfg, schema).await?)
        }
        "sqlite" => {
            let cfg: SqliteConnectorConfig = deserialize_config(&node.config)?;
            Box::new(SqliteSink::connect(&cfg, schema).await?)
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let cfg: MongoConnectorConfig = deserialize_config(&node.config)?;
            Box::new(MongoSink::connect(&cfg).await?)
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let cfg: MySqlConnectorConfig = deserialize_config(&node.config)?;
            Box::new(MySqlSink::connect(&cfg, schema).await?)
        }
        #[cfg(feature = "odbc")]
        "odbc" => {
            let cfg: OdbcConnectorConfig = deserialize_config(&node.config)?;
            Box::new(OdbcSink::connect(&cfg)?)
        }
        #[cfg(feature = "milvus")]
        "milvus" => {
            let cfg: MilvusConnectorConfig = deserialize_config(&node.config)?;
            Box::new(MilvusSink::connect(&cfg).await?)
        }
        #[cfg(feature = "qdrant")]
        "qdrant" => {
            let cfg: QdrantConnectorConfig = deserialize_config(&node.config)?;
            Box::new(QdrantSink::connect(&cfg)?)
        }
        #[cfg(feature = "lancedb")]
        "lancedb" => {
            let cfg: LanceDbConnectorConfig = deserialize_config(&node.config)?;
            Box::new(LanceDbSink::connect(&cfg).await?)
        }
        #[cfg(feature = "pgvector")]
        "pgvector" => {
            let cfg: PgVectorConnectorConfig = deserialize_config(&node.config)?;
            // `PgVectorSink::connect`'s `columns` are documented as "the
            // non-embedding data columns" — it binds the embedding
            // separately via `rows::extract_embeddings` (a `vector(N)`
            // column has no generic Arrow-to-SQL mapping the way
            // int64/float64/boolean/utf8 do). Passing the raw column list
            // straight through left `embedding_column` in it, so every
            // write hit `row_params`'s catch-all "unsupported data type"
            // error for the `FixedSizeList<Float32>` array — a real bug
            // found testing this connector end to end this session, not
            // just a stale config.
            let non_embedding_columns: Vec<String> = columns
                .iter()
                .filter(|c| *c != &cfg.embedding_column)
                .cloned()
                .collect();
            Box::new(PgVectorSink::connect(&cfg, &non_embedding_columns).await?)
        }
        #[cfg(feature = "pinecone")]
        "pinecone" => {
            let cfg: PineconeConnectorConfig = deserialize_config(&node.config)?;
            Box::new(PineconeSink::connect(&cfg)?)
        }
        #[cfg(feature = "chromadb")]
        "chromadb" => {
            let cfg: ChromaConnectorConfig = deserialize_config(&node.config)?;
            Box::new(ChromaSink::connect(&cfg).await?)
        }
        #[cfg(feature = "rest")]
        "webhook" => {
            let cfg: WebhookSinkConfig = deserialize_config(&node.config)?;
            Box::new(WebhookSink::connect(&cfg)?)
        }
        #[cfg(feature = "deltalake")]
        "deltalake" => {
            let cfg: DeltaConnectorConfig = deserialize_config(&node.config)?;
            Box::new(DeltaSink::connect(&cfg)?)
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => {
            let cfg: IcebergConnectorConfig = deserialize_config(&node.config)?;
            Box::new(IcebergSink::connect(&cfg)?)
        }
        #[cfg(feature = "parquet")]
        "parquet" => {
            let cfg: ParquetConnectorConfig = deserialize_config(&node.config)?;
            Box::new(ParquetSink::connect(&cfg)?)
        }
        #[cfg(feature = "ailake")]
        "ailake" => {
            let cfg: AilakeConnectorConfig = deserialize_config(&node.config)?;
            Box::new(AilakeSink::connect(&cfg)?)
        }
        #[cfg(feature = "csv")]
        "csv" => {
            let cfg: CsvConnectorConfig = deserialize_config(&node.config)?;
            Box::new(CsvSink::connect(&cfg)?)
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            let cfg: ClickHouseConnectorConfig = deserialize_config(&node.config)?;
            Box::new(ClickHouseSink::connect(&cfg, &columns).await?)
        }
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            let cfg: DuckdbConnectorConfig = deserialize_config(&node.config)?;
            Box::new(DuckdbSink::connect(&cfg, schema).await?)
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let cfg: KafkaConnectorConfig = deserialize_config(&node.config)?;
            Box::new(KafkaSink::connect(&cfg)?)
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let cfg: RedisConnectorConfig = deserialize_config(&node.config)?;
            Box::new(RedisSink::connect(&cfg).await?)
        }
        #[cfg(feature = "nats")]
        "nats" => {
            let cfg: NatsConnectorConfig = deserialize_config(&node.config)?;
            Box::new(NatsSink::connect(&cfg).await?)
        }
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let cfg: RabbitmqConnectorConfig = deserialize_config(&node.config)?;
            Box::new(RabbitmqSink::connect(&cfg).await?)
        }
        other => match ConnectorRegistry::find_sink_builder(other) {
            Some(builder) => (builder.build)(node.config.clone()).await?,
            None => anyhow::bail!("unsupported sink connector: {other:?}"),
        },
    };
    Ok((name, sink))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_core::ConnectorCapability;

    #[test]
    fn config_limits_reject_excessive_resources() {
        assert!(validate_config_limits(&serde_json::json!({"uri": "sqlite://local"})).is_ok());
        assert!(validate_config_limits(&serde_json::json!([])).is_err());
        assert!(
            validate_config_limits(&serde_json::json!({"token": "x".repeat(256 * 1024)})).is_err()
        );
        assert!(validate_config_limits(&serde_json::json!({"items": vec![0; 4097]})).is_err());
        let mut nested = serde_json::json!(null);
        for _ in 0..34 {
            nested = serde_json::json!({"nested": nested});
        }
        assert!(validate_config_limits(&nested).is_err());
    }

    #[test]
    fn typed_config_parser_checks_encoded_size_and_shape() {
        let valid = serde_json::json!({"uri": "sqlite://local"});
        let parsed: serde_json::Value = deserialize_config(&valid).unwrap();
        assert_eq!(parsed, valid);
        // Escaping expands the JSON beyond the byte limit despite fitting the
        // structural budget. Reject it before typed deserialization.
        let expanded = serde_json::json!({"token": "\u{0001}".repeat(50_000)});
        assert!(validate_config_limits(&expanded).is_ok());
        assert!(deserialize_config::<serde_json::Value>(&expanded).is_err());
        assert!(deserialize_config::<PostgresConnectorConfig>(&serde_json::json!({})).is_err());
    }

    // --- Plugin builder fallback (ROADMAP.md Fase 12 Bloco 3 prerequisite) ---
    // Stands in for a connector crate (e.g. Excel) that has no match arm
    // in build_source/build_sink above — it registers a
    // SourceBuilder/SinkBuilder instead (nexus-core/registry.rs).

    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};

    struct DummyPluginSource;

    #[async_trait]
    impl Source for DummyPluginSource {
        async fn read_batches(
            &mut self,
        ) -> Result<
            BoxStream<'_, Result<arrow_array::RecordBatch, nexus_core::NexusError>>,
            nexus_core::NexusError,
        > {
            Ok(Box::pin(stream::empty()))
        }

        fn schema(&self) -> arrow_schema::SchemaRef {
            std::sync::Arc::new(arrow_schema::Schema::empty())
        }
    }

    struct DummyPluginSink;

    #[async_trait]
    impl Sink for DummyPluginSink {
        async fn write_batch(
            &mut self,
            _batch: arrow_array::RecordBatch,
        ) -> Result<(), nexus_core::NexusError> {
            Ok(())
        }

        async fn commit_checkpoint(
            &mut self,
            _cursor: nexus_core::CheckpointCursor,
        ) -> Result<(), nexus_core::NexusError> {
            Ok(())
        }
    }

    #[derive(serde::Deserialize, schemars::JsonSchema)]
    struct TestPluginConnectorConfig {
        #[allow(dead_code)]
        value: String,
    }

    fn validate_test_plugin_connector_config(
        cfg: &serde_json::Value,
    ) -> Result<(), nexus_core::NexusError> {
        serde_json::from_value::<TestPluginConnectorConfig>(cfg.clone())
            .map(|_| ())
            .map_err(|e| nexus_core::NexusError::Serialization(e.to_string()))
    }

    nexus_core::submit_connector!(
        "test-plugin-connector",
        ConnectorCapability::Bridged,
        TestPluginConnectorConfig
    );

    nexus_core::submit_source_builder!(
        "test-plugin-connector",
        validate_test_plugin_connector_config,
        |cfg: serde_json::Value| -> futures::future::BoxFuture<
            'static,
            Result<Box<dyn Source>, nexus_core::NexusError>,
        > {
            Box::pin(async move {
                let _parsed: TestPluginConnectorConfig = serde_json::from_value(cfg)
                    .map_err(|e| nexus_core::NexusError::Serialization(e.to_string()))?;
                Ok(Box::new(DummyPluginSource) as Box<dyn Source>)
            })
        }
    );

    nexus_core::submit_sink_builder!(
        "test-plugin-connector",
        validate_test_plugin_connector_config,
        |cfg: serde_json::Value| -> futures::future::BoxFuture<
            'static,
            Result<Box<dyn Sink>, nexus_core::NexusError>,
        > {
            Box::pin(async move {
                let _parsed: TestPluginConnectorConfig = serde_json::from_value(cfg)
                    .map_err(|e| nexus_core::NexusError::Serialization(e.to_string()))?;
                Ok(Box::new(DummyPluginSink) as Box<dyn Sink>)
            })
        }
    );

    #[tokio::test]
    async fn build_source_falls_back_to_a_registered_plugin_builder() {
        let node = NodeSpec {
            name: None,
            connector: "test-plugin-connector".to_string(),
            config: serde_json::json!({"value": "x"}),
        };
        let (_, source) = build_source(&node, 0).await.expect("builds via plugin");
        assert_eq!(source.schema().fields().len(), 0);
    }

    #[tokio::test]
    async fn build_sink_falls_back_to_a_registered_plugin_builder() {
        use arrow_schema::Schema;
        use std::sync::Arc;

        let node = NodeSpec {
            name: None,
            connector: "test-plugin-connector".to_string(),
            config: serde_json::json!({"value": "x"}),
        };
        let schema = Arc::new(Schema::empty());
        build_sink(&node, 0, &schema)
            .await
            .expect("builds via plugin");
    }

    #[test]
    fn unknown_connector_still_gets_the_old_error_when_no_plugin_matches() {
        let node = NodeSpec {
            name: None,
            connector: "totally-made-up".to_string(),
            config: serde_json::json!({}),
        };
        let err = validate_source_config(&node).unwrap_err();
        assert!(err.to_string().contains("unsupported source connector"));
    }
}
