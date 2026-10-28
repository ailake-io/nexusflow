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
    match node.connector.as_str() {
        "postgres" => {
            let _: PostgresConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        "sqlite" => {
            let _: SqliteConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let _: MongoConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let _: MySqlConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let _: KafkaConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "mqtt")]
        "mqtt" => {
            let _: MqttConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "rest")]
        "rest" => {
            let _: RestConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "odbc")]
        "odbc" => {
            let _: OdbcConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "deltalake")]
        "deltalake" => {
            let _: DeltaConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => {
            let _: IcebergConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "parquet")]
        "parquet" => {
            let _: ParquetConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "ailake")]
        "ailake" => {
            let _: AilakeConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "csv")]
        "csv" => {
            let _: CsvConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            let _: ClickHouseConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            let _: DuckdbConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let cfg: RedisConnectorConfig = serde_json::from_value(node.config.clone())?;
            cfg.validate()?;
        }
        #[cfg(feature = "nats")]
        "nats" => {
            let cfg: NatsConnectorConfig = serde_json::from_value(node.config.clone())?;
            cfg.validate()?;
        }
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let cfg: RabbitmqConnectorConfig = serde_json::from_value(node.config.clone())?;
            cfg.validate()?;
        }
        #[cfg(feature = "postgres-cdc")]
        "postgres-cdc" => {
            let _: PostgresCdcConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "mongodb")]
        "mongodb-cdc" => {
            let _: MongoCdcConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "mysql-cdc")]
        "mysql-cdc" => {
            let _: MySqlCdcConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "deltalake-cdc")]
        "deltalake-cdc" => {
            let _: DeltaCdcConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "iceberg-cdc")]
        "iceberg-cdc" => {
            let _: IcebergCdcConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "ailake-cdc")]
        "ailake-cdc" => {
            let _: AilakeCdcConfig = serde_json::from_value(node.config.clone())?;
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
    let name = node.resolved_name(index, "source")?;
    let source: Box<dyn Source> = match node.connector.as_str() {
        "postgres" => {
            let cfg: PostgresConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(PostgresSource::connect(&cfg, None).await?)
        }
        "sqlite" => {
            let cfg: SqliteConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(SqliteSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let cfg: MongoConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MongoSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let cfg: MySqlConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MySqlSource::connect(&cfg).await?)
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let cfg: KafkaConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(KafkaSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mqtt")]
        "mqtt" => {
            let cfg: MqttConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MqttSource::connect(&cfg).await?)
        }
        #[cfg(feature = "rest")]
        "rest" => {
            let cfg: RestConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(RestSource::connect(&cfg).await?)
        }
        #[cfg(feature = "odbc")]
        "odbc" => {
            let cfg: OdbcConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(OdbcSource::connect(&cfg)?)
        }
        #[cfg(feature = "deltalake")]
        "deltalake" => {
            let cfg: DeltaConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(DeltaSource::connect(&cfg).await?)
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => {
            let cfg: IcebergConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(IcebergSource::connect(&cfg).await?)
        }
        #[cfg(feature = "parquet")]
        "parquet" => {
            let cfg: ParquetConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(ParquetSource::connect(&cfg).await?)
        }
        #[cfg(feature = "ailake")]
        "ailake" => {
            let cfg: AilakeConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(AilakeSource::connect(&cfg).await?)
        }
        #[cfg(feature = "csv")]
        "csv" => {
            let cfg: CsvConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(CsvSource::connect(&cfg).await?)
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            let cfg: ClickHouseConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(ClickHouseSource::connect(&cfg, None).await?)
        }
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            let cfg: DuckdbConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(DuckdbSource::connect(&cfg).await?)
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let cfg: RedisConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(RedisSource::connect(&cfg).await?)
        }
        #[cfg(feature = "nats")]
        "nats" => {
            let cfg: NatsConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(NatsSource::connect(&cfg).await?)
        }
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let cfg: RabbitmqConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(RabbitmqSource::connect(&cfg).await?)
        }
        #[cfg(feature = "postgres-cdc")]
        "postgres-cdc" => {
            let cfg: PostgresCdcConfig = serde_json::from_value(node.config.clone())?;
            Box::new(PostgresCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mongodb")]
        "mongodb-cdc" => {
            let cfg: MongoCdcConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MongoCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "mysql-cdc")]
        "mysql-cdc" => {
            let cfg: MySqlCdcConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MySqlCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "deltalake-cdc")]
        "deltalake-cdc" => {
            let cfg: DeltaCdcConfig = serde_json::from_value(node.config.clone())?;
            Box::new(DeltaCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "iceberg-cdc")]
        "iceberg-cdc" => {
            let cfg: IcebergCdcConfig = serde_json::from_value(node.config.clone())?;
            Box::new(IcebergCdcSource::connect(&cfg).await?)
        }
        #[cfg(feature = "ailake-cdc")]
        "ailake-cdc" => {
            let cfg: AilakeCdcConfig = serde_json::from_value(node.config.clone())?;
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
    match node.connector.as_str() {
        "postgres" => {
            let _: PostgresConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        "sqlite" => {
            let _: SqliteConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let _: MongoConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let _: MySqlConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "odbc")]
        "odbc" => {
            let _: OdbcConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "milvus")]
        "milvus" => {
            let _: MilvusConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "qdrant")]
        "qdrant" => {
            let _: QdrantConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "lancedb")]
        "lancedb" => {
            let _: LanceDbConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "pgvector")]
        "pgvector" => {
            let _: PgVectorConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "pinecone")]
        "pinecone" => {
            let _: PineconeConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "chromadb")]
        "chromadb" => {
            let _: ChromaConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "rest")]
        "webhook" => {
            let _: WebhookSinkConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "deltalake")]
        "deltalake" => {
            let _: DeltaConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => {
            let _: IcebergConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "parquet")]
        "parquet" => {
            let _: ParquetConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "ailake")]
        "ailake" => {
            let _: AilakeConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "csv")]
        "csv" => {
            let _: CsvConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            let _: ClickHouseConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            let _: DuckdbConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let _: KafkaConnectorConfig = serde_json::from_value(node.config.clone())?;
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let cfg: RedisConnectorConfig = serde_json::from_value(node.config.clone())?;
            cfg.validate()?;
        }
        #[cfg(feature = "nats")]
        "nats" => {
            let cfg: NatsConnectorConfig = serde_json::from_value(node.config.clone())?;
            cfg.validate()?;
        }
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let cfg: RabbitmqConnectorConfig = serde_json::from_value(node.config.clone())?;
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
    let name = node.resolved_name(index, "sink")?;
    // Most connectors below only ever needed the column *names* — kept as
    // `columns` so their call sites don't change. postgres/sqlite now take
    // `schema` directly (types included) to drive `CREATE TABLE IF NOT
    // EXISTS`; see their own `connect()` doc comments for why.
    #[cfg(any(feature = "pgvector", feature = "clickhouse"))]
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let sink: Box<dyn Sink> = match node.connector.as_str() {
        "postgres" => {
            let cfg: PostgresConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(PostgresSink::connect(&cfg, schema).await?)
        }
        "sqlite" => {
            let cfg: SqliteConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(SqliteSink::connect(&cfg, schema).await?)
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let cfg: MongoConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MongoSink::connect(&cfg).await?)
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let cfg: MySqlConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MySqlSink::connect(&cfg, schema).await?)
        }
        #[cfg(feature = "odbc")]
        "odbc" => {
            let cfg: OdbcConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(OdbcSink::connect(&cfg)?)
        }
        #[cfg(feature = "milvus")]
        "milvus" => {
            let cfg: MilvusConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MilvusSink::connect(&cfg).await?)
        }
        #[cfg(feature = "qdrant")]
        "qdrant" => {
            let cfg: QdrantConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(QdrantSink::connect(&cfg)?)
        }
        #[cfg(feature = "lancedb")]
        "lancedb" => {
            let cfg: LanceDbConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(LanceDbSink::connect(&cfg).await?)
        }
        #[cfg(feature = "pgvector")]
        "pgvector" => {
            let cfg: PgVectorConnectorConfig = serde_json::from_value(node.config.clone())?;
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
            let cfg: PineconeConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(PineconeSink::connect(&cfg)?)
        }
        #[cfg(feature = "chromadb")]
        "chromadb" => {
            let cfg: ChromaConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(ChromaSink::connect(&cfg).await?)
        }
        #[cfg(feature = "rest")]
        "webhook" => {
            let cfg: WebhookSinkConfig = serde_json::from_value(node.config.clone())?;
            Box::new(WebhookSink::connect(&cfg)?)
        }
        #[cfg(feature = "deltalake")]
        "deltalake" => {
            let cfg: DeltaConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(DeltaSink::connect(&cfg)?)
        }
        #[cfg(feature = "iceberg")]
        "iceberg" => {
            let cfg: IcebergConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(IcebergSink::connect(&cfg)?)
        }
        #[cfg(feature = "parquet")]
        "parquet" => {
            let cfg: ParquetConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(ParquetSink::connect(&cfg)?)
        }
        #[cfg(feature = "ailake")]
        "ailake" => {
            let cfg: AilakeConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(AilakeSink::connect(&cfg)?)
        }
        #[cfg(feature = "csv")]
        "csv" => {
            let cfg: CsvConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(CsvSink::connect(&cfg)?)
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            let cfg: ClickHouseConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(ClickHouseSink::connect(&cfg, &columns).await?)
        }
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            let cfg: DuckdbConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(DuckdbSink::connect(&cfg, schema).await?)
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let cfg: KafkaConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(KafkaSink::connect(&cfg)?)
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let cfg: RedisConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(RedisSink::connect(&cfg).await?)
        }
        #[cfg(feature = "nats")]
        "nats" => {
            let cfg: NatsConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(NatsSink::connect(&cfg).await?)
        }
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let cfg: RabbitmqConnectorConfig = serde_json::from_value(node.config.clone())?;
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
