use nexus_connector_postgres::{PostgresConnectorConfig, PostgresSink, PostgresSource};
use nexus_connector_sqlite::{SqliteConnectorConfig, SqliteSink, SqliteSource};
use nexus_core::{NodeSpec, Sink, Source};

#[cfg(feature = "ailake")]
use nexus_connector_ailake::{AilakeConnectorConfig, AilakeSink, AilakeSource};
#[cfg(feature = "chromadb")]
use nexus_connector_chromadb::{ChromaConnectorConfig, ChromaSink};
#[cfg(feature = "deltalake")]
use nexus_connector_deltalake::{DeltaConnectorConfig, DeltaSink, DeltaSource};
#[cfg(feature = "iceberg")]
use nexus_connector_iceberg::{IcebergConnectorConfig, IcebergSink, IcebergSource};
#[cfg(feature = "kafka")]
use nexus_connector_kafka::{KafkaConnectorConfig, KafkaSource};
#[cfg(feature = "lancedb")]
use nexus_connector_lancedb::{LanceDbConnectorConfig, LanceDbSink};
#[cfg(feature = "milvus")]
use nexus_connector_milvus::{MilvusConnectorConfig, MilvusSink};
#[cfg(feature = "mongodb")]
use nexus_connector_mongodb::{MongoConnectorConfig, MongoSink, MongoSource};
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
#[cfg(feature = "rest")]
use nexus_connector_rest::{RestConnectorConfig, RestSource};

/// The only place that knows which connector names exist and how to build
/// them — `nexus-core`/`PipelineEngine` never hardcode a connector list, see
/// `ConnectorRegistry` (ARCHITECTURE.md §3). Adding a connector means adding
/// a match arm here (behind that connector's own Cargo feature — see
/// nexus-server/Cargo.toml), nowhere else in nexus-server.
///
/// The six vector-DB sinks (milvus/qdrant/lancedb/pgvector/pinecone/
/// chromadb) and kafka/rest have no `Sink`/`Source` counterpart at all —
/// they're AI Lakehouse destinations or read-only bridging sources by
/// design (see each crate's own src/lib.rs doc comment), not an oversight
/// here.
pub async fn build_source(
    node: &NodeSpec,
    index: usize,
) -> anyhow::Result<(String, Box<dyn Source>)> {
    let name = node.resolved_name(index, "source");
    let source: Box<dyn Source> = match node.connector.as_str() {
        "postgres" => {
            let cfg: PostgresConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(PostgresSource::connect(&cfg, None)?)
        }
        "sqlite" => {
            let cfg: SqliteConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(SqliteSource::connect(&cfg)?)
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let cfg: MongoConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MongoSource::connect(&cfg).await?)
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let cfg: KafkaConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(KafkaSource::connect(&cfg)?)
        }
        #[cfg(feature = "rest")]
        "rest" => {
            let cfg: RestConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(RestSource::connect(&cfg)?)
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
        other => anyhow::bail!("unsupported source connector: {other:?}"),
    };
    Ok((name, source))
}

pub async fn build_sink(
    node: &NodeSpec,
    index: usize,
    columns: &[String],
) -> anyhow::Result<(String, Box<dyn Sink>)> {
    let name = node.resolved_name(index, "sink");
    let sink: Box<dyn Sink> = match node.connector.as_str() {
        "postgres" => {
            let cfg: PostgresConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(PostgresSink::connect(&cfg, columns)?)
        }
        "sqlite" => {
            let cfg: SqliteConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(SqliteSink::connect(&cfg, columns)?)
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            let cfg: MongoConnectorConfig = serde_json::from_value(node.config.clone())?;
            Box::new(MongoSink::connect(&cfg).await?)
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
            Box::new(PgVectorSink::connect(&cfg, columns).await?)
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
        other => anyhow::bail!("unsupported sink connector: {other:?}"),
    };
    Ok((name, sink))
}
