use nexus_connector_postgres::{PostgresConnectorConfig, PostgresSink, PostgresSource};
use nexus_connector_sqlite::{SqliteConnectorConfig, SqliteSink, SqliteSource};
use nexus_core::{NodeSpec, Sink, Source};

/// The only place that knows which connector names exist and how to build
/// them — `nexus-core`/`PipelineEngine` never hardcode a connector list, see
/// `ConnectorRegistry` (ARCHITECTURE.md §3). Adding a third connector means
/// adding a match arm here, nowhere else in nexus-server.
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
        other => anyhow::bail!("unsupported source connector: {other:?}"),
    };
    Ok((name, source))
}

pub fn build_sink(
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
        other => anyhow::bail!("unsupported sink connector: {other:?}"),
    };
    Ok((name, sink))
}
