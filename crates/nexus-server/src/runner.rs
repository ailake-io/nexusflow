use crate::checkpoint_store::CheckpointStore;
use nexus_connector_postgres::{
    primary_key_bounds, split_into_partitions, table_schema, PostgresConnectorConfig, PostgresSink,
    PostgresSource,
};
use nexus_core::{CheckpointCursor, PartitionHandle, PartitionStats, PipelineEngine, PipelineSpec};

/// Builds partitions from the spec, skips any partition already checkpointed
/// for this `pipeline_id` (resume behavior — ARCHITECTURE.md §5), runs the
/// rest, and persists a checkpoint for each partition that succeeds. Only the
/// `postgres` connector is wired up in Marco 1.
pub async fn run_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
) -> anyhow::Result<Vec<PartitionStats>> {
    if spec.source.connector != "postgres" || spec.sink.connector != "postgres" {
        anyhow::bail!(
            "unsupported connector: only 'postgres' is wired up in Marco 1 (got source={:?}, sink={:?})",
            spec.source.connector,
            spec.sink.connector
        );
    }

    let source_cfg: PostgresConnectorConfig = serde_json::from_value(spec.source.config.clone())?;
    let sink_cfg: PostgresConnectorConfig = serde_json::from_value(spec.sink.config.clone())?;

    let schema = table_schema(&source_cfg).await?;
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let Some((min, max)) = primary_key_bounds(&source_cfg).await? else {
        return Ok(Vec::new());
    };

    let ranges = split_into_partitions(min, max, spec.partitions);
    let done = checkpoints.done_partitions(&spec.pipeline_id).await?;

    let mut handles = Vec::new();
    for (i, range) in ranges.into_iter().enumerate() {
        let partition_id = format!("p{i}");
        if done.contains(&partition_id) {
            continue;
        }
        let source = PostgresSource::connect(&source_cfg, range)?;
        let sink = PostgresSink::connect(&sink_cfg, &columns)?;
        handles.push(PartitionHandle {
            partition_id,
            source: Box::new(source),
            sink: Box::new(sink),
        });
    }

    let engine = PipelineEngine::new(spec.channel_capacity);
    let results = engine.run(handles).await;

    let mut stats = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(stat) => {
                checkpoints
                    .commit(
                        &spec.pipeline_id,
                        &CheckpointCursor::new(stat.partition_id.clone()),
                    )
                    .await?;
                stats.push(stat);
            }
            Err(e) => errors.push(e),
        }
    }

    if !errors.is_empty() {
        anyhow::bail!(
            "{} of {} partition(s) failed: {errors:?}",
            errors.len(),
            errors.len() + stats.len()
        );
    }

    Ok(stats)
}
