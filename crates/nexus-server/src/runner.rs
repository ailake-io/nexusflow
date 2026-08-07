use crate::checkpoint_store::CheckpointStore;
use crate::connectors::{build_sink, build_source};
use nexus_connector_postgres::{
    primary_key_bounds, split_into_partitions, table_schema, PostgresConnectorConfig, PostgresSink,
    PostgresSource,
};
use nexus_core::{
    CheckpointCursor, DataFusionTransform, PartitionHandle, PartitionStats, PipelineEngine,
    PipelineSpec, ProgressSender, Transform,
};

#[cfg(any(feature = "embeddings", feature = "embeddings-api"))]
use arrow_array::RecordBatch as ArrowRecordBatch;
#[cfg(any(feature = "embeddings", feature = "embeddings-api"))]
use arrow_schema::SchemaRef as ArrowSchemaRef;

#[tracing::instrument(skip_all, fields(pipeline_id = %spec.pipeline_id))]
pub async fn run_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
) -> anyhow::Result<Vec<PartitionStats>> {
    if spec.has_transform() {
        run_transform_pipeline(spec, checkpoints, progress).await
    } else {
        run_linear_pipeline(spec, checkpoints, progress).await
    }
}

/// Marco 1's path: exactly 1 source, 1 sink, partitioned by PK range,
/// resumable per partition. Postgres-only for now — see IMPLEMENTATION_PLAN.md
/// Marco 1.
#[tracing::instrument(skip_all, fields(pipeline_id = %spec.pipeline_id))]
async fn run_linear_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
) -> anyhow::Result<Vec<PartitionStats>> {
    if spec.embedding.is_some() {
        anyhow::bail!(
            "embedding stage is not supported on the no-transform (postgres→postgres) path; \
             add a transform node to use embeddings"
        );
    }

    let source_node = &spec.sources[0];
    let sink_node = &spec.sinks[0];

    if source_node.connector != "postgres" || sink_node.connector != "postgres" {
        anyhow::bail!(
            "unsupported connector: the partitioned (no-transform) path only supports \
             'postgres' for now (got source={:?}, sink={:?})",
            source_node.connector,
            sink_node.connector
        );
    }

    let source_cfg: PostgresConnectorConfig = serde_json::from_value(source_node.config.clone())?;
    let sink_cfg: PostgresConnectorConfig = serde_json::from_value(sink_node.config.clone())?;

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
        let source = PostgresSource::connect(&source_cfg, Some(range))?;
        let sink = PostgresSink::connect(&sink_cfg, &columns)?;
        handles.push(PartitionHandle {
            partition_id,
            source: Box::new(source),
            sink: Box::new(sink),
        });
    }

    let engine = PipelineEngine::new(spec.channel_capacity);
    let results = engine.run(handles, progress).await;

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

/// Marco 2's path: N sources (fan-in) -> 1 SQL transform -> M sinks
/// (fan-out), connector-agnostic (dispatches through `connectors.rs`).
/// Unpartitioned — every source is read in full, see ARCHITECTURE.md §6.
/// Sinks are only built after the transform runs, since their column list
/// comes from the transform's *output* schema, not any single source's.
#[tracing::instrument(skip_all, fields(pipeline_id = %spec.pipeline_id))]
async fn run_transform_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
) -> anyhow::Result<Vec<PartitionStats>> {
    let transform_spec = spec
        .transform
        .as_ref()
        .expect("run_transform_pipeline called on a spec without a transform");

    let done = checkpoints.done_partitions(&spec.pipeline_id).await?;

    let mut sources = Vec::with_capacity(spec.sources.len());
    for (i, node) in spec.sources.iter().enumerate() {
        sources.push(build_source(node, i).await?);
    }

    let inputs = PipelineEngine::drain_sources(sources).await?;

    #[cfg(any(feature = "embeddings", feature = "embeddings-api"))]
    let inputs = apply_embedding_stage(inputs, spec.embedding.as_ref()).await?;
    #[cfg(not(any(feature = "embeddings", feature = "embeddings-api")))]
    if spec.embedding.is_some() {
        anyhow::bail!(
            "pipeline contains an embedding node but the server was built without \
             the 'embeddings' or 'embeddings-api' feature"
        );
    }

    let transform = DataFusionTransform::new(&transform_spec.sql);
    let output = transform.apply(inputs).await?;

    let columns: Vec<String> = output
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default();

    let mut sinks = Vec::with_capacity(spec.sinks.len());
    for (i, node) in spec.sinks.iter().enumerate() {
        let (name, sink) = build_sink(node, i, &columns).await?;
        if done.contains(&name) {
            continue; // already committed in a prior run of this pipeline_id
        }
        sinks.push((name, sink));
    }

    let engine = PipelineEngine::new(spec.channel_capacity);
    let results = engine.fan_out_write(&output, sinks, progress).await;

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
            "{} of {} sink(s) failed: {errors:?}",
            errors.len(),
            errors.len() + stats.len()
        );
    }

    Ok(stats)
}

#[cfg(any(feature = "embeddings", feature = "embeddings-api"))]
async fn apply_embedding_stage(
    inputs: Vec<(String, ArrowSchemaRef, Vec<ArrowRecordBatch>)>,
    embedding_spec: Option<&nexus_core::EmbeddingSpec>,
) -> anyhow::Result<Vec<(String, ArrowSchemaRef, Vec<ArrowRecordBatch>)>> {
    let Some(spec) = embedding_spec else {
        return Ok(inputs);
    };
    let mut out = Vec::with_capacity(inputs.len());
    for (name, schema, batches) in inputs {
        let mut embedded = Vec::with_capacity(batches.len());
        for batch in &batches {
            embedded.push(nexus_ai::embedding::apply_embedding(batch, spec).await?);
        }
        out.push((name, schema, embedded));
    }
    Ok(out)
}
