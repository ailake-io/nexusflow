use crate::checkpoint_store::CheckpointStore;
use crate::connectors::{build_sink, build_source};
use crate::progress::RunLogger;
use crate::python_transform;
use nexus_connector_postgres::{
    primary_key_bounds, split_into_partitions, table_schema, PkPartitionKind,
    PostgresConnectorConfig, PostgresSink, PostgresSource,
};
use nexus_core::{
    CheckpointCursor, DataFusionTransform, NodeSpec, PartitionHandle, PartitionStats,
    PipelineEngine, PipelineSpec, ProgressEvent, ProgressSender, Transform,
};

#[cfg(any(feature = "embeddings", feature = "embeddings-api"))]
use arrow_array::RecordBatch as ArrowRecordBatch;
#[cfg(any(feature = "embeddings", feature = "embeddings-api"))]
use arrow_schema::SchemaRef as ArrowSchemaRef;

/// Narrates a fallible step to the run's execution log (`RunLogger`, see
/// `progress.rs`) before returning the same `Result` unchanged — lets call
/// sites keep their existing `?`-based error handling while still getting a
/// log line on failure. A no-op pass-through when `log` is `None` (tests
/// that don't care about logging, same convention as `progress:
/// Option<ProgressSender>`).
///
/// Runs the same `error::sanitize_error` redaction `record_run_failure`
/// (lib.rs) applies to the final run error — a connect failure's message
/// routinely embeds the connection URI (`postgres://user:pass@host/db`),
/// and unlike that final summary this line is persisted to `RunLogStore`
/// and broadcast live, so it needs the same credential scrubbing, not a
/// weaker bar just because it's a narration line instead of the terminal
/// error.
async fn log_on_err<T, E: std::fmt::Display>(
    log: Option<&RunLogger>,
    context: &str,
    result: Result<T, E>,
) -> Result<T, E> {
    if let Err(e) = &result {
        if let Some(logger) = log {
            logger
                .error(format!(
                    "{context}: {}",
                    crate::error::sanitize_error(&e.to_string())
                ))
                .await;
        }
    }
    result
}

async fn log_info(log: Option<&RunLogger>, message: impl Into<String>) {
    if let Some(logger) = log {
        logger.info(message).await;
    }
}

async fn log_error(log: Option<&RunLogger>, message: impl Into<String>) {
    if let Some(logger) = log {
        logger.error(message).await;
    }
}

/// Wraps a `ProgressSender` so the engine's progress events are still
/// forwarded to live WebSocket subscribers while also driving user-facing
/// percentage logs. `total_units` is the number of partitions/sinks that must
/// report `done = true` to reach 100%.
fn log_progress(
    log: Option<&RunLogger>,
    progress: Option<ProgressSender>,
    total_units: usize,
    unit_name: &'static str,
) -> (Option<ProgressSender>, tokio::task::JoinHandle<()>) {
    let Some(logger) = log else {
        // No logger: forward progress directly without spawning a task.
        return (progress, tokio::spawn(async {}));
    };
    let logger = logger.clone();
    let (tx, mut rx) = tokio::sync::broadcast::channel::<ProgressEvent>(1024);

    let handle = tokio::spawn(async move {
        let mut done = 0usize;
        let mut last_milestone = 0usize;
        while let Ok(event) = rx.recv().await {
            let is_done = event.done;
            if let Some(p) = &progress {
                let _ = p.send(event);
            }
            if is_done {
                done += 1;
                let percent = (done * 100) / total_units.max(1);
                let milestone = (percent / 10) * 10;
                if milestone > last_milestone || percent == 100 {
                    logger
                        .info(format!(
                            "progress: {percent}% ({done}/{total} {unit_name} completed)",
                            total = total_units
                        ))
                        .await;
                    last_milestone = milestone;
                }
            }
        }
    });

    (Some(tx), handle)
}

#[tracing::instrument(skip_all, fields(pipeline_id = %spec.pipeline_id))]
pub async fn run_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
) -> anyhow::Result<Vec<PartitionStats>> {
    if spec.has_transform() || spec.python.is_some() {
        run_transform_pipeline(spec, checkpoints, progress, log).await
    } else {
        run_linear_pipeline(spec, checkpoints, progress, log).await
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
    log: Option<&RunLogger>,
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

    let done = checkpoints.done_partitions(&spec.pipeline_id).await?;
    let mut handles = Vec::new();

    match primary_key_bounds(&source_cfg).await? {
        None => return Ok(Vec::new()),
        Some(PkPartitionKind::NonNumeric) => {
            if !done.contains("p0") {
                let source = log_on_err(
                    log,
                    "p0 source connect failed",
                    PostgresSource::connect(&source_cfg, None).await,
                )
                .await?;
                let sink = log_on_err(
                    log,
                    "p0 sink connect failed",
                    PostgresSink::connect(&sink_cfg, &columns).await,
                )
                .await?;
                handles.push(PartitionHandle {
                    partition_id: "p0".to_string(),
                    source: Box::new(source),
                    sink: Box::new(sink),
                });
            }
        }
        Some(PkPartitionKind::Int64(min, max)) => {
            let ranges = split_into_partitions(min, max, spec.partitions);
            for (i, range) in ranges.into_iter().enumerate() {
                let partition_id = format!("p{i}");
                if done.contains(&partition_id) {
                    continue;
                }
                let source = log_on_err(
                    log,
                    &format!("{partition_id} source connect failed"),
                    PostgresSource::connect(&source_cfg, Some(range)).await,
                )
                .await?;
                let sink = log_on_err(
                    log,
                    &format!("{partition_id} sink connect failed"),
                    PostgresSink::connect(&sink_cfg, &columns).await,
                )
                .await?;
                handles.push(PartitionHandle {
                    partition_id,
                    source: Box::new(source),
                    sink: Box::new(sink),
                });
            }
        }
    }

    log_info(log, format!("{} partition(s) to process", handles.len())).await;

    let total_partitions = handles.len();
    let engine = PipelineEngine::new(spec.channel_capacity);
    let (progress, progress_handle) = log_progress(log, progress, total_partitions, "partitions");
    let results = engine.run(handles, progress).await;
    let _ = progress_handle.await;

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
            Err(e) => {
                log_error(
                    log,
                    format!(
                        "partition failed: {}",
                        crate::error::sanitize_error(&e.to_string())
                    ),
                )
                .await;
                errors.push(e);
            }
        }
    }

    if !errors.is_empty() {
        anyhow::bail!(
            "{} of {} partition(s) failed",
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
///
/// Also the entry point for a python-only pipeline (`spec.python` set,
/// `spec.transform` absent) — see `run_pipeline`'s dispatch condition and
/// `dag.rs::validate()` for why that still requires exactly 1 source: with
/// no SQL stage to fan multiple sources into one table, `python` always
/// operates on a single upstream table's batches. When both are set, the
/// order is SQL transform, then python, over its output.
#[tracing::instrument(skip_all, fields(pipeline_id = %spec.pipeline_id))]
async fn run_transform_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
) -> anyhow::Result<Vec<PartitionStats>> {
    let done = checkpoints.done_partitions(&spec.pipeline_id).await?;

    let mut sources = Vec::with_capacity(spec.sources.len());
    for (i, node) in spec.sources.iter().enumerate() {
        let source = log_on_err(
            log,
            &format!("source {i} ({}) connect failed", node.connector),
            build_source(node, i).await,
        )
        .await?;
        sources.push(source);
    }
    log_info(log, format!("{} source(s) connected", sources.len())).await;

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

    let output = if let Some(transform_spec) = &spec.transform {
        let transform = DataFusionTransform::new(&transform_spec.sql);
        transform.apply(inputs).await?
    } else {
        // No SQL transform — a python-only pipeline, validated as exactly
        // 1 source (dag.rs::validate()), so there's exactly one entry to
        // unwrap here.
        inputs
            .into_iter()
            .next()
            .map(|(_, _, batches)| batches)
            .unwrap_or_default()
    };

    let output = if let Some(python_spec) = &spec.python {
        match output.first().map(|b| b.schema()) {
            Some(schema) => {
                log_info(log, "running python transform").await;
                let result = log_on_err(
                    log,
                    "python transform failed",
                    python_transform::apply(schema, output, python_spec).await,
                )
                .await?;
                log_info(log, "python transform finished").await;
                result
            }
            None => output, // nothing to transform
        }
    } else {
        output
    };

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
        let (name, sink) = log_on_err(
            log,
            &format!("sink {i} ({}) connect failed", node.connector),
            build_sink(node, i, &columns).await,
        )
        .await?;
        if done.contains(&name) {
            continue; // already committed in a prior run of this pipeline_id
        }
        sinks.push((name, sink));
    }
    log_info(log, format!("{} sink(s) connected", sinks.len())).await;

    let total_sinks = sinks.len();
    let engine = PipelineEngine::new(spec.channel_capacity);
    let (progress, progress_handle) = log_progress(log, progress, total_sinks, "sinks");
    let results = engine.fan_out_write(&output, sinks, progress).await;
    let _ = progress_handle.await;

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
            Err(e) => {
                log_error(
                    log,
                    format!(
                        "sink failed: {}",
                        crate::error::sanitize_error(&e.to_string())
                    ),
                )
                .await;
                errors.push(e);
            }
        }
    }

    if !errors.is_empty() {
        anyhow::bail!(
            "{} of {} sink(s) failed",
            errors.len(),
            errors.len() + stats.len()
        );
    }

    Ok(stats)
}

/// True ETL extension of the ELT dbt step (Marco 10 follow-up): once
/// `dbt::run` succeeds, reads its transformed result back out of
/// `dbt.output` (the same warehouse `run_pipeline`'s own sinks just loaded)
/// and fans it out to `spec.post_dbt_sinks`. Reuses the same
/// drain/build_sink/fan_out_write tail as `run_transform_pipeline` — the
/// only difference is a single already-built source instead of N.
///
/// Checkpoint names are prefixed with `post_dbt_` because `build_sink`
/// resolves unnamed nodes to `sink0`, `sink1`, ... — the same names
/// `spec.sinks` resolves to. Without the prefix, a fully-resumed run's
/// checkpoint lookup for this stage would collide with (and appear
/// satisfied by) the main load stage's checkpoints.
#[tracing::instrument(skip_all, fields(pipeline_id = %spec.pipeline_id))]
pub async fn run_post_dbt_stage(
    spec: &PipelineSpec,
    output_node: &NodeSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
) -> anyhow::Result<Vec<PartitionStats>> {
    let done = checkpoints.done_partitions(&spec.pipeline_id).await?;

    let source = log_on_err(
        log,
        "post-dbt source connect failed",
        build_source(output_node, 0).await,
    )
    .await?;
    let inputs = PipelineEngine::drain_sources(vec![source]).await?;
    let batches: Vec<_> = inputs.into_iter().flat_map(|(_, _, b)| b).collect();

    let columns: Vec<String> = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default();

    let mut sinks = Vec::with_capacity(spec.post_dbt_sinks.len());
    for (i, node) in spec.post_dbt_sinks.iter().enumerate() {
        let (raw_name, sink) = log_on_err(
            log,
            &format!("post-dbt sink {i} ({}) connect failed", node.connector),
            build_sink(node, i, &columns).await,
        )
        .await?;
        let name = format!("post_dbt_{raw_name}");
        if done.contains(&name) {
            continue; // already committed in a prior run of this pipeline_id
        }
        sinks.push((name, sink));
    }
    log_info(log, format!("{} post-dbt sink(s) connected", sinks.len())).await;

    let total_sinks = sinks.len();
    let engine = PipelineEngine::new(spec.channel_capacity);
    let (progress, progress_handle) = log_progress(log, progress, total_sinks, "post-dbt sinks");
    let results = engine.fan_out_write(&batches, sinks, progress).await;
    let _ = progress_handle.await;

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
            Err(e) => {
                log_error(
                    log,
                    format!(
                        "post-dbt sink failed: {}",
                        crate::error::sanitize_error(&e.to_string())
                    ),
                )
                .await;
                errors.push(e);
            }
        }
    }

    if !errors.is_empty() {
        anyhow::bail!(
            "{} of {} post-dbt sink(s) failed",
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

    // Load the embedding backend once per run, not once per batch — ONNX
    // model loading and tokenizer initialization are expensive and must not
    // repeat for every RecordBatch (PROJECT_REVIEW.md C13).
    let backend = nexus_ai::embedding::load_embedding_backend(spec).await?;

    let mut out = Vec::with_capacity(inputs.len());
    for (name, schema, batches) in inputs {
        let mut embedded = Vec::with_capacity(batches.len());
        for batch in &batches {
            embedded.push(nexus_ai::embedding::apply_embedding(batch, spec, &backend).await?);
        }
        out.push((name, schema, embedded));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::{LogLevel, RunLogger};
    use crate::run_log_store::RunLogStore;

    /// A connect failure's `Display` routinely embeds the connection URI
    /// with credentials (e.g. ADBC/tokio-postgres error messages) — this
    /// line is persisted (`RunLogStore`) and broadcast live, so it must go
    /// through the same `error::sanitize_error` redaction as the final run
    /// error `record_run_failure` (lib.rs) already applies, not a weaker
    /// bar just because it's a narration line.
    #[tokio::test]
    async fn log_on_err_redacts_credentials_from_the_error_message() {
        let store = RunLogStore::connect("sqlite::memory:").await.unwrap();
        let (tx, _rx) = tokio::sync::broadcast::channel(8);
        let logger = RunLogger::new(1, tx, store.clone());

        let err: Result<(), String> = Err(
            "connect failed: postgres://admin:s3cret@db.internal:5432/app: timeout".to_string(),
        );
        let result = log_on_err(Some(&logger), "source connect failed", err).await;
        assert!(result.is_err());

        let logs = store.list(1).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].level, LogLevel::Error);
        assert!(
            !logs[0].message.contains("s3cret"),
            "credential must never reach the persisted run log: {}",
            logs[0].message
        );
        assert!(logs[0]
            .message
            .contains("postgres://***@db.internal:5432/app"));
    }
}
