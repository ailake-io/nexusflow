use crate::checkpoint_store::CheckpointStore;
use crate::connectors::{build_sink, build_source};
use crate::license::LicenseClaims;
use crate::progress::RunLogger;
use crate::python_transform;
use futures_util::StreamExt;
use nexus_connector_postgres::{
    primary_key_bounds, split_into_partitions, table_schema, PkPartitionKind,
    PostgresConnectorConfig, PostgresSink, PostgresSource,
};
use nexus_core::{
    CheckpointCursor, DataFusionTransform, NodeSpec, PartitionHandle, PartitionStats,
    PipelineEngine, PipelineSpec, ProgressEvent, ProgressSender, Transform, OPCODE_COLUMN,
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
    active_license: Option<&LicenseClaims>,
) -> anyhow::Result<Vec<PartitionStats>> {
    // A `*-cdc` source with a plain SQL transform (the only documented CDC
    // shape — `SELECT * FROM source0`, required to preserve `__opcode` for
    // the sink's insert/update/delete routing) gets its own streaming path.
    // `run_transform_pipeline` below fully materializes every source via
    // `PipelineEngine::drain_sources` *before* running the transform — for
    // a CDC source, "materialized" means "the source's `read_batches`
    // stream ended", which only happens at `max_batch_events` (default
    // 1000) or an error. Reproduced directly: a CDC pipeline at the
    // default cutoff stayed `running` forever and never wrote a handful of
    // real changes to its sink. Embedding/python/dbt stages aren't
    // supported on this fast path (falls through to the regular
    // materializing path below, same restriction it already has for those
    // combinations) — none of them are part of the documented CDC-mirror
    // pattern, and streaming them per micro-batch isn't a well-defined
    // upgrade the way a plain projection/filter transform is.
    if spec.sources.len() == 1
        && spec.sources[0].connector.ends_with("-cdc")
        && spec.transform.is_some()
        && spec.embedding.is_none()
        && spec.python.is_none()
        && spec.dbt.is_none()
    {
        run_streaming_cdc_pipeline(spec, checkpoints, progress, log, active_license).await
    } else if spec.has_transform() || spec.python.is_some() {
        run_transform_pipeline(spec, checkpoints, progress, log, active_license).await
    } else {
        // The postgres→postgres branch below never builds through
        // connectors.rs's build_source/build_sink (uses PostgresSource/
        // PostgresSink directly) — postgres isn't an enterprise connector,
        // nothing to check there. The passthrough fallback DOES go
        // through build_source/build_sink (same as the transform path),
        // so it needs active_license threaded through too, or any
        // licensed connector would be usable unlicensed just by omitting
        // a Transform node.
        run_linear_pipeline(spec, checkpoints, progress, log, active_license).await
    }
}

/// Marco 1's path: exactly 1 source, 1 sink, no transform node. Two
/// implementations live behind this one entry point:
/// - postgres→postgres: partitioned by PK range, resumable per partition
///   (the rest of this function) — a real optimization that depends on
///   ADBC + a SQL `WHERE pk >= / <` range predicate + a boundable integer
///   PK, none of which exists for non-SQL/bridging connectors or CDC.
/// - anything else: [`run_passthrough_pipeline`] — connector-agnostic,
///   unpartitioned, just streams batches straight from source to sink.
///   Added so "just move data from A to B, no transformation" doesn't
///   force adding a no-op Transform node merely to dodge this function's
///   old postgres-only restriction — see IMPLEMENTATION_PLAN.md Marco 1.
#[tracing::instrument(skip_all, fields(pipeline_id = %spec.pipeline_id))]
async fn run_linear_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
    active_license: Option<&LicenseClaims>,
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
        return run_passthrough_pipeline(spec, checkpoints, progress, log, active_license).await;
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
                        &CheckpointCursor {
                            resume_state: stat.resume_state.clone(),
                            ..CheckpointCursor::new(stat.partition_id.clone())
                        },
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

/// Merges a previously-committed resume position back into a `*-cdc`
/// source's config before connecting — the only way a CDC source without
/// its own server-side resume mechanism (unlike postgres-cdc's replication
/// slot) can actually continue instead of restarting from scratch. Field
/// names are the specific config keys each `*-cdc` connector already
/// exposes for exactly this ("start from here") purpose; `resume_state`'s
/// format is whatever that connector's own `Source::position_handle`
/// produces. A no-op for a non-CDC source or when there's no prior
/// checkpoint for `partition_id` yet.
async fn inject_cdc_resume_state(
    node: &NodeSpec,
    checkpoints: &CheckpointStore,
    pipeline_id: &str,
    partition_id: &str,
) -> anyhow::Result<NodeSpec> {
    let mut node = node.clone();
    if !node.connector.ends_with("-cdc") {
        return Ok(node);
    }
    if let Some(cursor) = checkpoints.get(pipeline_id, partition_id).await? {
        if let Some(resume_state) = cursor.resume_state {
            match node.connector.as_str() {
                "mysql-cdc" => {
                    if let Some((filename, position)) = resume_state.split_once(':') {
                        if let Ok(position) = position.parse::<u32>() {
                            node.config["binlog_filename"] =
                                serde_json::Value::String(filename.to_string());
                            node.config["binlog_position"] = serde_json::Value::from(position);
                        }
                    }
                }
                "mongodb-cdc" => {
                    node.config["resume_token"] = serde_json::Value::String(resume_state);
                }
                // mssql-cdc's Source::position_handle reports the LSN
                // pre-formatted as a hex string (nexus-connector-mssql's
                // own lsn_hex_literal helper) - passes straight through.
                "mssql-cdc" => {
                    node.config["start_lsn"] = serde_json::Value::String(resume_state);
                }
                // oracle-cdc reports the SCN as a plain decimal string -
                // start_scn is a JSON number, not a string, so this
                // parses it back rather than passing the string through.
                "oracle-cdc" => {
                    if let Ok(scn) = resume_state.parse::<i64>() {
                        node.config["start_scn"] = serde_json::Value::from(scn);
                    }
                }
                // Any other *-cdc connector either manages its own
                // server-side resume (postgres-cdc) or doesn't implement
                // `position_handle` yet (no resume_state would ever be
                // stored for it in the first place).
                _ => {}
            }
        }
    }
    Ok(node)
}

/// Streaming counterpart to `run_transform_pipeline`, for the one
/// documented CDC-mirror shape: exactly 1 `*-cdc` source, a plain SQL
/// transform, N sinks (see `run_pipeline`'s dispatch comment for why —
/// `run_transform_pipeline` fully materializes its source first, which
/// for a CDC source means waiting for its stream to end, and that only
/// happens at `max_batch_events` or an error).
///
/// Applies the transform to each micro-batch as it streams off the source
/// and writes the transformed batch straight to every sink immediately —
/// same reader-then-writer shape `PipelineEngine::run_partition` already
/// uses for the no-transform passthrough path, just with a transform step
/// spliced in and support for more than one sink. Real semantic
/// consequence, not hidden: the transform SQL runs once *per micro-batch*,
/// not once over the whole (unbounded) stream — a plain projection/filter
/// like the documented `SELECT * FROM source0` behaves identically either
/// way, but an aggregate (`SELECT count(*) FROM source0`) would produce a
/// per-micro-batch count, not a running total. No aggregate-over-a-live-
/// CDC-stream pattern is documented anywhere in this codebase; this isn't
/// a regression from a previously-correct behavior, since the old
/// materializing path never actually produced any output for a realistic
/// (sub-`max_batch_events`) CDC pipeline in the first place.
#[tracing::instrument(skip_all, fields(pipeline_id = %spec.pipeline_id))]
async fn run_streaming_cdc_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
    active_license: Option<&LicenseClaims>,
) -> anyhow::Result<Vec<PartitionStats>> {
    // Resume-state lookup is anchored on the first sink's resolved name
    // ("sink0" when unnamed) — every sink commits the same source position
    // at the end of a run, so any of them would do; this just picks one
    // consistently. Every documented/tested CDC-mirror pipeline has
    // exactly 1 sink anyway.
    let anchor_partition = spec
        .sinks
        .first()
        .map(|n| n.resolved_name(0, "sink"))
        .transpose()?
        .unwrap_or_else(|| "sink0".to_string());
    let source_node = inject_cdc_resume_state(
        &spec.sources[0],
        checkpoints,
        &spec.pipeline_id,
        &anchor_partition,
    )
    .await?;

    let (source_name, mut source) = log_on_err(
        log,
        &format!("source 0 ({}) connect failed", source_node.connector),
        build_source(&source_node, 0, active_license).await,
    )
    .await?;
    let source_schema = source.schema();

    let transform_spec = spec
        .transform
        .as_ref()
        .expect("run_pipeline's dispatch guarantees spec.transform is Some here");
    let transform = DataFusionTransform::new(&transform_spec.sql);

    let output_schema = log_on_err(
        log,
        "transform schema resolution failed",
        transform
            .output_schema(vec![(source_name.clone(), source_schema.clone())])
            .await,
    )
    .await?;
    let columns: Vec<String> = output_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|name| name != OPCODE_COLUMN)
        .collect();

    let done = checkpoints.done_partitions(&spec.pipeline_id).await?;
    let mut sinks = Vec::with_capacity(spec.sinks.len());
    for (i, node) in spec.sinks.iter().enumerate() {
        let (name, sink) = log_on_err(
            log,
            &format!("sink {i} ({}) connect failed", node.connector),
            build_sink(node, i, &columns, active_license).await,
        )
        .await?;
        if done.contains(&name) {
            continue; // already committed in a prior run of this pipeline_id
        }
        sinks.push((name, sink));
    }
    log_info(
        log,
        format!("{} sink(s) connected (streaming CDC)", sinks.len()),
    )
    .await;

    if sinks.is_empty() {
        return Ok(Vec::new());
    }

    // Must be fetched before `source` moves into `read_batches` below — see
    // `Source::position_handle`'s doc comment for why a plain `&self` call
    // after the stream starts doesn't work.
    let position_handle = source.position_handle();
    let mut stream = log_on_err(
        log,
        "source 0 read_batches failed",
        source.read_batches().await,
    )
    .await?;

    let mut batches_written = 0usize;
    let mut rows_written = 0usize;
    let mut bytes_written = 0usize;

    while let Some(item) = stream.next().await {
        let batch = log_on_err(log, "source 0 read failed", item).await?;
        let transformed = log_on_err(
            log,
            "transform failed",
            transform
                .apply(vec![(
                    source_name.clone(),
                    source_schema.clone(),
                    vec![batch],
                )])
                .await,
        )
        .await?;
        for out_batch in transformed {
            batches_written += 1;
            rows_written += out_batch.num_rows();
            bytes_written += out_batch.get_array_memory_size();
            for (name, sink) in sinks.iter_mut() {
                log_on_err(
                    log,
                    &format!("sink ({name}) write failed"),
                    sink.write_batch(out_batch.clone()).await,
                )
                .await?;
            }
            if let Some(tx) = &progress {
                for (name, _) in sinks.iter() {
                    let _ = tx.send(ProgressEvent {
                        partition_id: name.clone(),
                        batches_written,
                        rows_written,
                        bytes_written,
                        done: false,
                    });
                }
            }
        }
    }
    drop(stream);

    let resume_state = position_handle
        .as_ref()
        .and_then(|h| h.lock().expect("position_handle mutex poisoned").clone());

    let mut stats = Vec::with_capacity(sinks.len());
    for (name, sink) in sinks.iter_mut() {
        sink.commit_checkpoint(CheckpointCursor {
            resume_state: resume_state.clone(),
            ..CheckpointCursor::new(name.clone())
        })
        .await?;
        if let Some(tx) = &progress {
            let _ = tx.send(ProgressEvent {
                partition_id: name.clone(),
                batches_written,
                rows_written,
                bytes_written,
                done: true,
            });
        }
        stats.push(PartitionStats {
            partition_id: name.clone(),
            batches_written,
            rows_written,
            resume_state: resume_state.clone(),
        });
    }
    Ok(stats)
}

/// Fallback for [`run_linear_pipeline`] when the source/sink pair isn't
/// postgres→postgres: exactly 1 source, 1 sink, no transform, no
/// partitioning — batches stream straight from `Source::read_batches`
/// into `Sink::write_batch` via [`PipelineEngine::run`], the same
/// connector-agnostic I/O driver `run_transform_pipeline` already uses
/// (minus the SQL step in between; `build_source`/`build_sink` dispatch
/// through `connectors.rs` exactly like that path does). Any connector
/// pair works here — csv, mysql, mongodb, or any `*-cdc` source — since
/// nothing here depends on SQL/ADBC or a boundable primary key range the
/// way the postgres-partitioned path above does.
///
/// Single "p0" partition, same resumability contract as the postgres
/// path's `NonNumeric` case: if `p0` already committed in a prior run of
/// this `pipeline_id`, this is a no-op.
#[tracing::instrument(skip_all, fields(pipeline_id = %spec.pipeline_id))]
async fn run_passthrough_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
    active_license: Option<&LicenseClaims>,
) -> anyhow::Result<Vec<PartitionStats>> {
    if spec.embedding.is_some() {
        anyhow::bail!(
            "embedding stage is not supported on the no-transform passthrough path; \
             add a transform node to use embeddings"
        );
    }

    let source_node = &spec.sources[0];
    let sink_node = &spec.sinks[0];

    // CDC sources (`*-cdc`) are meant to run again every scheduler tick,
    // not once-and-done — they use `resume_state` for continuity, not the
    // "already finished" marker every batch connector's single run leaves
    // behind. Applying the batch done-check to them would mean any `-cdc`
    // source routed through this path (everything except postgres-cdc,
    // which stays on the transform/other paths — this passthrough fallback
    // only fires for the no-transform case) would commit once via
    // `PipelineEngine::run_partition`'s post-`max_batch_events` checkpoint
    // and then never run again on any later scheduler tick.
    let is_cdc = source_node.connector.ends_with("-cdc");
    if !is_cdc {
        let done = checkpoints.done_partitions(&spec.pipeline_id).await?;
        if done.contains("p0") {
            return Ok(Vec::new());
        }
    }

    // Merge a previously-committed resume position back into the source's
    // config before connecting — the only way a CDC source without its own
    // server-side resume mechanism (unlike postgres-cdc's replication slot)
    // can actually continue instead of restarting from scratch.
    let source_node =
        inject_cdc_resume_state(source_node, checkpoints, &spec.pipeline_id, "p0").await?;
    let source_node = &source_node;

    let (_source_name, source) = log_on_err(
        log,
        &format!("source 0 ({}) connect failed", source_node.connector),
        build_source(source_node, 0, active_license).await,
    )
    .await?;

    // Same `__opcode` exclusion as `run_transform_pipeline` below — a CDC
    // source's own declared schema includes it (see e.g. postgres-cdc's
    // `build_schema`), and it's never a real destination column.
    let columns: Vec<String> = source
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|name| name != OPCODE_COLUMN)
        .collect();

    let (_name, sink) = log_on_err(
        log,
        &format!("sink 0 ({}) connect failed", sink_node.connector),
        build_sink(sink_node, 0, &columns, active_license).await,
    )
    .await?;

    let handle = PartitionHandle {
        partition_id: "p0".to_string(),
        source,
        sink,
    };

    log_info(
        log,
        "1 partition (passthrough, no transform) to process".to_string(),
    )
    .await;

    let engine = PipelineEngine::new(spec.channel_capacity);
    let (progress, progress_handle) = log_progress(log, progress, 1, "partitions");
    let results = engine.run(vec![handle], progress).await;
    let _ = progress_handle.await;

    let mut stats = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(stat) => {
                checkpoints
                    .commit(
                        &spec.pipeline_id,
                        &CheckpointCursor {
                            resume_state: stat.resume_state.clone(),
                            ..CheckpointCursor::new(stat.partition_id.clone())
                        },
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
    active_license: Option<&LicenseClaims>,
) -> anyhow::Result<Vec<PartitionStats>> {
    // Same reasoning as `run_passthrough_pipeline`'s `is_cdc` check: a `-cdc`
    // source is meant to run again every scheduler tick, using
    // `resume_state` for continuity, not the "already finished" marker a
    // batch connector's single run leaves behind. Real bug found testing
    // postgres-cdc/mysql-cdc/mongodb-cdc end to end this session — every
    // documented CDC-to-relational-sink pipeline goes through *this*
    // function (it requires `SELECT * FROM source0` to preserve `__opcode`,
    // see ARCHITECTURE.md §5/§7), and without this check its sink commits a
    // checkpoint after its first successful run, then gets silently skipped
    // (`done.contains(&name)` below) on every later run forever — the
    // pipeline reports `success` each time but stops mirroring anything
    // after the very first change.
    let is_cdc = spec.sources.iter().any(|s| s.connector.ends_with("-cdc"));
    let done = if is_cdc {
        std::collections::HashSet::new()
    } else {
        checkpoints.done_partitions(&spec.pipeline_id).await?
    };

    let mut sources = Vec::with_capacity(spec.sources.len());
    for (i, node) in spec.sources.iter().enumerate() {
        let source = log_on_err(
            log,
            &format!("source {i} ({}) connect failed", node.connector),
            build_source(node, i, active_license).await,
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

    // `__opcode` (CDC metadata, added by the source, carried through
    // untouched by `SELECT * FROM source0`) is never a real destination
    // column — `Sink::write_batch` strips it from the row data itself via
    // `split_by_opcode`, but the column list handed to `build_sink` also
    // needs it excluded, or a sink that builds its SQL text from this list
    // (e.g. `PostgresSink::connect`'s `build_upsert_sql`) ends up
    // referencing a column that doesn't exist on the real table. Bug found
    // testing postgres-cdc -> postgres end to end this session: every CDC
    // pipeline with `SELECT * FROM source0` (the pattern the docs require,
    // to preserve `__opcode` for the sink's own insert/delete routing)
    // failed every single write with "column \"__opcode\" of relation ...
    // does not exist" — the SQL text and the bound values disagreed on the
    // column count.
    let columns: Vec<String> = output
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .filter(|name| name != OPCODE_COLUMN)
                .collect()
        })
        .unwrap_or_default();

    let mut sinks = Vec::with_capacity(spec.sinks.len());
    for (i, node) in spec.sinks.iter().enumerate() {
        let (name, sink) = log_on_err(
            log,
            &format!("sink {i} ({}) connect failed", node.connector),
            build_sink(node, i, &columns, active_license).await,
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
                        &CheckpointCursor {
                            resume_state: stat.resume_state.clone(),
                            ..CheckpointCursor::new(stat.partition_id.clone())
                        },
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
    active_license: Option<&LicenseClaims>,
) -> anyhow::Result<Vec<PartitionStats>> {
    let done = checkpoints.done_partitions(&spec.pipeline_id).await?;

    let source = log_on_err(
        log,
        "post-dbt source connect failed",
        build_source(output_node, 0, active_license).await,
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
            build_sink(node, i, &columns, active_license).await,
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
                        &CheckpointCursor {
                            resume_state: stat.resume_state.clone(),
                            ..CheckpointCursor::new(stat.partition_id.clone())
                        },
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
