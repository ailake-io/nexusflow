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

#[cfg(any(feature = "embeddings", feature = "embeddings-api", feature = "llm"))]
use arrow_array::RecordBatch as ArrowRecordBatch;
#[cfg(any(feature = "embeddings", feature = "embeddings-api", feature = "llm"))]
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

/// Builds the `ColumnMasker` (Fase 28) for `spec.masking`, if any —
/// `Ok(None)` when the pipeline has no masking configured, `Err` when it
/// does but `masking_salt` is unset (a run must fail loudly rather than
/// silently write unmasked PII; the same condition is also checked at save
/// time in `create_pipeline_handler`/`update_pipeline_handler`, but the
/// salt could still be removed from the environment between a pipeline
/// being saved and a later scheduled run actually executing it). Returns
/// the `Arc` itself (not a pre-boxed `BatchTransform`) so a caller can
/// *also* use `ColumnMasker::mask_schema` to fix up a sink's declared
/// schema (masked columns become `Utf8`) before ever building a
/// `BatchTransform` closure from it — see `masking_batch_transform` below.
fn build_masker(
    spec: &PipelineSpec,
    masking_salt: Option<&[u8]>,
) -> anyhow::Result<Option<std::sync::Arc<nexus_core::ColumnMasker>>> {
    if spec.masking.is_empty() {
        return Ok(None);
    }
    let salt = masking_salt.ok_or_else(|| {
        anyhow::anyhow!(
            "pipeline has masking configured but NEXUS_MASKING_SALT is not set on this server"
        )
    })?;
    Ok(Some(std::sync::Arc::new(nexus_core::ColumnMasker::new(
        &spec.masking,
        salt,
    ))))
}

/// Wraps a `ColumnMasker` as a `BatchTransform` — cheap to call, the `Arc`
/// is only cloned, never rebuilt, per batch, same "loaded once per run"
/// posture `run_passthrough_pipeline`'s embedding backend already has.
fn masking_batch_transform(masker: &std::sync::Arc<nexus_core::ColumnMasker>) -> nexus_core::BatchTransform {
    let masker = masker.clone();
    Box::new(move |batch: arrow_array::RecordBatch| {
        let masker = masker.clone();
        Box::pin(async move { masker.mask_batch(batch) })
            as futures::future::BoxFuture<'static, Result<arrow_array::RecordBatch, nexus_core::NexusError>>
    })
}

/// Arrow `Field`s -> the plain, serializable shape `PipelineSchemaStore`
/// persists — filters out `__opcode` (CDC metadata, never a real column the
/// Lineage tab's schema view should show, same exclusion every sink/column
/// list in this file already applies).
fn column_infos(schema: &arrow_schema::Schema) -> Vec<crate::pipeline_schema_store::ColumnInfo> {
    schema
        .fields()
        .iter()
        .filter(|f| f.name() != OPCODE_COLUMN)
        .map(|f| crate::pipeline_schema_store::ColumnInfo {
            name: f.name().clone(),
            data_type: f.data_type().to_string(),
        })
        .collect()
}

/// `schema` minus `__opcode` (CDC metadata, never a real destination
/// column) — same exclusion `column_infos` above applies, but returning a
/// real `SchemaRef` instead of the Lineage tab's `ColumnInfo` shape, for
/// `build_sink`'s `schema` argument (drives `CREATE TABLE IF NOT EXISTS`
/// on the postgres/sqlite sinks — a `__opcode` column would otherwise get
/// created alongside the real ones).
fn schema_without_opcode(schema: &arrow_schema::SchemaRef) -> arrow_schema::SchemaRef {
    std::sync::Arc::new(arrow_schema::Schema::new(
        schema
            .fields()
            .iter()
            .filter(|f| f.name() != OPCODE_COLUMN)
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

/// Persists a pipeline's captured schema (Fase "Linhagem — colunas e
/// tipos"). Best-effort, same posture as dbt lineage/test-result
/// persistence in `lib.rs::execute_pipeline_run`: a failure here is logged
/// and never fails the run itself — this is observability, not pipeline
/// correctness.
///
/// When the captured schema differs from the pipeline's previously
/// captured one (`PipelineSchemaStore::record`'s return value — `None` on
/// the first-ever capture or when nothing changed), fires an alert through
/// the same per-pipeline channels (`spec.alerts`) a run success/failure
/// already uses (`AlertNotifier::notify_pipeline_run`) — schema drift is
/// exactly the kind of thing a run can succeed at while still being worth
/// a human's attention.
#[allow(clippy::too_many_arguments)]
async fn record_pipeline_schema(
    schema_store: &crate::pipeline_schema_store::PipelineSchemaStore,
    alerts: &crate::alerts::AlertNotifier,
    pipeline_alerts: Option<&nexus_core::AlertsConfig>,
    run_id: i64,
    pipeline_id: &str,
    source_columns: &[crate::pipeline_schema_store::ColumnInfo],
    output_columns: &[crate::pipeline_schema_store::ColumnInfo],
    column_lineage: Option<&[crate::pipeline_schema_store::ColumnLineageInfo]>,
) {
    match schema_store
        .record(pipeline_id, source_columns, output_columns, column_lineage)
        .await
    {
        Ok(Some(drift)) => {
            alerts.notify_pipeline_run(pipeline_alerts, pipeline_id, run_id, true, &drift);
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(error = %e, "failed to persist pipeline schema");
        }
    }
}

/// Converts `nexus_core::transform::ColumnLineage` (borrowed `Expr` walk
/// result, core-crate type) into the store's serializable
/// `ColumnLineageInfo` — kept as a free function since both transform-based
/// capture sites (`run_transform_pipeline`, `run_streaming_cdc_pipeline`)
/// need it.
fn to_lineage_infos(
    lineage: Vec<nexus_core::transform::ColumnLineage>,
) -> Vec<crate::pipeline_schema_store::ColumnLineageInfo> {
    lineage
        .into_iter()
        .map(|l| crate::pipeline_schema_store::ColumnLineageInfo {
            output_column: l.output_column,
            source_columns: l.source_columns,
        })
        .collect()
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
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
    active_license: Option<&LicenseClaims>,
    schema_store: &crate::pipeline_schema_store::PipelineSchemaStore,
    alerts: &crate::alerts::AlertNotifier,
    run_id: i64,
    quality_store: &crate::quality_check_store::QualityCheckStore,
    llm_stats_store: &crate::pipeline_run_llm_stats_store::PipelineRunLlmStatsStore,
    prompt_templates: &crate::prompt_template_store::PromptTemplateStore,
    llm_eval_store: &crate::llm_eval_result_store::LlmEvalResultStore,
    masking_salt: Option<&[u8]>,
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
        run_streaming_cdc_pipeline(
            spec,
            checkpoints,
            progress,
            log,
            active_license,
            schema_store,
            alerts,
            run_id,
            prompt_templates,
            llm_eval_store,
            masking_salt,
        )
        .await
    } else if spec.has_transform() || spec.python.is_some() {
        run_transform_pipeline(
            spec,
            checkpoints,
            progress,
            log,
            active_license,
            schema_store,
            alerts,
            run_id,
            quality_store,
            llm_stats_store,
            prompt_templates,
            llm_eval_store,
            masking_salt,
        )
        .await
    } else {
        // The postgres→postgres branch below never builds through
        // connectors.rs's build_source/build_sink (uses PostgresSource/
        // PostgresSink directly) — postgres isn't an enterprise connector,
        // nothing to check there. The passthrough fallback DOES go
        // through build_source/build_sink (same as the transform path),
        // so it needs active_license threaded through too, or any
        // licensed connector would be usable unlicensed just by omitting
        // a Transform node.
        run_linear_pipeline(
            spec,
            checkpoints,
            progress,
            log,
            active_license,
            schema_store,
            alerts,
            run_id,
            prompt_templates,
            llm_eval_store,
            masking_salt,
        )
        .await
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
#[allow(clippy::too_many_arguments)]
// `prompt_templates`/`llm_eval_store` are only read inside the
// `#[cfg(feature = "llm")]` call to `maybe_run_llm_eval` below — unused (by
// design, not a bug) when that feature is off, same reasoning
// `run_transform_pipeline` already has for its own copies of these.
#[cfg_attr(not(feature = "llm"), allow(unused_variables))]
async fn run_linear_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
    active_license: Option<&LicenseClaims>,
    schema_store: &crate::pipeline_schema_store::PipelineSchemaStore,
    alerts: &crate::alerts::AlertNotifier,
    run_id: i64,
    prompt_templates: &crate::prompt_template_store::PromptTemplateStore,
    llm_eval_store: &crate::llm_eval_result_store::LlmEvalResultStore,
    masking_salt: Option<&[u8]>,
) -> anyhow::Result<Vec<PartitionStats>> {
    // Golden-dataset eval (Marco L7) doesn't depend on which sub-path below
    // actually runs (postgres-partitioned or `run_passthrough_pipeline`) —
    // one call here covers both, no need to duplicate it into
    // `run_passthrough_pipeline` too (its only caller is this function).
    #[cfg(feature = "llm")]
    maybe_run_llm_eval(spec, run_id, log, llm_eval_store, prompt_templates).await;

    let source_node = &spec.sources[0];
    let sink_node = &spec.sinks[0];

    if source_node.connector != "postgres" || sink_node.connector != "postgres" {
        // `run_passthrough_pipeline` supports `embedding` (Marco L6) — this
        // check used to live at the top of this function, unconditionally,
        // which rejected embedding for *both* sub-paths before either ever
        // ran; moved below so it only applies to the postgres-native path.
        return run_passthrough_pipeline(
            spec,
            checkpoints,
            progress,
            log,
            active_license,
            schema_store,
            alerts,
            run_id,
            masking_salt,
        )
        .await;
    }

    if spec.embedding.is_some() {
        anyhow::bail!(
            "embedding stage is not supported on the no-transform (postgres→postgres) path; \
             add a transform node to use embeddings"
        );
    }
    if !spec.masking.is_empty() {
        // Same restriction as embedding immediately above, and for the same
        // reason: this fast path uses `PipelineEngine::run` across possibly
        // many PK-range partitions at once (see its own doc comment), which
        // has no per-partition `batch_transform` slot the way
        // `run_partition` (used everywhere else) does — see Fase 28's plan
        // notes for why extending `PipelineEngine::run` itself wasn't worth
        // it for this one fast path alone.
        anyhow::bail!(
            "masking is not supported on the no-transform (postgres→postgres) path; \
             add a transform node to use column masking"
        );
    }

    let source_cfg: PostgresConnectorConfig = serde_json::from_value(source_node.config.clone())?;
    let sink_cfg: PostgresConnectorConfig = serde_json::from_value(sink_node.config.clone())?;

    let schema = table_schema(&source_cfg).await?;

    // No transform on this path — the sink receives exactly the source's
    // own schema.
    let source_columns = column_infos(&schema);
    record_pipeline_schema(
        schema_store,
        alerts,
        spec.alerts.as_ref(),
        run_id,
        &spec.pipeline_id,
        &source_columns,
        &source_columns,
        None,
    )
    .await;

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
                    PostgresSink::connect(&sink_cfg, &schema).await,
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
                    PostgresSink::connect(&sink_cfg, &schema).await,
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
                // deltalake-cdc reports the highest `_commit_version` read
                // as a decimal string; the connector expects `starting_version`
                // as u64.
                "deltalake-cdc" => {
                    if let Ok(version) = resume_state.parse::<u64>() {
                        node.config["starting_version"] = serde_json::Value::from(version);
                    }
                }
                // iceberg-cdc / ailake-cdc report the newest snapshot id read
                // as a decimal string; the connectors expect `starting_snapshot_id`
                // as i64.
                "iceberg-cdc" | "ailake-cdc" => {
                    if let Ok(snapshot_id) = resume_state.parse::<i64>() {
                        node.config["starting_snapshot_id"] = serde_json::Value::from(snapshot_id);
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
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(feature = "llm"), allow(unused_variables))]
async fn run_streaming_cdc_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
    active_license: Option<&LicenseClaims>,
    schema_store: &crate::pipeline_schema_store::PipelineSchemaStore,
    alerts: &crate::alerts::AlertNotifier,
    run_id: i64,
    prompt_templates: &crate::prompt_template_store::PromptTemplateStore,
    llm_eval_store: &crate::llm_eval_result_store::LlmEvalResultStore,
    masking_salt: Option<&[u8]>,
) -> anyhow::Result<Vec<PartitionStats>> {
    #[cfg(feature = "llm")]
    maybe_run_llm_eval(spec, run_id, log, llm_eval_store, prompt_templates).await;

    let masker = build_masker(spec, masking_salt)?;

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
    // Fase 28: the schema DataFusion resolves the transform SQL against
    // must already reflect masking (masked columns become `Utf8`) — every
    // batch handed to `transform.apply` below is masked *before* it gets
    // there, so a stale (pre-masking) schema here would disagree with the
    // actual Arrow array types in every batch DataFusion receives.
    let source_schema = match &masker {
        Some(m) => m.mask_schema(&source.schema()),
        None => source.schema(),
    };

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
    let sink_schema = schema_without_opcode(&output_schema);

    let column_lineage = transform
        .column_lineage(vec![(source_name.clone(), source_schema.clone())])
        .await
        .ok()
        .map(to_lineage_infos);
    record_pipeline_schema(
        schema_store,
        alerts,
        spec.alerts.as_ref(),
        run_id,
        &spec.pipeline_id,
        &column_infos(&source_schema),
        &column_infos(&output_schema),
        column_lineage.as_deref(),
    )
    .await;

    let done = checkpoints.done_partitions(&spec.pipeline_id).await?;
    let mut sinks = Vec::with_capacity(spec.sinks.len());
    for (i, node) in spec.sinks.iter().enumerate() {
        let (name, sink) = log_on_err(
            log,
            &format!("sink {i} ({}) connect failed", node.connector),
            build_sink(node, i, &sink_schema, active_license).await,
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
        // Fase 28: masked before the transform sees it, not after — a
        // `GROUP BY`/join on a masked column inside `transform_spec.sql`
        // only works against the token, never the original value.
        let batch = match &masker {
            Some(m) => log_on_err(log, "masking failed", m.mask_batch(batch)).await?,
            None => batch,
        };
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
#[allow(clippy::too_many_arguments)]
async fn run_passthrough_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
    active_license: Option<&LicenseClaims>,
    schema_store: &crate::pipeline_schema_store::PipelineSchemaStore,
    alerts: &crate::alerts::AlertNotifier,
    run_id: i64,
    masking_salt: Option<&[u8]>,
) -> anyhow::Result<Vec<PartitionStats>> {
    // Enterprise gate (LLMOPS_IMPLEMENTATION_PLAN.md Marco L8) — reactive
    // RAG (a `*-cdc` source combined with `embedding`, which is exactly
    // what the `batch_transform` block below builds) is the paid
    // diferencial, not embedding-on-passthrough in general: a plain batch
    // source (e.g. `csv`) with `embedding` and no transform stays OSS.
    // Deliberately checked *before* `batch_transform` is built below — that
    // block does real work (loading an embedding model, possibly
    // downloading it) for a combination this gate might reject outright;
    // failing fast here means a missing license never pays that cost.
    // Reuses the same mechanism already enforced for enterprise
    // connectors; see `capability_registry.rs`'s doc comment for why the
    // slug is registered from this crate instead of a private one.
    if spec.sources[0].connector.ends_with("-cdc") && spec.embedding.is_some() {
        crate::connectors::check_connector_license("reactive-rag-cdc", active_license)?;
    }

    // Marco L6: `embedding` on this path is what makes reactive RAG
    // possible — a `postgres-cdc -> embedding -> lancedb` pipeline with no
    // `transform` node streams straight through (this path), unlike
    // `run_transform_pipeline` which needs `drain_sources` first (never
    // completes for a CDC source, see `ARCHITECTURE.md §7`). Loaded once
    // per run, not once per batch (same reasoning `apply_embedding_stage`
    // gives for `run_transform_pipeline`'s equivalent), then wrapped as a
    // `BatchTransform` the engine applies to every batch between read and
    // write — see `nexus_core::pipeline::BatchTransform`'s doc comment for
    // why this lives in nexus-core as a generic hook instead of a
    // hardcoded embedding call.
    #[cfg(any(feature = "embeddings", feature = "embeddings-api"))]
    let batch_transform: Option<nexus_core::BatchTransform> = match &spec.embedding {
        Some(embedding_spec) => {
            let backend = std::sync::Arc::new(
                nexus_ai::embedding::load_embedding_backend(embedding_spec).await?,
            );
            let embedding_spec = embedding_spec.clone();
            Some(Box::new(move |batch: ArrowRecordBatch| {
                let backend = backend.clone();
                let embedding_spec = embedding_spec.clone();
                Box::pin(async move {
                    nexus_ai::embedding::apply_embedding(&batch, &embedding_spec, &backend)
                        .await
                        .map_err(|e| nexus_core::NexusError::Connector(e.to_string()))
                })
                    as futures::future::BoxFuture<
                        'static,
                        Result<ArrowRecordBatch, nexus_core::NexusError>,
                    >
            }) as nexus_core::BatchTransform)
        }
        None => None,
    };
    #[cfg(not(any(feature = "embeddings", feature = "embeddings-api")))]
    let batch_transform: Option<nexus_core::BatchTransform> = if spec.embedding.is_some() {
        anyhow::bail!(
            "pipeline contains an embedding node but the server was built without \
             the 'embeddings' or 'embeddings-api' feature"
        );
    } else {
        None
    };
    // Fase 28: masking runs *before* embedding — a masked text column
    // being embedded should embed the token, not the original PII, same
    // "mask before anything downstream sees it" ordering
    // `run_transform_pipeline`/`run_streaming_cdc_pipeline` apply relative
    // to the SQL transform.
    let masker = build_masker(spec, masking_salt)?;
    let batch_transform = nexus_core::chain_batch_transforms(
        masker.as_ref().map(masking_batch_transform),
        batch_transform,
    );

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
    let source_schema = source.schema();
    let sink_schema = schema_without_opcode(&source_schema);
    // Fase 28: the sink must be built from the *masked* schema (masked
    // columns become `Utf8`) — it receives whatever `batch_transform`
    // above actually outputs, not `source_schema` unchanged. Embedding's
    // own output-column addition on this path predates this and isn't
    // reflected here either; unlike embedding, masking never changes a
    // sink's column *count*, only some columns' types, so this is the
    // narrower, safe fix rather than a broader schema-reconciliation
    // rewrite of this whole function.
    let sink_schema = match &masker {
        Some(m) => m.mask_schema(&sink_schema),
        None => sink_schema,
    };

    // No transform on this path — the sink receives exactly the source's
    // own schema (minus `__opcode`, `column_infos` filters it the same way).
    let source_columns = column_infos(&source_schema);
    record_pipeline_schema(
        schema_store,
        alerts,
        spec.alerts.as_ref(),
        run_id,
        &spec.pipeline_id,
        &source_columns,
        &source_columns,
        None,
    )
    .await;

    let (_name, sink) = log_on_err(
        log,
        &format!("sink 0 ({}) connect failed", sink_node.connector),
        build_sink(sink_node, 0, &sink_schema, active_license).await,
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
    // Always exactly one partition ("p0") on this path (see this
    // function's doc comment) — calls `run_partition` directly instead of
    // `.run(vec![handle], ...)` so `batch_transform` (Marco L6) has
    // somewhere to go; `.run()`'s signature is shared with the genuinely
    // multi-partition postgres-native path above, which doesn't need it.
    let result = engine
        .run_partition(handle, progress, batch_transform)
        .await;
    let _ = progress_handle.await;

    let mut stats = Vec::new();
    let mut errors = Vec::new();
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
#[allow(clippy::too_many_arguments)]
// `llm_stats_store` is only read inside the `#[cfg(feature = "llm")]` block
// below — unused (by design, not a bug) when that feature is off.
#[cfg_attr(not(feature = "llm"), allow(unused_variables))]
async fn run_transform_pipeline(
    spec: &PipelineSpec,
    checkpoints: &CheckpointStore,
    progress: Option<ProgressSender>,
    log: Option<&RunLogger>,
    active_license: Option<&LicenseClaims>,
    schema_store: &crate::pipeline_schema_store::PipelineSchemaStore,
    alerts: &crate::alerts::AlertNotifier,
    run_id: i64,
    quality_store: &crate::quality_check_store::QualityCheckStore,
    llm_stats_store: &crate::pipeline_run_llm_stats_store::PipelineRunLlmStatsStore,
    prompt_templates: &crate::prompt_template_store::PromptTemplateStore,
    llm_eval_store: &crate::llm_eval_result_store::LlmEvalResultStore,
    masking_salt: Option<&[u8]>,
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

    // Fase 28: masked before the SQL transform ever sees any of it — a
    // `GROUP BY`/join on a masked column inside a Transform node only
    // works against the token, never the original value. Each source's
    // schema is rebuilt alongside its batches (masked columns become
    // `Utf8`) so DataFusion resolves the transform SQL against types that
    // actually match what's in the batches, not the pre-masking source
    // schema.
    let masker = build_masker(spec, masking_salt)?;
    let inputs = match &masker {
        Some(m) => inputs
            .into_iter()
            .map(|(name, schema, batches)| -> anyhow::Result<_> {
                let masked_batches = batches
                    .into_iter()
                    .map(|b| m.mask_batch(b))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((name, m.mask_schema(&schema), masked_batches))
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        None => inputs,
    };

    #[cfg(any(feature = "embeddings", feature = "embeddings-api"))]
    let inputs = apply_embedding_stage(inputs, spec.embedding.as_ref()).await?;
    #[cfg(not(any(feature = "embeddings", feature = "embeddings-api")))]
    if spec.embedding.is_some() {
        anyhow::bail!(
            "pipeline contains an embedding node but the server was built without \
             the 'embeddings' or 'embeddings-api' feature"
        );
    }

    #[cfg(feature = "llm")]
    let inputs = apply_llm_stage(
        inputs,
        spec.llm.as_ref(),
        log,
        run_id,
        llm_stats_store,
        prompt_templates,
    )
    .await?;
    #[cfg(not(feature = "llm"))]
    if spec.llm.is_some() {
        anyhow::bail!(
            "pipeline contains an llm node but the server was built without the 'llm' feature"
        );
    }

    #[cfg(feature = "llm")]
    maybe_run_llm_eval(spec, run_id, log, llm_eval_store, prompt_templates).await;

    // Captured before `inputs` is consumed below — flattens every source's
    // schema into one column list (a fan-in transform reads all of them;
    // dedup by name since a join's key columns legitimately show up on more
    // than one side).
    let mut source_columns = Vec::new();
    let mut seen_source_columns = std::collections::HashSet::new();
    for (_, schema, _) in &inputs {
        for col in column_infos(schema) {
            if seen_source_columns.insert(col.name.clone()) {
                source_columns.push(col);
            }
        }
    }

    let mut column_lineage = None;
    let output = if let Some(transform_spec) = &spec.transform {
        let transform = DataFusionTransform::new(&transform_spec.sql);
        let input_schemas: Vec<_> = inputs
            .iter()
            .map(|(n, s, _)| (n.clone(), s.clone()))
            .collect();
        column_lineage = transform
            .column_lineage(input_schemas)
            .await
            .ok()
            .map(to_lineage_infos);
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
    let sink_schema = output
        .first()
        .map(|b| schema_without_opcode(&b.schema()))
        .unwrap_or_else(|| std::sync::Arc::new(arrow_schema::Schema::empty()));

    let output_columns = output
        .first()
        .map(|b| column_infos(&b.schema()))
        .unwrap_or_default();
    record_pipeline_schema(
        schema_store,
        alerts,
        spec.alerts.as_ref(),
        run_id,
        &spec.pipeline_id,
        &source_columns,
        &output_columns,
        column_lineage.as_deref(),
    )
    .await;

    // Native quality checks: only registered, never blocking (per product
    // decision — a failing check must not stop a pipeline run, only be
    // recorded for the Quality tab to surface). Evaluated here because this
    // is the only path holding the fully materialized `output` in memory;
    // see `nexus_core::quality`'s doc comment for why CDC/passthrough
    // pipelines aren't covered in v1.
    if !spec.quality_checks.is_empty() {
        let outcomes = nexus_core::evaluate_quality_checks(&output, &spec.quality_checks);
        if let Err(e) = quality_store
            .record_all(&spec.pipeline_id, run_id, &outcomes)
            .await
        {
            log_error(log, format!("failed to persist quality check results: {e}")).await;
        }
    }

    let mut sinks = Vec::with_capacity(spec.sinks.len());
    for (i, node) in spec.sinks.iter().enumerate() {
        let (name, sink) = log_on_err(
            log,
            &format!("sink {i} ({}) connect failed", node.connector),
            build_sink(node, i, &sink_schema, active_license).await,
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

    let sink_schema = batches
        .first()
        .map(|b| schema_without_opcode(&b.schema()))
        .unwrap_or_else(|| std::sync::Arc::new(arrow_schema::Schema::empty()));

    let mut sinks = Vec::with_capacity(spec.post_dbt_sinks.len());
    for (i, node) in spec.post_dbt_sinks.iter().enumerate() {
        let (raw_name, sink) = log_on_err(
            log,
            &format!("post-dbt sink {i} ({}) connect failed", node.connector),
            build_sink(node, i, &sink_schema, active_license).await,
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
        // `embedded`'s batches carry a real extra column (the embedding)
        // that `schema` (captured before this loop, from the un-embedded
        // input) doesn't know about — registering that stale schema
        // against the new batches later (`DataFusionTransform`'s MemTable)
        // fails with "Mismatch between schema and batches" for every
        // pipeline combining an embedding stage with a SQL transform,
        // which is every vector-sink pipeline that isn't pure passthrough.
        // The first embedded batch's own schema is the source of truth;
        // fall back to the original only when there were no batches at all
        // (nothing to derive a schema from).
        let updated_schema = embedded.first().map(|b| b.schema()).unwrap_or(schema);
        out.push((name, updated_schema, embedded));
    }
    Ok(out)
}

/// Wraps `nexus_connector_redis::RedisKvClient` to implement nexus-ai's
/// `LlmCache` trait (LLMOPS_IMPLEMENTATION_PLAN.md Marco L3) — lives here,
/// not in nexus-ai, so that crate never depends on a specific connector
/// (same layering reasoning as `LlmCache` itself being a trait). Errors are
/// swallowed (logged, not propagated): a cache miss/write failure is a
/// cost/latency regression, never a reason to fail the pipeline run.
#[cfg(all(feature = "llm", feature = "redis"))]
struct RedisLlmCache(nexus_connector_redis::RedisKvClient);

#[cfg(all(feature = "llm", feature = "redis"))]
#[async_trait::async_trait]
impl nexus_ai::llm::LlmCache for RedisLlmCache {
    async fn get(&self, key: &str) -> Option<String> {
        match self.0.get(key).await {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(error = %e, "llm cache GET failed, treating as a miss");
                None
            }
        }
    }

    async fn set(&self, key: &str, value: &str, ttl_seconds: u64) {
        if let Err(e) = self.0.set_ex(key, value, ttl_seconds).await {
            tracing::warn!(error = %e, "llm cache SETEX failed");
        }
    }
}

/// Connects the cache backend `spec.cache` asks for, if any. A `Some(cache)`
/// spec on a binary built without the "redis" feature is a clear
/// config/build-mismatch error, not a silent no-cache fallback — same
/// posture as the embedding backend's "not compiled into this binary"
/// errors.
#[cfg(feature = "llm")]
async fn connect_llm_cache(
    spec: &nexus_core::LlmNodeSpec,
) -> anyhow::Result<Option<Box<dyn nexus_ai::llm::LlmCache>>> {
    if spec.cache.is_none() {
        return Ok(None);
    }
    #[cfg(feature = "redis")]
    {
        let cache_spec = spec.cache.as_ref().expect("checked above");
        let client = nexus_connector_redis::RedisKvClient::connect(&cache_spec.url).await?;
        Ok(Some(
            Box::new(RedisLlmCache(client)) as Box<dyn nexus_ai::llm::LlmCache>
        ))
    }
    #[cfg(not(feature = "redis"))]
    {
        anyhow::bail!(
            "pipeline's llm node has a cache configured but the server was built without the 'redis' feature"
        )
    }
}

/// Same shape as `apply_embedding_stage` — loads the backend once per run,
/// then applies it to every batch of every named input. Runs after
/// `embedding` (see `PipelineSpec::llm`'s doc comment for the stage order),
/// so a pipeline can chunk+embed *and* ask an LLM something about the
/// original text, both landing in the same sink.
#[cfg(feature = "llm")]
async fn apply_llm_stage(
    inputs: Vec<(String, ArrowSchemaRef, Vec<ArrowRecordBatch>)>,
    llm_spec: Option<&nexus_core::LlmNodeSpec>,
    log: Option<&RunLogger>,
    run_id: i64,
    llm_stats_store: &crate::pipeline_run_llm_stats_store::PipelineRunLlmStatsStore,
    prompt_templates: &crate::prompt_template_store::PromptTemplateStore,
) -> anyhow::Result<Vec<(String, ArrowSchemaRef, Vec<ArrowRecordBatch>)>> {
    let Some(spec) = llm_spec else {
        return Ok(inputs);
    };

    let template = prompt_templates
        .resolve(&spec.prompt.name, spec.prompt.version)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "llm node references prompt {:?} version {:?}, which doesn't exist",
                spec.prompt.name,
                spec.prompt.version
            )
        })?;
    let resolved_version = match spec.prompt.version {
        Some(v) => v,
        // Re-resolve which version "latest" actually was, so the log line
        // below records a concrete number instead of "None" — only runs
        // once per run, not per call.
        None => prompt_templates
            .latest_version(&spec.prompt.name)
            .await?
            .unwrap_or(0),
    };

    let backend = nexus_ai::llm::load_llm_backend(spec);
    let cache = connect_llm_cache(spec).await?;
    let (model, cost_per_1k_prompt_tokens, cost_per_1k_completion_tokens) = match &spec.model {
        nexus_core::LlmModelConfig::Api {
            model,
            cost_per_1k_prompt_tokens,
            cost_per_1k_completion_tokens,
            ..
        } => (
            model,
            cost_per_1k_prompt_tokens,
            cost_per_1k_completion_tokens,
        ),
        nexus_core::LlmModelConfig::Anthropic {
            model,
            cost_per_1k_prompt_tokens,
            cost_per_1k_completion_tokens,
            ..
        } => (
            model,
            cost_per_1k_prompt_tokens,
            cost_per_1k_completion_tokens,
        ),
    };

    let mut out = Vec::with_capacity(inputs.len());
    for (name, schema, batches) in inputs {
        let mut transformed = Vec::with_capacity(batches.len());
        for batch in &batches {
            let result =
                nexus_ai::llm::apply_llm(batch, spec, &template, &backend, cache.as_deref())
                    .await?;
            for call in &result.calls {
                let cost_estimate = cost_per_1k_prompt_tokens.unwrap_or(0.0)
                    * (call.tokens_prompt as f64 / 1000.0)
                    + cost_per_1k_completion_tokens.unwrap_or(0.0)
                        * (call.tokens_completion as f64 / 1000.0);
                if let Err(e) = llm_stats_store
                    .record_call(
                        run_id,
                        call.tokens_prompt,
                        call.tokens_completion,
                        cost_estimate,
                    )
                    .await
                {
                    tracing::warn!(error = %e, run_id, "failed to persist llm run stats");
                }
                // Never the prompt/response text itself unless the spec
                // opts in — same posture as every other log line touching
                // user content in this codebase (CLAUDE.md §5). Built via
                // serde_json rather than hand-rolled string formatting so
                // arbitrary model names/content never produce malformed or
                // injectable JSON.
                let mut fields = serde_json::json!({
                    "model": model,
                    "prompt_name": spec.prompt.name,
                    "prompt_version": resolved_version,
                    "tokens_prompt": call.tokens_prompt,
                    "tokens_completion": call.tokens_completion,
                    "latency_ms": call.latency_ms,
                    "prompt_len_chars": call.prompt_len_chars,
                    "response_len_chars": call.response_len_chars,
                });
                if spec.log_full_content {
                    fields["prompt"] = serde_json::Value::String(call.prompt.clone());
                    fields["response"] = serde_json::Value::String(call.response.clone());
                }
                log_info(log, format!("llm call: {fields}")).await;
            }
            transformed.push(result.batch);
        }
        let updated_schema = transformed.first().map(|b| b.schema()).unwrap_or(schema);
        out.push((name, updated_schema, transformed));
    }
    Ok(out)
}

/// Entry point shared by every pipeline shape's `run_*` function
/// (`run_transform_pipeline`, `run_linear_pipeline`,
/// `run_streaming_cdc_pipeline`) — golden-dataset evaluation
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7) is independent of the batch's
/// actual row data (a fixed set of question/expected-answer pairs, not
/// derived from what the pipeline actually moved), so it runs the same way
/// regardless of which path executed the pipeline. Never blocking, same
/// posture as `run_llm_eval` itself.
#[cfg(feature = "llm")]
async fn maybe_run_llm_eval(
    spec: &PipelineSpec,
    run_id: i64,
    log: Option<&RunLogger>,
    llm_eval_store: &crate::llm_eval_result_store::LlmEvalResultStore,
    prompt_templates: &crate::prompt_template_store::PromptTemplateStore,
) {
    if let Some(llm_spec) = spec.llm.as_ref() {
        if !llm_spec.eval.is_empty() {
            run_llm_eval(
                llm_spec,
                run_id,
                &spec.pipeline_id,
                log,
                llm_eval_store,
                prompt_templates,
            )
            .await;
        }
    }
}

/// Runs `llm_spec.eval`'s golden dataset (LLMOPS_IMPLEMENTATION_PLAN.md
/// Marco L7) once per run and persists the scores — never fails the run
/// itself (mirrors the native quality-check block's non-blocking posture in
/// `run_transform_pipeline`), since a failing eval case is a signal to
/// surface on the Quality tab, not a reason to stop moving data.
#[cfg(feature = "llm")]
async fn run_llm_eval(
    llm_spec: &nexus_core::LlmNodeSpec,
    run_id: i64,
    pipeline_id: &str,
    log: Option<&RunLogger>,
    llm_eval_store: &crate::llm_eval_result_store::LlmEvalResultStore,
    prompt_templates: &crate::prompt_template_store::PromptTemplateStore,
) {
    let template = match prompt_templates
        .resolve(&llm_spec.prompt.name, llm_spec.prompt.version)
        .await
    {
        Ok(Some(template)) => template,
        Ok(None) => {
            log_error(
                log,
                format!(
                    "llm eval: prompt {:?} version {:?} not found, skipping golden dataset",
                    llm_spec.prompt.name, llm_spec.prompt.version
                ),
            )
            .await;
            return;
        }
        Err(e) => {
            log_error(log, format!("llm eval: failed to resolve prompt: {e}")).await;
            return;
        }
    };
    let resolved_version = match llm_spec.prompt.version {
        Some(v) => v,
        None => prompt_templates
            .latest_version(&llm_spec.prompt.name)
            .await
            .ok()
            .flatten()
            .unwrap_or(0),
    };

    let backend = nexus_ai::llm::load_llm_backend(llm_spec);
    let outcomes = nexus_ai::llm::run_eval_cases(llm_spec, &template, &backend).await;
    let outcomes: Vec<_> = outcomes
        .into_iter()
        .map(|o| crate::llm_eval_result_store::LlmEvalOutcome {
            eval_name: o.eval_name,
            prompt_version: resolved_version,
            score: o.score,
            passed: o.passed,
            message: Some(o.answer),
        })
        .collect();
    if let Err(e) = llm_eval_store
        .record_all(pipeline_id, run_id, &outcomes)
        .await
    {
        log_error(log, format!("failed to persist llm eval results: {e}")).await;
    }
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

    /// Marco L7's own "done" criterion (LLMOPS_IMPLEMENTATION_PLAN.md):
    /// swapping a prompt's version must move the golden dataset's average
    /// score in a measurable way — proves `run_llm_eval` actually re-resolves
    /// the prompt per run instead of reusing whatever it saw first, and that
    /// `score_answer` really distinguishes a good answer from a bad one.
    #[cfg(feature = "llm")]
    #[tokio::test]
    async fn swapping_prompt_version_measurably_changes_the_average_eval_score() {
        use crate::llm_eval_result_store::LlmEvalResultStore;
        use crate::prompt_template_store::PromptTemplateStore;
        use nexus_core::{LlmEvalCase, LlmModelConfig, LlmNodeSpec, PromptRef};
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // v1's template steers the model toward the right answer; v2's
        // steers it toward a useless one — same two golden questions, only
        // the prompt text differs, exactly what a real prompt-version swap
        // would do to a real model's output.
        for (style, country, answer) in [
            ("STYLE_GOOD", "france", "paris"),
            ("STYLE_GOOD", "japan", "tokyo"),
            ("STYLE_BAD", "france", "i have no idea"),
            ("STYLE_BAD", "japan", "i have no idea"),
        ] {
            Mock::given(method("POST"))
                .and(path("/chat/completions"))
                .and(body_string_contains(style))
                .and(body_string_contains(country))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "choices": [{"message": {"content": answer}}],
                    "usage": {"prompt_tokens": 4, "completion_tokens": 2}
                })))
                .mount(&server)
                .await;
        }

        let prompt_templates = PromptTemplateStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let eval_store = LlmEvalResultStore::connect("sqlite::memory:")
            .await
            .unwrap();
        prompt_templates
            .create(
                "eval-prompt",
                "STYLE_GOOD: what is the capital of {country}?",
                "alice",
            )
            .await
            .unwrap();
        prompt_templates
            .create(
                "eval-prompt",
                "STYLE_BAD: what is the capital of {country}?",
                "alice",
            )
            .await
            .unwrap();

        let mut france_inputs = std::collections::BTreeMap::new();
        france_inputs.insert("country".to_string(), "france".to_string());
        let mut japan_inputs = std::collections::BTreeMap::new();
        japan_inputs.insert("country".to_string(), "japan".to_string());
        let eval_cases = vec![
            LlmEvalCase {
                name: "capital-of-france".to_string(),
                inputs: france_inputs,
                expected_answer: "paris".to_string(),
            },
            LlmEvalCase {
                name: "capital-of-japan".to_string(),
                inputs: japan_inputs,
                expected_answer: "tokyo".to_string(),
            },
        ];

        let make_spec = |version: u32| LlmNodeSpec {
            prompt: PromptRef {
                name: "eval-prompt".to_string(),
                version: Some(version),
            },
            input_columns: vec![],
            output_column: "answer".to_string(),
            model: LlmModelConfig::Api {
                base_url: server.uri(),
                model: "gpt-test".to_string(),
                api_key_env: None,
                cost_per_1k_prompt_tokens: None,
                cost_per_1k_completion_tokens: None,
            },
            max_tokens: None,
            temperature: None,
            log_full_content: false,
            cache: None,
            eval: eval_cases.clone(),
            eval_scoring: nexus_core::EvalScoringMode::TokenSimilarity,
        };

        run_llm_eval(
            &make_spec(1),
            1,
            "pipe-1",
            None,
            &eval_store,
            &prompt_templates,
        )
        .await;
        run_llm_eval(
            &make_spec(2),
            2,
            "pipe-1",
            None,
            &eval_store,
            &prompt_templates,
        )
        .await;

        let results = eval_store.list_for_pipeline("pipe-1").await.unwrap();
        let avg_for_version = |v: u32| {
            let scores: Vec<f64> = results
                .iter()
                .filter(|r| r.prompt_version == v)
                .map(|r| r.score)
                .collect();
            scores.iter().sum::<f64>() / scores.len() as f64
        };
        let avg_v1 = avg_for_version(1);
        let avg_v2 = avg_for_version(2);
        assert!(
            avg_v1 > 0.9,
            "v1's average score was {avg_v1}, expected near 1.0"
        );
        assert!(
            avg_v2 < 0.1,
            "v2's average score was {avg_v2}, expected near 0.0"
        );
        assert!(
            (avg_v1 - avg_v2).abs() > 0.5,
            "prompt version swap must measurably move the average score: v1={avg_v1} v2={avg_v2}"
        );
    }

    /// Marco L8's enterprise gate on reactive RAG (`*-cdc` source +
    /// `embedding` on the passthrough path). Deliberately doesn't spin up
    /// a real postgres-cdc container — the license check in
    /// `run_passthrough_pipeline` runs before any connector is actually
    /// built, so a `postgres-cdc` config that could never connect (bogus
    /// URI) is enough to prove the gate fires without needing the full
    /// `reactive_rag_cdc_pipeline.rs` environment. "With a covering
    /// license" isn't tested here — `license::test_support` (the signing
    /// key) only exists under `#[cfg(test)]`, so it's exercised directly
    /// in `capability_registry.rs`'s tests instead; this test only proves
    /// the wiring (right condition, right slug), not `covers()` itself.
    #[cfg(all(
        feature = "llm",
        any(feature = "embeddings", feature = "embeddings-api")
    ))]
    #[tokio::test]
    async fn reactive_rag_cdc_combination_is_denied_without_a_covering_license() {
        let checkpoints = CheckpointStore::connect("sqlite::memory:").await.unwrap();
        let schema_store =
            crate::pipeline_schema_store::PipelineSchemaStore::connect("sqlite::memory:")
                .await
                .unwrap();
        let alerts =
            crate::alerts::AlertNotifier::new(crate::alerts::AlertConfig::default(), false);

        let spec: PipelineSpec = serde_json::from_value(serde_json::json!({
            "pipeline_id": "reactive-rag-gate-test",
            "sources": [{
                "connector": "postgres-cdc",
                "config": {
                    "uri": "postgres://nobody:nobody@127.0.0.1:1/nowhere",
                    "table": "docs",
                    "publication_name": "pub_docs",
                    "slot_name": "slot_docs",
                    "fields": [{"name": "id", "data_type": "int64", "nullable": false}]
                }
            }],
            "embedding": {
                "source_column": "id",
                "output_column": "embedding",
                "dimension": 8,
                "model": {
                    "backend": "onnx",
                    "repo": "unused",
                    "revision": "main",
                    "filename": "unused",
                    "tokenizer_filename": "unused",
                    "max_length": 8
                },
                "chunking": {
                    "strategy": "fixed_window",
                    "chunk_size": 1000,
                    "overlap": 0
                }
            },
            "sinks": [{"connector": "lancedb", "config": {}}]
        }))
        .unwrap();

        let result = run_passthrough_pipeline(
            &spec,
            &checkpoints,
            None,
            None,
            None, // no active license
            &schema_store,
            &alerts,
            1,
        )
        .await;

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("reactive-rag-cdc"),
            "expected the license-gate error, got: {err}"
        );
    }

    /// Golden-dataset eval (Marco L7) previously only ran inside
    /// `run_transform_pipeline` — this proves `run_linear_pipeline` (Marco
    /// 1's postgres-partitioned-or-passthrough entry point) now runs it
    /// too, via `maybe_run_llm_eval` at the top of the function, before any
    /// real connector is touched. The pipeline itself is expected to fail
    /// after that (bogus csv source) — only the eval side effect matters
    /// here, same reasoning as the license-gate test above.
    #[cfg(feature = "llm")]
    #[tokio::test]
    async fn run_linear_pipeline_runs_golden_dataset_eval_via_passthrough() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "42"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1}
            })))
            .mount(&server)
            .await;

        let checkpoints = CheckpointStore::connect("sqlite::memory:").await.unwrap();
        let schema_store =
            crate::pipeline_schema_store::PipelineSchemaStore::connect("sqlite::memory:")
                .await
                .unwrap();
        let alerts =
            crate::alerts::AlertNotifier::new(crate::alerts::AlertConfig::default(), false);
        let prompt_templates =
            crate::prompt_template_store::PromptTemplateStore::connect("sqlite::memory:")
                .await
                .unwrap();
        prompt_templates
            .create("linear-eval-prompt", "Q: {question}", "alice")
            .await
            .unwrap();
        let llm_eval_store =
            crate::llm_eval_result_store::LlmEvalResultStore::connect("sqlite::memory:")
                .await
                .unwrap();

        let mut inputs = std::collections::BTreeMap::new();
        inputs.insert("question".to_string(), "anything".to_string());
        let spec: PipelineSpec = serde_json::from_value(serde_json::json!({
            "pipeline_id": "linear-eval-test",
            "sources": [{"connector": "csv", "config": {"path": "/nonexistent.csv"}}],
            "sinks": [{"connector": "csv", "config": {"path": "/tmp/nonexistent-out.csv"}}],
            "llm": {
                "prompt": {"name": "linear-eval-prompt"},
                "input_columns": [],
                "output_column": "answer",
                "model": {"backend": "api", "base_url": server.uri(), "model": "gpt-test"},
                "eval": [{
                    "name": "golden-1",
                    "inputs": inputs,
                    "expected_answer": "42"
                }]
            }
        }))
        .unwrap();

        // Real connectors are never reachable — expected to fail after the
        // eval hook already ran. Only the eval side effect is asserted.
        let _ = run_linear_pipeline(
            &spec,
            &checkpoints,
            None,
            None,
            None,
            &schema_store,
            &alerts,
            1,
            &prompt_templates,
            &llm_eval_store,
        )
        .await;

        let results = llm_eval_store
            .list_for_pipeline("linear-eval-test")
            .await
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "eval must run even on the non-transform path"
        );
        assert_eq!(results[0].eval_name, "golden-1");
        assert!(results[0].passed, "score was {}", results[0].score);
    }

    /// Same proof as above, for `run_streaming_cdc_pipeline` (the CDC+SQL
    /// fast path) — the third and last pipeline shape that skipped eval
    /// before this change.
    #[cfg(feature = "llm")]
    #[tokio::test]
    async fn run_streaming_cdc_pipeline_runs_golden_dataset_eval() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "42"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1}
            })))
            .mount(&server)
            .await;

        let checkpoints = CheckpointStore::connect("sqlite::memory:").await.unwrap();
        let schema_store =
            crate::pipeline_schema_store::PipelineSchemaStore::connect("sqlite::memory:")
                .await
                .unwrap();
        let alerts =
            crate::alerts::AlertNotifier::new(crate::alerts::AlertConfig::default(), false);
        let prompt_templates =
            crate::prompt_template_store::PromptTemplateStore::connect("sqlite::memory:")
                .await
                .unwrap();
        prompt_templates
            .create("cdc-eval-prompt", "Q: {question}", "alice")
            .await
            .unwrap();
        let llm_eval_store =
            crate::llm_eval_result_store::LlmEvalResultStore::connect("sqlite::memory:")
                .await
                .unwrap();

        let mut inputs = std::collections::BTreeMap::new();
        inputs.insert("question".to_string(), "anything".to_string());
        let spec: PipelineSpec = serde_json::from_value(serde_json::json!({
            "pipeline_id": "cdc-eval-test",
            "sources": [{
                "connector": "postgres-cdc",
                "config": {
                    "uri": "postgres://nobody:nobody@127.0.0.1:1/nowhere",
                    "table": "docs",
                    "publication_name": "pub_docs",
                    "slot_name": "slot_docs",
                    "fields": [{"name": "id", "data_type": "int64", "nullable": false}]
                }
            }],
            "transform": {"sql": "SELECT * FROM source0"},
            "sinks": [{"connector": "csv", "config": {"path": "/tmp/nonexistent-out2.csv"}}],
            "llm": {
                "prompt": {"name": "cdc-eval-prompt"},
                "input_columns": [],
                "output_column": "answer",
                "model": {"backend": "api", "base_url": server.uri(), "model": "gpt-test"},
                "eval": [{
                    "name": "golden-1",
                    "inputs": inputs,
                    "expected_answer": "42"
                }]
            }
        }))
        .unwrap();

        let _ = run_streaming_cdc_pipeline(
            &spec,
            &checkpoints,
            None,
            None,
            None,
            &schema_store,
            &alerts,
            1,
            &prompt_templates,
            &llm_eval_store,
        )
        .await;

        let results = llm_eval_store
            .list_for_pipeline("cdc-eval-test")
            .await
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "eval must run on the streaming CDC path too"
        );
        assert_eq!(results[0].eval_name, "golden-1");
        assert!(results[0].passed, "score was {}", results[0].score);
    }
}
