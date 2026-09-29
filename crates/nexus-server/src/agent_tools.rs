//! Executes one `AgentToolKind` (ROADMAP.md Fase 31) — each function here
//! is a thin wrapper over code that already exists elsewhere in this crate
//! (preview/search/run-pipeline/webhook/python-viz machinery), no new
//! execution engine. `execute_tool` takes both the tool's *static* config
//! (`AgentToolKind`, set when the agent was configured) and the model's
//! *dynamic* tool-call argument (`args`, decided per call, shape described
//! by `AgentToolKind::json_schema()`) — `agent_runner.rs` is the only
//! caller, after a `LlmTurn::ToolCalls` entry names which tool and supplies
//! `args`.

use crate::AppState;
use nexus_core::{AgentToolKind, ConnectorRegistry, Transform};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct AgentToolError(String);

impl AgentToolError {
    fn from_display(e: impl std::fmt::Display) -> Self {
        AgentToolError(e.to_string())
    }
}

/// A tool's raw output — `agent_runner.rs` decides how each variant is
/// summarized back into the model's conversation (`ToolMessage::ToolResult`
/// needs a plain string) and how it's persisted in `agent_steps.result`.
/// Kept separate from that concern here: this module only runs the tool.
#[derive(Debug)]
pub enum ToolOutput {
    Text(String),
    Chart(crate::python_viz::RenderedChart),
}

/// Caps `QueryData`/`GenerateChart`'s source read — an agent tool answers
/// one question, it isn't a batch pipeline moving millions of rows; same
/// order of magnitude as `DEFAULT_PREVIEW_LIMIT` elsewhere in this crate.
const QUERY_DATA_ROW_LIMIT: usize = 500;

/// How long `RunPipeline{wait_for_result: true}` polls before giving up and
/// reporting "still running" rather than hanging the agent loop (and the
/// human waiting on it) indefinitely on a long-running pipeline.
const RUN_PIPELINE_WAIT_TIMEOUT_SECS: u64 = 300;
const RUN_PIPELINE_POLL_INTERVAL_SECS: u64 = 2;

pub async fn execute_tool(
    state: &AppState,
    tool: &AgentToolKind,
    args: &Value,
) -> Result<ToolOutput, AgentToolError> {
    match tool {
        AgentToolKind::QueryData { source } => query_data(state, source, args).await,
        AgentToolKind::SearchVectors { pipeline_id, top_k } => {
            search_vectors(state, pipeline_id, *top_k, args).await
        }
        AgentToolKind::RunPipeline {
            pipeline_id,
            wait_for_result,
        } => run_pipeline(state, pipeline_id, *wait_for_result).await,
        AgentToolKind::CallWebhook { url, method } => call_webhook(state, url, method, args).await,
        AgentToolKind::GenerateChart {
            source,
            script,
            timeout_seconds,
        } => generate_chart(state, source, script, *timeout_seconds, args).await,
        AgentToolKind::DraftPipeline => draft_pipeline(state, args).await,
        AgentToolKind::GetPipelineStatus => get_pipeline_status(state, args).await,
        AgentToolKind::EditPipeline => edit_pipeline(state, args).await,
    }
}

/// Live `ToolDef.schema` for `draft_pipeline` — the one variant whose
/// argument shape needs real I/O (the connector catalog), which
/// `nexus-core` deliberately never touches. `agent_runner.rs` calls this
/// instead of `AgentToolKind::json_schema()` for this one tool; every other
/// tool still uses the pure method. Kept in the same shape as
/// `AgentToolKind::DraftPipeline`'s own static fallback, just with a real
/// `enum` of connector names instead of a bare string.
pub fn draft_pipeline_schema() -> Value {
    let connector_names: Vec<&'static str> = ConnectorRegistry::all()
        .filter(|d| d.capability != nexus_core::ConnectorCapability::Capability)
        .map(|d| d.name)
        .collect();
    let node_schema = serde_json::json!({
        "type": "object",
        "properties": {
            "connector": {
                "type": "string",
                "enum": connector_names,
                "description": "Connector name from the live catalog (GET /connectors)."
            },
            "config": {
                "type": "object",
                "description": "Connector-specific config — field names vary per connector \
                    (e.g. csv wants \"path\"/\"fields\", postgres wants \"uri\"/\"table\"). If \
                    unsure, a first query_data-style attempt with a best guess is fine — a \
                    validation error comes back describing what's wrong."
            }
        },
        "required": ["connector", "config"]
    });
    serde_json::json!({
        "type": "object",
        "properties": {
            "pipeline_id": {
                "type": "string",
                "description": "Unique id for the new pipeline, e.g. \"vendas-por-regiao\" \
                    (letters, digits, '_', '-' only)."
            },
            "sources": {"type": "array", "items": node_schema.clone()},
            "sinks": {"type": "array", "items": node_schema},
            "transform": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "Optional SQL over the source(s), referenced as \
                            source0/source1/... Required whenever there's more than one \
                            source or sink."
                    }
                }
            },
            "schedule": {
                "type": "string",
                "description": "Optional cron expression to run this pipeline automatically. \
                    Omit to leave it on-demand only."
            }
        },
        "required": ["pipeline_id", "sources", "sinks"]
    })
}

/// Builds a brand-new saved pipeline from the model's structured argument.
/// Deliberately reuses the exact same validation a human-drawn pipeline
/// goes through (`PipelineSpec::validate`/`validate_security_with`) —
/// there's no separate, looser path for an agent-authored spec. A
/// deserialization or validation failure becomes this tool's result text,
/// which `agent_runner.rs` feeds back into the conversation so the model
/// can retry with a corrected spec on its next turn.
async fn draft_pipeline(state: &AppState, args: &Value) -> Result<ToolOutput, AgentToolError> {
    let mut spec: nexus_core::PipelineSpec = serde_json::from_value(args.clone())
        .map_err(|e| AgentToolError(format!("draft_pipeline: invalid pipeline spec: {e}")))?;
    // The model only ever fills in the fields exposed by
    // `draft_pipeline_schema()` above — every other `PipelineSpec` field
    // already has a `#[serde(default)]` (channel_capacity/partitions get
    // sensible non-zero defaults, everything else empty/None), so this
    // partial JSON deserializes into a complete, well-formed spec. `draft`
    // is forced `false` explicitly anyway (not just relying on its default)
    // — this tool only ever produces a pipeline meant to be run for real,
    // never the Canvas's own "save incomplete work" draft flag.
    spec.draft = false;

    spec.validate()
        .map_err(|e| AgentToolError(format!("draft_pipeline: {e}")))?;
    spec.validate_security_with(state.allow_internal_hosts)
        .map_err(|e| AgentToolError(format!("draft_pipeline: {e}")))?;

    state
        .pipelines
        .create(&spec, &state.secrets, "agent")
        .await
        .map_err(|e| AgentToolError(format!("draft_pipeline: failed to save: {e}")))?;

    Ok(ToolOutput::Text(format!(
        "created pipeline {:?} with {} source(s) and {} sink(s){}",
        spec.pipeline_id,
        spec.sources.len(),
        spec.sinks.len(),
        if spec.schedule.is_some() {
            " (scheduled)"
        } else {
            " (on-demand only, not scheduled)"
        }
    )))
}

async fn get_pipeline_status(state: &AppState, args: &Value) -> Result<ToolOutput, AgentToolError> {
    let pipeline_id = args
        .get("pipeline_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AgentToolError("get_pipeline_status: missing required \"pipeline_id\" argument".into())
        })?;

    let summary = state
        .pipelines
        .get_summary(pipeline_id, &state.secrets)
        .await
        .map_err(AgentToolError::from_display)?;

    Ok(ToolOutput::Text(
        serde_json::to_string(&summary)
            .unwrap_or_else(|_| "(failed to serialize pipeline status)".to_string()),
    ))
}

async fn edit_pipeline(state: &AppState, args: &Value) -> Result<ToolOutput, AgentToolError> {
    let mut spec: nexus_core::PipelineSpec = serde_json::from_value(args.clone())
        .map_err(|e| AgentToolError(format!("edit_pipeline: invalid pipeline spec: {e}")))?;
    // Same reasoning as `draft_pipeline`: this tool only ever produces a
    // pipeline meant to be run for real, never the Canvas's "save
    // incomplete work" draft flag.
    spec.draft = false;

    spec.validate()
        .map_err(|e| AgentToolError(format!("edit_pipeline: {e}")))?;
    spec.validate_security_with(state.allow_internal_hosts)
        .map_err(|e| AgentToolError(format!("edit_pipeline: {e}")))?;

    // `PipelineStore::update` itself 404s (`PipelineStoreError::NotFound`)
    // if `pipeline_id` doesn't already exist — no separate existence check
    // needed, and that error message ("pipeline {id:?} not found") is
    // already a clear, retryable signal back to the model (e.g. it should
    // have called `draft_pipeline` instead).
    state
        .pipelines
        .update(&spec.pipeline_id, &spec, &state.secrets, "agent")
        .await
        .map_err(|e| AgentToolError(format!("edit_pipeline: failed to save: {e}")))?;

    Ok(ToolOutput::Text(format!(
        "updated pipeline {:?} with {} source(s) and {} sink(s){}",
        spec.pipeline_id,
        spec.sources.len(),
        spec.sinks.len(),
        if spec.schedule.is_some() {
            " (scheduled)"
        } else {
            " (on-demand only, not scheduled)"
        }
    )))
}

fn optional_sql_arg(args: &Value) -> Option<&str> {
    args.get("sql")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
}

async fn query_data(
    state: &AppState,
    source: &nexus_core::NodeSpec,
    args: &Value,
) -> Result<ToolOutput, AgentToolError> {
    let active_license = state.license_store.active().await.unwrap_or(None);
    let (table_name, src) = crate::connectors::build_source(source, 0, active_license.as_ref())
        .await
        .map_err(AgentToolError::from_display)?;
    let schema = src.schema();
    let batches = crate::read_preview_batches(src, QUERY_DATA_ROW_LIMIT)
        .await
        .map_err(|e| AgentToolError(e.message().to_string()))?;

    let output_batches = match optional_sql_arg(args) {
        Some(sql) => nexus_core::DataFusionTransform::new(sql.to_string())
            .apply(vec![(table_name, schema, batches)])
            .await
            .map_err(AgentToolError::from_display)?,
        None => batches,
    };

    let json = crate::batches_to_preview_json(&output_batches)
        .map_err(|e| AgentToolError(e.message().to_string()))?;
    Ok(ToolOutput::Text(
        serde_json::to_string(&json).map_err(AgentToolError::from_display)?,
    ))
}

/// Same 6-connector dispatch `rag.rs`'s `rag_query_handler` uses — reuses
/// its `search_*` functions directly (bumped to `pub(crate)` for this)
/// rather than a second copy of the match.
const SUPPORTED_VECTOR_SINKS: [&str; 6] = [
    "lancedb", "qdrant", "milvus", "pgvector", "pinecone", "chromadb",
];

async fn search_vectors(
    state: &AppState,
    pipeline_id: &str,
    top_k: usize,
    args: &Value,
) -> Result<ToolOutput, AgentToolError> {
    let query = args.get("query").and_then(Value::as_str).ok_or_else(|| {
        AgentToolError("search_vectors requires a \"query\" argument".to_string())
    })?;

    let spec = state
        .pipelines
        .get_spec(pipeline_id, &state.secrets)
        .await
        .map_err(AgentToolError::from_display)?;
    let embedding_spec = spec.embedding.as_ref().ok_or_else(|| {
        AgentToolError(format!(
            "pipeline {pipeline_id:?} has no embedding config, required for search_vectors"
        ))
    })?;
    let sink_node = spec
        .sinks
        .iter()
        .find(|s| SUPPORTED_VECTOR_SINKS.contains(&s.connector.as_str()))
        .ok_or_else(|| {
            AgentToolError(format!(
                "pipeline {pipeline_id:?} has no supported vector sink — needs one of \
                 {SUPPORTED_VECTOR_SINKS:?}"
            ))
        })?;

    let embedding_backend = nexus_ai::embedding::load_embedding_backend(embedding_spec)
        .await
        .map_err(AgentToolError::from_display)?;
    let mut query_vectors = embedding_backend
        .embed(std::slice::from_ref(&query.to_string()))
        .await
        .map_err(AgentToolError::from_display)?;
    let query_vector = query_vectors
        .pop()
        .ok_or_else(|| AgentToolError("embedding backend returned no vector".to_string()))?;

    let (_keys, texts): (Vec<String>, Vec<String>) = match sink_node.connector.as_str() {
        "lancedb" => crate::rag::search_lancedb(
            &sink_node.config,
            &embedding_spec.source_column,
            query_vector,
            top_k,
        )
        .await
        .map_err(|e| AgentToolError(e.message().to_string()))?,
        "qdrant" => crate::rag::search_qdrant(
            &sink_node.config,
            &embedding_spec.source_column,
            query_vector,
            top_k,
        )
        .await
        .map_err(|e| AgentToolError(e.message().to_string()))?,
        "milvus" => crate::rag::search_milvus(
            &sink_node.config,
            &embedding_spec.source_column,
            query_vector,
            top_k,
        )
        .await
        .map_err(|e| AgentToolError(e.message().to_string()))?,
        "pgvector" => crate::rag::search_pgvector(
            &sink_node.config,
            &embedding_spec.source_column,
            query_vector,
            top_k,
        )
        .await
        .map_err(|e| AgentToolError(e.message().to_string()))?,
        "pinecone" => crate::rag::search_pinecone(
            &sink_node.config,
            &embedding_spec.source_column,
            query_vector,
            top_k,
        )
        .await
        .map_err(|e| AgentToolError(e.message().to_string()))?,
        "chromadb" => crate::rag::search_chromadb(
            &sink_node.config,
            &embedding_spec.source_column,
            query_vector,
            top_k,
        )
        .await
        .map_err(|e| AgentToolError(e.message().to_string()))?,
        other => unreachable!("SUPPORTED_VECTOR_SINKS filtered to a known name, got {other:?}"),
    };

    Ok(ToolOutput::Text(texts.join("\n\n")))
}

async fn run_pipeline(
    state: &AppState,
    pipeline_id: &str,
    wait_for_result: bool,
) -> Result<ToolOutput, AgentToolError> {
    let spec = state
        .pipelines
        .get_spec(pipeline_id, &state.secrets)
        .await
        .map_err(AgentToolError::from_display)?;

    if state
        .pipelines
        .has_running_run(pipeline_id)
        .await
        .map_err(AgentToolError::from_display)?
    {
        return Ok(ToolOutput::Text(format!(
            "pipeline {pipeline_id:?} already has a run in progress, not starting a new one"
        )));
    }

    let run_id = crate::start_pipeline_run(state, &spec)
        .await
        .map_err(AgentToolError::from_display)?;

    if !wait_for_result {
        return Ok(ToolOutput::Text(format!(
            "started run {run_id} for pipeline {pipeline_id:?} (not waiting for completion)"
        )));
    }

    let deadline = tokio::time::Instant::now()
        + tokio::time::Duration::from_secs(RUN_PIPELINE_WAIT_TIMEOUT_SECS);
    loop {
        if !state
            .pipelines
            .has_running_run(pipeline_id)
            .await
            .map_err(AgentToolError::from_display)?
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(ToolOutput::Text(format!(
                "run {run_id} for pipeline {pipeline_id:?} is still running after \
                 {RUN_PIPELINE_WAIT_TIMEOUT_SECS}s, giving up waiting"
            )));
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(
            RUN_PIPELINE_POLL_INTERVAL_SECS,
        ))
        .await;
    }

    let runs = state
        .pipelines
        .list_runs(pipeline_id, 20, 0)
        .await
        .map_err(AgentToolError::from_display)?;
    match runs.into_iter().find(|r| r.id == run_id) {
        Some(run) if run.status == "failed" => Ok(ToolOutput::Text(format!(
            "run {run_id} for pipeline {pipeline_id:?} failed: {}",
            run.error.as_deref().unwrap_or("(no error message)")
        ))),
        Some(run) => Ok(ToolOutput::Text(format!(
            "run {run_id} for pipeline {pipeline_id:?} finished with status {:?}",
            run.status
        ))),
        None => Ok(ToolOutput::Text(format!(
            "run {run_id} for pipeline {pipeline_id:?} finished, but its record could no longer \
             be found in run history"
        ))),
    }
}

fn ssrf_safe_client(allow_internal_hosts: bool) -> reqwest::Client {
    if allow_internal_hosts {
        reqwest::Client::new()
    } else {
        reqwest::Client::builder()
            .dns_resolver(std::sync::Arc::new(crate::dns_guard::SsrfSafeResolver))
            .build()
            .expect("reqwest client with a custom DNS resolver must build")
    }
}

async fn call_webhook(
    state: &AppState,
    url: &str,
    method: &str,
    args: &Value,
) -> Result<ToolOutput, AgentToolError> {
    let client = ssrf_safe_client(state.allow_internal_hosts);
    let body = args.get("body").cloned().unwrap_or(Value::Null);

    let method = reqwest::Method::from_bytes(method.as_bytes())
        .map_err(|_| AgentToolError(format!("call_webhook: unsupported HTTP method {method:?}")))?;
    let response = client
        .request(method, url)
        .json(&body)
        .send()
        .await
        .map_err(AgentToolError::from_display)?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    Ok(ToolOutput::Text(format!(
        "webhook responded {status}: {}",
        crate::error::sanitize_error(&text)
    )))
}

async fn generate_chart(
    state: &AppState,
    source: &nexus_core::NodeSpec,
    script: &str,
    timeout_seconds: Option<u64>,
    args: &Value,
) -> Result<ToolOutput, AgentToolError> {
    let active_license = state.license_store.active().await.unwrap_or(None);
    let (table_name, src) = crate::connectors::build_source(source, 0, active_license.as_ref())
        .await
        .map_err(AgentToolError::from_display)?;
    let schema = src.schema();
    let batches = crate::read_preview_batches(src, QUERY_DATA_ROW_LIMIT)
        .await
        .map_err(|e| AgentToolError(e.message().to_string()))?;

    let (schema, batches) = match optional_sql_arg(args) {
        Some(sql) => {
            let output = nexus_core::DataFusionTransform::new(sql.to_string())
                .apply(vec![(table_name, schema.clone(), batches)])
                .await
                .map_err(AgentToolError::from_display)?;
            let output_schema = output.first().map(|b| b.schema()).unwrap_or(schema);
            (output_schema, output)
        }
        None => (schema, batches),
    };

    let chart = crate::python_viz::render(schema, batches, script, timeout_seconds)
        .await
        .map_err(AgentToolError::from_display)?;
    Ok(ToolOutput::Chart(chart))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_core::NodeSpec;
    use sqlx::sqlite::SqliteConnectOptions;
    use std::str::FromStr;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Seeds a real on-disk SQLite file (source/sink connectors need a real
    /// file — `:memory:` is connection-scoped, and `build_source` opens its
    /// own fresh connection) with a 2-row table, and returns the temp
    /// directory alongside the `NodeSpec` pointing at it. The directory is
    /// returned (not dropped) so it outlives the test.
    async fn sqlite_fixture(table: &str) -> (tempfile::TempDir, NodeSpec) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fixture.db");
        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
            .unwrap()
            .create_if_missing(true);
        let pool = sqlx::sqlite::SqlitePool::connect_with(opts).await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
        )))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} (id, name) VALUES (1, 'alice'), (2, 'bob')"
        )))
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let node = NodeSpec {
            name: None,
            connector: "sqlite".to_string(),
            config: serde_json::json!({
                "file_path": db_path.display().to_string(),
                "table": table,
                "primary_key": "id"
            }),
        };
        (dir, node)
    }

    #[tokio::test]
    async fn query_data_without_sql_returns_raw_rows() {
        let state = crate::tests::test_state().await;
        let (_dir, source) = sqlite_fixture("items").await;

        let output = execute_tool(
            &state,
            &AgentToolKind::QueryData { source },
            &serde_json::json!({}),
        )
        .await
        .unwrap();

        let ToolOutput::Text(text) = output else {
            panic!("expected Text output");
        };
        let rows: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn query_data_with_sql_filters_rows() {
        let state = crate::tests::test_state().await;
        let (_dir, source) = sqlite_fixture("items").await;

        let output = execute_tool(
            &state,
            &AgentToolKind::QueryData { source },
            &serde_json::json!({"sql": "SELECT * FROM source0 WHERE name = 'alice'"}),
        )
        .await
        .unwrap();

        let ToolOutput::Text(text) = output else {
            panic!("expected Text output");
        };
        let rows: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["name"], "alice");
    }

    #[tokio::test]
    async fn search_vectors_missing_pipeline_is_an_error() {
        let state = crate::tests::test_state().await;
        let err = execute_tool(
            &state,
            &AgentToolKind::SearchVectors {
                pipeline_id: "does-not-exist".to_string(),
                top_k: 5,
            },
            &serde_json::json!({"query": "hello"}),
        )
        .await
        .unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn search_vectors_requires_query_argument() {
        let state = crate::tests::test_state().await;
        let err = execute_tool(
            &state,
            &AgentToolKind::SearchVectors {
                pipeline_id: "whatever".to_string(),
                top_k: 5,
            },
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("query"));
    }

    #[tokio::test]
    async fn run_pipeline_missing_pipeline_is_an_error() {
        let state = crate::tests::test_state().await;
        let err = execute_tool(
            &state,
            &AgentToolKind::RunPipeline {
                pipeline_id: "does-not-exist".to_string(),
                wait_for_result: false,
            },
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn run_pipeline_starts_and_waits_for_a_real_run() {
        let state = crate::tests::test_state().await;
        let (_src_dir, source) = sqlite_fixture("items").await;
        let sink_dir = tempfile::tempdir().unwrap();
        let sink_path = sink_dir.path().join("sink.db");

        let spec = nexus_core::PipelineSpec {
            pipeline_id: "agent-tool-test-pipeline".to_string(),
            sources: vec![source],
            transform: None,
            sinks: vec![NodeSpec {
                name: None,
                connector: "sqlite".to_string(),
                config: serde_json::json!({
                    "file_path": sink_path.display().to_string(),
                    "table": "items_copy",
                    "primary_key": "id"
                }),
            }],
            embedding: None,
            llm: None,
            python: None,
            visualization: None,
            channel_capacity: 100,
            partitions: 1,
            dbt: None,
            post_dbt_sinks: Vec::new(),
            schedule: None,
            depends_on: Vec::new(),
            dependency_mode: nexus_core::DependencyMode::Any,
            alerts: None,
            quality_checks: Vec::new(),
            anomaly_alerts: false,
            masking: Vec::new(),
            draft: false,
            clean_blocks: Vec::new(),
        };
        state
            .pipelines
            .create(&spec, &state.secrets, "test")
            .await
            .unwrap();

        let output = execute_tool(
            &state,
            &AgentToolKind::RunPipeline {
                pipeline_id: spec.pipeline_id.clone(),
                wait_for_result: true,
            },
            &serde_json::json!({}),
        )
        .await
        .unwrap();

        let ToolOutput::Text(text) = output else {
            panic!("expected Text output");
        };
        assert!(
            text.contains("succeeded") || text.contains("finished with status"),
            "unexpected tool output: {text}"
        );
    }

    #[tokio::test]
    async fn call_webhook_reports_response_status() {
        let state = crate::tests::test_state().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hook"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&server)
            .await;

        let output = execute_tool(
            &state,
            &AgentToolKind::CallWebhook {
                url: format!("{}/hook", server.uri()),
                method: "POST".to_string(),
            },
            &serde_json::json!({"body": {"hello": "world"}}),
        )
        .await
        .unwrap();

        let ToolOutput::Text(text) = output else {
            panic!("expected Text output");
        };
        assert!(text.contains("200"));
    }

    #[tokio::test]
    async fn draft_pipeline_creates_a_real_saved_pipeline() {
        let state = crate::tests::test_state().await;
        let (_src_dir, source_node) = sqlite_fixture("items").await;
        let source_config = source_node.config;
        let sink_dir = tempfile::tempdir().unwrap();
        let sink_path = sink_dir.path().join("sink.db");

        let args = serde_json::json!({
            "pipeline_id": "agent-drafted-pipeline",
            "sources": [{"connector": "sqlite", "config": source_config}],
            "sinks": [{"connector": "sqlite", "config": {
                "file_path": sink_path.display().to_string(),
                "table": "items_copy",
                "primary_key": "id"
            }}]
        });

        let output = execute_tool(&state, &AgentToolKind::DraftPipeline, &args)
            .await
            .unwrap();
        let ToolOutput::Text(text) = output else {
            panic!("expected Text output");
        };
        assert!(
            text.contains("agent-drafted-pipeline"),
            "unexpected: {text}"
        );

        // Not just "the tool said so" — confirm it's a real, fully valid,
        // runnable saved pipeline (same store any human-created one uses).
        let saved = state
            .pipelines
            .get_spec("agent-drafted-pipeline", &state.secrets)
            .await
            .unwrap();
        assert_eq!(saved.sources.len(), 1);
        assert_eq!(saved.sinks.len(), 1);
        assert!(!saved.draft);
    }

    #[tokio::test]
    async fn draft_pipeline_rejects_invalid_spec_with_a_retryable_error() {
        let state = crate::tests::test_state().await;
        // Two sources, no transform — PipelineSpec::validate() rejects this
        // (fan-in needs a transform) exactly like it would for a human.
        let args = serde_json::json!({
            "pipeline_id": "agent-invalid-pipeline",
            "sources": [
                {"connector": "csv", "config": {"path": "/tmp/a.csv"}},
                {"connector": "csv", "config": {"path": "/tmp/b.csv"}}
            ],
            "sinks": [{"connector": "csv", "config": {"path": "/tmp/out.csv"}}]
        });

        let err = execute_tool(&state, &AgentToolKind::DraftPipeline, &args)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("transform"),
            "expected a validate()-shaped error, got: {err}"
        );
    }

    #[tokio::test]
    async fn draft_pipeline_rejects_malformed_json_shape() {
        let state = crate::tests::test_state().await;
        let args = serde_json::json!({"pipeline_id": "bad-shape", "sources": "not-an-array"});

        let err = execute_tool(&state, &AgentToolKind::DraftPipeline, &args)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid pipeline spec"));
    }

    #[tokio::test]
    async fn get_pipeline_status_returns_summary_for_existing_pipeline() {
        let state = crate::tests::test_state().await;
        let (_src_dir, source_node) = sqlite_fixture("items").await;
        let sink_dir = tempfile::tempdir().unwrap();
        let sink_path = sink_dir.path().join("sink.db");
        let spec = nexus_core::PipelineSpec {
            pipeline_id: "status-check-pipeline".to_string(),
            sources: vec![source_node],
            sinks: vec![NodeSpec {
                name: None,
                connector: "sqlite".to_string(),
                config: serde_json::json!({
                    "file_path": sink_path.display().to_string(),
                    "table": "items_copy",
                    "primary_key": "id"
                }),
            }],
            transform: None,
            embedding: None,
            llm: None,
            python: None,
            visualization: None,
            channel_capacity: 100,
            partitions: 1,
            dbt: None,
            post_dbt_sinks: Vec::new(),
            schedule: None,
            depends_on: Vec::new(),
            dependency_mode: nexus_core::DependencyMode::Any,
            alerts: None,
            quality_checks: Vec::new(),
            anomaly_alerts: false,
            masking: Vec::new(),
            draft: false,
            clean_blocks: Vec::new(),
        };
        state
            .pipelines
            .create(&spec, &state.secrets, "test")
            .await
            .unwrap();

        let output = execute_tool(
            &state,
            &AgentToolKind::GetPipelineStatus,
            &serde_json::json!({"pipeline_id": "status-check-pipeline"}),
        )
        .await
        .unwrap();
        let ToolOutput::Text(text) = output else {
            panic!("expected Text output");
        };
        assert!(text.contains("status-check-pipeline"));
        assert!(text.contains("\"last_run_status\":null"));
    }

    #[tokio::test]
    async fn get_pipeline_status_missing_arg_is_an_error() {
        let state = crate::tests::test_state().await;
        let err = execute_tool(
            &state,
            &AgentToolKind::GetPipelineStatus,
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("pipeline_id"));
    }

    #[tokio::test]
    async fn get_pipeline_status_unknown_pipeline_is_an_error() {
        let state = crate::tests::test_state().await;
        let err = execute_tool(
            &state,
            &AgentToolKind::GetPipelineStatus,
            &serde_json::json!({"pipeline_id": "does-not-exist"}),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn edit_pipeline_updates_an_existing_pipeline() {
        let state = crate::tests::test_state().await;
        let (_src_dir, source_node) = sqlite_fixture("items").await;
        let source_config = source_node.config.clone();
        let sink_dir = tempfile::tempdir().unwrap();
        let sink_path = sink_dir.path().join("sink.db");
        let spec = nexus_core::PipelineSpec {
            pipeline_id: "editable-pipeline".to_string(),
            sources: vec![source_node],
            sinks: vec![NodeSpec {
                name: None,
                connector: "sqlite".to_string(),
                config: serde_json::json!({
                    "file_path": sink_path.display().to_string(),
                    "table": "items_copy",
                    "primary_key": "id"
                }),
            }],
            transform: None,
            embedding: None,
            llm: None,
            python: None,
            visualization: None,
            channel_capacity: 100,
            partitions: 1,
            dbt: None,
            post_dbt_sinks: Vec::new(),
            schedule: None,
            depends_on: Vec::new(),
            dependency_mode: nexus_core::DependencyMode::Any,
            alerts: None,
            quality_checks: Vec::new(),
            anomaly_alerts: false,
            masking: Vec::new(),
            draft: false,
            clean_blocks: Vec::new(),
        };
        state
            .pipelines
            .create(&spec, &state.secrets, "test")
            .await
            .unwrap();

        let args = serde_json::json!({
            "pipeline_id": "editable-pipeline",
            "sources": [{"connector": "sqlite", "config": source_config}],
            "sinks": [{"connector": "sqlite", "config": {
                "file_path": sink_path.display().to_string(),
                "table": "items_copy",
                "primary_key": "id"
            }}],
            "schedule": "0 0 * * *"
        });

        let output = execute_tool(&state, &AgentToolKind::EditPipeline, &args)
            .await
            .unwrap();
        let ToolOutput::Text(text) = output else {
            panic!("expected Text output");
        };
        assert!(text.contains("editable-pipeline"));
        assert!(text.contains("scheduled"));

        let saved = state
            .pipelines
            .get_spec("editable-pipeline", &state.secrets)
            .await
            .unwrap();
        assert_eq!(saved.schedule.as_deref(), Some("0 0 * * *"));
    }

    #[tokio::test]
    async fn edit_pipeline_rejects_unknown_pipeline_id() {
        let state = crate::tests::test_state().await;
        let args = serde_json::json!({
            "pipeline_id": "does-not-exist",
            "sources": [{"connector": "csv", "config": {"path": "/tmp/a.csv"}}],
            "sinks": [{"connector": "csv", "config": {"path": "/tmp/out.csv"}}]
        });

        let err = execute_tool(&state, &AgentToolKind::EditPipeline, &args)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not found"), "got: {err}");
    }

    #[tokio::test]
    async fn edit_pipeline_rejects_invalid_spec_with_a_retryable_error() {
        let state = crate::tests::test_state().await;
        let args = serde_json::json!({
            "pipeline_id": "editable-pipeline",
            "sources": [
                {"connector": "csv", "config": {"path": "/tmp/a.csv"}},
                {"connector": "csv", "config": {"path": "/tmp/b.csv"}}
            ],
            "sinks": [{"connector": "csv", "config": {"path": "/tmp/out.csv"}}]
        });

        let err = execute_tool(&state, &AgentToolKind::EditPipeline, &args)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("transform"), "got: {err}");
    }

    #[tokio::test]
    async fn generate_chart_returns_bytes_from_tuple_return() {
        let state = crate::tests::test_state().await;
        let (_dir, source) = sqlite_fixture("items").await;

        let output = execute_tool(
            &state,
            &AgentToolKind::GenerateChart {
                source,
                script: "def visualize(df):\n    return (b'chart-bytes', 'text/plain')\n"
                    .to_string(),
                timeout_seconds: None,
            },
            &serde_json::json!({}),
        )
        .await;

        // Skip (not fail) when this environment's python3 lacks
        // pandas/pyarrow — same posture as python_viz.rs's own tests.
        match output {
            Ok(ToolOutput::Chart(chart)) => {
                assert_eq!(chart.content_type, "text/plain");
                assert_eq!(chart.bytes, b"chart-bytes");
            }
            Ok(ToolOutput::Text(_)) => panic!("expected Chart output"),
            Err(e) if e.to_string().contains("is python3 on PATH") => {
                eprintln!("skipping: python3 not available");
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
}
