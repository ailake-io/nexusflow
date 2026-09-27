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
use nexus_core::{AgentToolKind, Transform};
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
    }
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
