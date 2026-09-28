//! `POST /agents` and friends (ROADMAP.md Fase 31) — CRUD for `AgentSpec`
//! plus the run/approve/reject endpoints that drive `agent_runner.rs`'s
//! loop. Mirrors `rag.rs`'s and the pipeline handlers' own patterns: a
//! redacted `AgentSummary` for `Read`-tier list/get (connector configs
//! inside `QueryData`/`GenerateChart` can carry secrets, same reasoning as
//! `PipelineSummary`), full `AgentSpec` only behind `Write`.

use crate::agent_run_store::{AgentRunStoreError, AgentStep};
use crate::agent_runner::AgentRunnerError;
use crate::agent_store::AgentStoreError;
use crate::auth::{require_role, Claims, Role};
use crate::error::ApiError;
use crate::pipeline_store::NodeSummary;
use crate::AppState;
use axum::extract::{Path, State};
use axum::routing::{get, post, put};
use axum::{middleware, Extension, Json, Router};
use nexus_core::{AgentSpec, AgentToolKind, LlmModelConfig};
use serde::{Deserialize, Serialize};

impl From<AgentStoreError> for ApiError {
    fn from(err: AgentStoreError) -> Self {
        match err {
            AgentStoreError::AlreadyExists(id) => {
                ApiError::conflict(format!("agent {id:?} already exists"))
            }
            AgentStoreError::NotFound(id) => ApiError::not_found(format!("agent {id:?} not found")),
            AgentStoreError::Corrupt(msg) => ApiError::internal(msg),
            AgentStoreError::Sqlx(e) => ApiError::internal(e),
        }
    }
}

impl From<AgentRunStoreError> for ApiError {
    fn from(err: AgentRunStoreError) -> Self {
        match err {
            AgentRunStoreError::RunNotFound(id) => {
                ApiError::not_found(format!("agent run {id} not found"))
            }
            AgentRunStoreError::StepNotFound(id) => {
                ApiError::not_found(format!("agent step {id} not found"))
            }
            AgentRunStoreError::Sqlx(e) => ApiError::internal(e),
        }
    }
}

impl From<AgentRunnerError> for ApiError {
    fn from(err: AgentRunnerError) -> Self {
        match err {
            AgentRunnerError::Store(e) => e.into(),
            AgentRunnerError::Sqlx(e) => ApiError::internal(e),
            AgentRunnerError::PromptNotFound(name) => {
                ApiError::bad_request(format!("agent prompt {name:?} not found"))
            }
            AgentRunnerError::Internal(msg) => ApiError::internal(msg),
            AgentRunnerError::StepNotPending(id) => {
                ApiError::conflict(format!("step {id} is not pending approval"))
            }
            AgentRunnerError::UnknownTool(id, tool) => {
                ApiError::internal(format!("step {id} names unknown tool {tool:?}"))
            }
        }
    }
}

/// A tool's config, redacted for `Read`-tier — same connector-name-only
/// shape as `NodeSummary` for the two tool kinds that embed a `NodeSpec`.
#[derive(Serialize)]
struct AgentToolSummary {
    kind: &'static str,
    approval: nexus_core::ApprovalMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<NodeSummary>,
}

#[derive(Serialize)]
struct AgentSummary {
    agent_id: String,
    name: String,
    model: LlmModelConfig,
    tools: Vec<AgentToolSummary>,
    max_steps: u32,
    schedule: Option<String>,
    created_at: String,
    updated_at: String,
}

fn summarize(spec: AgentSpec, created_at: String, updated_at: String) -> AgentSummary {
    let tools = spec
        .tools
        .into_iter()
        .map(|tc| {
            let source = match &tc.tool {
                AgentToolKind::QueryData { source }
                | AgentToolKind::GenerateChart { source, .. } => Some(NodeSummary {
                    connector: source.connector.clone(),
                    name: source.name.clone(),
                }),
                _ => None,
            };
            AgentToolSummary {
                kind: tc.tool.name(),
                approval: tc.approval,
                source,
            }
        })
        .collect();
    AgentSummary {
        agent_id: spec.agent_id,
        name: spec.name,
        model: spec.model,
        tools,
        max_steps: spec.max_steps,
        schedule: spec.schedule,
        created_at,
        updated_at,
    }
}

async fn create_agent_handler(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(spec): Json<AgentSpec>,
) -> Result<(axum::http::StatusCode, Json<AgentSpec>), ApiError> {
    spec.validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    spec.validate_security_with(state.allow_internal_hosts)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    state
        .agents
        .create(&spec, &state.secrets, &claims.sub)
        .await?;
    Ok((axum::http::StatusCode::CREATED, Json(spec)))
}

async fn update_agent_handler(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(spec): Json<AgentSpec>,
) -> Result<Json<AgentSpec>, ApiError> {
    if spec.agent_id != id {
        return Err(ApiError::bad_request(format!(
            "path id {id:?} does not match body.agent_id {:?}",
            spec.agent_id
        )));
    }
    spec.validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    spec.validate_security_with(state.allow_internal_hosts)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    state
        .agents
        .update(&id, &spec, &state.secrets, &claims.sub)
        .await?;
    Ok(Json(spec))
}

async fn delete_agent_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::http::StatusCode, ApiError> {
    state.agents.delete(&id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn list_agents_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<AgentSummary>>, ApiError> {
    let specs = state.agents.list_all_specs(&state.secrets).await?;
    // No per-agent created_at/updated_at surfaced yet (store doesn't expose
    // them outside the raw spec table) — same "" placeholder posture would
    // be wrong to fake, so this reuses `""` only where genuinely unknown.
    Ok(Json(
        specs
            .into_iter()
            .map(|s| summarize(s, String::new(), String::new()))
            .collect(),
    ))
}

async fn get_agent_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<AgentSummary>, ApiError> {
    let spec = state.agents.get_spec(&id, &state.secrets).await?;
    Ok(Json(summarize(spec, String::new(), String::new())))
}

async fn get_agent_spec_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<AgentSpec>, ApiError> {
    Ok(Json(state.agents.get_spec(&id, &state.secrets).await?))
}

#[derive(Deserialize)]
struct RunAgentRequest {
    question: String,
    #[serde(default)]
    model_override: Option<LlmModelConfig>,
}

#[derive(Serialize)]
struct RunAccepted {
    run_id: i64,
}

async fn run_agent_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<RunAgentRequest>,
) -> Result<(axum::http::StatusCode, Json<RunAccepted>), ApiError> {
    if body.question.trim().is_empty() {
        return Err(ApiError::bad_request("question must not be empty"));
    }
    if let Some(model) = &body.model_override {
        nexus_core::validate_llm_security(model, state.allow_internal_hosts)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
    }
    let spec = state.agents.get_spec(&id, &state.secrets).await?;
    let run_id =
        crate::agent_runner::start_run(&state, &spec, body.question, body.model_override).await?;
    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(RunAccepted { run_id }),
    ))
}

#[derive(Serialize)]
struct RunDetail {
    #[serde(flatten)]
    run: crate::agent_run_store::AgentRun,
    steps: Vec<AgentStep>,
}

async fn list_agent_runs_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<crate::agent_run_store::AgentRun>>, ApiError> {
    Ok(Json(
        state
            .agent_runs
            .list_runs(&id)
            .await
            .map_err(ApiError::internal)?,
    ))
}

async fn get_agent_run_handler(
    State(state): State<AppState>,
    Path((_id, run_id)): Path<(String, i64)>,
) -> Result<Json<RunDetail>, ApiError> {
    let run = state.agent_runs.get_run(run_id).await?;
    let steps = state
        .agent_runs
        .list_steps(run_id)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(RunDetail { run, steps }))
}

#[derive(Deserialize, Default)]
struct ApprovalDecisionRequest {}

async fn resolve_approval(
    state: &AppState,
    claims: &Claims,
    run_id: i64,
    step_id: i64,
    approved: bool,
) -> Result<axum::http::StatusCode, ApiError> {
    let run = state.agent_runs.get_run(run_id).await?;
    let spec = state.agents.get_spec(&run.agent_id, &state.secrets).await?;
    crate::agent_runner::resume_after_approval(
        state,
        &spec,
        run_id,
        step_id,
        approved,
        &claims.sub,
    )
    .await?;
    Ok(axum::http::StatusCode::ACCEPTED)
}

async fn approve_step_handler(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path((run_id, step_id)): Path<(i64, i64)>,
    Json(_body): Json<ApprovalDecisionRequest>,
) -> Result<axum::http::StatusCode, ApiError> {
    resolve_approval(&state, &claims, run_id, step_id, true).await
}

async fn reject_step_handler(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path((run_id, step_id)): Path<(i64, i64)>,
    Json(_body): Json<ApprovalDecisionRequest>,
) -> Result<axum::http::StatusCode, ApiError> {
    resolve_approval(&state, &claims, run_id, step_id, false).await
}

pub fn routes(state: AppState) -> Router {
    let write_routes = Router::new()
        .route("/agents", post(create_agent_handler))
        .route(
            "/agents/{id}",
            put(update_agent_handler).delete(delete_agent_handler),
        )
        .route("/agents/{id}/spec", get(get_agent_spec_handler))
        .route(
            "/agents/runs/{run_id}/steps/{step_id}/approve",
            post(approve_step_handler),
        )
        .route(
            "/agents/runs/{run_id}/steps/{step_id}/reject",
            post(reject_step_handler),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Write));

    let execute_routes = Router::new()
        .route("/agents/{id}/run", post(run_agent_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Execute));

    let read_routes = Router::new()
        .route("/agents", get(list_agents_handler))
        .route("/agents/{id}", get(get_agent_handler))
        .route("/agents/{id}/runs", get(list_agent_runs_handler))
        .route("/agents/{id}/runs/{run_id}", get(get_agent_run_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Read));

    Router::new()
        .merge(write_routes)
        .merge(execute_routes)
        .merge(read_routes)
        .with_state(state)
}
