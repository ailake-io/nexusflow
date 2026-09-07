//! RAG (retrieval-augmented generation) via `POST /rag/query`
//! (LLMOPS_IMPLEMENTATION_PLAN.md Marco L5) — sits deliberately outside
//! `PipelineSpec`/the batch engine: a question is answered on demand, one
//! at a time, using a saved pipeline's `embedding` + `llm` config and a
//! `lancedb` sink as the vector store to search. Reuses building blocks
//! from L1-L4 directly (`nexus_ai::llm::{build_prompt, load_llm_backend}`,
//! `nexus_ai::embedding::load_embedding_backend`) instead of going through
//! `apply_llm`/`apply_embedding`, which are both `RecordBatch`-oriented
//! (batch, columnar) — a RAG question is a single ad-hoc string, not a
//! batch.
//!
//! v1 scope: only a `lancedb` sink is supported as the vector store (see
//! `docs/LLMOPS_IMPLEMENTATION_PLAN.md` Marco L5 — this session's own
//! investigation found zero vector connectors with any search/query
//! capability before this module; LanceDB is the first).

use crate::auth::{require_role, Role};
use crate::error::ApiError;
use crate::llm_generation_store::NewGeneration;
use crate::AppState;
use arrow_cast::display::array_value_to_string;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{middleware, Extension, Json, Router};
use nexus_connector_lancedb::{LanceDbConnectorConfig, LanceDbSearchClient};
use nexus_core::LlmModelConfig;
use serde::{Deserialize, Serialize};

const DEFAULT_TOP_K: usize = 5;

#[derive(Deserialize)]
struct RagQueryRequest {
    pipeline_id: String,
    question: String,
    #[serde(default)]
    top_k: Option<usize>,
}

#[derive(Serialize)]
struct RagQueryResponse {
    generation_id: i64,
    answer: String,
    context_keys: Vec<String>,
}

/// Same trust bar as running a pipeline (`Execute`) — this makes a live
/// vector search + LLM call using the pipeline's own (decrypted)
/// connector config, same class of action as `POST /pipelines/{id}/run`.
async fn rag_query_handler(
    State(state): State<AppState>,
    Json(body): Json<RagQueryRequest>,
) -> Result<Json<RagQueryResponse>, ApiError> {
    if body.question.trim().is_empty() {
        return Err(ApiError::bad_request("question must not be empty"));
    }
    let top_k = body.top_k.unwrap_or(DEFAULT_TOP_K).max(1);

    let spec = state
        .pipelines
        .get_spec(&body.pipeline_id, &state.secrets)
        .await
        .map_err(|e| match e {
            crate::pipeline_store::PipelineStoreError::NotFound(id) => {
                ApiError::not_found(format!("pipeline {id:?} not found"))
            }
            other => ApiError::internal(other),
        })?;

    let embedding_spec = spec.embedding.as_ref().ok_or_else(|| {
        ApiError::bad_request("pipeline has no embedding config, required for RAG")
    })?;
    let llm_spec = spec
        .llm
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("pipeline has no llm config, required for RAG"))?;
    let sink_node = spec
        .sinks
        .iter()
        .find(|s| s.connector == "lancedb")
        .ok_or_else(|| {
            ApiError::bad_request(
                "pipeline has no lancedb sink — RAG v1 only supports LanceDB as the vector store",
            )
        })?;
    let sink_cfg: LanceDbConnectorConfig = serde_json::from_value(sink_node.config.clone())
        .map_err(|e| ApiError::internal(format!("invalid lancedb sink config: {e}")))?;

    // Same model that embedded the stored rows in the first place — a
    // question embedded by a different model would search a vector space
    // the stored vectors don't live in.
    let embedding_backend = nexus_ai::embedding::load_embedding_backend(embedding_spec)
        .await
        .map_err(ApiError::internal)?;
    let mut query_vectors = embedding_backend
        .embed(std::slice::from_ref(&body.question))
        .await
        .map_err(ApiError::internal)?;
    let query_vector = query_vectors
        .pop()
        .ok_or_else(|| ApiError::internal("embedding backend returned no vector"))?;

    let search_client = LanceDbSearchClient::connect(&sink_cfg)
        .await
        .map_err(ApiError::internal)?;
    let batches = search_client
        .search(query_vector, &sink_cfg.embedding_column, top_k)
        .await
        .map_err(ApiError::internal)?;

    let mut context_parts = Vec::new();
    let mut context_keys = Vec::new();
    for batch in &batches {
        let Ok(text_idx) = batch.schema().index_of(&embedding_spec.source_column) else {
            continue;
        };
        let Ok(key_idx) = batch.schema().index_of(&sink_cfg.primary_key) else {
            continue;
        };
        for row in 0..batch.num_rows() {
            if let Ok(text) = array_value_to_string(batch.column(text_idx), row) {
                context_parts.push(text);
            }
            if let Ok(key) = array_value_to_string(batch.column(key_idx), row) {
                context_keys.push(key);
            }
        }
    }
    if context_parts.is_empty() {
        return Err(ApiError::not_found(
            "no context found in the vector store for this question — has the pipeline run yet?",
        ));
    }
    let context = context_parts
        .iter()
        .enumerate()
        .map(|(i, text)| format!("[{}] {text}", i + 1))
        .collect::<Vec<_>>()
        .join("\n\n");

    let template = state
        .prompt_templates
        .resolve(&llm_spec.prompt.name, llm_spec.prompt.version)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "llm node references prompt {:?} version {:?}, which doesn't exist",
                llm_spec.prompt.name, llm_spec.prompt.version
            ))
        })?;
    // RAG convention (documented here, not shared with the batch `llm`
    // node's `input_columns` interpolation): the prompt template must
    // reference `{context}` and `{question}`, not arbitrary columns —
    // there is no RecordBatch here, just these two strings.
    let prompt = nexus_ai::llm::build_prompt(
        &template,
        &[("context", context), ("question", body.question.clone())],
    );

    let backend = nexus_ai::llm::load_llm_backend(llm_spec);
    let (model_name, resolved_version) = model_name_and_resolved_version(llm_spec, &state).await?;
    let response = match &backend {
        nexus_ai::llm::LlmBackend::Api(client) => client
            .call(&prompt, llm_spec.max_tokens, llm_spec.temperature)
            .await
            .map_err(ApiError::internal)?,
        nexus_ai::llm::LlmBackend::Anthropic(client) => client
            .call(&prompt, llm_spec.max_tokens, llm_spec.temperature)
            .await
            .map_err(ApiError::internal)?,
    };

    let resource_id = crate::lineage::resource_node_id(&sink_node.connector, &sink_node.config);

    let generation_id = state
        .llm_generations
        .record(NewGeneration {
            pipeline_id: &body.pipeline_id,
            question: &body.question,
            answer: &response.text,
            prompt_name: &llm_spec.prompt.name,
            prompt_version: resolved_version,
            model: &model_name,
            tokens_prompt: response.tokens_prompt,
            tokens_completion: response.tokens_completion,
            resource_id: resource_id.as_deref(),
            context_keys: &context_keys,
        })
        .await
        .map_err(ApiError::internal)?;

    Ok(Json(RagQueryResponse {
        generation_id,
        answer: response.text,
        context_keys,
    }))
}

/// `spec.llm.prompt.version` is `None` for "always latest" — re-resolves
/// which version that actually was, same reasoning
/// `runner.rs::apply_llm_stage` re-resolves it for its own log line.
async fn model_name_and_resolved_version(
    llm_spec: &nexus_core::LlmNodeSpec,
    state: &AppState,
) -> Result<(String, u32), ApiError> {
    let model_name = match &llm_spec.model {
        LlmModelConfig::Api { model, .. } => model.clone(),
        LlmModelConfig::Anthropic { model, .. } => model.clone(),
    };
    let resolved_version = match llm_spec.prompt.version {
        Some(v) => v,
        None => state
            .prompt_templates
            .latest_version(&llm_spec.prompt.name)
            .await
            .map_err(ApiError::internal)?
            .unwrap_or(0),
    };
    Ok((model_name, resolved_version))
}

#[derive(Serialize)]
struct GenerationDetailResponse {
    nodes: Vec<crate::lineage::LineageNode>,
    edges: Vec<crate::lineage::LineageEdge>,
    question: String,
    answer: String,
    prompt_name: String,
    prompt_version: u32,
    model: String,
    context_keys: Vec<String>,
}

/// Same RBAC tier as `GET /lineage` (`Read`) — this only ever hands back
/// a single generation's own record, not arbitrary pipeline configs.
async fn generation_detail_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<GenerationDetailResponse>, ApiError> {
    // Enterprise gate (LLMOPS_IMPLEMENTATION_PLAN.md Marco L8) — row→
    // generation lineage is the paid diferencial, not `POST /rag/query`
    // itself (asking a question stays OSS). Reuses the exact mechanism
    // already enforced for enterprise connectors; see
    // `capability_registry.rs`'s doc comment for why the slug is
    // registered from this crate instead of a private one.
    let active_license = state.license_store.active().await.unwrap_or(None);
    crate::connectors::check_connector_license("llm-lineage-tracking", active_license.as_ref())
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    let generation = state
        .llm_generations
        .get(id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("generation {id} not found")))?;

    let generation_node_id = format!("generation::{}", generation.id);
    let mut nodes = vec![crate::lineage::LineageNode::Generation {
        id: generation_node_id.clone(),
        label: format!("\"{}\"", truncate(&generation.question, 60)),
    }];
    let mut edges = Vec::new();

    if let Some(resource_id) = &generation.resource_id {
        // `resource_id` is always `"resource::{connector}::{identifier}"`
        // (see `lineage::resource_node_id`) — parsed back out here rather
        // than re-fetching the pipeline spec (which may have changed or
        // been deleted since this generation was recorded).
        if let Some((connector, identifier)) = parse_resource_id(resource_id) {
            nodes.push(crate::lineage::LineageNode::Resource {
                id: resource_id.clone(),
                label: identifier,
                connector,
                // v1 RAG only supports lancedb sinks, always a Table —
                // see `lineage.rs`'s own `"lancedb" => ResourceKind::Table`.
                resource_kind: crate::lineage::ResourceKind::Table,
            });
            edges.push(crate::lineage::LineageEdge {
                from: resource_id.clone(),
                to: generation_node_id,
            });
        }
    }

    Ok(Json(GenerationDetailResponse {
        nodes,
        edges,
        question: generation.question,
        answer: generation.answer,
        prompt_name: generation.prompt_name,
        prompt_version: generation.prompt_version,
        model: generation.model,
        context_keys: generation.context_keys,
    }))
}

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    s.chars().take(max_chars).collect::<String>() + "…"
}

fn parse_resource_id(resource_id: &str) -> Option<(String, String)> {
    let mut parts = resource_id.splitn(3, "::");
    let _prefix = parts.next().filter(|p| *p == "resource")?;
    let connector = parts.next()?;
    let identifier = parts.next()?;
    Some((connector.to_string(), identifier.to_string()))
}

pub fn routes(state: AppState) -> Router {
    let execute_routes = Router::new()
        .route("/rag/query", post(rag_query_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Execute));

    let read_routes = Router::new()
        .route("/lineage/generation/{id}", get(generation_detail_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Read));

    Router::new()
        .merge(execute_routes)
        .merge(read_routes)
        .with_state(state)
}
