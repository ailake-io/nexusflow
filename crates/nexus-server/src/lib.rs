mod alerts;
mod auth;
mod auth_store;
mod browse;
mod capability_registry;
mod checkpoint_store;
mod connectors;
mod crypto;
mod db;
mod dbt;
mod dbt_lineage_store;
mod dbt_test_result_store;
mod dns_guard;
#[cfg(feature = "embed-ui")]
mod embedded_ui;
mod error;
#[cfg(feature = "version-history")]
mod git_history_store;
#[cfg(feature = "version-history")]
mod git_remote_config_store;
mod hardware_stats;
mod license;
mod license_store;
mod lineage;
mod llm_eval_result_store;
mod llm_generation_store;
pub mod migrate;
mod pipeline_run_llm_stats_store;
mod pipeline_schema_store;
mod pipeline_store;
mod progress;
mod prompt_template_store;
mod python_transform;
mod quality_check_store;
#[cfg(all(
    feature = "llm",
    any(feature = "embeddings", feature = "embeddings-api"),
    any(
        feature = "lancedb",
        feature = "qdrant",
        feature = "milvus",
        feature = "pgvector",
        feature = "pinecone",
        feature = "chromadb"
    )
))]
mod rag;
mod rate_limit;
mod resource_stats;
mod run_log_store;
mod runner;
mod scheduler;
mod server_metrics;
pub mod telemetry;

use alerts::{AlertConfig, AlertNotifier};
use auth::{require_role, Claims, JwtCodec, Role, TokenBlocklist};
use auth_store::AuthStore;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, FromRef, Path, Query, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::Response;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use checkpoint_store::CheckpointStore;
use crypto::SecretCipher;
use error::ApiError;
use futures_util::StreamExt;
use license_store::{LicenseStore, LicenseStoreError};
use nexus_core::{ConnectorRegistry, NodeSpec, PipelineSpec, ProgressSender};
use pipeline_store::{
    DeleteRunOutcome, PipelineStore, PipelineStoreError, PipelineSummary, RunRecord,
};
use progress::{ProgressHub, RunLogEvent, RunLogger};
use run_log_store::RunLogStore;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

const DEFAULT_PAGE_LIMIT: i64 = 50;
const MAX_PAGE_LIMIT: i64 = 1000;
const DEFAULT_PREVIEW_LIMIT: usize = 50;
const MAX_PREVIEW_LIMIT: usize = 500;

/// Query parameters for paginated list endpoints.
#[derive(Debug, Deserialize)]
struct Pagination {
    #[serde(default)]
    offset: i64,
    #[serde(default = "default_page_limit")]
    limit: i64,
}

fn default_page_limit() -> i64 {
    DEFAULT_PAGE_LIMIT
}

impl Pagination {
    fn validated(self) -> Result<(i64, i64), ApiError> {
        if self.offset < 0 {
            return Err(ApiError::bad_request("offset must be >= 0"));
        }
        let limit = self.limit.clamp(1, MAX_PAGE_LIMIT);
        Ok((limit, self.offset))
    }
}

#[derive(Clone)]
struct AppState {
    checkpoints: CheckpointStore,
    auth_store: AuthStore,
    jwt: JwtCodec,
    /// Encrypts connector secrets before `pipelines` persists them (CLAUDE.md §5).
    secrets: SecretCipher,
    pipelines: PipelineStore,
    run_logs: RunLogStore,
    license_store: LicenseStore,
    resource_stats: resource_stats::ResourceStatsStore,
    dbt_lineage: dbt_lineage_store::DbtLineageStore,
    pipeline_schemas: pipeline_schema_store::PipelineSchemaStore,
    // Only read from `execute_pipeline_run`'s `#[cfg(feature = "dbt")]`
    // block — kept on `AppState` unconditionally so build_state/test_state
    // don't need their own feature-gated construction path.
    #[allow(dead_code)]
    dbt_test_results: dbt_test_result_store::DbtTestResultStore,
    // Only read from `run_transform_pipeline`'s native quality-check hook —
    // see `dbt_test_results`'s note above for why it's unconditional here.
    #[allow(dead_code)]
    quality_checks: quality_check_store::QualityCheckStore,
    // Written by `apply_llm_stage`'s `#[cfg(feature = "llm")]` block, but
    // read unconditionally by `list_runs_handler` (empty table when the
    // feature is off, not a compile-time concern) — no `#[allow(dead_code)]`
    // needed here unlike `dbt_test_results`/`quality_checks` above.
    llm_stats: pipeline_run_llm_stats_store::PipelineRunLlmStatsStore,
    prompt_templates: prompt_template_store::PromptTemplateStore,
    // Written by `run_llm_eval`'s `#[cfg(feature = "llm")]` block (Marco
    // L7), read unconditionally by `list_llm_eval_results_handler` — same
    // reasoning as `llm_stats` above.
    llm_eval_results: llm_eval_result_store::LlmEvalResultStore,
    // Only written/read by `rag.rs`'s `#[cfg(all(feature = "llm", ...))]`
    // module (Marco L5) — kept unconditional on AppState, same reasoning
    // as `dbt_test_results`/`quality_checks` above.
    #[allow(dead_code)]
    llm_generations: llm_generation_store::LlmGenerationStore,
    progress: ProgressHub,
    alerts: AlertNotifier,
    login_rate_limiter: std::sync::Arc<rate_limit::LoginRateLimiter>,
    /// `NEXUS_TRUST_PROXY_HEADERS` — see `rate_limit::TrustProxyHeaders`'s doc
    /// comment for why this defaults to `false`.
    trust_proxy_headers: bool,
    /// `NEXUS_ALLOW_INTERNAL_HOSTS` — see `PipelineSpec::validate_security_with`.
    allow_internal_hosts: bool,
    /// Embedded git history for pipeline/prompt artifacts (git-versioning
    /// follow-up to LLMOPS_IMPLEMENTATION_PLAN.md's L4/L7) — see
    /// `git_history_store.rs`. `Clone` is cheap (just a `PathBuf` behind
    /// an `Arc`), same reasoning as every other store field here.
    #[cfg(feature = "version-history")]
    git_history: git_history_store::GitHistoryStore,
    /// Optional external GitHub mirror config for `git_history` above
    /// ("caso o usuário queira" — Part 4 of the git-versioning follow-up
    /// plan). `None`-shaped by an empty table, not an `Option` field —
    /// same "absent row means off" contract as `AppState.alerts`'
    /// channels.
    #[cfg(feature = "version-history")]
    git_remote: git_remote_config_store::GitRemoteConfigStore,
}

impl From<PipelineStoreError> for ApiError {
    fn from(err: PipelineStoreError) -> Self {
        match err {
            PipelineStoreError::AlreadyExists(id) => {
                ApiError::conflict(format!("pipeline {id:?} already exists"))
            }
            PipelineStoreError::NotFound(id) => {
                ApiError::not_found(format!("pipeline {id:?} not found"))
            }
            PipelineStoreError::Corrupt(msg) => ApiError::internal(msg),
            PipelineStoreError::Sqlx(e) => ApiError::internal(e),
        }
    }
}

impl From<LicenseStoreError> for ApiError {
    fn from(err: LicenseStoreError) -> Self {
        match err {
            LicenseStoreError::License(e) => ApiError::bad_request(e.to_string()),
            LicenseStoreError::Sqlx(e) => ApiError::internal(e),
        }
    }
}

/// Lets `Claims`/`require_role` (defined generically over any state `S`
/// carrying a `JwtCodec`) pull the codec out of the concrete `AppState`.
impl FromRef<AppState> for JwtCodec {
    fn from_ref(state: &AppState) -> Self {
        state.jwt.clone()
    }
}

impl FromRef<AppState> for SecretCipher {
    fn from_ref(state: &AppState) -> Self {
        state.secrets.clone()
    }
}

/// Builds the Axum app. Kept separate from `run()` so it's testable via
/// `tower::ServiceExt::oneshot` without binding a real socket.
fn router(state: AppState) -> Router {
    // RBAC checked in middleware, before the handler runs — never inside
    // the handler body (ARCHITECTURE.md §10). Running a pipeline requires
    // at least `Execute`; higher-privilege routes (pipeline CRUD, etc.)
    // will get their own `Extension(Role::X)` tier as they're added.
    // `.layer()` calls wrap innermost-first: the *last* `.layer()` call
    // becomes outermost, running first on the way in. `require_role` reads
    // `Extension<Role>` as one of its own parameters, so the `Extension`
    // layer has to be outermost (applied last here) — otherwise `require_role`
    // runs before the extension is inserted and every request 500s on a
    // missing-extension rejection instead of getting a real 401/403.
    let execute_protected = Router::new()
        .route("/pipelines/{id}/run", post(run_pipeline_handler))
        // Same role as running the pipeline: a real, live connection to an
        // external system with a decrypted credential, same trust bar.
        .route("/pipelines/{id}/preview", get(preview_node_handler))
        // Same trust bar as the saved-pipeline preview above — a bare
        // connector config isn't backed by validate_security_with() at
        // save time the way a persisted node is, so the handler itself
        // runs that check before connecting (see its doc comment). Read
        // alone isn't enough for "make an arbitrary outbound connection".
        .route("/connectors/preview", post(preview_adhoc_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Execute));

    // Creating/editing/deleting a pipeline definition needs `Write`;
    // running one (above) only needs `Execute` — matches the existing
    // Read < Execute < Write < Admin hierarchy (ARCHITECTURE.md §10).
    let write_protected = Router::new()
        .route("/pipelines", post(create_pipeline_handler))
        .route(
            "/pipelines/{id}",
            put(update_pipeline_handler).delete(delete_pipeline_handler),
        )
        // Deleting one run from the history is the same tier as deleting
        // the pipeline itself — both are destructive, irreversible edits
        // to state a `Read`/`Execute` caller shouldn't be able to make.
        .route("/pipelines/{id}/runs/{run_id}", delete(delete_run_handler))
        // Full spec (connector configs, secrets included) for reloading a
        // saved pipeline back onto the canvas to edit it. Gated behind
        // `Write` (not `Read`) because it's symmetric to create/update: only
        // a caller already trusted to type/submit connector secrets gets
        // them back. `get_pipeline_handler` above stays masked for anyone
        // with only `Read` (Marco 8 task #17).
        .route("/pipelines/{id}/spec", get(get_pipeline_spec_handler))
        // Backs the Canvas "browse path" button (frontend/SchemaForm.tsx)
        // for local-path connector fields (csv/parquet/sqlite/...). Same
        // role as editing a node's config in the first place — see
        // `browse_fs_handler`'s doc comment for why no extra sandbox is
        // layered underneath this.
        .route("/system/browse-fs", get(browse_fs_handler))
        // Prompt templates (LLMOPS_IMPLEMENTATION_PLAN.md Marco L4) — same
        // tier as editing a pipeline's config, since an `llm` node's
        // `PromptRef` points at one of these.
        .route(
            "/prompts",
            get(list_prompts_handler).post(create_prompt_handler),
        );
    // Rollback creates a *new* commit/version (never rewrites history) via
    // the same `update`/`create` path as an ordinary save — same trust bar
    // as editing the pipeline directly. Split out of the chain above so
    // this route still picks up the `.layer()`s applied right below,
    // whether or not the feature is compiled in (see
    // `git_history_store.rs`'s module doc comment).
    #[cfg(feature = "version-history")]
    let write_protected = write_protected.route(
        "/pipelines/{id}/versions/{commit}/rollback",
        post(rollback_pipeline_handler),
    );
    let write_protected = write_protected
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Write));

    // The connector catalog (Marco 8: canvas nodes come from here, never
    // hardcoded in the frontend — ARCHITECTURE.md §3) and reading pipeline
    // definitions/run history only need `Read`.
    let read_protected = Router::new()
        .route("/connectors", get(list_connectors_handler))
        .route("/pipelines", get(list_pipelines_handler))
        .route("/pipelines/{id}", get(get_pipeline_handler))
        .route("/pipelines/{id}/runs", get(list_runs_handler))
        .route(
            "/pipelines/{id}/runs/{run_id}/logs",
            get(list_run_logs_handler),
        )
        // Observability data, not an action — same tier as /connectors and
        // /pipelines above (see resource_stats.rs).
        .route("/system/resource-stats", get(resource_stats_handler))
        .route(
            "/pipelines/{id}/dbt-tests",
            get(list_dbt_test_results_handler),
        )
        .route(
            "/pipelines/{id}/quality-checks",
            get(list_quality_check_results_handler),
        )
        .route(
            "/pipelines/{id}/llm-eval-results",
            get(list_llm_eval_results_handler),
        )
        // Whole-catalog graph, not a per-pipeline secret — the handler
        // below only ever hands back connector names + allowlisted
        // resource identifiers, never raw config (see lineage.rs).
        .route("/lineage", get(lineage_handler))
        .route("/lineage/{id}/schema", get(pipeline_schema_handler));
    // Version history is read-only browsing (diffs run through the same
    // secret-safe `PipelineSummary` shape as `get_pipeline_handler`, never
    // raw connector config) — same `Read` tier as everything else in this
    // group. Split out of the chain (see `write_protected`'s rollback
    // route above for why) so it still picks up the `.layer()`s below.
    #[cfg(feature = "version-history")]
    let read_protected = read_protected
        .route(
            "/pipelines/{id}/versions",
            get(list_pipeline_versions_handler),
        )
        .route(
            "/pipelines/{id}/versions/{commit}/diff",
            get(diff_pipeline_versions_handler),
        )
        .route(
            "/prompts/{name}/versions",
            get(list_prompt_versions_handler),
        )
        .route(
            "/prompts/{name}/versions/{version}/diff",
            get(diff_prompt_versions_handler),
        );
    let read_protected = read_protected
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Read));

    // User management is Admin-only.
    let admin_protected = Router::new()
        .route("/users", get(list_users_handler).post(create_user_handler))
        .route(
            "/users/{username}",
            get(get_user_handler).delete(delete_user_handler),
        )
        .route("/users/{username}/role", put(update_user_role_handler))
        // Installing/inspecting the enterprise license is an access-control
        // action, same trust bar as user management — see
        // `docs/ENTERPRISE_LICENSING.md`.
        .route(
            "/license",
            post(install_license_handler).get(license_status_handler),
        );
    // Configuring the optional GitHub push mirror is the same trust bar
    // as installing the license itself — both gate an enterprise
    // capability and, here, also hand the server a credential with write
    // access to an external repo. Split out of the chain (see
    // `write_protected`'s rollback route earlier for why) so it still
    // picks up the `.layer()`s below.
    #[cfg(feature = "version-history")]
    let admin_protected = admin_protected.route(
        "/settings/git-remote",
        put(set_git_remote_handler).delete(delete_git_remote_handler),
    );
    let admin_protected = admin_protected
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Admin));

    // Rate-limit the login endpoint per IP before it hits Argon2 verification.
    // Layer order: Extension is outermost so the middleware sees it on the
    // way in (axum applies layers inside-out, last .layer runs first).
    let login_routes = Router::new()
        .route("/auth/login", post(login_handler))
        .layer(middleware::from_fn(rate_limit::login_rate_limit))
        .layer(Extension(rate_limit::TrustProxyHeaders(
            state.trust_proxy_headers,
        )))
        .layer(Extension(state.login_rate_limiter.clone()));

    // Logout requires any valid token; the token is revoked so it can't be
    // reused until expiry. Protected by Read role (minimum authenticated role).
    let logout_routes = Router::new()
        .route("/auth/logout", post(logout_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Read));

    // Cloned before `state` is moved into `.with_state(state)` below —
    // `rag::routes` (Marco L5, cfg-gated) needs its own `AppState` to
    // build its sub-router, merged in after the main app is stateless.
    #[cfg(all(
        feature = "llm",
        any(feature = "embeddings", feature = "embeddings-api"),
        any(
            feature = "lancedb",
            feature = "qdrant",
            feature = "milvus",
            feature = "pgvector",
            feature = "pinecone",
            feature = "chromadb"
        )
    ))]
    let rag_state = state.clone();

    let app = Router::new()
        .route("/health", get(health))
        // Unauthenticated like /health — Prometheus scrapers don't carry a
        // JWT, and RBAC over metrics access would need a whole separate
        // scrape-auth story; network segmentation is the intended guard
        // here (ARCHITECTURE.md §9/§10).
        .route("/metrics", get(metrics_handler))
        .merge(login_routes)
        .merge(logout_routes)
        // Not behind `require_role` — a browser's `WebSocket` API can't set
        // an `Authorization` header, so this route reads the JWT from the
        // `Sec-WebSocket-Protocol` subprotocol and checks the role itself
        // (see `progress_ws_handler`).
        .route(
            "/pipelines/{id}/runs/{run_id}/progress",
            get(progress_ws_handler),
        )
        .merge(admin_protected)
        .merge(execute_protected)
        .merge(write_protected)
        .merge(read_protected)
        .with_state(state);

    #[cfg(all(
        feature = "llm",
        any(feature = "embeddings", feature = "embeddings-api"),
        any(
            feature = "lancedb",
            feature = "qdrant",
            feature = "milvus",
            feature = "pgvector",
            feature = "pinecone",
            feature = "chromadb"
        )
    ))]
    let app = app.merge(rag::routes(rag_state));

    // Only wired in for the single-binary build (Marco 11) — without the
    // feature, an unmatched route just gets axum's default 404, same as
    // every build before this one.
    #[cfg(feature = "embed-ui")]
    let app = app.fallback(embedded_ui::handler);

    app
}

async fn health() -> &'static str {
    "ok"
}

/// Same counters the progress WebSocket reads from (Marco 9 task #20) —
/// see `telemetry::PROMETHEUS_REGISTRY`'s doc comment.
async fn metrics_handler() -> impl axum::response::IntoResponse {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = telemetry::PROMETHEUS_REGISTRY.gather();
    let mut buf = Vec::new();
    encoder
        .encode(&metric_families, &mut buf)
        .expect("encoding already-validated Prometheus metrics cannot fail");
    (
        [(
            axum::http::header::CONTENT_TYPE,
            encoder.format_type().to_string(),
        )],
        buf,
    )
}

/// Dynamic connector catalog for the canvas (Marco 8) — every connector
/// crate linked into this binary registers itself via `submit_connector!`
/// (ARCHITECTURE.md §3), so this list reflects what's actually usable, not
/// a hardcoded frontend assumption. `config_schema` (JSON Schema for that
/// connector's Config struct) lets the canvas render a real form instead of
/// a raw JSON textarea — `ConnectorDescriptor` itself skips it in its own
/// `Serialize` impl (a fn pointer isn't `Serialize`), so this DTO computes
/// it once per response instead.
#[derive(Serialize)]
struct ConnectorCatalogEntry {
    name: &'static str,
    capability: nexus_core::ConnectorCapability,
    config_schema: serde_json::Value,
    /// `true` for every OSS connector; for an enterprise connector
    /// (`requires_license: Some(_)` — none exist in this repo yet, see
    /// `docs/ENTERPRISE_LICENSING.md`), `true` only if the installed
    /// license's `connectors` list covers its slug. The canvas uses this to
    /// show a locked/"upgrade" state instead of hiding the node outright.
    licensed: bool,
    /// `Some(slug)` for an enterprise-gated connector (mirrors
    /// `ConnectorDescriptor::requires_license`), `None` for OSS. The Store
    /// page (frontend) uses this to tell "always free" apart from
    /// "enterprise, and here's whether you own it" — `licensed` alone can't
    /// distinguish those two cases (both read `true` for OSS).
    #[serde(skip_serializing_if = "Option::is_none")]
    requires_license: Option<&'static str>,
}

async fn list_connectors_handler(
    State(state): State<AppState>,
) -> Json<Vec<ConnectorCatalogEntry>> {
    let active_license = state.license_store.active().await.ok().flatten();
    Json(
        ConnectorRegistry::all()
            // `Capability`-kind descriptors (Marco L8) are license-check
            // targets, not real connectors — never expose them as a node
            // type the Canvas could try to add to a DAG.
            .filter(|d| d.capability != nexus_core::ConnectorCapability::Capability)
            .map(|d| ConnectorCatalogEntry {
                name: d.name,
                capability: d.capability,
                config_schema: (d.config_schema)(),
                licensed: match d.requires_license {
                    None => true,
                    Some(slug) => active_license
                        .as_ref()
                        .is_some_and(|claims| claims.covers(slug)),
                },
                requires_license: d.requires_license,
            })
            .collect(),
    )
}

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResponse {
    token: String,
}

async fn login_handler(
    State(state): State<AppState>,
    client_ip: Option<Extension<rate_limit::ClientIp>>,
    Json(body): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    let client_ip = client_ip.map(|ext| ext.0 .0).unwrap_or_default();
    let result = state
        .auth_store
        .verify(&body.username, &body.password)
        .await
        .map_err(ApiError::internal);

    let log_outcome = match &result {
        Ok(Some(_)) => (true, "login succeeded"),
        Ok(None) => (false, "invalid username or password"),
        Err(_) => (false, "login verification error"),
    };
    server_metrics::record_login_attempt(match &result {
        Ok(Some(_)) => "success",
        Ok(None) => "invalid_credentials",
        Err(_) => "error",
    });
    // Log to stdout/tracing and to the durable audit table. Do not block the
    // response on audit persistence, but surface failures in server logs.
    tracing::info!(
        username = %body.username,
        success = log_outcome.0,
        client_ip = %client_ip,
        "{}", log_outcome.1
    );
    if let Err(e) = state
        .auth_store
        .log_security_event(
            Some(&body.username),
            "login",
            log_outcome.0,
            Some(&client_ip),
        )
        .await
    {
        tracing::warn!(error = %e, "failed to write login audit log");
    }

    let role = result?.ok_or_else(|| ApiError::unauthorized("invalid username or password"))?;
    let token = state.jwt.issue(&body.username, role)?;
    Ok(Json(LoginResponse { token }))
}

#[derive(Serialize)]
struct LogoutResponse {
    revoked: bool,
}

async fn logout_handler(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    headers: axum::http::HeaderMap,
) -> Result<Json<LogoutResponse>, ApiError> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::unauthorized("missing Authorization header"))?;
    let token = header
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::unauthorized("Authorization header must be 'Bearer <token>'"))?;
    state.jwt.blocklist().revoke(token.to_string(), claims.exp);
    Ok(Json(LogoutResponse { revoked: true }))
}

#[derive(Serialize)]
struct RunAccepted {
    run_id: i64,
}

/// `POST /pipelines/{id}/run` — records the run and returns **202 Accepted**
/// with its id immediately; the pipeline itself executes in a detached
/// supervisor task (`execute_pipeline_run`) that always records the terminal
/// state. Before this, the pipeline ran inline in the request: a client
/// disconnect (or process restart) stranded the row as `'running'` forever,
/// and the scheduler — which never overlaps a pipeline with itself — would
/// skip it indefinitely.
///
/// RBAC/security: if a pipeline with this id has already been persisted,
/// the stored definition is always used, regardless of what (if anything) the
/// caller put in the body. That prevents an `Execute` caller from smuggling an
/// arbitrary spec in the body of an existing-pipeline run. If the pipeline does
/// not exist, only `Write`/`Admin` callers may submit an ad-hoc spec, and it is
/// validated for SSRF and arbitrary local-file reads before execution.
#[tracing::instrument(skip(state, spec, claims), fields(pipeline_id = %id))]
async fn run_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Extension(claims): Extension<Claims>,
    Json(spec): Json<PipelineSpec>,
) -> Result<(StatusCode, Json<RunAccepted>), ApiError> {
    if spec.pipeline_id != id {
        return Err(ApiError::bad_request(format!(
            "path id {id:?} does not match body.pipeline_id {:?}",
            spec.pipeline_id
        )));
    }
    spec.validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    let persisted = match state.pipelines.get_spec(&id, &state.secrets).await {
        Ok(spec) => Some(spec),
        Err(PipelineStoreError::NotFound(_)) => None,
        Err(e) => return Err(ApiError::from(e)),
    };

    let effective_spec = if let Some(spec) = persisted {
        spec
    } else if claims.role >= Role::Write {
        spec.validate_security_with(state.allow_internal_hosts)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
        spec
    } else {
        return Err(ApiError::forbidden(
            "ad-hoc runs require Write or Admin role",
        ));
    };

    if state
        .pipelines
        .has_running_run(&id)
        .await
        .map_err(ApiError::internal)?
    {
        return Err(ApiError::conflict(
            "a run for this pipeline is already in progress",
        ));
    }

    let run_id = start_pipeline_run(&state, &effective_spec)
        .await
        .map_err(ApiError::internal)?;
    Ok((StatusCode::ACCEPTED, Json(RunAccepted { run_id })))
}

/// Creates the run row and spawns the supervisor task that executes the
/// pipeline — shared by the manual `POST /pipelines/{id}/run` handler above
/// and `scheduler.rs`'s cron-triggered runs, so a scheduled run gets
/// exactly the same history/dbt/alerting behavior as a manually-triggered
/// one, not a second slightly-different code path.
///
/// The progress channel is registered here, *before* the caller's 202
/// response can reach the client — otherwise a client subscribing to
/// `/runs/{run_id}/progress` immediately after the 202 could win the race
/// against the supervisor's own `progress.start` and get a spurious 404.
pub(crate) async fn start_pipeline_run(
    state: &AppState,
    spec: &PipelineSpec,
) -> Result<i64, PipelineStoreError> {
    // Recorded regardless of whether `spec.pipeline_id` was ever persisted
    // via `POST /pipelines` — ad-hoc runs (body-only, no prior create)
    // still show up in `GET /pipelines/{id}/runs`, same as always-persisted
    // ones.
    let run_id = state.pipelines.start_run(&spec.pipeline_id).await?;
    let (progress_tx, log_tx) = state.progress.start(run_id).await;
    let logger = RunLogger::new(run_id, log_tx, state.run_logs.clone());

    let supervisor = state.clone();
    let spec = spec.clone();
    tokio::spawn(async move {
        execute_pipeline_run(supervisor, spec, run_id, progress_tx, logger).await;
    });
    Ok(run_id)
}

/// Supervisor for one pipeline run: executes the pipeline and *always*
/// records the terminal state (`finish_run_success` / `finish_run_failure`)
/// while the process lives — a run row must never be stranded as
/// `'running'` (death of the process itself is handled by the boot-time
/// reaper, `PipelineStore::fail_interrupted_runs`).
#[tracing::instrument(skip(state, spec, progress_tx, logger), fields(pipeline_id = %spec.pipeline_id, run_id))]
async fn execute_pipeline_run(
    state: AppState,
    spec: PipelineSpec,
    run_id: i64,
    progress_tx: ProgressSender,
    logger: RunLogger,
) {
    let started = std::time::Instant::now();
    let mode = if spec.has_transform() {
        if spec.dbt.is_some() {
            "transform+dbt"
        } else {
            "transform"
        }
    } else {
        "linear"
    };
    let source_connectors: Vec<_> = spec.sources.iter().map(|s| s.connector.as_str()).collect();
    let sink_connectors: Vec<_> = spec.sinks.iter().map(|s| s.connector.as_str()).collect();
    logger
        .info(format!(
            "Pipeline {} started (run {run_id}): mode={mode}, sources={source_connectors:?}, sinks={sink_connectors:?}",
            spec.pipeline_id
        ))
        .await;

    // Fetched once per run, not per connector — an expired/uninstalled
    // license just means `active_license` is `None`, which
    // `connectors::check_connector_license` treats the same as "not
    // covered" for any enterprise-gated connector (ROADMAP.md Fase 12,
    // Bloco 1).
    let active_license = state.license_store.active().await.unwrap_or(None);

    let result = runner::run_pipeline(
        &spec,
        &state.checkpoints,
        Some(progress_tx.clone()),
        Some(&logger),
        active_license.as_ref(),
        &state.pipeline_schemas,
        &state.alerts,
        run_id,
        &state.quality_checks,
        &state.llm_stats,
        &state.prompt_templates,
        &state.llm_eval_results,
    )
    .await;
    state.progress.finish(run_id).await;

    match result {
        Ok(stats) => {
            // ELT mode (Marco 10): dbt runs against the sink warehouse only
            // after the raw load lands — a dbt failure fails the whole run,
            // same recording/alerting as a load failure, not a separate
            // "partial success" state.
            let mut dbt_summary = None;
            let mut stats = stats;
            if let Some(dbt_config) = &spec.dbt {
                logger.info("running dbt").await;
                match dbt::run(dbt_config).await {
                    Ok(outcome) => {
                        outcome.log_summary();
                        logger.info("dbt finished").await;
                        // Persist what dbt already computes (parent_map,
                        // per-test detail) instead of letting it vanish
                        // once `outcome` goes out of scope below — feeds
                        // the Lineage tab's dbt sub-graph and (later) the
                        // Quality tab's test history. A failure here logs
                        // and moves on: observability must never fail an
                        // otherwise-successful run (same posture as
                        // `resource_stats`'s background sampler).
                        #[cfg(feature = "dbt")]
                        {
                            if let Some(lineage) = &outcome.lineage {
                                if let Err(e) = state
                                    .dbt_lineage
                                    .record(&spec.pipeline_id, &lineage.parent_map)
                                    .await
                                {
                                    tracing::warn!(error = %e, "failed to persist dbt lineage");
                                }
                            }
                            if let Some(rr) = &outcome.run_results {
                                let test_results: Vec<dbt_test_result_store::DbtTestOutcome> = rr
                                    .results
                                    .iter()
                                    .filter(|r| r.unique_id.split('.').next() == Some("test"))
                                    .map(|r| dbt_test_result_store::DbtTestOutcome {
                                        unique_id: r.unique_id.clone(),
                                        status: r.status.clone(),
                                        message: r.message.clone(),
                                        execution_time: r.execution_time,
                                    })
                                    .collect();
                                if !test_results.is_empty() {
                                    if let Err(e) = state
                                        .dbt_test_results
                                        .record_all(&spec.pipeline_id, run_id, &test_results)
                                        .await
                                    {
                                        tracing::warn!(error = %e, "failed to persist dbt test results");
                                    }
                                    server_metrics::record_dbt_test_results(
                                        &spec.pipeline_id,
                                        &test_results,
                                    );
                                }
                            }
                        }
                        dbt_summary = outcome.summary_json();
                    }
                    Err(e) => {
                        record_run_failure(
                            &state,
                            run_id,
                            &spec.pipeline_id,
                            &e,
                            &logger,
                            started,
                            spec.alerts.as_ref(),
                        )
                        .await;
                        return;
                    }
                }
                // True ETL (approved plan): dbt.output + post_dbt_sinks lets a
                // pipeline read dbt's transformed result back out and write it
                // to a final destination, instead of dbt staying a terminal
                // ELT step. A failure here fails the whole run, same as a dbt
                // failure itself.
                if let Some(output_node) = &dbt_config.output {
                    if !spec.post_dbt_sinks.is_empty() {
                        match runner::run_post_dbt_stage(
                            &spec,
                            output_node,
                            &state.checkpoints,
                            Some(progress_tx.clone()),
                            Some(&logger),
                            active_license.as_ref(),
                        )
                        .await
                        {
                            Ok(post_dbt_stats) => stats.extend(post_dbt_stats),
                            Err(e) => {
                                record_run_failure(
                                    &state,
                                    run_id,
                                    &spec.pipeline_id,
                                    &e,
                                    &logger,
                                    started,
                                    spec.alerts.as_ref(),
                                )
                                .await;
                                return;
                            }
                        }
                    }
                }
            }
            let total_rows: usize = stats.iter().map(|s| s.rows_written).sum();
            logger
                .info(format!(
                    "run succeeded: {total_rows} row(s) across {} partition(s)/sink(s)",
                    stats.len()
                ))
                .await;
            if let Err(e) = state
                .pipelines
                .finish_run_success(run_id, &stats, dbt_summary.as_ref())
                .await
            {
                tracing::warn!(error = %e, "failed to record successful pipeline run");
            }
            server_metrics::record_run_outcome(&spec.pipeline_id, "success", started.elapsed());
            state.alerts.notify_pipeline_run(
                spec.alerts.as_ref(),
                &spec.pipeline_id,
                run_id,
                true,
                &format!(
                    "{total_rows} row(s) across {} partition(s)/sink(s)",
                    stats.len()
                ),
            );
        }
        Err(e) => {
            record_run_failure(
                &state,
                run_id,
                &spec.pipeline_id,
                &e,
                &logger,
                started,
                spec.alerts.as_ref(),
            )
            .await
        }
    }
}

async fn record_run_failure(
    state: &AppState,
    run_id: i64,
    pipeline_id: &str,
    error: &anyhow::Error,
    logger: &RunLogger,
    started: std::time::Instant,
    alerts: Option<&nexus_core::AlertsConfig>,
) {
    let error_debug = format!("{error:?}");
    // Full detail (cause chain included) stays in the server log, but the
    // log itself is scrubbed so credentials don't end up in log aggregators.
    tracing::error!(error = %error::sanitize_error(&error_debug), "pipeline run failed");
    // …what gets persisted (readable by any `Read` role via GET
    // /pipelines/{id}/runs) and forwarded to Slack is the sanitized
    // version: connector errors routinely embed connection URIs with
    // credentials.
    let sanitized = error::sanitize_error(&error.to_string());
    logger.error(format!("run failed: {sanitized}")).await;
    if let Err(record_err) = state.pipelines.finish_run_failure(run_id, &sanitized).await {
        tracing::warn!(error = %record_err, "failed to record failed pipeline run");
    }
    server_metrics::record_run_outcome(pipeline_id, "failed", started.elapsed());
    state
        .alerts
        .notify_pipeline_failed(pipeline_id, run_id, &sanitized);
    state
        .alerts
        .notify_pipeline_run(alerts, pipeline_id, run_id, false, &sanitized);
}

async fn create_pipeline_handler(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(spec): Json<PipelineSpec>,
) -> Result<(StatusCode, Json<PipelineSummary>), ApiError> {
    spec.validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if !spec.draft {
        spec.validate_security_with(state.allow_internal_hosts)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
        let active_license = state.license_store.active().await.unwrap_or(None);
        connectors::validate_pipeline_configs(&spec, active_license.as_ref())
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
    }
    state
        .pipelines
        .create(&spec, &state.secrets, &claims.sub)
        .await?;
    #[cfg(feature = "version-history")]
    commit_pipeline_history(
        &state,
        &spec,
        &claims.sub,
        &format!("create pipeline {}", spec.pipeline_id),
    )
    .await;
    let summary = state
        .pipelines
        .get_summary(&spec.pipeline_id, &state.secrets)
        .await?;
    Ok((StatusCode::CREATED, Json(summary)))
}

async fn list_pipelines_handler(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
) -> Result<Json<Vec<PipelineSummary>>, ApiError> {
    let (limit, offset) = pagination.validated()?;
    Ok(Json(
        state
            .pipelines
            .list_summaries(&state.secrets, limit, offset)
            .await?,
    ))
}

async fn get_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<PipelineSummary>, ApiError> {
    Ok(Json(
        state.pipelines.get_summary(&id, &state.secrets).await?,
    ))
}

async fn get_pipeline_spec_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<PipelineSpec>, ApiError> {
    Ok(Json(state.pipelines.get_spec(&id, &state.secrets).await?))
}

/// Query parameters for `GET /pipelines/{id}/preview`.
#[derive(Debug, Deserialize)]
struct PreviewParams {
    /// Resolved name of the source/sink node to preview — same string
    /// `NodeSpec::resolved_name` produces (`source0`, `sink0`, or an
    /// explicit `name` if the node has one).
    node: String,
    #[serde(default)]
    limit: Option<usize>,
}

/// Reads the first `limit` rows of a saved pipeline's source or sink node,
/// for inspecting data without leaving the app. Reuses `build_source` —
/// the exact function a real pipeline run uses — so it only works for
/// connectors that can act as a `Source` (every bidirectional connector,
/// e.g. postgres/sqlite/mongodb/csv, on either their `sources` or `sinks`
/// entry) and clearly rejects the sink-only ones (milvus/qdrant/lancedb/
/// pgvector/pinecone/chromadb/webhook) via that same function's existing
/// "unsupported source connector" error — no per-connector code needed
/// here. Ad-hoc (unsaved) pipelines aren't supported, same restriction as
/// `get_pipeline_spec_handler` above.
async fn preview_node_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<PreviewParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let spec = state.pipelines.get_spec(&id, &state.secrets).await?;
    let limit = params
        .limit
        .unwrap_or(DEFAULT_PREVIEW_LIMIT)
        .min(MAX_PREVIEW_LIMIT);

    let node = find_node_by_resolved_name(&spec, &params.node).ok_or_else(|| {
        ApiError::not_found(format!(
            "node {:?} not found in pipeline {id:?}",
            params.node
        ))
    })?;

    let active_license = state.license_store.active().await.unwrap_or(None);
    let (_, source) = crate::connectors::build_source(node, 0, active_license.as_ref())
        .await
        .map_err(|e| ApiError::bad_request(crate::error::sanitize_error(&e.to_string())))?;

    let rows = read_preview_rows(source, limit).await?;
    Ok(Json(serde_json::json!({ "rows": rows })))
}

/// Pulls the first `limit` rows off a freshly-connected `Source` and
/// renders them as JSON — shared by `preview_node_handler` (saved
/// pipeline, node looked up by name) and `preview_adhoc_handler` (a bare
/// connector/config pair, no pipeline needed). The trimming logic exists
/// because a connector's batch size rarely divides `limit` evenly — the
/// last batch pulled is sliced back down instead of just capping the
/// batch count, so the row count in the response always matches `limit`
/// exactly (when the source has that many rows to give).
async fn read_preview_rows(
    mut source: Box<dyn nexus_core::Source>,
    limit: usize,
) -> Result<Vec<serde_json::Value>, ApiError> {
    // A connect/read failure here is the caller actively testing their own
    // connector config in the Preview tab, so the real (sanitized) reason
    // is exactly what they need — not a flat "internal server error" that
    // gives no clue whether the host, credentials, or something else is
    // wrong. Deliberately `upstream_connector_failed` (502), not
    // `bad_request` (400): the frontend's `DataPreviewPanel` treats a 400
    // from this same endpoint as "connector can't be previewed at all"
    // (from `build_source`'s "unsupported source connector" error a few
    // lines up in both callers) — conflating the two would misreport a
    // real connection failure as "not supported".
    let mut stream = source.read_batches().await.map_err(|e| {
        ApiError::upstream_connector_failed(crate::error::sanitize_error(&e.to_string()))
    })?;
    let mut collected = Vec::new();
    let mut row_count = 0usize;
    while row_count < limit {
        match stream.next().await {
            Some(Ok(batch)) => {
                row_count += batch.num_rows();
                collected.push(batch);
            }
            Some(Err(e)) => {
                return Err(ApiError::upstream_connector_failed(
                    crate::error::sanitize_error(&e.to_string()),
                ))
            }
            None => break,
        }
    }
    drop(stream);

    // The last batch pulled may have overshot `limit` — trim it back.
    let total: usize = collected.iter().map(|b| b.num_rows()).sum();
    if total > limit {
        if let Some(last) = collected.pop() {
            let already: usize = collected.iter().map(|b| b.num_rows()).sum();
            collected.push(last.slice(0, limit.saturating_sub(already)));
        }
    }

    if collected.is_empty() {
        return Ok(Vec::new());
    }
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::writer::ArrayWriter::new(&mut buf);
        for batch in &collected {
            writer.write(batch).map_err(ApiError::internal)?;
        }
        writer.finish().map_err(ApiError::internal)?;
    }
    serde_json::from_slice(&buf).map_err(ApiError::internal)
}

/// Body for `POST /connectors/preview`.
#[derive(Debug, Deserialize)]
struct AdhocPreviewRequest {
    /// A single source or sink node — same shape as an entry in
    /// `PipelineSpec::sources`/`sinks`. `name` is ignored (nothing to
    /// resolve against, there's no pipeline).
    #[serde(flatten)]
    node: NodeSpec,
    #[serde(default)]
    limit: Option<usize>,
}

/// Previews a connector's data directly from its config — no saved
/// pipeline required, unlike `GET /pipelines/{id}/preview`. Lets the
/// Canvas show a data sample the moment a source *or* sink node's config
/// is filled in, before the pipeline is ever saved. Same underlying
/// mechanism as the saved-pipeline preview (`build_source` +
/// `read_preview_rows`): works for any connector that can act as a
/// `Source` on either side, same "sink-only connectors are rejected"
/// behavior via `build_source`'s own error for those.
///
/// Unlike a saved pipeline's node — already checked by
/// `validate_security_with` in `create_pipeline_handler`/
/// `update_pipeline_handler` before it was ever persisted — this config
/// arrives fresh on every call and was never validated by anything. Run
/// the same SSRF/path-traversal check here, for the same reason
/// `run_pipeline_handler` requires `Write`/`Admin` for an unsaved spec:
/// a bare config is an arbitrary caller-controlled outbound connection
/// (REST URL, Mongo/Kafka/MQTT/Postgres/MySQL/ODBC host, csv/parquet
/// object-store URI) until proven otherwise.
async fn preview_adhoc_handler(
    State(state): State<AppState>,
    Json(req): Json<AdhocPreviewRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = req
        .limit
        .unwrap_or(DEFAULT_PREVIEW_LIMIT)
        .min(MAX_PREVIEW_LIMIT);

    let probe_spec = PipelineSpec {
        pipeline_id: "adhoc-preview".to_string(),
        sources: vec![req.node.clone()],
        transform: None,
        sinks: Vec::new(),
        embedding: None,
        llm: None,
        python: None,
        channel_capacity: 100,
        partitions: 1,
        dbt: None,
        post_dbt_sinks: Vec::new(),
        schedule: None,
        alerts: None,
        quality_checks: Vec::new(),
        draft: false,
    };
    probe_spec
        .validate_security_with(state.allow_internal_hosts)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    let active_license = state.license_store.active().await.unwrap_or(None);
    let (_, source) = crate::connectors::build_source(&req.node, 0, active_license.as_ref())
        .await
        .map_err(|e| ApiError::bad_request(crate::error::sanitize_error(&e.to_string())))?;

    let rows = read_preview_rows(source, limit).await?;
    Ok(Json(serde_json::json!({ "rows": rows })))
}

/// Finds a source or sink node by its resolved name (`source0`/`sink0`, or
/// an explicit `name` — see `NodeSpec::resolved_name`), searching sources
/// first, then sinks.
fn find_node_by_resolved_name<'a>(spec: &'a PipelineSpec, name: &str) -> Option<&'a NodeSpec> {
    spec.sources
        .iter()
        .enumerate()
        .find(|(i, n)| n.resolved_name(*i, "source").as_deref().ok() == Some(name))
        .or_else(|| {
            spec.sinks
                .iter()
                .enumerate()
                .find(|(i, n)| n.resolved_name(*i, "sink").as_deref().ok() == Some(name))
        })
        .map(|(_, n)| n)
}

async fn update_pipeline_handler(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(spec): Json<PipelineSpec>,
) -> Result<Json<PipelineSummary>, ApiError> {
    if spec.pipeline_id != id {
        return Err(ApiError::bad_request(format!(
            "path id {id:?} does not match body.pipeline_id {:?}",
            spec.pipeline_id
        )));
    }
    spec.validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if !spec.draft {
        spec.validate_security_with(state.allow_internal_hosts)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
        let active_license = state.license_store.active().await.unwrap_or(None);
        connectors::validate_pipeline_configs(&spec, active_license.as_ref())
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
    }
    state
        .pipelines
        .update(&id, &spec, &state.secrets, &claims.sub)
        .await?;
    #[cfg(feature = "version-history")]
    commit_pipeline_history(
        &state,
        &spec,
        &claims.sub,
        &format!("update pipeline {}", spec.pipeline_id),
    )
    .await;
    Ok(Json(
        state.pipelines.get_summary(&id, &state.secrets).await?,
    ))
}

/// Mirrors a just-persisted `PipelineSpec` save into `state.git_history`,
/// at `pipelines/{id}.json`. Content is the same `spec_ciphertext` the SQL
/// row already carries (`pipeline_store::encode_spec`, re-encrypted here
/// under the same key) — never the decrypted JSON, so the git history
/// never becomes a second, unrevocable place connector secrets leak to
/// (see CLAUDE.md §5). Best-effort: a failure here is a real gap in
/// history — but it must never fail the save itself (the SQL write above
/// already committed): logged via `tracing::warn!` and swallowed, same
/// posture as `alerts.rs`'s fire-and-forget notifications.
#[cfg(feature = "version-history")]
async fn commit_pipeline_history(
    state: &AppState,
    spec: &PipelineSpec,
    author: &str,
    message: &str,
) {
    let ciphertext = pipeline_store::encode_spec(spec, &state.secrets);
    let path = format!("pipelines/{}.json", spec.pipeline_id);
    match state
        .git_history
        .commit_blob(&path, ciphertext.as_bytes(), message, author)
        .await
    {
        Ok(_) => maybe_push_git_history_to_remote(state).await,
        Err(e) => tracing::warn!(
            pipeline_id = %spec.pipeline_id,
            error = %e,
            "failed to record git version history for this pipeline save"
        ),
    }
}

/// If a GitHub remote is configured (`PUT /settings/git-remote`) *and* the
/// active license covers `git-history-github-sync`, mirrors `main` to it
/// in the background — Part 4 of the git-versioning follow-up plan,
/// "caso o usuário queira". Both the license check and the push itself
/// are best-effort and fire-and-forget: neither ever holds up or fails
/// the pipeline/prompt save that triggered it, same posture as
/// `alerts.rs`'s notifications and `commit_pipeline_history` above. Not
/// gated by license at all when no remote is configured — this is a
/// no-op, not a check that needs to run either way.
#[cfg(feature = "version-history")]
async fn maybe_push_git_history_to_remote(state: &AppState) {
    let Ok(Some(remote)) = state.git_remote.get(&state.secrets).await else {
        return;
    };
    let active_license = state.license_store.active().await.unwrap_or(None);
    if connectors::check_connector_license("git-history-github-sync", active_license.as_ref())
        .is_err()
    {
        return;
    }
    let git_history = state.git_history.clone();
    tokio::spawn(async move {
        if let Err(e) = git_history
            .push_to_remote(&remote.remote_url, &remote.token)
            .await
        {
            tracing::warn!(
                error = %e,
                "failed to push git version history to the configured GitHub remote"
            );
        }
    });
}

#[cfg(feature = "version-history")]
#[derive(Debug, Deserialize)]
struct SetGitRemoteRequest {
    /// e.g. `https://github.com/acme/pipelines.git`.
    remote_url: String,
    /// A GitHub personal access token (or App installation token) with
    /// push access to `remote_url` — sent over HTTPS as `x-access-token`
    /// (see `git_history_store::GitHistoryStore::push_to_remote`).
    /// Encrypted at rest, never returned by any endpoint.
    token: String,
}

/// `PUT /settings/git-remote` — configures (or replaces) the optional
/// GitHub mirror for `git_history`. Does not itself check the
/// `git-history-github-sync` license (that check happens per-push, in
/// `maybe_push_git_history_to_remote`) — an unlicensed remote can be
/// configured ahead of time, it just won't be pushed to until a covering
/// license is installed.
#[cfg(feature = "version-history")]
async fn set_git_remote_handler(
    State(state): State<AppState>,
    Json(body): Json<SetGitRemoteRequest>,
) -> Result<StatusCode, ApiError> {
    if body.remote_url.trim().is_empty() {
        return Err(ApiError::bad_request("remote_url must not be empty"));
    }
    if body.token.trim().is_empty() {
        return Err(ApiError::bad_request("token must not be empty"));
    }
    state
        .git_remote
        .set(&body.remote_url, &body.token, &state.secrets)
        .await
        .map_err(ApiError::internal)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /settings/git-remote` — turns the push mirror back off.
/// `git_history` itself (the embedded local repo) is entirely unaffected —
/// this only stops the best-effort push, never touches history already
/// recorded.
#[cfg(feature = "version-history")]
async fn delete_git_remote_handler(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    state
        .git_remote
        .delete()
        .await
        .map_err(ApiError::internal)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /pipelines/{id}/versions` — every commit that changed this
/// pipeline, newest first. Metadata only (author/message/timestamp), no
/// spec content — `.../diff` is where the actual (redacted) content
/// comparison happens.
#[cfg(feature = "version-history")]
async fn list_pipeline_versions_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<git_history_store::VersionMeta>>, ApiError> {
    let path = format!("pipelines/{id}.json");
    Ok(Json(
        state
            .git_history
            .history_for(&path)
            .await
            .map_err(ApiError::internal)?,
    ))
}

#[cfg(feature = "version-history")]
#[derive(Debug, Deserialize)]
struct PipelineDiffParams {
    /// Commit to diff against — defaults to the pipeline's current, live
    /// state (same shape `GET /pipelines/{id}` returns) when omitted.
    against: Option<String>,
}

/// `GET /pipelines/{id}/versions/{commit}/diff?against={commit}` — a
/// unified text diff between two snapshots of a pipeline. Never touches
/// raw `NodeSpec.config` (which may carry connector secrets): both sides
/// are reduced to the same redacted shape `PipelineSummary` already
/// exposes over the API (`pipeline_store::redact_for_diff`) before being
/// diffed, so this can only ever reveal what `GET /pipelines/{id}`
/// already would.
#[cfg(feature = "version-history")]
async fn diff_pipeline_versions_handler(
    State(state): State<AppState>,
    Path((id, commit)): Path<(String, String)>,
    Query(params): Query<PipelineDiffParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let path = format!("pipelines/{id}.json");
    let old_spec = load_pipeline_spec_at_commit(&state, &commit, &path).await?;
    let new_json = match &params.against {
        Some(against) => {
            let new_spec = load_pipeline_spec_at_commit(&state, against, &path).await?;
            pipeline_store::redact_for_diff(&new_spec)
        }
        None => {
            pipeline_store::redact_for_diff(&state.pipelines.get_spec(&id, &state.secrets).await?)
        }
    };
    let old_json = pipeline_store::redact_for_diff(&old_spec);
    Ok(Json(serde_json::json!({
        "diff": unified_text_diff(&old_json, &new_json),
    })))
}

/// `POST /pipelines/{id}/versions/{commit}/rollback` — restores the
/// pipeline to an old commit's content by writing it through the exact
/// same `PipelineStore::update` path a normal save uses, under the
/// current caller's name. This *creates a new commit* on top of history
/// (`"rollback pipeline {id} to {commit}"`); it never rewrites or resets
/// the git ref — same "immutable log" posture as checkpoints elsewhere in
/// this codebase.
#[cfg(feature = "version-history")]
async fn rollback_pipeline_handler(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path((id, commit)): Path<(String, String)>,
) -> Result<Json<PipelineSummary>, ApiError> {
    let path = format!("pipelines/{id}.json");
    let old_spec = load_pipeline_spec_at_commit(&state, &commit, &path).await?;
    if old_spec.pipeline_id != id {
        return Err(ApiError::bad_request(format!(
            "commit {commit:?} does not belong to pipeline {id:?}"
        )));
    }
    old_spec
        .validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    state
        .pipelines
        .update(&id, &old_spec, &state.secrets, &claims.sub)
        .await?;
    commit_pipeline_history(
        &state,
        &old_spec,
        &claims.sub,
        &format!("rollback pipeline {id} to {commit}"),
    )
    .await;
    Ok(Json(
        state.pipelines.get_summary(&id, &state.secrets).await?,
    ))
}

/// Shared by the diff and rollback handlers above: reads the ciphertext
/// blob at `commit`/`path` out of git history and decrypts it back into a
/// `PipelineSpec`, mapping every failure mode (bad commit, path not in
/// that commit, corrupt/undecryptable ciphertext) to a clear 404/500
/// instead of a panic.
#[cfg(feature = "version-history")]
async fn load_pipeline_spec_at_commit(
    state: &AppState,
    commit: &str,
    path: &str,
) -> Result<PipelineSpec, ApiError> {
    let bytes = state
        .git_history
        .blob_at(commit, path)
        .await
        .map_err(|e| ApiError::not_found(e.to_string()))?;
    let ciphertext = String::from_utf8(bytes)
        .map_err(|e| ApiError::internal(format!("corrupt git history blob: {e}")))?;
    pipeline_store::decode_spec(&ciphertext, &state.secrets).map_err(ApiError::from)
}

/// `GET /prompts/{name}/versions` — every version of `name` (already
/// tracked by `PromptTemplateStore` itself, LLMOPS_IMPLEMENTATION_PLAN.md
/// Marco L4) enriched with the git author/commit that recorded it. Each
/// version lives at its own never-overwritten path
/// (`prompts/{name}/v{n}.txt`), so `history_for` always resolves to
/// exactly one commit per version — `None` only for a version saved
/// before this feature existed (git history didn't exist yet to record
/// it).
#[cfg(feature = "version-history")]
#[derive(Debug, Serialize)]
struct PromptVersionInfo {
    version: u32,
    created_at: String,
    author: Option<String>,
    commit: Option<String>,
}

#[cfg(feature = "version-history")]
async fn list_prompt_versions_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<PromptVersionInfo>>, ApiError> {
    let all = state
        .prompt_templates
        .list()
        .await
        .map_err(ApiError::internal)?;
    let mut out = Vec::new();
    for p in all.into_iter().filter(|p| p.name == name) {
        let path = format!("prompts/{}/v{}.txt", p.name, p.version);
        let commit_meta = state
            .git_history
            .history_for(&path)
            .await
            .map_err(ApiError::internal)?
            .into_iter()
            .next();
        out.push(PromptVersionInfo {
            version: p.version,
            created_at: p.created_at,
            author: commit_meta.as_ref().map(|c| c.author.clone()),
            commit: commit_meta.map(|c| c.commit),
        });
    }
    Ok(Json(out))
}

#[cfg(feature = "version-history")]
#[derive(Debug, Deserialize)]
struct PromptDiffParams {
    /// Version to diff against — defaults to the newest version when
    /// omitted (same "None means latest" convention as the pipeline diff
    /// endpoint above).
    against: Option<u32>,
}

/// `GET /prompts/{name}/versions/{version}/diff?against={version}` — plain
/// text diff between two versions' template text. Reads straight from
/// `PromptTemplateStore::resolve` (already the source of truth for prompt
/// text) — unlike the pipeline diff endpoint, this never needs to touch
/// git history at all, since prompt versions are never overwritten in SQL
/// either. Prompts carry no secrets (CLAUDE.md §5 is about connector
/// config, not prompt text), so this is a plain, unredacted text diff.
#[cfg(feature = "version-history")]
async fn diff_prompt_versions_handler(
    State(state): State<AppState>,
    Path((name, version)): Path<(String, u32)>,
    Query(params): Query<PromptDiffParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let old_text = state
        .prompt_templates
        .resolve(&name, Some(version))
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("prompt {name:?} has no version {version}")))?;
    let new_text = state
        .prompt_templates
        .resolve(&name, params.against)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| match params.against {
            Some(v) => ApiError::not_found(format!("prompt {name:?} has no version {v}")),
            None => ApiError::not_found(format!("prompt {name:?} not found")),
        })?;
    Ok(Json(serde_json::json!({
        "diff": similar::TextDiff::from_lines(&old_text, &new_text)
            .unified_diff()
            .to_string(),
    })))
}

/// Shared unified-diff renderer for the two JSON-shaped (redacted)
/// pipeline snapshots `diff_pipeline_versions_handler` compares — both
/// sides are pretty-printed first so the diff reads as one line per
/// field, not one giant single-line JSON blob.
#[cfg(feature = "version-history")]
fn unified_text_diff(old: &serde_json::Value, new: &serde_json::Value) -> String {
    let old_text = serde_json::to_string_pretty(old).unwrap_or_default();
    let new_text = serde_json::to_string_pretty(new).unwrap_or_default();
    similar::TextDiff::from_lines(&old_text, &new_text)
        .unified_diff()
        .to_string()
}

async fn delete_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let run_ids = state.pipelines.delete(&id).await?;
    // Best-effort, same as delete_run_handler: a failure here can't roll
    // back the pipeline deletion above.
    for run_id in run_ids {
        if let Err(e) = state.run_logs.delete(run_id).await {
            tracing::warn!(run_id, error = %e, "failed to delete logs for a deleted pipeline's run");
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_run_handler(
    State(state): State<AppState>,
    Path((pipeline_id, run_id)): Path<(String, i64)>,
) -> Result<StatusCode, ApiError> {
    match state.pipelines.delete_run(&pipeline_id, run_id).await? {
        DeleteRunOutcome::Deleted => {
            // Best-effort: pipeline_run_logs has no FK to pipeline_runs
            // (see run_log_store.rs), so a failure here can't roll back
            // the run deletion above — just leaves orphaned log rows,
            // same trade-off as everywhere else in this codebase that
            // treats logging as best-effort.
            if let Err(e) = state.run_logs.delete(run_id).await {
                tracing::warn!(run_id, error = %e, "failed to delete logs for a deleted run");
            }
            Ok(StatusCode::NO_CONTENT)
        }
        DeleteRunOutcome::StillRunning => Err(ApiError::conflict(format!(
            "run {run_id} is still running — wait for it to finish before deleting it"
        ))),
        DeleteRunOutcome::NotFound => Err(ApiError::not_found(format!(
            "run {run_id} not found for pipeline {pipeline_id:?}"
        ))),
    }
}

async fn list_runs_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(pagination): Query<Pagination>,
) -> Result<Json<Vec<RunRecord>>, ApiError> {
    let (limit, offset) = pagination.validated()?;
    let mut runs = state.pipelines.list_runs(&id, limit, offset).await?;
    // `list_runs` never joins llm_stats itself (separate store/table, see
    // `RunRecord.llm_stats`'s doc comment) — filled in here, one lookup per
    // run in the page (bounded by `limit`, same cost class as the dbt/
    // stats JSON already parsed per row above).
    for run in &mut runs {
        if let Ok(Some(stats)) = state.llm_stats.get(run.id).await {
            run.llm_stats = Some(serde_json::json!({
                "tokens_prompt": stats.tokens_prompt,
                "tokens_completion": stats.tokens_completion,
                "cost_estimate": stats.cost_estimate,
            }));
        }
    }
    Ok(Json(runs))
}

/// Every recorded dbt test result for this pipeline, grouped by test —
/// what the Quality tab renders as a per-test pass/fail history. The
/// aggregate counts (`tests_total`/`tests_passed`) already ride along on
/// each `RunRecord.dbt_summary` from `GET /pipelines/{id}/runs`; this is
/// the detail behind them (`dbt_test_result_store.rs`). Companion to
/// `list_quality_check_results_handler` below (native, no-dbt checks,
/// separate store) — the Quality tab merges both, tagged by source.
async fn list_dbt_test_results_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<dbt_test_result_store::DbtTestOutcome>>, ApiError> {
    Ok(Json(
        state
            .dbt_test_results
            .list_for_pipeline(&id)
            .await
            .map_err(ApiError::internal)?,
    ))
}

/// Every recorded native (dbt-independent) quality check result for this
/// pipeline — the `QualityCheckSpec`s configured on the pipeline, evaluated
/// against its materialized output on the `run_transform_pipeline` path
/// (see `nexus_core::quality`'s doc comment and `quality_check_store.rs`).
/// Companion to `list_dbt_test_results_handler`; the Quality tab merges
/// both, tagged by source.
async fn list_quality_check_results_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<nexus_core::QualityCheckOutcome>>, ApiError> {
    Ok(Json(
        state
            .quality_checks
            .list_for_pipeline(&id)
            .await
            .map_err(ApiError::internal)?,
    ))
}

/// Every recorded golden-dataset eval result for this pipeline
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7) — scored per run against the
/// `llm` node's current prompt version, evaluated by `run_llm_eval` on the
/// `run_transform_pipeline` path (`llm_eval_result_store.rs`). Companion to
/// `list_dbt_test_results_handler`/`list_quality_check_results_handler`;
/// the Quality tab renders all three, tagged by source.
async fn list_llm_eval_results_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<llm_eval_result_store::LlmEvalOutcome>>, ApiError> {
    Ok(Json(
        state
            .llm_eval_results
            .list_for_pipeline(&id)
            .await
            .map_err(ApiError::internal)?,
    ))
}

/// Historical/replayable view of a run's execution log — works for a run
/// still in progress, one that already finished, and (the whole point) one
/// nobody had the live WebSocket open for, e.g. a scheduler-triggered run
/// (see `RunLogger`/`RunLogStore`). `_id` (pipeline id) is part of the URL
/// for REST consistency with the other `/pipelines/{id}/runs/...` routes
/// but isn't needed to look the logs up — `run_id` alone is the store's key.
async fn list_run_logs_handler(
    State(state): State<AppState>,
    Path((_id, run_id)): Path<(String, i64)>,
) -> Result<Json<Vec<RunLogEvent>>, ApiError> {
    Ok(Json(
        state
            .run_logs
            .list(run_id)
            .await
            .map_err(ApiError::internal)?,
    ))
}

/// Whole-catalog, pipeline-level lineage graph — every saved pipeline plus
/// the resources its sources/sinks touch, computed fresh on every request
/// (no persistence, no background task; same cost `list_summaries` already
/// pays to decrypt every saved spec). See `lineage.rs`'s module doc for the
/// allowlist that keeps connector secrets out of the response.
async fn lineage_handler(
    State(state): State<AppState>,
) -> Result<Json<lineage::LineageGraph>, ApiError> {
    let specs: Vec<_> = state
        .pipelines
        .list_all_specs(&state.secrets)
        .await
        .map_err(ApiError::internal)?
        .into_iter()
        // Drafts only have `pipeline_id` validated (dag.rs's own doc
        // comment on `draft`) — connector configs may be incomplete or
        // garbage, so they'd pollute the graph rather than inform it.
        .filter(|spec| !spec.draft)
        .collect();
    let dbt_lineages = state
        .dbt_lineage
        .get_all()
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(lineage::build_graph(&specs, &dbt_lineages)))
}

/// Column-level detail for one pipeline's Lineage node — source/output
/// columns and (when the pipeline has a SQL transform) column-to-column
/// provenance. Fetched on demand (clicking the node in the Lineage tab),
/// not bundled into `GET /lineage` — most pipelines are never inspected at
/// this level, no reason to decrypt+compute it for every node on every
/// tab load. 404 when the pipeline has never run (`PipelineSchemaStore`
/// only ever holds a captured run's schema, see `runner.rs`).
async fn pipeline_schema_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<pipeline_schema_store::PipelineSchema>, ApiError> {
    state
        .pipeline_schemas
        .get(&id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("no schema captured yet for pipeline {id:?}")))
        .map(Json)
}

#[derive(Deserialize)]
struct ResourceStatsQuery {
    /// `<number><unit>` (`5m`/`45m`/`3h`/`12d`, ...) — free-form, not just
    /// the 5 preset shortcuts the frontend also offers. Defaults to `5m`,
    /// same default the frontend applies when the tab first opens.
    range: Option<String>,
}

/// Historical CPU/memory/disk usage for the "resources" tab, bucketed to
/// ~120 points regardless of range (see `resource_stats::bucket_samples`).
/// Sampling itself runs continuously in the background
/// (`resource_stats::spawn`), independent of any pipeline run — unlike the
/// per-run `hardware_stats` frame on the progress WebSocket.
async fn resource_stats_handler(
    State(state): State<AppState>,
    Query(query): Query<ResourceStatsQuery>,
) -> Result<Json<Vec<resource_stats::ResourceStatsBucket>>, ApiError> {
    let range_str = query.range.as_deref().unwrap_or("5m");
    let lookback = resource_stats::parse_range(range_str)
        .map_err(|e| ApiError::bad_request(format!("invalid range {range_str:?}: {e}")))?;
    let cutoff =
        chrono::Utc::now() - chrono::Duration::from_std(lookback).expect("bounded by parse_range");
    let samples = state
        .resource_stats
        .range(cutoff)
        .await
        .map_err(ApiError::internal)?;
    let bucket_width = resource_stats::bucket_width_for(lookback);
    Ok(Json(resource_stats::bucket_samples(&samples, bucket_width)))
}

#[derive(Deserialize)]
struct BrowseQuery {
    /// Absolute path to list. Empty/absent lists `/`.
    path: Option<String>,
}

/// Lists a directory's contents for the Canvas "browse path" file picker.
///
/// Deliberately **no extra sandbox root** on top of whatever the server
/// process can read: a local-path connector (`csv`/`parquet`/`sqlite`/...)
/// already reads/writes any absolute path the process can access the moment
/// it's typed into a node's config (`nexus_core::
/// submit_local_path_connector!` is the existing opt-out from the SSRF path
/// guard in `dag.rs`). This endpoint grants no new capability — it only
/// makes that same space visually discoverable instead of requiring the
/// caller to already know the path blind. Gated at `Write`, the same role
/// already required to edit a Canvas node's config.
async fn browse_fs_handler(
    Query(query): Query<BrowseQuery>,
) -> Result<Json<browse::BrowseListing>, ApiError> {
    let path = query
        .path
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "/".to_string());
    tokio::task::spawn_blocking(move || browse::list_directory(std::path::Path::new(&path)))
        .await
        .map_err(ApiError::internal)?
        .map(Json)
        .map_err(|e| ApiError::bad_request(format!("could not list path: {e}")))
}

#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
    password: String,
    role: Role,
}

#[derive(Serialize)]
struct UserResponse {
    username: String,
    role: Role,
}

async fn list_users_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<UserResponse>>, ApiError> {
    let users = state
        .auth_store
        .list_users()
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(
        users
            .into_iter()
            .map(|(username, role)| UserResponse { username, role })
            .collect(),
    ))
}

async fn get_user_handler(
    State(state): State<AppState>,
    Path(username): Path<String>,
) -> Result<Json<UserResponse>, ApiError> {
    let user = state
        .auth_store
        .get_user(&username)
        .await
        .map_err(ApiError::internal)?;
    match user {
        Some((username, role)) => Ok(Json(UserResponse { username, role })),
        None => Err(ApiError::not_found(format!("user {username:?} not found"))),
    }
}

async fn create_user_handler(
    State(state): State<AppState>,
    Json(body): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<UserResponse>), ApiError> {
    if body.username.trim().is_empty() {
        return Err(ApiError::bad_request("username must not be empty"));
    }
    if body.password.len() < 8 {
        return Err(ApiError::bad_request(
            "password must be at least 8 characters",
        ));
    }
    state
        .auth_store
        .create_user(&body.username, &body.password, body.role)
        .await
        .map_err(|e| match e.to_string().contains("UNIQUE") {
            true => ApiError::conflict(format!("user {:?} already exists", body.username)),
            false => ApiError::internal(e),
        })?;
    Ok((
        StatusCode::CREATED,
        Json(UserResponse {
            username: body.username,
            role: body.role,
        }),
    ))
}

#[derive(Deserialize)]
struct InstallLicenseRequest {
    license_key: String,
}

#[derive(Serialize)]
struct LicenseStatusResponse {
    active: bool,
    connectors: Vec<String>,
    seats: u32,
    expires_at: Option<i64>,
}

impl LicenseStatusResponse {
    fn inactive() -> Self {
        Self {
            active: false,
            connectors: Vec::new(),
            seats: 0,
            expires_at: None,
        }
    }
}

/// Installs (or replaces) the enterprise license key — see
/// `docs/ENTERPRISE_LICENSING.md`. No enterprise connector actually reads
/// this yet (none exist in this repo); this is the verification + storage
/// half of the gate, ready for a future connector registration path to
/// consult via `LicenseStore::is_connector_licensed`.
async fn install_license_handler(
    State(state): State<AppState>,
    Json(body): Json<InstallLicenseRequest>,
) -> Result<Json<LicenseStatusResponse>, ApiError> {
    let claims = state.license_store.install(&body.license_key).await?;
    Ok(Json(LicenseStatusResponse {
        active: true,
        connectors: claims.connectors,
        seats: claims.seats,
        expires_at: Some(claims.exp),
    }))
}

async fn license_status_handler(
    State(state): State<AppState>,
) -> Result<Json<LicenseStatusResponse>, ApiError> {
    let status = match state
        .license_store
        .active()
        .await
        .map_err(ApiError::internal)?
    {
        Some(claims) => LicenseStatusResponse {
            active: true,
            connectors: claims.connectors,
            seats: claims.seats,
            expires_at: Some(claims.exp),
        },
        None => LicenseStatusResponse::inactive(),
    };
    Ok(Json(status))
}

#[derive(Deserialize)]
struct CreatePromptRequest {
    name: String,
    template: String,
}

#[derive(Serialize)]
struct CreatePromptResponse {
    name: String,
    version: u32,
}

/// Always inserts a new version — see `PromptTemplateStore::create`'s doc
/// comment for why this never overwrites (LLMOPS_IMPLEMENTATION_PLAN.md
/// Marco L4).
async fn create_prompt_handler(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<CreatePromptRequest>,
) -> Result<Json<CreatePromptResponse>, ApiError> {
    if body.name.trim().is_empty() {
        return Err(ApiError::bad_request("name must not be empty"));
    }
    if body.template.trim().is_empty() {
        return Err(ApiError::bad_request("template must not be empty"));
    }
    let version = state
        .prompt_templates
        .create(&body.name, &body.template, &claims.sub)
        .await
        .map_err(ApiError::internal)?;
    #[cfg(feature = "version-history")]
    {
        let path = format!("prompts/{}/v{}.txt", body.name, version);
        let message = format!("create prompt {} v{}", body.name, version);
        match state
            .git_history
            .commit_blob(&path, body.template.as_bytes(), &message, &claims.sub)
            .await
        {
            Ok(_) => maybe_push_git_history_to_remote(&state).await,
            Err(e) => tracing::warn!(
                name = %body.name,
                version,
                error = %e,
                "failed to record git version history for this prompt save"
            ),
        }
    }
    Ok(Json(CreatePromptResponse {
        name: body.name,
        version,
    }))
}

async fn list_prompts_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<prompt_template_store::PromptTemplate>>, ApiError> {
    Ok(Json(
        state
            .prompt_templates
            .list()
            .await
            .map_err(ApiError::internal)?,
    ))
}

#[derive(Deserialize)]
struct UpdateRoleRequest {
    role: Role,
}

async fn update_user_role_handler(
    State(state): State<AppState>,
    Path(username): Path<String>,
    Json(body): Json<UpdateRoleRequest>,
) -> Result<Json<UserResponse>, ApiError> {
    let updated = state
        .auth_store
        .update_user_role(&username, body.role)
        .await
        .map_err(ApiError::internal)?;
    if !updated {
        return Err(ApiError::not_found(format!("user {username:?} not found")));
    }
    Ok(Json(UserResponse {
        username,
        role: body.role,
    }))
}

async fn delete_user_handler(
    State(state): State<AppState>,
    Path(username): Path<String>,
) -> Result<StatusCode, ApiError> {
    let deleted = state
        .auth_store
        .delete_user(&username)
        .await
        .map_err(ApiError::internal)?;
    if !deleted {
        return Err(ApiError::not_found(format!("user {username:?} not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `/pipelines/{id}/runs/{run_id}/progress` — streams that run's
/// `ProgressEvent`s as JSON text frames until the run finishes (server
/// closes) or the client disconnects. `{id}` isn't used for the lookup
/// (progress is keyed by `run_id` alone) — kept in the path purely so the
/// URL reads as "this run, under this pipeline", matching `GET .../runs`.
///
/// The JWT is carried in the `Sec-WebSocket-Protocol` header instead of the
/// query string, so it never appears in URLs, server logs, or browser
/// history. The browser WebSocket API can't set a custom `Authorization`
/// header, but it can request a subprotocol. The client requests
/// `nexusflow-<token>`; we strip the prefix to recover the JWT and echo the
/// same protocol back in the handshake response.
async fn progress_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path((_id, run_id)): Path<(String, i64)>,
) -> Result<Response, ApiError> {
    let proto = ws
        .requested_protocols()
        .next()
        .and_then(|h| h.to_str().ok().map(String::from))
        .ok_or_else(|| ApiError::unauthorized("missing Sec-WebSocket-Protocol"))?;
    let token = proto
        .strip_prefix("nexusflow-")
        .ok_or_else(|| ApiError::unauthorized("expected nexusflow-<token> protocol"))?;
    let (progress_rx, log_rx) = authorize_progress_subscription(&state, token, run_id).await?;
    let llm_stats = state.llm_stats.clone();
    Ok(ws
        .protocols([proto.clone()])
        .on_upgrade(move |socket| forward_progress(socket, progress_rx, log_rx, run_id, llm_stats)))
}

/// Split out from `progress_ws_handler` so it's callable directly from a
/// test — a `WebSocketUpgrade` extractor requires a real hyper connection
/// (`tower::ServiceExt::oneshot` doesn't provide one, so it always rejects
/// with 426 regardless of what this function would have decided).
#[allow(clippy::type_complexity)]
async fn authorize_progress_subscription(
    state: &AppState,
    token: &str,
    run_id: i64,
) -> Result<
    (
        tokio::sync::broadcast::Receiver<nexus_core::ProgressEvent>,
        tokio::sync::broadcast::Receiver<RunLogEvent>,
    ),
    ApiError,
> {
    let claims = state.jwt.verify(token)?;
    if claims.role < Role::Read {
        return Err(ApiError::forbidden(format!(
            "requires {:?} role or higher, caller has {:?}",
            Role::Read,
            claims.role
        )));
    }

    state
        .progress
        .subscribe(run_id)
        .await
        .ok_or_else(|| ApiError::not_found(format!("run {run_id} not found or already finished")))
}

/// How often a `hardware_stats` frame is interleaved into the progress
/// stream — frequent enough to feel "live" without meaningfully competing
/// with `ProgressEvent` traffic for bandwidth.
const HARDWARE_STATS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

async fn forward_progress(
    mut socket: WebSocket,
    mut rx: tokio::sync::broadcast::Receiver<nexus_core::ProgressEvent>,
    mut log_rx: tokio::sync::broadcast::Receiver<RunLogEvent>,
    run_id: i64,
    llm_stats: pipeline_run_llm_stats_store::PipelineRunLlmStatsStore,
) {
    let mut hardware = hardware_stats::HardwareMonitor::new();
    let mut hardware_ticker = tokio::time::interval(HARDWARE_STATS_INTERVAL);
    // The first tick fires immediately; sysinfo's CPU usage is only
    // meaningful as a delta between two refreshes, so the first sample sent
    // to the client is discarded rather than shipped as a misleading 0%.
    hardware_ticker.tick().await;
    hardware.sample();
    // Same interval as hardware_stats, separate ticker (LLMOPS_IMPLEMENTATION_PLAN.md
    // Marco L2) — a DB read per tick instead of an in-memory sample, but
    // negligible for one indexed row every 2s on a single active run.
    // Skipped entirely (no frame sent) when the run has no llm node, unlike
    // hardware_stats which is unconditionally useful for every run.
    let mut llm_stats_ticker = tokio::time::interval(HARDWARE_STATS_INTERVAL);
    llm_stats_ticker.tick().await;

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        let json = serde_json::to_string(&event)
                            .expect("ProgressEvent always serializes");
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    // A slow client missed some events — cumulative counts
                    // mean the next one it does get is still consistent.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            // Persistence of log lines (so `GET .../logs` can replay them
            // later) happens once, at `RunLogger::log` — *not* here. This
            // loop only fans a run's already-persisted events out to
            // whichever WebSocket clients happen to be connected right now;
            // running it per-connection means anything persisted here would
            // duplicate once per subscriber.
            event = log_rx.recv() => {
                match event {
                    Ok(event) => {
                        // `"type": "log"` distinguishes this frame from the
                        // untagged `ProgressEvent`/`hardware_stats` shapes
                        // above — see `RunLogEvent`'s doc comment.
                        let mut payload = serde_json::to_value(&event)
                            .expect("RunLogEvent always serializes");
                        payload["type"] = serde_json::Value::String("log".to_string());
                        if socket.send(Message::Text(payload.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = hardware_ticker.tick() => {
                let stats = hardware.sample();
                let json = serde_json::to_string(&serde_json::json!({ "hardware_stats": stats }))
                    .expect("HardwareStats always serializes");
                if socket.send(Message::Text(json.into())).await.is_err() {
                    break;
                }
            }
            _ = llm_stats_ticker.tick() => {
                if let Ok(Some(stats)) = llm_stats.get(run_id).await {
                    let json = serde_json::to_string(&serde_json::json!({ "llm_stats": {
                        "tokens_prompt": stats.tokens_prompt,
                        "tokens_completion": stats.tokens_completion,
                        "cost_estimate": stats.cost_estimate,
                    } }))
                    .expect("llm stats always serialize");
                    if socket.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
            }
            incoming = socket.recv() => {
                // No client->server protocol — any message or a closed
                // connection both mean "stop forwarding".
                if incoming.is_none() {
                    break;
                }
            }
        }
    }
}

pub struct ServerConfig {
    pub checkpoint_database_url: String,
    pub auth_database_url: String,
    pub pipelines_database_url: String,
    /// Never hardcoded — comes from `NEXUS_JWT_SECRET` (ARCHITECTURE.md §10).
    pub jwt_secret: String,
    pub jwt_ttl_seconds: u64,
    /// `(username, password)` bootstrapped as the sole `Admin` account the
    /// first time the users table is empty — a no-op on every later boot.
    pub bootstrap_admin: Option<(String, String)>,
    /// 64-char hex string (32 raw bytes) — comes from `NEXUS_ENCRYPTION_KEY`.
    /// Encrypts connector secrets at rest (CLAUDE.md §5). See `crypto.rs`.
    pub encryption_key_hex: String,
    /// `NEXUS_SLACK_WEBHOOK_URL` — `None` just means alerting is off, not a
    /// startup failure (see `alerts.rs`).
    pub slack_webhook_url: Option<String>,
    /// `NEXUS_TEAMS_WEBHOOK_URL` — same "off is fine" contract as Slack's.
    pub teams_webhook_url: Option<String>,
    /// `NEXUS_PAGERDUTY_ROUTING_KEY` — same "off is fine" contract as
    /// Slack's. PagerDuty's Events API posts to one fixed endpoint for
    /// every account, so this is a routing key, not a URL (see alerts.rs).
    pub pagerduty_routing_key: Option<String>,
    /// Email alert channel config — `None` means the channel is off.
    pub email: Option<alerts::EmailConfig>,
    /// `NEXUS_ALERT_WEBHOOK_URL` — same "off is fine" contract as Slack's.
    pub webhook_url: Option<String>,
    /// `NEXUS_ALLOW_INTERNAL_HOSTS` — opt-in for self-hosted deployments that
    /// need pipelines to reach their own private network. See
    /// `PipelineSpec::validate_security_with`'s doc for why this defaults to
    /// `false`. Off by default even in this struct: every other field here
    /// documents an env var that degrades gracefully when unset (alerts just
    /// stay off); this one weakens SSRF protection, so it's opt-in only.
    pub allow_internal_hosts: bool,
    /// `NEXUS_TRUST_PROXY_HEADERS` — see `rate_limit::TrustProxyHeaders`'s
    /// doc comment. Same "opt-in only" reasoning as `allow_internal_hosts`
    /// above: trusting `X-Forwarded-For` weakens the login rate limiter
    /// unless a reverse proxy is confirmed to own that header.
    pub trust_proxy_headers: bool,
    /// `NEXUS_GIT_HISTORY_PATH` — where the embedded bare git repo backing
    /// pipeline/prompt version history lives (see `git_history_store.rs`).
    /// Defaults to a path next to the pipelines database.
    #[cfg(feature = "version-history")]
    pub git_history_path: String,
}

async fn build_state(config: &ServerConfig) -> anyhow::Result<AppState> {
    let checkpoints = CheckpointStore::connect(&config.checkpoint_database_url).await?;
    let auth_store = AuthStore::connect(&config.auth_database_url).await?;
    let pipelines = PipelineStore::connect(&config.pipelines_database_url).await?;
    // Same database as `pipeline_runs` (which these logs narrate) — not a
    // 5th env var to operate.
    let run_logs = RunLogStore::connect(&config.pipelines_database_url).await?;
    // Same database as auth (access-control data), not a 4th env var to
    // operate — a license is a single-row table, not worth its own
    // connection pool/URL to configure.
    let license_store = LicenseStore::connect(&config.auth_database_url).await?;
    // Same database as checkpoints, not a 5th env var — same reasoning as
    // `run_logs`/`license_store` above, one small table doesn't need its
    // own connection pool/URL.
    let resource_stats =
        resource_stats::ResourceStatsStore::connect(&config.checkpoint_database_url).await?;
    // Same database as pipelines/run_logs — dbt lineage/test results are
    // intrinsically tied to a pipeline's own runs, not worth a 6th env var.
    let dbt_lineage =
        dbt_lineage_store::DbtLineageStore::connect(&config.pipelines_database_url).await?;
    let dbt_test_results =
        dbt_test_result_store::DbtTestResultStore::connect(&config.pipelines_database_url).await?;
    let pipeline_schemas =
        pipeline_schema_store::PipelineSchemaStore::connect(&config.pipelines_database_url).await?;
    let quality_checks =
        quality_check_store::QualityCheckStore::connect(&config.pipelines_database_url).await?;
    let llm_stats = pipeline_run_llm_stats_store::PipelineRunLlmStatsStore::connect(
        &config.pipelines_database_url,
    )
    .await?;
    let prompt_templates =
        prompt_template_store::PromptTemplateStore::connect(&config.pipelines_database_url).await?;
    let llm_generations =
        llm_generation_store::LlmGenerationStore::connect(&config.pipelines_database_url).await?;
    let llm_eval_results =
        llm_eval_result_store::LlmEvalResultStore::connect(&config.pipelines_database_url).await?;
    if let Some((username, password)) = &config.bootstrap_admin {
        auth_store.seed_admin_if_empty(username, password).await?;
    }
    let token_blocklist = TokenBlocklist::new();
    let jwt = JwtCodec::with_blocklist(
        config.jwt_secret.as_bytes(),
        config.jwt_ttl_seconds,
        token_blocklist,
    );
    let secrets = SecretCipher::from_hex_key(&config.encryption_key_hex)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    #[cfg(feature = "version-history")]
    let git_history = git_history_store::GitHistoryStore::open(&config.git_history_path)?;
    #[cfg(feature = "version-history")]
    let git_remote =
        git_remote_config_store::GitRemoteConfigStore::connect(&config.pipelines_database_url)
            .await?;
    Ok(AppState {
        checkpoints,
        auth_store,
        jwt,
        secrets,
        pipelines,
        run_logs,
        license_store,
        resource_stats,
        dbt_lineage,
        dbt_test_results,
        pipeline_schemas,
        quality_checks,
        llm_stats,
        prompt_templates,
        llm_generations,
        llm_eval_results,
        progress: ProgressHub::default(),
        alerts: AlertNotifier::new(
            AlertConfig {
                slack_webhook_url: config.slack_webhook_url.clone(),
                teams_webhook_url: config.teams_webhook_url.clone(),
                pagerduty_routing_key: config.pagerduty_routing_key.clone(),
                email: config.email.clone(),
                webhook_url: config.webhook_url.clone(),
            },
            config.allow_internal_hosts,
        ),
        login_rate_limiter: std::sync::Arc::new(rate_limit::LoginRateLimiter::default()),
        allow_internal_hosts: config.allow_internal_hosts,
        trust_proxy_headers: config.trust_proxy_headers,
        #[cfg(feature = "version-history")]
        git_history,
        #[cfg(feature = "version-history")]
        git_remote,
    })
}

/// Builds the app without binding a socket — the entrypoint integration
/// tests use (via `tower::ServiceExt::oneshot`) to drive real pipeline runs
/// against a testcontainers Postgres. See IMPLEMENTATION_PLAN.md Marco 1.
/// Deliberately does *not* spawn `scheduler::spawn` — tests built on this
/// don't want an extra background task ticking against their (often
/// in-memory, per-test) `PipelineStore`; `run()` is the only real boot path
/// that starts the scheduler.
pub async fn build_app(config: &ServerConfig) -> anyhow::Result<Router> {
    let state = build_state(config).await?;
    Ok(router(state))
}

/// Parses the optional Email alert channel from environment variables.
/// Returns `None` if the minimum required fields (`SMTP_HOST`, `FROM`, `TO`)
/// are not all present — same "channel is off" contract as the webhook
/// channels.
fn parse_email_config_from_env() -> Option<alerts::EmailConfig> {
    let smtp_host = std::env::var("NEXUS_EMAIL_SMTP_HOST").ok()?;
    let smtp_port = std::env::var("NEXUS_EMAIL_SMTP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(587);
    let from = std::env::var("NEXUS_EMAIL_FROM").ok()?;
    let to = std::env::var("NEXUS_EMAIL_TO")
        .ok()?
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    if to.is_empty() {
        return None;
    }
    Some(alerts::EmailConfig {
        smtp_host,
        smtp_port,
        username: std::env::var("NEXUS_EMAIL_SMTP_USERNAME").ok(),
        password: std::env::var("NEXUS_EMAIL_SMTP_PASSWORD").ok(),
        from,
        to,
    })
}

/// Boots the server. This is the only orchestration entrypoint — `src/main.rs`
/// just calls this, no separate scheduler lives in the main binary
/// (ARCHITECTURE.md §1).
fn validate_jwt_secret(secret: &str) -> anyhow::Result<()> {
    if secret.len() < 32 {
        anyhow::bail!(
            "NEXUS_JWT_SECRET must be at least 32 bytes (256 bits) of entropy for HS256 security; \
             generate one with: openssl rand -hex 32 (ARCHITECTURE.md §10)"
        );
    }
    Ok(())
}

pub async fn run() -> anyhow::Result<()> {
    let database_url =
        std::env::var("NEXUS_CHECKPOINT_DB").unwrap_or_else(|_| "sqlite://nexusflow.db".into());
    let auth_database_url =
        std::env::var("NEXUS_AUTH_DB").unwrap_or_else(|_| "sqlite://nexusflow-auth.db".into());
    let pipelines_database_url = std::env::var("NEXUS_PIPELINES_DB")
        .unwrap_or_else(|_| "sqlite://nexusflow-pipelines.db".into());
    let jwt_secret = std::env::var("NEXUS_JWT_SECRET")
        .map_err(|_| anyhow::anyhow!("NEXUS_JWT_SECRET must be set (ARCHITECTURE.md §10)"))?;
    validate_jwt_secret(&jwt_secret)?;
    let encryption_key_hex = std::env::var("NEXUS_ENCRYPTION_KEY").map_err(|_| {
        anyhow::anyhow!(
            "NEXUS_ENCRYPTION_KEY must be set — a 64-char hex string (32 bytes), \
             e.g. `openssl rand -hex 32` (CLAUDE.md §5)"
        )
    })?;
    let bootstrap_admin = match (
        std::env::var("NEXUS_ADMIN_USERNAME"),
        std::env::var("NEXUS_ADMIN_PASSWORD"),
    ) {
        (Ok(username), Ok(password)) => Some((username, password)),
        _ => {
            tracing::warn!(
                "NEXUS_ADMIN_USERNAME/NEXUS_ADMIN_PASSWORD not set — no admin account will be \
                 bootstrapped if the users table is empty"
            );
            None
        }
    };
    let slack_webhook_url = std::env::var("NEXUS_SLACK_WEBHOOK_URL").ok();
    if slack_webhook_url.is_none() {
        tracing::warn!(
            "NEXUS_SLACK_WEBHOOK_URL not set — pipeline failures will not raise a Slack alert"
        );
    }
    let teams_webhook_url = std::env::var("NEXUS_TEAMS_WEBHOOK_URL").ok();
    if teams_webhook_url.is_none() {
        tracing::warn!(
            "NEXUS_TEAMS_WEBHOOK_URL not set — pipeline failures will not raise a Teams alert"
        );
    }
    let pagerduty_routing_key = std::env::var("NEXUS_PAGERDUTY_ROUTING_KEY").ok();
    if pagerduty_routing_key.is_none() {
        tracing::warn!(
            "NEXUS_PAGERDUTY_ROUTING_KEY not set — pipeline failures will not page PagerDuty"
        );
    }
    let email = parse_email_config_from_env();
    if email.is_none() {
        tracing::warn!(
            "NEXUS_EMAIL_SMTP_HOST/NEXUS_EMAIL_TO not set — pipeline failures will not send email alerts"
        );
    }
    let webhook_url = std::env::var("NEXUS_ALERT_WEBHOOK_URL").ok();
    if webhook_url.is_none() {
        tracing::warn!(
            "NEXUS_ALERT_WEBHOOK_URL not set — pipeline failures will not raise a generic webhook alert"
        );
    }
    let allow_internal_hosts = std::env::var("NEXUS_ALLOW_INTERNAL_HOSTS")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    if allow_internal_hosts {
        tracing::warn!(
            "NEXUS_ALLOW_INTERNAL_HOSTS=true — pipelines may target this deployment's private \
             network (10.0.0.0/8, 192.168.0.0/16, 127.0.0.1, etc). Only set this on trusted \
             self-hosted deployments, never on a multi-tenant/shared instance."
        );
    }
    let trust_proxy_headers = std::env::var("NEXUS_TRUST_PROXY_HEADERS")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    if trust_proxy_headers {
        tracing::warn!(
            "NEXUS_TRUST_PROXY_HEADERS=true — the login rate limiter will trust the \
             X-Forwarded-For header. Only set this when a reverse proxy in front of this \
             process overwrites that header itself; otherwise any direct caller can spoof it \
             and bypass per-IP login rate limiting entirely."
        );
    }
    // Default used to be a bare relative path ("nexusflow-version-history.git",
    // resolved against the process's CWD) — inside the published image that's
    // "/" (no WORKDIR set in the runtime stage), owned by root, not writable
    // by the non-root `nexusflow` user (uid 1001). Nothing had ever turned
    // `version-history` on in a built image before the LLMOps Store
    // integration (see docs/ENTERPRISE_LICENSING.md), so this went
    // undetected: the server crashed on boot ("Permission denied") the
    // first time the feature was actually exercised end-to-end.
    //
    // First fix attempt used "$HOME/nexusflow-version-history.git" — wrong
    // too, verified empirically: this image's minimal runtime environment
    // never exports HOME at all (useradd -r creates no home entry that a
    // non-login process inherits), so it silently fell through to the same
    // "." fallback and crashed the same way. `std::env::temp_dir()`
    // resolves via $TMPDIR with a hardcoded "/tmp" fallback — always
    // exists, always writable, no environment precondition at all. Same
    // "doesn't need to survive a container recreate" posture already used
    // for this deployment's sqlite metadata DBs in local testbeds
    // (docker-compose.nexusflow-test.yml's NEXUS_CHECKPOINT_DB, etc.) — a
    // deployment that wants the git history to persist across restarts
    // sets NEXUS_GIT_HISTORY_PATH to a mounted volume, same as it already
    // must for sqlite-backed metadata.
    #[cfg(feature = "version-history")]
    let git_history_path = std::env::var("NEXUS_GIT_HISTORY_PATH").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join("nexusflow-version-history.git")
            .to_string_lossy()
            .into_owned()
    });

    let state = build_state(&ServerConfig {
        checkpoint_database_url: database_url,
        auth_database_url,
        pipelines_database_url,
        jwt_secret,
        jwt_ttl_seconds: 3600,
        bootstrap_admin,
        encryption_key_hex,
        slack_webhook_url,
        teams_webhook_url,
        pagerduty_routing_key,
        email,
        webhook_url,
        allow_internal_hosts,
        trust_proxy_headers,
        #[cfg(feature = "version-history")]
        git_history_path,
    })
    .await?;

    // Reap runs stranded as 'running' by a previous process (crash, kill,
    // deploy). A run's supervisor always records its terminal state while
    // the process lives, so anything still 'running' at boot is dead — and
    // would otherwise make the scheduler skip that pipeline forever.
    match state.pipelines.fail_interrupted_runs().await {
        Ok(0) => {}
        Ok(reaped) => tracing::warn!(
            reaped,
            "marked interrupted runs from a previous process as failed"
        ),
        Err(e) => {
            tracing::warn!(error = %e, "failed to reap interrupted runs from a previous process")
        }
    }

    // Cron-based automatic pipeline triggering (see scheduler.rs) — only
    // started on the real boot path, not by `build_app` (tests don't want
    // it racing their own assertions).
    scheduler::spawn(state.clone());

    // CPU/memory/disk sampler for the "resources" tab (see
    // resource_stats.rs) — same "only the real boot path" rule as the
    // scheduler above.
    resource_stats::spawn(state.clone());

    let app = router(state);

    let port: u16 = std::env::var("NEXUS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!(%addr, "nexus-server listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}

/// Ctrl+C or SIGTERM (SIGTERM is what Docker/Kubernetes send on stop) —
/// axum then stops accepting new connections and lets in-flight requests
/// finish. Already-spawned run supervisors are detached tasks and are *not*
/// awaited: a run cut short mid-flight is reaped as 'failed' by the next
/// boot's `fail_interrupted_runs`.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received, finishing in-flight requests");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use tower::ServiceExt;

    /// Fresh, isolated bare repo per call — `test_state()`/`rate_limited_state()`
    /// each get their own so parallel `#[tokio::test]`s never share one.
    /// Leaked on purpose (`.keep()`): these are short-lived test processes,
    /// same trade-off other tests make with real filesystem fixtures.
    #[cfg(feature = "version-history")]
    fn test_git_history() -> git_history_store::GitHistoryStore {
        let dir = tempfile::tempdir().unwrap().keep();
        git_history_store::GitHistoryStore::open(dir.join("history.git")).unwrap()
    }

    async fn test_state() -> AppState {
        let auth_store = AuthStore::connect("sqlite::memory:").await.unwrap();
        auth_store
            .seed_admin_if_empty("admin", "test-password")
            .await
            .unwrap();
        AppState {
            checkpoints: CheckpointStore::connect("sqlite::memory:").await.unwrap(),
            auth_store,
            jwt: JwtCodec::new(b"test-secret", 3600),
            secrets: SecretCipher::from_hex_key(&"ab".repeat(32)).unwrap(),
            pipelines: PipelineStore::connect("sqlite::memory:").await.unwrap(),
            run_logs: RunLogStore::connect("sqlite::memory:").await.unwrap(),
            license_store: LicenseStore::connect("sqlite::memory:").await.unwrap(),
            resource_stats: resource_stats::ResourceStatsStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            dbt_lineage: dbt_lineage_store::DbtLineageStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            dbt_test_results: dbt_test_result_store::DbtTestResultStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            pipeline_schemas: pipeline_schema_store::PipelineSchemaStore::connect(
                "sqlite::memory:",
            )
            .await
            .unwrap(),
            quality_checks: quality_check_store::QualityCheckStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            llm_stats: pipeline_run_llm_stats_store::PipelineRunLlmStatsStore::connect(
                "sqlite::memory:",
            )
            .await
            .unwrap(),
            prompt_templates: prompt_template_store::PromptTemplateStore::connect(
                "sqlite::memory:",
            )
            .await
            .unwrap(),
            llm_generations: llm_generation_store::LlmGenerationStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            llm_eval_results: llm_eval_result_store::LlmEvalResultStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            progress: ProgressHub::default(),
            alerts: AlertNotifier::new(AlertConfig::default(), false),
            login_rate_limiter: std::sync::Arc::new(rate_limit::LoginRateLimiter::new(
                std::time::Duration::from_secs(60),
                10_000,
            )),
            allow_internal_hosts: false,
            trust_proxy_headers: false,
            #[cfg(feature = "version-history")]
            git_history: test_git_history(),
            #[cfg(feature = "version-history")]
            git_remote: git_remote_config_store::GitRemoteConfigStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        }
    }

    fn bearer(state: &AppState, role: Role) -> String {
        format!("Bearer {}", state.jwt.issue("test-user", role).unwrap())
    }

    #[tokio::test]
    async fn health_returns_200_ok() {
        let app = router(test_state().await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_endpoint_needs_no_auth_and_returns_prometheus_text() {
        let app = router(test_state().await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(content_type.starts_with("text/plain"));

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        // Valid (if currently empty, since this process hasn't run any real
        // pipeline batches) Prometheus exposition text — must not panic.
        String::from_utf8(bytes.to_vec()).expect("Prometheus text output is valid UTF-8");
    }

    /// Proves the *real* chain end to end — nexus_core's counters, the
    /// process-global OTel meter provider, `PROMETHEUS_REGISTRY`, and the
    /// `/metrics` text encoder — by running a real `PipelineEngine::run_partition`
    /// (with a fake Source/Sink; only the connector is mocked, not the
    /// observability stack) and reading the resulting counter back out of
    /// the HTTP endpoint. Same counters the progress WebSocket reads from
    /// (task #9) — this is the "one source of truth" the task asked for.
    #[tokio::test]
    async fn metrics_reflect_real_batches_written_by_the_engine() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use async_trait::async_trait;
        use futures::stream::{self, BoxStream};
        use nexus_core::{
            CheckpointCursor, NexusError, PartitionHandle, PipelineEngine, Sink, Source,
        };
        use std::sync::Arc;

        struct OneBatchSource(Option<RecordBatch>);
        #[async_trait]
        impl Source for OneBatchSource {
            async fn read_batches(
                &mut self,
            ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
                let batch = self.0.take();
                Ok(Box::pin(stream::iter(batch.into_iter().map(Ok))))
            }
            fn schema(&self) -> arrow_schema::SchemaRef {
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
            }
        }

        struct NullSink;
        #[async_trait]
        impl Sink for NullSink {
            async fn write_batch(&mut self, _batch: RecordBatch) -> Result<(), NexusError> {
                Ok(())
            }
            async fn commit_checkpoint(
                &mut self,
                _cursor: CheckpointCursor,
            ) -> Result<(), NexusError> {
                Ok(())
            }
        }

        // Installs the real global meter provider (idempotent enough for
        // tests: the tracing-subscriber half may already be set by an
        // earlier test in this binary, which is fine — we only need the
        // meter provider side to have run at least once).
        let _ = telemetry::init();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let engine = PipelineEngine::new(8);
        engine
            .run_partition(
                PartitionHandle {
                    partition_id: "metrics-test-partition".to_string(),
                    source: Box::new(OneBatchSource(Some(batch))),
                    sink: Box::new(NullSink),
                },
                None,
                None,
            )
            .await
            .expect("partition runs successfully");

        let app = router(test_state().await);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();

        assert!(text.contains("nexus_pipeline_rows_written_total"));
        assert!(text.contains("metrics-test-partition"));
    }

    #[tokio::test]
    async fn connectors_catalog_requires_at_least_read_role() {
        let app = router(test_state().await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/connectors")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn connectors_catalog_lists_linked_connectors() {
        let state = test_state().await;
        let token = bearer(&state, Role::Read);
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/connectors")
                    .header("authorization", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let names: Vec<&str> = body
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        // nexus-server only links postgres/sqlite today — the point of this
        // test is that the list comes from the registry, not a hardcoded
        // constant, so it'll grow the day another connector is linked in.
        assert!(names.contains(&"postgres"));
        assert!(names.contains(&"sqlite"));
    }

    #[tokio::test]
    async fn login_returns_valid_jwt_for_correct_credentials() {
        let state = test_state().await;
        let app = router(state);

        let peer: SocketAddr = "203.0.113.1:12345".parse().unwrap();
        let response = app
            .oneshot(login_credentials_request(peer, "admin", "test-password"))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn login_rejects_wrong_password() {
        let app = router(test_state().await);

        let peer: SocketAddr = "203.0.113.1:12345".parse().unwrap();
        let response = app
            .oneshot(login_credentials_request(peer, "admin", "wrong"))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn run_pipeline_without_token_is_rejected() {
        let app = router(test_state().await);

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "postgres", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/pipelines/p1/run")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn run_pipeline_with_read_only_role_is_forbidden() {
        let state = test_state().await;
        let token = bearer(&state, Role::Read);
        let app = router(state);

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "postgres", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/pipelines/p1/run")
                    .header("content-type", "application/json")
                    .header("authorization", token)
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn run_rejects_path_body_pipeline_id_mismatch() {
        let state = test_state().await;
        let token = bearer(&state, Role::Execute);
        let app = router(state);

        let body = serde_json::json!({
            "pipeline_id": "body-id",
            "sources": [{"connector": "postgres", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/pipelines/path-id/run")
                    .header("content-type", "application/json")
                    .header("authorization", token)
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn run_rejects_overlap_when_a_run_is_already_in_progress() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);
        let app = router(state.clone());

        // Persist a pipeline so the manual run uses the stored definition.
        let create_response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);

        // Seed a run that is still 'running' without going through the handler.
        state.pipelines.start_run("p1").await.unwrap();

        // A second manual run must be rejected with 409 Conflict (A02).
        let body = sample_pipeline("p1");
        let response = app
            .oneshot(json_request(
                "POST",
                "/pipelines/p1/run",
                &execute_token,
                body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let json = body_json(response).await;
        assert!(json["error"]
            .as_str()
            .unwrap()
            .contains("already in progress"));
    }

    /// Polls GET /pipelines/{id}/runs until the newest run reaches a
    /// terminal state — runs execute in a background supervisor task, so
    /// assertions on run history have to wait for it to settle.
    async fn wait_for_terminal_run(
        app: &Router,
        token: &str,
        pipeline_id: &str,
    ) -> serde_json::Value {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/pipelines/{pipeline_id}/runs"))
                        .header("authorization", token)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let runs = body_json(response).await;
            if let Some(record) = runs.as_array().and_then(|a| a.first()) {
                if record["finished_at"].is_string() {
                    return record.clone();
                }
            }
            if std::time::Instant::now() > deadline {
                panic!("run for {pipeline_id} never reached a terminal state: {runs}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn run_with_unsupported_connector_is_accepted_then_fails_in_history() {
        // Connector support is only known once the background supervisor
        // executes the run, so the run is *accepted* (202) and the failure
        // lands in the run history — not in the HTTP response.
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);
        let app = router(state);

        // Execute callers may only trigger persisted pipelines.
        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "mongodb", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        });
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/pipelines/p1/run",
                &execute_token,
                body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let record = wait_for_terminal_run(&app, &execute_token, "p1").await;
        assert_eq!(record["status"], "failed");
        // Empty `{}` config is invalid for every real connector regardless
        // of which pair the no-transform path routes it through (postgres
        // partitioned vs. the generic passthrough fallback — see
        // runner.rs's `run_passthrough_pipeline`), so this still fails
        // asynchronously; the exact error text is connector-specific
        // (a serde deserialization message), not a fixed "unsupported
        // connector" string anymore.
        assert!(!record["error"].as_str().unwrap().is_empty());
    }

    /// The whole point of persisting logs (not just broadcasting them) is
    /// that `GET .../logs` works after the fact, for a run nobody had the
    /// live WebSocket open for — this hits the endpoint only *after*
    /// `wait_for_terminal_run` confirms the supervisor already finished, so
    /// there's no live subscriber involved at all.
    #[tokio::test]
    async fn run_logs_endpoint_replays_start_and_failure_lines_after_the_run_finished() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "mongodb", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        });
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/pipelines/p1/run",
                &execute_token,
                body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let record = wait_for_terminal_run(&app, &execute_token, "p1").await;
        let run_id = record["id"].as_i64().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/pipelines/p1/runs/{run_id}/logs"))
                    .header("authorization", &execute_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let logs = body_json(response).await;
        let logs = logs.as_array().unwrap();
        assert!(
            logs.iter().any(|l| l["message"]
                .as_str()
                .unwrap_or("")
                .contains("Pipeline p1 started")),
            "expected a 'Pipeline p1 started' line, got: {logs:?}"
        );
        let failure = logs
            .iter()
            .find(|l| l["level"] == "error")
            .expect("an error-level line for the failed run");
        // Empty `{}` mongodb config fails to connect (via
        // `run_passthrough_pipeline`'s `build_source` call, logged through
        // `log_on_err`'s "source 0 (mongodb) connect failed" context) —
        // not the old "unsupported connector" bail, which no longer exists
        // for this connector pair.
        assert!(failure["message"]
            .as_str()
            .unwrap()
            .contains("connect failed"));
    }

    #[tokio::test]
    async fn dbt_test_results_endpoint_returns_what_was_recorded() {
        let state = test_state().await;
        let read_token = bearer(&state, Role::Read);
        // Seeded directly (not via a real dbt run) — this endpoint's own
        // job is just serving back what `DbtTestResultStore` already has,
        // same scope as `dbt_lineage_store.rs`'s own store-level tests.
        state
            .dbt_test_results
            .record_all(
                "p1",
                1,
                &[dbt_test_result_store::DbtTestOutcome {
                    unique_id: "test.proj.not_null_orders_id".to_string(),
                    status: "fail".to_string(),
                    message: Some("3 rows failed".to_string()),
                    execution_time: 0.01,
                }],
            )
            .await
            .unwrap();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1/dbt-tests")
                    .header("authorization", &read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let results = body_json(response).await;
        let results = results.as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["unique_id"], "test.proj.not_null_orders_id");
        assert_eq!(results[0]["status"], "fail");
    }

    // --- Marco L8: llm-lineage-tracking / reactive-rag-cdc enterprise gate ---

    #[cfg(all(
        feature = "llm",
        any(feature = "embeddings", feature = "embeddings-api"),
        any(
            feature = "lancedb",
            feature = "qdrant",
            feature = "milvus",
            feature = "pgvector",
            feature = "pinecone",
            feature = "chromadb"
        )
    ))]
    #[tokio::test]
    async fn generation_lineage_is_forbidden_without_a_covering_license() {
        let state = test_state().await;
        let read_token = bearer(&state, Role::Read);
        // Seeded directly — this test is about the license gate in front
        // of the handler, not about producing a real generation via RAG
        // (already covered by `reactive_rag_cdc_pipeline.rs`/
        // `lancedb_search_integration.rs`).
        let id = state
            .llm_generations
            .record(llm_generation_store::NewGeneration {
                pipeline_id: "p1",
                question: "what is nexusflow?",
                answer: "a data movement framework",
                prompt_name: "rag-prompt",
                prompt_version: 1,
                model: "gpt-test",
                tokens_prompt: 10,
                tokens_completion: 5,
                resource_id: None,
                context_keys: &[],
            })
            .await
            .unwrap();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/lineage/generation/{id}"))
                    .header("authorization", &read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "no license installed — the generation exists (id is real), so a non-403 \
             status here would mean the license check isn't actually gating this handler"
        );
    }

    #[cfg(all(
        feature = "llm",
        any(feature = "embeddings", feature = "embeddings-api"),
        any(
            feature = "lancedb",
            feature = "qdrant",
            feature = "milvus",
            feature = "pgvector",
            feature = "pinecone",
            feature = "chromadb"
        )
    ))]
    #[tokio::test]
    async fn generation_lineage_is_visible_with_a_covering_license() {
        use crate::license::test_support::{claims, sign};

        let state = test_state().await;
        state
            .license_store
            .install(&sign(&claims(vec!["llm-lineage-tracking"])))
            .await
            .unwrap();
        let read_token = bearer(&state, Role::Read);
        let id = state
            .llm_generations
            .record(llm_generation_store::NewGeneration {
                pipeline_id: "p1",
                question: "what is nexusflow?",
                answer: "a data movement framework",
                prompt_name: "rag-prompt",
                prompt_version: 1,
                model: "gpt-test",
                tokens_prompt: 10,
                tokens_completion: 5,
                resource_id: None,
                context_keys: &[],
            })
            .await
            .unwrap();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/lineage/generation/{id}"))
                    .header("authorization", &read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_json(response).await;
        assert_eq!(body["question"], "what is nexusflow?");
    }

    #[tokio::test]
    async fn connectors_catalog_never_exposes_capability_only_slugs() {
        // `llm-lineage-tracking`/`reactive-rag-cdc` (Marco L8,
        // `capability_registry.rs`) are license-check targets, not real
        // connectors — they must never show up as a node type the Canvas
        // could try to add to a DAG.
        let state = test_state().await;
        let read_token = bearer(&state, Role::Read);
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/connectors")
                    .header("authorization", &read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let entries = body_json(response).await;
        let names: Vec<&str> = entries
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap())
            .collect();
        assert!(!names.contains(&"llm-lineage-tracking"));
        assert!(!names.contains(&"reactive-rag-cdc"));
    }

    #[tokio::test]
    async fn dbt_test_results_endpoint_is_empty_for_a_pipeline_that_never_ran_dbt() {
        let state = test_state().await;
        let read_token = bearer(&state, Role::Read);
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/no-such-pipeline/dbt-tests")
                    .header("authorization", &read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await.as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn run_response_run_id_is_immediately_subscribable_for_progress() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);
        let app = router(state.clone());

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let response = app
            .oneshot(json_request(
                "POST",
                "/pipelines/p1/run",
                &execute_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let accepted = body_json(response).await;
        let run_id = accepted["run_id"]
            .as_i64()
            .expect("202 body carries the new run's id");

        // The channel is registered by `start_pipeline_run` *before* the
        // 202 goes out — a client connecting right after the response must
        // never get a spurious 404.
        let read_token = bearer(&state, Role::Read);
        let read_token = read_token.strip_prefix("Bearer ").unwrap().to_string();
        authorize_progress_subscription(&state, &read_token, run_id)
            .await
            .expect("progress channel exists from the moment the 202 is sent");
    }

    #[tokio::test]
    async fn run_rejects_ad_hoc_spec_for_execute_role() {
        let state = test_state().await;
        let token = bearer(&state, Role::Execute);
        let app = router(state);

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "postgres", "config": {}}],
            "sinks": [{"connector": "sqlite", "config": {}}]
        });

        let response = app
            .oneshot(json_request("POST", "/pipelines/p1/run", &token, body))
            .await
            .unwrap();
        // Pipeline p1 was never persisted, and Execute role is not allowed to
        // submit ad-hoc specs, so the request is forbidden.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn run_ad_hoc_rejects_absolute_local_path() {
        let state = test_state().await;
        let token = bearer(&state, Role::Write);
        let app = router(state);

        // "rest", not "sqlite" — sqlite/lancedb/ailake/iceberg/deltalake are
        // exempt from the absolute-path check, their config is local-path-
        // based by design (see dag.rs's `is_local_path_connector`).
        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "rest", "config": {"path": "/etc/passwd"}}],
            "sinks": [{"connector": "sqlite", "config": {"path": "out.db"}}]
        });

        let response = app
            .oneshot(json_request("POST", "/pipelines/p1/run", &token, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn run_ad_hoc_rejects_internal_url() {
        let state = test_state().await;
        let token = bearer(&state, Role::Write);
        let app = router(state);

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "rest", "config": {"base_url": "http://169.254.169.254/latest/meta-data"}}],
            "sinks": [{"connector": "sqlite", "config": {"path": "out.db"}}]
        });

        let response = app
            .oneshot(json_request("POST", "/pipelines/p1/run", &token, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    fn json_request(
        method: &str,
        uri: &str,
        token: &str,
        body: serde_json::Value,
    ) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header("authorization", token)
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn body_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn sample_pipeline(id: &str) -> serde_json::Value {
        serde_json::json!({
            "pipeline_id": id,
            "sources": [{"connector": "postgres", "config": {
                "uri": "postgres://user:pw@host/db",
                "table": "src",
                "primary_key": "id"
            }}],
            "sinks": [{"connector": "sqlite", "config": {
                "uri": "out.db",
                "table": "dst",
                "primary_key": "id"
            }}]
        })
    }

    #[tokio::test]
    async fn create_pipeline_requires_write_role() {
        let state = test_state().await;
        let token = bearer(&state, Role::Execute);
        let app = router(state);

        let response = app
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn create_pipeline_persists_and_masks_config() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let read_token = bearer(&state, Role::Read);
        let app = router(state);

        let create = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);

        let get = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1")
                    .header("authorization", read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::OK);

        let summary = body_json(get).await;
        assert_eq!(summary["pipeline_id"], "p1");
        assert_eq!(summary["sources"][0]["connector"], "postgres");
        assert!(
            summary["sources"][0].get("config").is_none(),
            "connector config (where secrets live) must never appear in a pipeline summary"
        );
    }

    #[tokio::test]
    async fn create_duplicate_pipeline_id_conflicts() {
        let state = test_state().await;
        let token = bearer(&state, Role::Write);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();
        let response = app
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn list_pipelines_returns_created_ones() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let read_token = bearer(&state, Role::Read);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines")
                    .header("authorization", read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let list = body_json(response).await;
        assert_eq!(list.as_array().unwrap().len(), 1);
        assert_eq!(list[0]["pipeline_id"], "p1");
    }

    /// `sample_pipeline` above, but with a distinguishable sink connector —
    /// the version-history tests below diff/rollback on exactly this
    /// field to prove they're reading the *old* content back, not just
    /// re-fetching the current one.
    #[cfg(feature = "version-history")]
    fn sample_pipeline_with_sink(id: &str, sink_connector: &str) -> serde_json::Value {
        let mut spec = sample_pipeline(id);
        spec["sinks"][0]["connector"] = serde_json::json!(sink_connector);
        spec
    }

    #[cfg(feature = "version-history")]
    #[tokio::test]
    async fn pipeline_versions_lists_one_entry_per_save_newest_first() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let read_token = bearer(&state, Role::Read);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline_with_sink("p1", "sqlite"),
            ))
            .await
            .unwrap();
        app.clone()
            .oneshot(json_request(
                "PUT",
                "/pipelines/p1",
                &write_token,
                sample_pipeline_with_sink("p1", "postgres"),
            ))
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1/versions")
                    .header("authorization", read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let versions = body_json(response).await;
        let versions = versions.as_array().unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0]["message"], "update pipeline p1", "newest first");
        assert_eq!(versions[1]["message"], "create pipeline p1");
    }

    #[cfg(feature = "version-history")]
    #[tokio::test]
    async fn pipeline_diff_reports_the_sink_connector_change() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let read_token = bearer(&state, Role::Read);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline_with_sink("p1", "sqlite"),
            ))
            .await
            .unwrap();
        app.clone()
            .oneshot(json_request(
                "PUT",
                "/pipelines/p1",
                &write_token,
                sample_pipeline_with_sink("p1", "postgres"),
            ))
            .await
            .unwrap();

        let versions = body_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/pipelines/p1/versions")
                        .header("authorization", &read_token)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        let create_commit = versions[1]["commit"].as_str().unwrap();

        // No `?against=` — diffs the old commit against the pipeline's
        // current (latest) state.
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/pipelines/p1/versions/{create_commit}/diff"))
                    .header("authorization", read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_json(response).await;
        let diff = body["diff"].as_str().unwrap();
        assert!(diff.contains("sqlite"), "diff:\n{diff}");
        assert!(diff.contains("postgres"), "diff:\n{diff}");
        assert!(
            !diff.contains("postgres://user:pw@host/db"),
            "diff must never leak connector config/secrets:\n{diff}"
        );
    }

    #[cfg(feature = "version-history")]
    #[tokio::test]
    async fn pipeline_rollback_restores_old_content_as_a_new_commit() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let read_token = bearer(&state, Role::Read);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline_with_sink("p1", "sqlite"),
            ))
            .await
            .unwrap();
        app.clone()
            .oneshot(json_request(
                "PUT",
                "/pipelines/p1",
                &write_token,
                sample_pipeline_with_sink("p1", "postgres"),
            ))
            .await
            .unwrap();

        let versions = body_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/pipelines/p1/versions")
                        .header("authorization", &read_token)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        let create_commit = versions[1]["commit"].as_str().unwrap().to_string();

        let rollback = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/pipelines/p1/versions/{create_commit}/rollback"))
                    .header("authorization", &write_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rollback.status(), StatusCode::OK);
        let summary = body_json(rollback).await;
        assert_eq!(
            summary["sinks"][0]["connector"], "sqlite",
            "rollback restored the original sink connector"
        );

        // Rollback is a new commit, not a history rewrite — 3 entries now,
        // not 2.
        let versions_after = body_json(
            app.oneshot(
                Request::builder()
                    .uri("/pipelines/p1/versions")
                    .header("authorization", read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
        )
        .await;
        let versions_after = versions_after.as_array().unwrap();
        assert_eq!(versions_after.len(), 3);
        assert!(versions_after[0]["message"]
            .as_str()
            .unwrap()
            .starts_with("rollback pipeline p1 to"));
    }

    #[cfg(feature = "version-history")]
    #[tokio::test]
    async fn prompt_versions_and_diff_track_authors_and_text_changes() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let read_token = bearer(&state, Role::Read);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/prompts",
                &write_token,
                serde_json::json!({"name": "greet", "template": "Hello v1"}),
            ))
            .await
            .unwrap();
        app.clone()
            .oneshot(json_request(
                "POST",
                "/prompts",
                &write_token,
                serde_json::json!({"name": "greet", "template": "Hello v2"}),
            ))
            .await
            .unwrap();

        let versions = body_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/prompts/greet/versions")
                        .header("authorization", &read_token)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        let versions = versions.as_array().unwrap();
        assert_eq!(versions.len(), 2);
        assert!(versions.iter().all(|v| v["commit"].is_string()));

        let diff = body_json(
            app.oneshot(
                Request::builder()
                    .uri("/prompts/greet/versions/1/diff?against=2")
                    .header("authorization", read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
        )
        .await;
        let diff_text = diff["diff"].as_str().unwrap();
        assert!(diff_text.contains("-Hello v1"), "diff:\n{diff_text}");
        assert!(diff_text.contains("+Hello v2"), "diff:\n{diff_text}");
    }

    #[tokio::test]
    async fn list_pipelines_pagination() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let read_token = bearer(&state, Role::Read);
        let app = router(state);

        for i in 1..=3 {
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/pipelines",
                    &write_token,
                    sample_pipeline(&format!("p{i}")),
                ))
                .await
                .unwrap();
        }

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/pipelines?limit=2&offset=0")
                    .header("authorization", read_token.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let first = body_json(response).await;
        assert_eq!(first.as_array().unwrap().len(), 2);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines?limit=2&offset=2")
                    .header("authorization", read_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let second = body_json(response).await;
        assert_eq!(second.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn create_pipeline_rejects_invalid_connector_config() {
        let state = test_state().await;
        let token = bearer(&state, Role::Write);
        let app = router(state);

        // Postgres config is missing required `table` and `primary_key`.
        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "postgres", "config": {"uri": "postgres://user:pw@host/db"}}],
            "sinks": [{"connector": "sqlite", "config": {"uri": ":memory:", "table": "dst", "primary_key": "id"}}]
        });

        let response = app
            .oneshot(json_request("POST", "/pipelines", &token, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn preview_requires_execute_role() {
        let state = test_state().await;
        let token = bearer(&state, Role::Read);
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/does-not-exist/preview?node=source0")
                    .header("authorization", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn preview_rejects_unknown_node_name() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1/preview?node=does-not-exist")
                    .header("authorization", execute_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[cfg(feature = "milvus")]
    #[tokio::test]
    async fn preview_rejects_connector_with_no_source_impl() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);
        let app = router(state);

        // milvus is sink-only (no `Source` impl exists for it anywhere in
        // the registry) — `preview` must surface `build_source`'s own
        // "unsupported source connector" error, not a 500.
        let spec = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "postgres", "config": {
                "uri": "postgres://user:pw@host/db", "table": "src", "primary_key": "id"
            }}],
            "sinks": [{"connector": "milvus", "config": {
                "url": "http://milvus.example.com:19530", "collection": "docs",
                "primary_key": "id", "embedding_column": "embedding", "dimension": 8
            }}]
        });
        let create = app
            .clone()
            .oneshot(json_request("POST", "/pipelines", &write_token, spec))
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1/preview?node=sink0")
                    .header("authorization", execute_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[cfg(feature = "csv")]
    #[tokio::test]
    async fn preview_reads_first_n_rows_of_a_real_source() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("events.csv");
        std::fs::write(&csv_path, "id,status\n1,pending\n2,paid\n3,pending\n").unwrap();

        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);
        let app = router(state);

        let spec = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "csv", "config": {
                "uri": csv_path.to_str().unwrap(),
                "fields": [
                    {"name": "id", "data_type": "int64"},
                    {"name": "status", "data_type": "utf8"}
                ]
            }}],
            "sinks": [{"connector": "sqlite", "config": {
                "uri": dir.path().join("out.db").to_str().unwrap(),
                "table": "dst",
                "primary_key": "id"
            }}]
        });
        let create = app
            .clone()
            .oneshot(json_request("POST", "/pipelines", &write_token, spec))
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1/preview?node=source0&limit=2")
                    .header("authorization", execute_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_json(response).await;
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2, "limit=2 must cap the row count: {rows:?}");
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[0]["status"], "pending");
    }

    #[tokio::test]
    async fn update_pipeline_changes_stored_spec() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let mut updated = sample_pipeline("p1");
        updated["sinks"][0]["connector"] = serde_json::json!("postgres");
        let response = app
            .clone()
            .oneshot(json_request("PUT", "/pipelines/p1", &write_token, updated))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let summary = body_json(response).await;
        assert_eq!(summary["sinks"][0]["connector"], "postgres");
    }

    #[tokio::test]
    async fn update_rejects_path_body_id_mismatch() {
        let state = test_state().await;
        let token = bearer(&state, Role::Write);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let response = app
            .oneshot(json_request(
                "PUT",
                "/pipelines/p1",
                &token,
                sample_pipeline("different-id"),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn delete_pipeline_then_get_returns_404() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let delete = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/pipelines/p1")
                    .header("authorization", &write_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);

        let get = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1")
                    .header("authorization", write_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn recreating_a_deleted_pipelines_id_does_not_inherit_its_run_history() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);

        // First "p1": create it, give it a couple of runs.
        state.pipelines.start_run("p1").await.unwrap();
        let run2 = state.pipelines.start_run("p1").await.unwrap();
        state
            .pipelines
            .finish_run_success(run2, &[], None)
            .await
            .unwrap();
        let (_progress_tx, log_tx) = state.progress.start(run2).await;
        let logger = RunLogger::new(run2, log_tx, state.run_logs.clone());
        logger.info("first p1's log line").await;

        let app = router(state);
        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let delete = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/pipelines/p1")
                    .header("authorization", &write_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);

        // Second "p1": brand new pipeline reusing the same id.
        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let runs = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1/runs")
                    .header("authorization", execute_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let runs = body_json(runs).await;
        assert!(
            runs.as_array().unwrap().is_empty(),
            "the new p1 must not inherit the deleted p1's run history: {runs:?}"
        );
    }

    #[tokio::test]
    async fn delete_run_removes_it_from_history() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "mongodb", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        });
        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines/p1/run",
                &execute_token,
                body,
            ))
            .await
            .unwrap();
        let record = wait_for_terminal_run(&app, &execute_token, "p1").await;
        let run_id = record["id"].as_i64().unwrap();

        let delete = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/pipelines/p1/runs/{run_id}"))
                    .header("authorization", &write_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);

        let runs = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1/runs")
                    .header("authorization", &write_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let runs = body_json(runs).await;
        assert!(runs.as_array().unwrap().is_empty());

        // Deleting again — already gone — is a 404, not a silent success.
        let redelete = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/pipelines/p1/runs/{run_id}"))
                    .header("authorization", write_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(redelete.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_run_rejects_a_run_still_in_progress() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let run_id = state.pipelines.start_run("p1").await.unwrap();
        let app = router(state);

        let delete = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/pipelines/p1/runs/{run_id}"))
                    .header("authorization", write_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn delete_run_is_scoped_to_the_url_pipeline_id() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let run_id = state.pipelines.start_run("p1").await.unwrap();
        state
            .pipelines
            .finish_run_success(run_id, &[], None)
            .await
            .unwrap();
        let app = router(state);

        // p1's run can't be deleted through p2's URL.
        let delete = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/pipelines/p2/runs/{run_id}"))
                    .header("authorization", write_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn run_records_failed_run_in_history() {
        let state = test_state().await;
        let write_token = bearer(&state, Role::Write);
        let execute_token = bearer(&state, Role::Execute);
        let app = router(state);

        app.clone()
            .oneshot(json_request(
                "POST",
                "/pipelines",
                &write_token,
                sample_pipeline("p1"),
            ))
            .await
            .unwrap();

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "mongodb", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        });
        let run = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/pipelines/p1/run",
                &execute_token,
                body,
            ))
            .await
            .unwrap();
        assert_eq!(run.status(), StatusCode::ACCEPTED);

        let record = wait_for_terminal_run(&app, &execute_token, "p1").await;
        assert_eq!(record["status"], "failed");
        // Same note as run_with_unsupported_connector_is_accepted_then_fails_in_history:
        // empty `{}` config still fails asynchronously, just with a
        // connector-specific deserialization error now instead of a fixed
        // "unsupported connector" string.
        assert!(!record["error"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn run_history_error_is_sanitized_of_credentials() {
        let state = test_state().await;
        let run_id = state.pipelines.start_run("p1").await.unwrap();

        let err = anyhow::anyhow!(
            "ADBC error: failed to connect to postgres://admin:s3cret@db.internal:5432/app: timeout"
        );
        let (_progress_tx, log_tx) = state.progress.start(run_id).await;
        let logger = RunLogger::new(run_id, log_tx, state.run_logs.clone());
        record_run_failure(
            &state,
            run_id,
            "p1",
            &err,
            &logger,
            std::time::Instant::now(),
            None,
        )
        .await;

        let runs = state.pipelines.list_runs("p1", 100, 0).await.unwrap();
        let stored = runs[0].error.as_deref().unwrap();
        assert!(
            !stored.contains("s3cret"),
            "credentials must never reach the run history: {stored}"
        );
        assert!(stored.contains("postgres://***@db.internal:5432/app"));
    }

    #[tokio::test]
    async fn progress_subscription_rejects_invalid_token() {
        let state = test_state().await;
        let err = authorize_progress_subscription(&state, "garbage", 1)
            .await
            .unwrap_err();
        assert_eq!(err.into_response().status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn progress_subscription_404s_for_unknown_run() {
        let state = test_state().await;
        let token = bearer(&state, Role::Read);
        let token = token.strip_prefix("Bearer ").unwrap();

        let err = authorize_progress_subscription(&state, token, 999)
            .await
            .unwrap_err();
        assert_eq!(err.into_response().status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn progress_subscription_succeeds_for_an_active_run() {
        let state = test_state().await;
        let token = bearer(&state, Role::Read);
        let token = token.strip_prefix("Bearer ").unwrap();
        let run_id = state.pipelines.start_run("p1").await.unwrap();
        let (tx, _log_tx) = state.progress.start(run_id).await;

        let (mut rx, _log_rx) = authorize_progress_subscription(&state, token, run_id)
            .await
            .unwrap();

        tx.send(nexus_core::ProgressEvent {
            partition_id: "p0".to_string(),
            batches_written: 1,
            rows_written: 10,
            bytes_written: 100,
            done: false,
        })
        .unwrap();
        assert_eq!(rx.recv().await.unwrap().rows_written, 10);
    }

    #[tokio::test]
    async fn progress_subscription_404s_after_run_finishes() {
        let state = test_state().await;
        let token = bearer(&state, Role::Read);
        let token = token.strip_prefix("Bearer ").unwrap();
        let run_id = state.pipelines.start_run("p1").await.unwrap();
        state.progress.start(run_id).await;
        state.progress.finish(run_id).await;

        let err = authorize_progress_subscription(&state, token, run_id)
            .await
            .unwrap_err();
        assert_eq!(err.into_response().status(), StatusCode::NOT_FOUND);
    }

    /// The only test in this file that binds a real socket — everything else
    /// goes through `tower::ServiceExt::oneshot`, which can't perform an
    /// actual WebSocket upgrade (no real hyper connection backs it, see
    /// `authorize_progress_subscription`'s doc comment). This proves the
    /// wire-level mechanics end to end: real HTTP upgrade, real broadcast
    /// forwarding, real JSON frames — without needing a real connector/ADBC
    /// driver, since it seeds progress directly rather than running a pipeline.
    #[tokio::test]
    async fn progress_websocket_delivers_real_events_over_a_real_socket() {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::Message as WsMessage;

        let state = test_state().await;
        let token = bearer(&state, Role::Read);
        let token = token.strip_prefix("Bearer ").unwrap().to_string();
        let run_id = state.pipelines.start_run("p1").await.unwrap();
        let (tx, _log_tx) = state.progress.start(run_id).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let mut request = format!("ws://{addr}/pipelines/p1/runs/{run_id}/progress")
            .into_client_request()
            .expect("request url is valid");
        request.headers_mut().insert(
            axum::http::header::SEC_WEBSOCKET_PROTOCOL,
            axum::http::HeaderValue::from_str(&format!("nexusflow-{token}"))
                .expect("protocol value is valid"),
        );
        let (mut ws, _response) = tokio_tungstenite::connect_async(request)
            .await
            .expect("real WebSocket handshake succeeds");

        tx.send(nexus_core::ProgressEvent {
            partition_id: "p0".to_string(),
            batches_written: 1,
            rows_written: 42,
            bytes_written: 999,
            done: false,
        })
        .unwrap();

        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
            .await
            .expect("received a message before timing out")
            .expect("stream is not closed")
            .expect("no transport error");
        let WsMessage::Text(text) = msg else {
            panic!("expected a text frame, got {msg:?}");
        };
        let event: nexus_core::ProgressEvent = serde_json::from_str(&text).unwrap();
        assert_eq!(event.partition_id, "p0");
        assert_eq!(event.rows_written, 42);
        assert_eq!(event.bytes_written, 999);

        ws.close(None).await.ok();
    }

    #[test]
    fn rejects_jwt_secret_shorter_than_32_bytes() {
        assert!(validate_jwt_secret("short").is_err());
        assert!(validate_jwt_secret("exactly-31-characters-long-!!!").is_err());
        assert!(validate_jwt_secret(&"x".repeat(32)).is_ok());
    }

    /// `AppState` builder shared by the login-rate-limit tests below —
    /// identical to `test_state()` except the caller picks the limiter and
    /// `trust_proxy_headers`, both of which the base helper hardcodes.
    async fn rate_limited_state(
        limiter: std::sync::Arc<rate_limit::LoginRateLimiter>,
        trust_proxy_headers: bool,
    ) -> AppState {
        let auth_store = AuthStore::connect("sqlite::memory:").await.unwrap();
        auth_store
            .seed_admin_if_empty("admin", "test-password")
            .await
            .unwrap();
        AppState {
            checkpoints: CheckpointStore::connect("sqlite::memory:").await.unwrap(),
            auth_store,
            jwt: JwtCodec::new(b"test-secret", 3600),
            secrets: SecretCipher::from_hex_key(&"ab".repeat(32)).unwrap(),
            pipelines: PipelineStore::connect("sqlite::memory:").await.unwrap(),
            run_logs: RunLogStore::connect("sqlite::memory:").await.unwrap(),
            license_store: LicenseStore::connect("sqlite::memory:").await.unwrap(),
            resource_stats: resource_stats::ResourceStatsStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            dbt_lineage: dbt_lineage_store::DbtLineageStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            dbt_test_results: dbt_test_result_store::DbtTestResultStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            pipeline_schemas: pipeline_schema_store::PipelineSchemaStore::connect(
                "sqlite::memory:",
            )
            .await
            .unwrap(),
            quality_checks: quality_check_store::QualityCheckStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            llm_stats: pipeline_run_llm_stats_store::PipelineRunLlmStatsStore::connect(
                "sqlite::memory:",
            )
            .await
            .unwrap(),
            prompt_templates: prompt_template_store::PromptTemplateStore::connect(
                "sqlite::memory:",
            )
            .await
            .unwrap(),
            llm_generations: llm_generation_store::LlmGenerationStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            llm_eval_results: llm_eval_result_store::LlmEvalResultStore::connect("sqlite::memory:")
                .await
                .unwrap(),
            progress: ProgressHub::default(),
            alerts: AlertNotifier::new(AlertConfig::default(), false),
            login_rate_limiter: limiter,
            allow_internal_hosts: false,
            trust_proxy_headers,
            #[cfg(feature = "version-history")]
            git_history: test_git_history(),
            #[cfg(feature = "version-history")]
            git_remote: git_remote_config_store::GitRemoteConfigStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        }
    }

    /// A login POST with `peer` attached as the connection's `ConnectInfo`
    /// — `oneshot` never opens a real socket, so this is how these tests
    /// simulate "requests arriving from this real address" without one.
    fn login_credentials_request(
        peer: SocketAddr,
        username: &str,
        password: &str,
    ) -> Request<Body> {
        let body = serde_json::json!({"username": username, "password": password});
        Request::builder()
            .method("POST")
            .uri("/auth/login")
            .header("content-type", "application/json")
            .extension(ConnectInfo(peer))
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn login_request(peer: SocketAddr, forwarded_for: Option<&str>) -> Request<Body> {
        let body = serde_json::json!({"username": "admin", "password": "wrong"});
        let mut builder = Request::builder()
            .method("POST")
            .uri("/auth/login")
            .header("content-type", "application/json")
            .extension(ConnectInfo(peer));
        if let Some(xff) = forwarded_for {
            builder = builder.header("x-forwarded-for", xff);
        }
        builder.body(Body::from(body.to_string())).unwrap()
    }

    #[tokio::test]
    async fn login_rate_limits_per_ip() {
        let limiter = std::sync::Arc::new(rate_limit::LoginRateLimiter::new(
            std::time::Duration::from_secs(60),
            2,
        ));
        let app = router(rate_limited_state(limiter, false).await);
        let peer: SocketAddr = "203.0.113.1:12345".parse().unwrap();

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(login_request(peer, None))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }

        let blocked = app.oneshot(login_request(peer, None)).await.unwrap();
        assert_eq!(blocked.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    /// The actual security property from this session's audit finding: with
    /// `trust_proxy_headers: false` (the default), a caller sending a
    /// different `X-Forwarded-For` on every request must not be able to
    /// evade the limit — the real peer address is what's counted.
    #[tokio::test]
    async fn spoofed_forwarded_for_does_not_bypass_the_limit_by_default() {
        let limiter = std::sync::Arc::new(rate_limit::LoginRateLimiter::new(
            std::time::Duration::from_secs(60),
            2,
        ));
        let app = router(rate_limited_state(limiter, false).await);
        let peer: SocketAddr = "203.0.113.1:12345".parse().unwrap();

        for i in 0..2 {
            let response = app
                .clone()
                .oneshot(login_request(peer, Some(&format!("10.0.0.{i}"))))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }

        let blocked = app
            .oneshot(login_request(peer, Some("10.0.0.99")))
            .await
            .unwrap();
        assert_eq!(
            blocked.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a fresh X-Forwarded-For value per request must not reset the limit \
             when the real peer address is unchanged"
        );
    }

    /// The opt-in path: an operator who has confirmed a trusted reverse
    /// proxy owns `X-Forwarded-For` gets the pre-existing behavior back.
    #[tokio::test]
    async fn trusted_proxy_headers_are_honored_when_opted_in() {
        let limiter = std::sync::Arc::new(rate_limit::LoginRateLimiter::new(
            std::time::Duration::from_secs(60),
            2,
        ));
        let app = router(rate_limited_state(limiter, true).await);
        // Same peer (as the proxy itself would present to nexus-server) but
        // distinct real clients behind it — each gets its own bucket.
        let proxy_peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(login_request(proxy_peer, Some("198.51.100.1")))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        let blocked = app
            .clone()
            .oneshot(login_request(proxy_peer, Some("198.51.100.1")))
            .await
            .unwrap();
        assert_eq!(blocked.status(), StatusCode::TOO_MANY_REQUESTS);

        // A different client IP behind the same proxy is unaffected.
        let other_client = app
            .oneshot(login_request(proxy_peer, Some("198.51.100.2")))
            .await
            .unwrap();
        assert_eq!(other_client.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn logout_revokes_token_immediately() {
        let state = test_state().await;
        let token = bearer(&state, Role::Read);
        let app = router(state.clone());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/logout")
                    .header("authorization", &token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // The same token must now be rejected on a protected route.
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/connectors")
                    .header("authorization", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
