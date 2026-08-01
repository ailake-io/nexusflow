mod alerts;
mod auth;
mod auth_store;
mod checkpoint_store;
mod connectors;
mod crypto;
mod error;
mod pipeline_store;
mod progress;
mod runner;

use alerts::AlertNotifier;
use auth::{require_role, JwtCodec, Role};
use auth_store::AuthStore;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, FromRef, Path, Query, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::Response;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use checkpoint_store::CheckpointStore;
use crypto::SecretCipher;
use error::ApiError;
use nexus_core::{ConnectorDescriptor, ConnectorRegistry, PartitionStats, PipelineSpec};
use pipeline_store::{PipelineStore, PipelineStoreError, PipelineSummary, RunRecord};
use progress::ProgressHub;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Clone)]
struct AppState {
    checkpoints: CheckpointStore,
    auth_store: AuthStore,
    jwt: JwtCodec,
    /// Encrypts connector secrets before `pipelines` persists them (CLAUDE.md §5).
    secrets: SecretCipher,
    pipelines: PipelineStore,
    progress: ProgressHub,
    alerts: AlertNotifier,
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
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Read));

    Router::new()
        .route("/health", get(health))
        .route("/auth/login", post(login_handler))
        // Not behind `require_role` — a browser's `WebSocket` API can't set
        // an `Authorization` header, so this route takes the JWT as a query
        // param and checks the role itself (see `progress_ws_handler`).
        .route(
            "/pipelines/{id}/runs/{run_id}/progress",
            get(progress_ws_handler),
        )
        .merge(execute_protected)
        .merge(write_protected)
        .merge(read_protected)
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

/// Dynamic connector catalog for the canvas (Marco 8) — every connector
/// crate linked into this binary registers itself via `submit_connector!`
/// (ARCHITECTURE.md §3), so this list reflects what's actually usable, not
/// a hardcoded frontend assumption.
async fn list_connectors_handler() -> Json<Vec<&'static ConnectorDescriptor>> {
    Json(ConnectorRegistry::all().collect())
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
    Json(body): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    let role = state
        .auth_store
        .verify(&body.username, &body.password)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::unauthorized("invalid username or password"))?;
    let token = state.jwt.issue(&body.username, role)?;
    Ok(Json(LoginResponse { token }))
}

async fn run_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(spec): Json<PipelineSpec>,
) -> Result<Json<Vec<PartitionStats>>, ApiError> {
    if spec.pipeline_id != id {
        return Err(ApiError::bad_request(format!(
            "path id {id:?} does not match body.pipeline_id {:?}",
            spec.pipeline_id
        )));
    }
    spec.validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    // Recorded regardless of whether `id` was ever persisted via
    // `POST /pipelines` — ad-hoc runs (body-only, no prior create) still
    // show up in `GET /pipelines/{id}/runs`, same as always-persisted ones.
    let run_id = state.pipelines.start_run(&spec.pipeline_id).await?;
    let progress_tx = state.progress.start(run_id);

    let result = runner::run_pipeline(&spec, &state.checkpoints, Some(progress_tx)).await;
    state.progress.finish(run_id);

    match result {
        Ok(stats) => {
            if let Err(e) = state.pipelines.finish_run_success(run_id, &stats).await {
                tracing::warn!(error = %e, "failed to record successful pipeline run");
            }
            Ok(Json(stats))
        }
        Err(e) => {
            if let Err(record_err) = state
                .pipelines
                .finish_run_failure(run_id, &e.to_string())
                .await
            {
                tracing::warn!(error = %record_err, "failed to record failed pipeline run");
            }
            state
                .alerts
                .notify_pipeline_failed(&spec.pipeline_id, run_id, &e.to_string());
            Err(ApiError::internal(e))
        }
    }
}

async fn create_pipeline_handler(
    State(state): State<AppState>,
    Json(spec): Json<PipelineSpec>,
) -> Result<(StatusCode, Json<PipelineSummary>), ApiError> {
    spec.validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    state.pipelines.create(&spec, &state.secrets).await?;
    let summary = state
        .pipelines
        .get_summary(&spec.pipeline_id, &state.secrets)
        .await?;
    Ok((StatusCode::CREATED, Json(summary)))
}

async fn list_pipelines_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<PipelineSummary>>, ApiError> {
    Ok(Json(state.pipelines.list_summaries(&state.secrets).await?))
}

async fn get_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<PipelineSummary>, ApiError> {
    Ok(Json(
        state.pipelines.get_summary(&id, &state.secrets).await?,
    ))
}

async fn update_pipeline_handler(
    State(state): State<AppState>,
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
    state.pipelines.update(&id, &spec, &state.secrets).await?;
    Ok(Json(
        state.pipelines.get_summary(&id, &state.secrets).await?,
    ))
}

async fn delete_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state.pipelines.delete(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_runs_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<RunRecord>>, ApiError> {
    Ok(Json(state.pipelines.list_runs(&id).await?))
}

#[derive(Deserialize)]
struct ProgressWsQuery {
    token: String,
}

/// `/pipelines/{id}/runs/{run_id}/progress` — streams that run's
/// `ProgressEvent`s as JSON text frames until the run finishes (server
/// closes) or the client disconnects. `{id}` isn't used for the lookup
/// (progress is keyed by `run_id` alone) — kept in the path purely so the
/// URL reads as "this run, under this pipeline", matching `GET .../runs`.
async fn progress_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path((_id, run_id)): Path<(String, i64)>,
    Query(query): Query<ProgressWsQuery>,
) -> Result<Response, ApiError> {
    let rx = authorize_progress_subscription(&state, &query.token, run_id).await?;
    Ok(ws.on_upgrade(move |socket| forward_progress(socket, rx)))
}

/// Split out from `progress_ws_handler` so it's callable directly from a
/// test — a `WebSocketUpgrade` extractor requires a real hyper connection
/// (`tower::ServiceExt::oneshot` doesn't provide one, so it always rejects
/// with 426 regardless of what this function would have decided).
async fn authorize_progress_subscription(
    state: &AppState,
    token: &str,
    run_id: i64,
) -> Result<tokio::sync::broadcast::Receiver<nexus_core::ProgressEvent>, ApiError> {
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
        .ok_or_else(|| ApiError::not_found(format!("run {run_id} not found or already finished")))
}

async fn forward_progress(
    mut socket: WebSocket,
    mut rx: tokio::sync::broadcast::Receiver<nexus_core::ProgressEvent>,
) {
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
}

/// Builds the app without binding a socket — the entrypoint integration
/// tests use (via `tower::ServiceExt::oneshot`) to drive real pipeline runs
/// against a testcontainers Postgres. See IMPLEMENTATION_PLAN.md Marco 1.
pub async fn build_app(config: &ServerConfig) -> anyhow::Result<Router> {
    let checkpoints = CheckpointStore::connect(&config.checkpoint_database_url).await?;
    let auth_store = AuthStore::connect(&config.auth_database_url).await?;
    let pipelines = PipelineStore::connect(&config.pipelines_database_url).await?;
    if let Some((username, password)) = &config.bootstrap_admin {
        auth_store.seed_admin_if_empty(username, password).await?;
    }
    let jwt = JwtCodec::new(config.jwt_secret.as_bytes(), config.jwt_ttl_seconds);
    let secrets = SecretCipher::from_hex_key(&config.encryption_key_hex)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(router(AppState {
        checkpoints,
        auth_store,
        jwt,
        secrets,
        pipelines,
        progress: ProgressHub::default(),
        alerts: AlertNotifier::new(config.slack_webhook_url.clone()),
    }))
}

/// Boots the server. This is the only orchestration entrypoint — `src/main.rs`
/// just calls this, no separate scheduler lives in the main binary
/// (ARCHITECTURE.md §1).
pub async fn run() -> anyhow::Result<()> {
    let database_url =
        std::env::var("NEXUS_CHECKPOINT_DB").unwrap_or_else(|_| "sqlite://nexusflow.db".into());
    let auth_database_url =
        std::env::var("NEXUS_AUTH_DB").unwrap_or_else(|_| "sqlite://nexusflow-auth.db".into());
    let pipelines_database_url = std::env::var("NEXUS_PIPELINES_DB")
        .unwrap_or_else(|_| "sqlite://nexusflow-pipelines.db".into());
    let jwt_secret = std::env::var("NEXUS_JWT_SECRET")
        .map_err(|_| anyhow::anyhow!("NEXUS_JWT_SECRET must be set (ARCHITECTURE.md §10)"))?;
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

    let app = build_app(&ServerConfig {
        checkpoint_database_url: database_url,
        auth_database_url,
        pipelines_database_url,
        jwt_secret,
        jwt_ttl_seconds: 3600,
        bootstrap_admin,
        encryption_key_hex,
        slack_webhook_url,
    })
    .await?;

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!(%addr, "nexus-server listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use tower::ServiceExt;

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
            progress: ProgressHub::default(),
            alerts: AlertNotifier::new(None),
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

        let body = serde_json::json!({"username": "admin", "password": "test-password"});
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/login")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn login_rejects_wrong_password() {
        let app = router(test_state().await);

        let body = serde_json::json!({"username": "admin", "password": "wrong"});
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/login")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
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
    async fn run_rejects_unsupported_connector() {
        let state = test_state().await;
        let token = bearer(&state, Role::Execute);
        let app = router(state);

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "mongodb", "config": {}}],
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

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
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
            "sources": [{"connector": "postgres", "config": {"uri": "postgres://user:pw@host/db"}}],
            "sinks": [{"connector": "sqlite", "config": {"path": "/tmp/out.db"}}]
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
    async fn run_records_failed_run_in_history() {
        let state = test_state().await;
        let token = bearer(&state, Role::Execute);
        let app = router(state);

        let body = serde_json::json!({
            "pipeline_id": "p1",
            "sources": [{"connector": "mongodb", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        });
        let run = app
            .clone()
            .oneshot(json_request("POST", "/pipelines/p1/run", &token, body))
            .await
            .unwrap();
        assert_eq!(run.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let runs = app
            .oneshot(
                Request::builder()
                    .uri("/pipelines/p1/runs")
                    .header("authorization", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(runs.status(), StatusCode::OK);

        let runs_body = body_json(runs).await;
        let runs_array = runs_body.as_array().unwrap();
        assert_eq!(runs_array.len(), 1);
        assert_eq!(runs_array[0]["status"], "failed");
        assert!(runs_array[0]["error"]
            .as_str()
            .unwrap()
            .contains("unsupported connector"));
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
        let tx = state.progress.start(run_id);

        let mut rx = authorize_progress_subscription(&state, token, run_id)
            .await
            .unwrap();

        tx.send(nexus_core::ProgressEvent {
            partition_id: "p0".to_string(),
            batches_written: 1,
            rows_written: 10,
            bytes_written: 100,
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
        state.progress.start(run_id);
        state.progress.finish(run_id);

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
        let tx = state.progress.start(run_id);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("ws://{addr}/pipelines/p1/runs/{run_id}/progress?token={token}");
        let (mut ws, _response) = tokio_tungstenite::connect_async(url)
            .await
            .expect("real WebSocket handshake succeeds");

        tx.send(nexus_core::ProgressEvent {
            partition_id: "p0".to_string(),
            batches_written: 1,
            rows_written: 42,
            bytes_written: 999,
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
}
