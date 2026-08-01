mod auth;
mod auth_store;
mod checkpoint_store;
mod connectors;
mod error;
mod runner;

use auth::{require_role, JwtCodec, Role};
use auth_store::AuthStore;
use axum::extract::{Extension, FromRef, Path, State};
use axum::middleware;
use axum::routing::{get, post};
use axum::{Json, Router};
use checkpoint_store::CheckpointStore;
use error::ApiError;
use nexus_core::{ConnectorDescriptor, ConnectorRegistry, PartitionStats, PipelineSpec};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Clone)]
struct AppState {
    checkpoints: CheckpointStore,
    auth_store: AuthStore,
    jwt: JwtCodec,
}

/// Lets `Claims`/`require_role` (defined generically over any state `S`
/// carrying a `JwtCodec`) pull the codec out of the concrete `AppState`.
impl FromRef<AppState> for JwtCodec {
    fn from_ref(state: &AppState) -> Self {
        state.jwt.clone()
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

    // The connector catalog (Marco 8: canvas nodes come from here, never
    // hardcoded in the frontend — ARCHITECTURE.md §3) only needs `Read`.
    let read_protected = Router::new()
        .route("/connectors", get(list_connectors_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Read));

    Router::new()
        .route("/health", get(health))
        .route("/auth/login", post(login_handler))
        .merge(execute_protected)
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

    let stats = runner::run_pipeline(&spec, &state.checkpoints)
        .await
        .map_err(ApiError::internal)?;

    Ok(Json(stats))
}

pub struct ServerConfig {
    pub checkpoint_database_url: String,
    pub auth_database_url: String,
    /// Never hardcoded — comes from `NEXUS_JWT_SECRET` (ARCHITECTURE.md §10).
    pub jwt_secret: String,
    pub jwt_ttl_seconds: u64,
    /// `(username, password)` bootstrapped as the sole `Admin` account the
    /// first time the users table is empty — a no-op on every later boot.
    pub bootstrap_admin: Option<(String, String)>,
}

/// Builds the app without binding a socket — the entrypoint integration
/// tests use (via `tower::ServiceExt::oneshot`) to drive real pipeline runs
/// against a testcontainers Postgres. See IMPLEMENTATION_PLAN.md Marco 1.
pub async fn build_app(config: &ServerConfig) -> anyhow::Result<Router> {
    let checkpoints = CheckpointStore::connect(&config.checkpoint_database_url).await?;
    let auth_store = AuthStore::connect(&config.auth_database_url).await?;
    if let Some((username, password)) = &config.bootstrap_admin {
        auth_store.seed_admin_if_empty(username, password).await?;
    }
    let jwt = JwtCodec::new(config.jwt_secret.as_bytes(), config.jwt_ttl_seconds);
    Ok(router(AppState {
        checkpoints,
        auth_store,
        jwt,
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
    let jwt_secret = std::env::var("NEXUS_JWT_SECRET")
        .map_err(|_| anyhow::anyhow!("NEXUS_JWT_SECRET must be set (ARCHITECTURE.md §10)"))?;
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

    let app = build_app(&ServerConfig {
        checkpoint_database_url: database_url,
        auth_database_url,
        jwt_secret,
        jwt_ttl_seconds: 3600,
        bootstrap_admin,
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
}
