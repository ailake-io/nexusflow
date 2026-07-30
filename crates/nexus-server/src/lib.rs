mod checkpoint_store;
mod connectors;
mod error;
mod runner;

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use checkpoint_store::CheckpointStore;
use error::ApiError;
use nexus_core::{PartitionStats, PipelineSpec};
use std::net::SocketAddr;

#[derive(Clone)]
struct AppState {
    checkpoints: CheckpointStore,
}

/// Builds the Axum app. Kept separate from `run()` so it's testable via
/// `tower::ServiceExt::oneshot` without binding a real socket.
fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/pipelines/{id}/run", post(run_pipeline_handler))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
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
}

/// Builds the app without binding a socket — the entrypoint integration
/// tests use (via `tower::ServiceExt::oneshot`) to drive real pipeline runs
/// against a testcontainers Postgres. See IMPLEMENTATION_PLAN.md Marco 1.
pub async fn build_app(config: &ServerConfig) -> anyhow::Result<Router> {
    let checkpoints = CheckpointStore::connect(&config.checkpoint_database_url).await?;
    Ok(router(AppState { checkpoints }))
}

/// Boots the server. This is the only orchestration entrypoint — `src/main.rs`
/// just calls this, no separate scheduler lives in the main binary
/// (ARCHITECTURE.md §1).
pub async fn run() -> anyhow::Result<()> {
    let database_url =
        std::env::var("NEXUS_CHECKPOINT_DB").unwrap_or_else(|_| "sqlite://nexusflow.db".into());
    let app = build_app(&ServerConfig {
        checkpoint_database_url: database_url,
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
        AppState {
            checkpoints: CheckpointStore::connect("sqlite::memory:").await.unwrap(),
        }
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
    async fn run_rejects_path_body_pipeline_id_mismatch() {
        let app = router(test_state().await);

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
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn run_rejects_unsupported_connector() {
        let app = router(test_state().await);

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
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
