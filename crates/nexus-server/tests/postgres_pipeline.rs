//! End-to-end Marco 1 "done" criteria (IMPLEMENTATION_PLAN.md): move 100k rows
//! Postgres -> Postgres through `POST /pipelines/{id}/run`, then simulate a
//! crash (some partitions never got to commit their checkpoint) and confirm
//! resume re-runs only the incomplete partitions without duplicating rows —
//! safe because every Sink write is an upsert (ARCHITECTURE.md §5).
//!
//! Requires the real ADBC PostgreSQL driver: run
//! `scripts/build-adbc-postgresql-driver.sh` first and export
//! `ADBC_DRIVER_POSTGRESQL_PATH` to point at the built `.so`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use nexus_server::{build_app, ServerConfig};
use serde_json::{json, Value};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const ROW_COUNT: i64 = 100_000;
const PARTITIONS: u32 = 8;

async fn login(app: Router, username: &str, password: &str) -> String {
    let body = json!({"username": username, "password": password});
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
    assert_eq!(response.status(), StatusCode::OK, "login must succeed");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    body["token"]
        .as_str()
        .expect("login response has a token")
        .to_string()
}

async fn post_run(
    app: Router,
    pipeline_id: &str,
    spec: &Value,
    token: &str,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/pipelines/{pipeline_id}/run"))
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(spec.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    (status, body)
}

/// POST /run returns 202 as soon as the run is recorded — the pipeline
/// executes in a background supervisor task, so the test polls the run
/// history until the supervisor records the terminal state.
async fn wait_for_run(app: &Router, pipeline_id: &str, run_id: i64, token: &str) -> Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/pipelines/{pipeline_id}/runs"))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let runs: Value = serde_json::from_slice(&bytes).unwrap();
        if let Some(record) = runs
            .as_array()
            .and_then(|a| a.iter().find(|r| r["id"].as_i64() == Some(run_id)))
        {
            if record["finished_at"].is_string() {
                return record.clone();
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "run {run_id} never reached a terminal state"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

fn test_server_config(checkpoint_database_url: String) -> ServerConfig {
    ServerConfig {
        checkpoint_database_url,
        auth_database_url: "sqlite::memory:".to_string(),
        pipelines_database_url: "sqlite::memory:".to_string(),
        jwt_secret: "test-secret".to_string(),
        jwt_ttl_seconds: 3600,
        bootstrap_admin: Some(("admin".to_string(), "test-password".to_string())),
        encryption_key_hex: "ab".repeat(32),
        slack_webhook_url: None,
        teams_webhook_url: None,
        pagerduty_routing_key: None,
        email: None,
    }
}

#[tokio::test]
async fn moves_100k_rows_and_resumes_after_partial_crash_without_duplicating() {
    if std::env::var("ADBC_DRIVER_POSTGRESQL_PATH").is_err() {
        panic!(
            "ADBC_DRIVER_POSTGRESQL_PATH not set — run \
             scripts/build-adbc-postgresql-driver.sh and export it first"
        );
    }

    let init_sql = format!(
        "CREATE TABLE events (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION);
         CREATE TABLE events_copy (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION);
         INSERT INTO events (id, name, score)
         SELECT g, 'name-' || g, g * 1.5 FROM generate_series(1, {ROW_COUNT}) AS g;"
    );

    let container = postgres::Postgres::default()
        .with_init_sql(init_sql.into_bytes())
        .start()
        .await
        .expect("postgres container starts");

    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let uri = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let spec = json!({
        "pipeline_id": "big-copy",
        "sources": [{"connector": "postgres", "config": {"uri": uri, "table": "events", "primary_key": "id"}}],
        "sinks": [{"connector": "postgres", "config": {"uri": uri, "table": "events_copy", "primary_key": "id"}}],
        "partitions": PARTITIONS
    });

    let checkpoint_db_path = std::env::temp_dir().join(format!(
        "nexus_integration_test_checkpoints_{}.db",
        std::process::id()
    ));
    let checkpoint_db_url = format!("sqlite://{}", checkpoint_db_path.display());
    let _ = std::fs::remove_file(&checkpoint_db_path);

    // --- Phase 1: full run, as a fresh process would do it. ---
    let app = build_app(&test_server_config(checkpoint_db_url.clone()))
        .await
        .expect("app builds");

    let token = login(app.clone(), "admin", "test-password").await;
    let (status, body) = post_run(app.clone(), "big-copy", &spec, &token).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "first run was not accepted: {body:?}"
    );
    let run_id = body["run_id"].as_i64().expect("202 body carries run_id");
    let record = wait_for_run(&app, "big-copy", run_id, &token).await;
    assert_eq!(record["status"], "success", "first run failed: {record:?}");
    let partitions_run = record["stats"].as_array().expect("stats array").len();
    assert_eq!(
        partitions_run, PARTITIONS as usize,
        "first run must execute every partition"
    );

    let pg_pool = sqlx::PgPool::connect(&uri)
        .await
        .expect("connects to postgres for assertions");
    let row_count: i64 = sqlx::query("SELECT count(*) FROM events_copy")
        .fetch_one(&pg_pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(row_count, ROW_COUNT, "all rows copied on first run");

    // --- Simulate a crash: two partitions never got to commit their
    // checkpoint (as if the process died mid-write, before
    // `checkpoint_store.commit()` ran for them). Their data may already be in
    // the sink or not — doesn't matter, re-running is always safe. ---
    let checkpoint_pool = SqlitePool::connect(&checkpoint_db_url)
        .await
        .expect("reconnect to checkpoint db, as a restarted process would");
    sqlx::query(
        "DELETE FROM checkpoints WHERE pipeline_id = 'big-copy' AND partition_id IN ('p1', 'p2')",
    )
    .execute(&checkpoint_pool)
    .await
    .unwrap();
    checkpoint_pool.close().await;

    // --- Phase 2: resume, as a restarted process would (fresh `build_app`
    // reconnecting to the same checkpoint file). ---
    let app2 = build_app(&test_server_config(checkpoint_db_url.clone()))
        .await
        .expect("app rebuilds against the same checkpoint db");

    let token2 = login(app2.clone(), "admin", "test-password").await;
    let (status2, body2) = post_run(app2.clone(), "big-copy", &spec, &token2).await;
    assert_eq!(
        status2,
        StatusCode::ACCEPTED,
        "resume run was not accepted: {body2:?}"
    );
    let run_id2 = body2["run_id"].as_i64().expect("202 body carries run_id");
    let record2 = wait_for_run(&app2, "big-copy", run_id2, &token2).await;
    assert_eq!(
        record2["status"], "success",
        "resume run failed: {record2:?}"
    );
    let mut resumed_partitions: Vec<String> = record2["stats"]
        .as_array()
        .expect("stats array")
        .iter()
        .map(|p| p["partition_id"].as_str().unwrap().to_string())
        .collect();
    resumed_partitions.sort(); // completion order is nondeterministic
    assert_eq!(
        resumed_partitions.len(),
        2,
        "resume must re-run only the 2 partitions missing a checkpoint, got {resumed_partitions:?}"
    );
    assert!(resumed_partitions.contains(&"p1".to_string()));
    assert!(resumed_partitions.contains(&"p2".to_string()));

    let row_count_after_resume: i64 = sqlx::query("SELECT count(*) FROM events_copy")
        .fetch_one(&pg_pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        row_count_after_resume, ROW_COUNT,
        "resume must not duplicate rows — upsert makes re-running p1/p2 safe"
    );

    let mismatched: i64 = sqlx::query(
        "SELECT count(*) FROM events e JOIN events_copy c
         ON e.id = c.id AND e.name = c.name AND e.score = c.score
         WHERE (SELECT count(*) FROM events) = (SELECT count(*) FROM events_copy)",
    )
    .fetch_one(&pg_pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(mismatched, ROW_COUNT, "every row's data must match exactly");

    let _ = std::fs::remove_file(&checkpoint_db_path);
}
