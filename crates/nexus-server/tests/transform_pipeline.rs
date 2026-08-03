//! Marco 2 "done" criteria (IMPLEMENTATION_PLAN.md): a pipeline with 2
//! fan-in Postgres sources -> a DataFusion SQL transform (join + filter) ->
//! a SQLite sink, proving both new Marco 2 capabilities (transform, and a
//! second connector) work together end to end through the real HTTP API.
//!
//! Requires both real ADBC drivers: run
//! `scripts/build-adbc-postgresql-driver.sh` and
//! `scripts/build-adbc-sqlite-driver.sh` first, and export
//! `ADBC_DRIVER_POSTGRESQL_PATH` / `ADBC_DRIVER_SQLITE_PATH`.

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

fn require_env(var: &str) {
    if std::env::var(var).is_err() {
        panic!("{var} not set — build the ADBC drivers first (see scripts/)");
    }
}

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
    }
}

#[tokio::test]
async fn fans_in_two_postgres_sources_transforms_and_writes_to_sqlite() {
    require_env("ADBC_DRIVER_POSTGRESQL_PATH");
    require_env("ADBC_DRIVER_SQLITE_PATH");

    let init_sql = "
        CREATE TABLE events (id BIGINT PRIMARY KEY, region TEXT NOT NULL, amount BIGINT NOT NULL);
        CREATE TABLE regions (region TEXT PRIMARY KEY, region_name TEXT NOT NULL);
        INSERT INTO regions (region, region_name) VALUES ('us', 'United States'), ('eu', 'Europe');
        INSERT INTO events (id, region, amount) VALUES
            (1, 'us', 5), (2, 'us', 20), (3, 'us', 30),
            (4, 'eu', 8), (5, 'eu', 15), (6, 'eu', 40);
    ";

    let container = postgres::Postgres::default()
        .with_init_sql(init_sql.as_bytes().to_vec())
        .start()
        .await
        .expect("postgres container starts");

    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let pg_uri = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let sqlite_db_path = std::env::temp_dir().join(format!(
        "nexus_transform_test_sink_{}.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&sqlite_db_path);
    let sqlite_uri = sqlite_db_path.display().to_string();

    // Sink table must already exist — same contract as the Postgres sink in
    // Marco 1's linear path.
    {
        let setup_pool = SqlitePool::connect(&format!("sqlite://{sqlite_uri}?mode=rwc"))
            .await
            .expect("create sink sqlite file");
        sqlx::query(
            "CREATE TABLE events_enriched (id INTEGER PRIMARY KEY, region_name TEXT, amount INTEGER)",
        )
        .execute(&setup_pool)
        .await
        .expect("create sink table");
        setup_pool.close().await;
    }

    let checkpoint_db_path = std::env::temp_dir().join(format!(
        "nexus_transform_test_checkpoints_{}.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&checkpoint_db_path);
    let checkpoint_db_url = format!("sqlite://{}", checkpoint_db_path.display());

    let spec = json!({
        "pipeline_id": "enrich-events",
        "sources": [
            {"name": "events", "connector": "postgres", "config": {"uri": pg_uri, "table": "events", "primary_key": "id"}},
            {"name": "regions", "connector": "postgres", "config": {"uri": pg_uri, "table": "regions", "primary_key": "region"}}
        ],
        "transform": {
            "sql": "SELECT events.id, regions.region_name, events.amount \
                     FROM events JOIN regions ON events.region = regions.region \
                     WHERE events.amount > 10 ORDER BY events.id"
        },
        "sinks": [
            {"connector": "sqlite", "config": {"uri": sqlite_uri, "table": "events_enriched", "primary_key": "id"}}
        ]
    });

    let app = build_app(&test_server_config(checkpoint_db_url.clone()))
        .await
        .expect("app builds");

    let token = login(app.clone(), "admin", "test-password").await;
    let (status, body) = post_run(app.clone(), "enrich-events", &spec, &token).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "run was not accepted: {body:?}"
    );
    let run_id = body["run_id"].as_i64().expect("202 body carries run_id");
    let record = wait_for_run(&app, "enrich-events", run_id, &token).await;
    assert_eq!(
        record["status"], "success",
        "pipeline run failed: {record:?}"
    );

    let stats = record["stats"].as_array().expect("stats array");
    assert_eq!(stats.len(), 1, "one stats entry for the one sink");
    assert_eq!(stats[0]["rows_written"], 4, "amount > 10: ids 2,3,5,6");

    let sink_pool = SqlitePool::connect(&format!("sqlite://{sqlite_uri}"))
        .await
        .expect("reconnect to sink db");

    let row_count: i64 = sqlx::query("SELECT count(*) FROM events_enriched")
        .fetch_one(&sink_pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(row_count, 4);

    let row: (String, i64) =
        sqlx::query_as("SELECT region_name, amount FROM events_enriched WHERE id = 2")
            .fetch_one(&sink_pool)
            .await
            .unwrap();
    assert_eq!(row, ("United States".to_string(), 20));
    sink_pool.close().await;

    // Re-running must skip the already-checkpointed sink and not duplicate —
    // same resume contract as the linear path (ARCHITECTURE.md §5).
    let app2 = build_app(&test_server_config(checkpoint_db_url))
        .await
        .expect("app rebuilds");
    let token2 = login(app2.clone(), "admin", "test-password").await;
    let (status2, body2) = post_run(app2.clone(), "enrich-events", &spec, &token2).await;
    assert_eq!(
        status2,
        StatusCode::ACCEPTED,
        "resume run was not accepted: {body2:?}"
    );
    let run_id2 = body2["run_id"].as_i64().expect("202 body carries run_id");
    let record2 = wait_for_run(&app2, "enrich-events", run_id2, &token2).await;
    assert_eq!(
        record2["status"], "success",
        "resume run failed: {record2:?}"
    );
    assert_eq!(
        record2["stats"].as_array().unwrap().len(),
        0,
        "sink already checkpointed, resume must skip it"
    );

    let sink_pool2 = SqlitePool::connect(&format!("sqlite://{sqlite_uri}"))
        .await
        .unwrap();
    let row_count_after_resume: i64 = sqlx::query("SELECT count(*) FROM events_enriched")
        .fetch_one(&sink_pool2)
        .await
        .unwrap()
        .get(0);
    assert_eq!(row_count_after_resume, 4, "resume must not duplicate rows");

    let _ = std::fs::remove_file(&sqlite_db_path);
    let _ = std::fs::remove_file(&checkpoint_db_path);
}
