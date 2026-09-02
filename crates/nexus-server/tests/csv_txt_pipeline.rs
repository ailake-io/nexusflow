//! Real end-to-end for the `csv` connector (CLAUDE.md's "CSV/TXT com vários
//! separadores"), through the actual HTTP API rather than the connector
//! crate's own unit-level test (`nexus-connector-csv/tests/csv_integration.rs`):
//! `Postgres -> transform -> csv (TXT, '|' delimiter)` writes a file, then a
//! second pipeline `csv -> transform -> Postgres` reads that same file back
//! and round-trips the data into a fresh table — proving the connector works
//! as both source and sink under a non-comma ("TXT") delimiter, driven by
//! the real pipeline engine and RBAC-protected API, not just direct
//! `CsvSource`/`CsvSink` calls.
//!
//! Requires the real ADBC PostgreSQL driver — see postgres_pipeline.rs.

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use axum::Router;
use nexus_server::{build_app, ServerConfig};
use serde_json::{json, Value};
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
    let peer: std::net::SocketAddr = "203.0.113.1:12345".parse().unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/login")
                .header("content-type", "application/json")
                .extension(ConnectInfo(peer))
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

async fn create_pipeline(app: Router, spec: &Value, token: &str) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/pipelines")
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

async fn wait_for_run(app: &Router, pipeline_id: &str, run_id: i64, token: &str) -> Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
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
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
}

fn test_server_config() -> ServerConfig {
    ServerConfig {
        checkpoint_database_url: "sqlite::memory:".to_string(),
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
        webhook_url: None,
        allow_internal_hosts: true,
        trust_proxy_headers: false,
    }
}

#[tokio::test]
async fn csv_txt_round_trips_through_postgres_via_the_real_api() {
    require_env("ADBC_DRIVER_POSTGRESQL_PATH");

    let init_sql = "
        CREATE TABLE raw_events (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, active BOOLEAN);
        CREATE TABLE roundtrip_events (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, active BOOLEAN);
        INSERT INTO raw_events (id, name, score, active) VALUES
            (1, 'alice', 9.5, true), (2, 'bob', 4.25, false), (3, 'carol', 7.0, true);
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
    let uri = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let csv_path =
        std::env::temp_dir().join(format!("nexus_csv_txt_test_{}.txt", std::process::id()));
    let _ = std::fs::remove_file(&csv_path);

    let fields = json!([
        {"name": "id", "data_type": "int64"},
        {"name": "name", "data_type": "utf8"},
        {"name": "score", "data_type": "float64"},
        {"name": "active", "data_type": "boolean"}
    ]);

    // --- Pipeline A: Postgres -> transform -> csv, pipe-delimited ("TXT") ---
    let spec_export = json!({
        "pipeline_id": "export-to-txt",
        "sources": [{"connector": "postgres", "config": {"uri": uri, "table": "raw_events", "primary_key": "id"}}],
        "transform": {"sql": "SELECT * FROM source0 ORDER BY id"},
        "sinks": [{"connector": "csv", "config": {
            "uri": csv_path.to_str().unwrap(),
            "delimiter": "|",
            "has_header": true,
            "fields": fields,
            "primary_key": "id"
        }}]
    });

    let app = build_app(&test_server_config()).await.expect("app builds");
    let token = login(app.clone(), "admin", "test-password").await;

    let (create_status, create_body) = create_pipeline(app.clone(), &spec_export, &token).await;
    assert_eq!(
        create_status,
        StatusCode::CREATED,
        "export pipeline not created: {create_body:?}"
    );

    let (status, body) = post_run(app.clone(), "export-to-txt", &spec_export, &token).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "export run not accepted: {body:?}"
    );
    let run_id = body["run_id"].as_i64().expect("202 body carries run_id");
    let record = wait_for_run(&app, "export-to-txt", run_id, &token).await;
    assert_eq!(record["status"], "success", "export run failed: {record:?}");

    let raw = std::fs::read_to_string(&csv_path).expect("csv file was written");
    assert!(
        raw.lines().next().unwrap().contains('|'),
        "header must use the configured '|' delimiter, got: {raw:?}"
    );
    assert_eq!(raw.lines().count(), 4, "1 header + 3 data rows: {raw:?}");

    // --- Pipeline B: csv -> transform -> Postgres (fresh table) ---
    let spec_import = json!({
        "pipeline_id": "import-from-txt",
        "sources": [{"connector": "csv", "config": {
            "uri": csv_path.to_str().unwrap(),
            "delimiter": "|",
            "has_header": true,
            "fields": fields
        }}],
        "transform": {"sql": "SELECT * FROM source0"},
        "sinks": [{"connector": "postgres", "config": {"uri": uri, "table": "roundtrip_events", "primary_key": "id"}}]
    });

    let (create_status2, create_body2) = create_pipeline(app.clone(), &spec_import, &token).await;
    assert_eq!(
        create_status2,
        StatusCode::CREATED,
        "import pipeline not created: {create_body2:?}"
    );

    let (status2, body2) = post_run(app.clone(), "import-from-txt", &spec_import, &token).await;
    assert_eq!(
        status2,
        StatusCode::ACCEPTED,
        "import run not accepted: {body2:?}"
    );
    let run_id2 = body2["run_id"].as_i64().expect("202 body carries run_id");
    let record2 = wait_for_run(&app, "import-from-txt", run_id2, &token).await;
    assert_eq!(
        record2["status"], "success",
        "import run failed: {record2:?}"
    );

    let pg_pool = sqlx::PgPool::connect(&uri)
        .await
        .expect("connects to postgres for assertions");
    let mut rows: Vec<(i64, String, f64, bool)> =
        sqlx::query("SELECT id, name, score, active FROM roundtrip_events ORDER BY id")
            .fetch_all(&pg_pool)
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
            .collect();
    rows.sort_by_key(|(id, ..)| *id);

    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 9.5, true),
            (2, "bob".to_string(), 4.25, false),
            (3, "carol".to_string(), 7.0, true),
        ],
        "data must round-trip Postgres -> TXT('|') -> Postgres unchanged"
    );

    let _ = std::fs::remove_file(&csv_path);
}
