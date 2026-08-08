//! True ETL via dbt (approved plan `peaceful-crunching-blanket.md`, extending
//! Marco 10's ELT-only dbt step): `[Postgres source] -> [Postgres staging
//! sink] -> [dbt transforms in place] -> [dbt.output reads the result back]
//! -> [post_dbt_sinks writes the final Postgres table]`, all in one
//! `POST /pipelines/{id}/run`.
//!
//! Requires the real ADBC PostgreSQL driver (see postgres_pipeline.rs) and
//! the real `dbt` CLI on PATH with a Postgres-capable adapter — skipped (not
//! failed) when `dbt` isn't found, same convention as dbt.rs's own tests,
//! since installing dbt is an explicit opt-in (`nexus-server`'s `dbt`
//! feature) not provisioned unconditionally in CI.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use nexus_server::{build_app, ServerConfig};
use serde_json::{json, Value};
use sqlx::Row;
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

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
        // testcontainers exposes Postgres on localhost — same escape hatch
        // postgres_pipeline.rs uses for its own SSRF-hardened ad-hoc run.
        allow_internal_hosts: true,
    }
}

fn write_dbt_project(dir: &std::path::Path, host: &str, port: u16) {
    std::fs::write(
        dir.join("dbt_project.yml"),
        r#"
name: 'nexus_etl_fixture'
version: '1.0.0'
profile: 'nexus_etl_fixture'
model-paths: ["models"]
"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("profiles.yml"),
        format!(
            r#"
nexus_etl_fixture:
  target: dev
  outputs:
    dev:
      type: postgres
      host: {host}
      port: {port}
      user: postgres
      password: postgres
      dbname: postgres
      schema: public
"#
        ),
    )
    .unwrap();
    std::fs::create_dir_all(dir.join("models")).unwrap();
    // Transforms whatever the pipeline's own sink just loaded into
    // `staging_events` — this is the "T" in ETL: dbt operates on the
    // already-landed staging table, not on nexus's in-flight Arrow batches.
    std::fs::write(
        dir.join("models").join("transformed_events.sql"),
        "select id, upper(name) as name_upper from staging_events",
    )
    .unwrap();
}

#[tokio::test]
async fn dbt_transforms_staged_data_and_writes_result_to_a_final_sink() {
    if std::env::var("ADBC_DRIVER_POSTGRESQL_PATH").is_err() {
        panic!(
            "ADBC_DRIVER_POSTGRESQL_PATH not set — run \
             scripts/build-adbc-postgresql-driver.sh and export it first"
        );
    }
    if tokio::process::Command::new("dbt")
        .arg("--version")
        .output()
        .await
        .is_err()
    {
        eprintln!("skipping: `dbt` CLI not found on PATH");
        return;
    }

    let init_sql = "
        CREATE TABLE raw_events (id BIGINT PRIMARY KEY, name TEXT);
        CREATE TABLE staging_events (id BIGINT PRIMARY KEY, name TEXT);
        CREATE TABLE final_events (id BIGINT PRIMARY KEY, name_upper TEXT);
        INSERT INTO raw_events (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
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

    // `validate_security` requires `dbt.project_dir` to be a relative path
    // (path-traversal guard, dag.rs's `validate_dbt_project_dir`) — unlike
    // dbt.rs's own unit tests, which call `dbt::run` directly and skip that
    // check, this test goes through the real HTTP API and must satisfy it.
    let rel_project_dir = format!("target/tmp/dbt_etl_fixture_{}", std::process::id());
    let dbt_dir = std::env::current_dir().unwrap().join(&rel_project_dir);
    std::fs::create_dir_all(&dbt_dir).unwrap();
    write_dbt_project(&dbt_dir, &host.to_string(), port);

    let spec = json!({
        "pipeline_id": "etl-pipeline",
        "sources": [{"connector": "postgres", "config": {"uri": uri, "table": "raw_events", "primary_key": "id"}}],
        "sinks": [{"connector": "postgres", "config": {"uri": uri, "table": "staging_events", "primary_key": "id"}}],
        "dbt": {
            "project_dir": rel_project_dir,
            "command": "run",
            "output": {"connector": "postgres", "config": {"uri": uri, "table": "transformed_events", "primary_key": "id"}}
        },
        "post_dbt_sinks": [
            {"connector": "postgres", "config": {"uri": uri, "table": "final_events", "primary_key": "id"}}
        ]
    });

    let app = build_app(&test_server_config()).await.expect("app builds");
    let token = login(app.clone(), "admin", "test-password").await;

    // dbt reads DBT_PROFILES_DIR from its own environment, inherited by the
    // subprocess `dbt::run` spawns (nexus-server never passes it explicitly
    // — see dbt.rs's own doc comment on why it isn't part of DbtConfig).
    std::env::set_var("DBT_PROFILES_DIR", &dbt_dir);
    let (status, body) = post_run(app.clone(), "etl-pipeline", &spec, &token).await;
    assert_eq!(status, StatusCode::ACCEPTED, "run was not accepted: {body:?}");
    let run_id = body["run_id"].as_i64().expect("202 body carries run_id");
    let record = wait_for_run(&app, "etl-pipeline", run_id, &token).await;
    std::env::remove_var("DBT_PROFILES_DIR");
    let _ = std::fs::remove_dir_all(&dbt_dir);

    assert_eq!(record["status"], "success", "ETL run failed: {record:?}");
    assert!(
        record["dbt_summary"].is_object(),
        "dbt step must have run and reported a summary: {record:?}"
    );

    let pg_pool = sqlx::PgPool::connect(&uri)
        .await
        .expect("connects to postgres for assertions");

    let staged_count: i64 = sqlx::query("SELECT count(*) FROM staging_events")
        .fetch_one(&pg_pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(staged_count, 3, "raw load into staging must have landed all rows");

    let mut final_rows: Vec<(i64, String)> = sqlx::query("SELECT id, name_upper FROM final_events ORDER BY id")
        .fetch_all(&pg_pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<i64, _>(0), r.get::<String, _>(1)))
        .collect();
    final_rows.sort();
    assert_eq!(
        final_rows,
        vec![
            (1, "ALICE".to_string()),
            (2, "BOB".to_string()),
            (3, "CAROL".to_string()),
        ],
        "final sink must contain dbt's transformed result, not the raw staged rows"
    );
}
