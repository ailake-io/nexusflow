#![cfg(all(
    feature = "postgres-cdc",
    feature = "lancedb",
    any(feature = "embeddings", feature = "embeddings-api")
))]

//! Real end-to-end: `postgres-cdc -> embedding -> lancedb`, no `transform`
//! node (LLMOPS_IMPLEMENTATION_PLAN.md Marco L6's own acceptance
//! criterion). Before this marco, `run_linear_pipeline` rejected any spec
//! with `embedding` set before even deciding which sub-path it would take
//! — a CDC source (which never goes through the `transform`/
//! `drain_sources` path, see `ARCHITECTURE.md §7`) could never combine
//! with `embedding` at all. This proves that combination now works
//! against a real Postgres logical-replication stream and a real
//! embedding model, not mocked.

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use axum::Router;
use nexus_connector_lancedb::{LanceDbConnectorConfig, LanceDbSearchClient, LanceDbStorageOptions};
use nexus_server::{build_app, ServerConfig};
use serde_json::{json, Value};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tower::ServiceExt;

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
        webhook_url: None,
        // Same escape hatch `postgres_pipeline.rs` already uses — testcontainers
        // exposes Postgres on localhost, which validate_security() blocks by
        // default (SSRF hardening, C5).
        allow_internal_hosts: true,
        trust_proxy_headers: false,
    }
}

#[tokio::test]
async fn embedding_stage_runs_on_the_cdc_passthrough_path() {
    let postgres = GenericImage::new("postgres", "16")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "nexus")
        .with_env_var("POSTGRES_PASSWORD", "nexus")
        .with_env_var("POSTGRES_DB", "nexus")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .expect("postgres starts");

    let host = postgres.get_host().await.expect("container host");
    let port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres host port");
    let uri = format!("postgres://nexus:nexus@{host}:{port}/nexus");

    let pg_pool = sqlx::PgPool::connect(&uri)
        .await
        .expect("connects to postgres for setup");
    // No seed row here — a replication slot only streams changes that
    // happen *after* it's created (`postgres-cdc`'s own
    // `postgres_cdc_integration.rs` connects the source before its DML,
    // for the same reason). The row is inserted below, after the pipeline
    // run has started and had time to actually create the slot.
    sqlx::raw_sql(
        "CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT); \
         ALTER TABLE docs REPLICA IDENTITY FULL; \
         CREATE PUBLICATION pub_docs FOR TABLE docs;",
    )
    .execute(&pg_pool)
    .await
    .expect("test table + publication created");

    let dir = tempfile::tempdir().expect("tempdir creates");
    let lancedb_uri = dir.path().to_str().unwrap().to_string();

    let spec = json!({
        "pipeline_id": "cdc-embed-lancedb",
        "sources": [{
            "connector": "postgres-cdc",
            "config": {
                "uri": uri,
                "table": "docs",
                "publication_name": "pub_docs",
                "slot_name": "slot_docs",
                // Stream ends after 1 event instead of the default 1000 —
                // this test only ever produces 1 (the insert below), and a
                // CDC source's stream otherwise blocks waiting for more
                // WAL activity that never comes, so the run would never
                // reach a terminal HTTP state.
                "max_batch_events": 1,
                "fields": [
                    {"name": "id", "data_type": "int64", "nullable": false},
                    {"name": "body", "data_type": "utf8", "nullable": false}
                ]
            }
        }],
        "embedding": {
            "source_column": "body",
            "output_column": "embedding",
            "dimension": 384,
            "model": {
                "backend": "onnx",
                "repo": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "main",
                "filename": "onnx/model.onnx",
                "tokenizer_filename": "tokenizer.json",
                "max_length": 128
            },
            "chunking": {
                "strategy": "fixed_window",
                // Larger than the seed row's text so it stays 1 chunk = 1
                // row — this test asserts the embedding stage ran at all
                // on the CDC passthrough path, not chunking's row-expansion
                // (already covered by nexus-core's own unit test).
                "chunk_size": 1000,
                "overlap": 0
            }
        },
        "sinks": [{
            "connector": "lancedb",
            "config": {
                "path": lancedb_uri.clone(),
                "table_name": "docs_embedded",
                "primary_key": "id",
                "embedding_column": "embedding",
                "dimension": 384
            }
        }]
    });

    let checkpoint_db_path = std::env::temp_dir().join(format!(
        "nexus_reactive_rag_test_checkpoints_{}.db",
        std::process::id()
    ));
    let checkpoint_db_url = format!("sqlite://{}", checkpoint_db_path.display());
    let _ = std::fs::remove_file(&checkpoint_db_path);

    let app = build_app(&test_server_config(checkpoint_db_url))
        .await
        .expect("app builds");

    let token = login(app.clone(), "admin", "test-password").await;
    let (status, body) = post_run(app.clone(), "cdc-embed-lancedb", &spec, &token).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "run was not accepted: {body:?}"
    );

    // Gives the background supervisor task time to actually call
    // `build_source`/`PostgresCdcSource::connect` (creates the replication
    // slot) before this insert happens — a slot only streams changes from
    // its creation point forward, so inserting too early would never be
    // seen by the CDC source at all (this is what made the first real run
    // of this test hang for the full 120s timeout instead of succeeding).
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    sqlx::raw_sql(
        "INSERT INTO docs (id, body) VALUES \
         (1, 'NexusFlow moves data at high speed across many connectors.');",
    )
    .execute(&pg_pool)
    .await
    .expect("seed row inserted after the CDC source should be listening");
    let run_id = body["run_id"].as_i64().expect("202 body carries run_id");
    let record = wait_for_run(&app, "cdc-embed-lancedb", run_id, &token).await;
    assert_eq!(
        record["status"], "success",
        "run must succeed — before Marco L6 this failed outright with \
         \"embedding stage is not supported on the no-transform passthrough \
         path\": {record:?}"
    );

    // Verify against LanceDB directly via `LanceDbSearchClient` (Marco L5)
    // — a zero vector still returns whatever rows exist (distance ordering
    // doesn't matter here, just presence) — the row that came through the
    // CDC source must have a real embedding vector, not just its original
    // columns, or this table wouldn't be queryable as a vector column at
    // all.
    let search_cfg = LanceDbConnectorConfig {
        uri: Some(lancedb_uri),
        path: None,
        storage_options: LanceDbStorageOptions::default(),
        table: None,
        table_name: Some("docs_embedded".to_string()),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
        timeout_seconds: 30,
    };
    let search_client = LanceDbSearchClient::connect(&search_cfg)
        .await
        .expect("connects to the sink's lancedb table");
    let results = search_client
        .search(vec![0.0f32; 384], "embedding", 10)
        .await
        .expect("search succeeds");
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "the one seed row must have made it through CDC+embedding"
    );
}
