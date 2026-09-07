#![cfg(all(
    feature = "postgres-cdc",
    feature = "lancedb",
    any(feature = "embeddings", feature = "embeddings-api")
))]

//! Reactive RAG (`postgres-cdc -> embedding -> lancedb`, no `transform`
//! node) is enterprise-gated as of LLMOPS_IMPLEMENTATION_PLAN.md Marco L8
//! (`"reactive-rag-cdc"` slug, `capability_registry.rs`) — this test now
//! proves an OSS binary with no license installed can't actually run this
//! combination, end to end through the real HTTP API, not just that
//! `check_connector_license` itself works (already covered directly by
//! `capability_registry.rs`'s unit tests) or that the gate is wired into
//! `run_passthrough_pipeline` (covered by `runner.rs`'s own
//! `reactive_rag_cdc_combination_is_denied_without_a_covering_license`,
//! which uses a bogus URI for speed).
//!
//! No real postgres container here on purpose: the license check in
//! `run_passthrough_pipeline` runs before any connector actually connects
//! (proven by the two tests above), so a real database would only add
//! ~1-2s of container startup for zero additional verification — the
//! `postgres-cdc` config below just needs to deserialize into
//! `PostgresCdcConfig`, never needs to be reachable.
//!
//! The positive case ("with a license covering `reactive-rag-cdc`, this
//! combination actually works") is **not** testable from this file, or
//! anywhere in the public `nexusflow` repo: `license::test_support`'s
//! signing key only exists under `#[cfg(test)]` inside `nexus-server`'s
//! own crate compilation — an external `tests/*.rs` file links against
//! the library built *without* `cfg(test)`, so it sees the real
//! (unsigned-here) production public key and no `test_support` module at
//! all. That positive test belongs wherever a real or test signing key
//! actually lives (`nexus-licensing`), not this OSS repo — same reasoning
//! `license.rs`'s own module doc gives for never committing the real
//! private key here.

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use axum::Router;
use nexus_server::{build_app, ServerConfig};
use serde_json::{json, Value};
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
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
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
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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
        allow_internal_hosts: true,
        trust_proxy_headers: false,
    }
}

#[tokio::test]
async fn reactive_rag_cdc_is_denied_without_a_covering_license() {
    let spec = json!({
        "pipeline_id": "cdc-embed-lancedb",
        "sources": [{
            "connector": "postgres-cdc",
            "config": {
                // Never dialed — the license check runs before any
                // connector actually connects (see module doc comment).
                "uri": "postgres://nobody:nobody@127.0.0.1:1/nowhere",
                "table": "docs",
                "publication_name": "pub_docs",
                "slot_name": "slot_docs",
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
            }
        },
        "sinks": [{
            "connector": "lancedb",
            "config": {
                "path": "/tmp/unused-reactive-rag-gate-test",
                "table_name": "docs_embedded",
                "primary_key": "id",
                "embedding_column": "embedding",
                "dimension": 384
            }
        }]
    });

    let checkpoint_db_path = std::env::temp_dir().join(format!(
        "nexus_reactive_rag_gate_test_checkpoints_{}.db",
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
        "run is still accepted synchronously — the license gate fires inside the \
         background run task, not at POST time: {body:?}"
    );

    let run_id = body["run_id"].as_i64().expect("202 body carries run_id");
    let record = wait_for_run(&app, "cdc-embed-lancedb", run_id, &token).await;
    assert_eq!(
        record["status"], "failed",
        "no license covers reactive-rag-cdc — the run must not succeed: {record:?}"
    );
    let error = record["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("reactive-rag-cdc"),
        "run's error should name the missing capability, got: {error:?}"
    );
}
