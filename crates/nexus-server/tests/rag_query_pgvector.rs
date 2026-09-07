#![cfg(all(feature = "llm", any(feature = "embeddings", feature = "embeddings-api"), feature = "pgvector"))]

//! `POST /rag/query` against a `pgvector` sink, end to end through the real
//! HTTP API (`build_app`) — the first vector store other than LanceDB
//! validated this way (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up,
//! "RAG multi-vetor"; `rag.rs`'s own dispatch previously only had
//! `lancedb_search_integration.rs`/`reactive_rag_cdc_pipeline.rs` as
//! server-adjacent coverage, neither of which is `/rag/query` itself over
//! HTTP — this fills that gap for pgvector specifically).

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use axum::Router;
use nexus_server::{build_app, ServerConfig};
use serde_json::{json, Value};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "login must succeed: {}",
        String::from_utf8_lossy(&bytes)
    );
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    body["token"].as_str().unwrap().to_string()
}

async fn post_json(app: Router, uri: &str, token: &str, body: &Value) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        panic!(
            "POST {uri} response body wasn't JSON (status {status}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, body)
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
        // pgvector container + wiremock LLM server are both on localhost —
        // same escape hatch every other localhost-container test in this
        // suite uses (SSRF hardening otherwise blocks it, C5).
        allow_internal_hosts: true,
        trust_proxy_headers: false,
        #[cfg(feature = "version-history")]
        git_history_path: tempfile::tempdir()
            .unwrap()
            .keep()
            .join("history.git")
            .to_string_lossy()
            .to_string(),
    }
}

#[tokio::test]
async fn rag_query_answers_using_a_pgvector_sink() {
    let postgres = GenericImage::new("pgvector/pgvector", "pg16")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "nexus")
        .with_env_var("POSTGRES_PASSWORD", "nexus")
        .with_env_var("POSTGRES_DB", "nexus")
        .start()
        .await
        .expect("pgvector postgres starts");
    let host_port = postgres.get_host_port_ipv4(5432).await.expect("postgres host port");
    let uri = format!("host=127.0.0.1 port={host_port} user=nexus password=nexus dbname=nexus");

    let pg_uri = format!("postgres://nexus:nexus@127.0.0.1:{host_port}/nexus");
    let pg_pool = sqlx::PgPool::connect(&pg_uri).await.expect("connects to postgres");
    sqlx::raw_sql(
        "CREATE EXTENSION IF NOT EXISTS vector; \
         CREATE TABLE docs (id BIGINT PRIMARY KEY, chunk TEXT, embedding VECTOR(384)); \
         INSERT INTO docs (id, chunk, embedding) VALUES \
         (1, 'NexusFlow moves data at high speed across many connectors.', \
          (SELECT ('[' || string_agg('0.01', ',') || ']') FROM generate_series(1, 384))::vector);",
    )
    .execute(&pg_pool)
    .await
    .expect("creates schema and seeds a row");

    let llm_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "NexusFlow moves data quickly."}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5}
        })))
        .mount(&llm_server)
        .await;

    let checkpoint_db_path = std::env::temp_dir()
        .join(format!("nexus_rag_pgvector_test_checkpoints_{}.db", std::process::id()));
    let checkpoint_db_url = format!("sqlite://{}", checkpoint_db_path.display());
    let _ = std::fs::remove_file(&checkpoint_db_path);

    let app = build_app(&test_server_config(checkpoint_db_url)).await.expect("app builds");
    let token = login(app.clone(), "admin", "test-password").await;

    let (status, body) = post_json(
        app.clone(),
        "/prompts",
        &token,
        &json!({"name": "rag-pgvector-prompt", "template": "Context:\n{context}\n\nQuestion: {question}"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prompt must save: {body:?}");

    let spec = json!({
        "pipeline_id": "rag-pgvector-test",
        "draft": true,
        "sources": [{"connector": "csv", "config": {"path": "/unused.csv"}}],
        "sinks": [{
            "connector": "pgvector",
            "config": {
                "uri": uri,
                "table": "docs",
                "primary_key": "id",
                "embedding_column": "embedding",
                "dimension": 384
            }
        }],
        "embedding": {
            "source_column": "chunk",
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
            "chunking": {"strategy": "fixed_window", "chunk_size": 1000, "overlap": 0}
        },
        "llm": {
            "prompt": {"name": "rag-pgvector-prompt"},
            "input_columns": [],
            "output_column": "answer",
            "model": {"backend": "api", "base_url": llm_server.uri(), "model": "gpt-test"}
        }
    });
    let (status, body) = post_json(app.clone(), "/pipelines", &token, &spec).await;
    assert_eq!(status, StatusCode::CREATED, "pipeline must save: {body:?}");

    let (status, body) = post_json(
        app.clone(),
        "/rag/query",
        &token,
        &json!({"pipeline_id": "rag-pgvector-test", "question": "What does NexusFlow do?"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "rag query must succeed: {body:?}");
    assert_eq!(body["answer"], "NexusFlow moves data quickly.");
    assert_eq!(body["context_keys"], json!(["1"]));
}
