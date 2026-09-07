//! `PineconeSearchClient` (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up
//! — RAG multi-vetor) — mocked, same reasoning as `pinecone_integration.rs`
//! (no self-hosted/Docker option for Pinecone, see `config.rs`): this is
//! the one vector search client in this round tested against a `wiremock`
//! server instead of a real service, a deliberate exception to the rest of
//! this round's real-container tests.

use nexus_connector_pinecone::{PineconeConnectorConfig, PineconeSearchClient};
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn cfg(host: String) -> PineconeConnectorConfig {
    PineconeConnectorConfig {
        host,
        api_key: "test-key".to_string(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 2,
        namespace: None,
        timeout_seconds: 30,
        port: None,
        grpc_url: None,
        index_name: None,
    }
}

#[tokio::test]
async fn search_extracts_id_and_source_text_from_matches() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/query"))
        .and(header("Api-Key", "test-key"))
        .and(body_json(serde_json::json!({
            "vector": [0.5, 0.25],
            "topK": 2,
            "includeMetadata": true
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "matches": [
                {"id": "1", "score": 0.98, "metadata": {"text": "nexusflow moves data fast"}},
                {"id": "2", "score": 0.10, "metadata": {"text": "the weather is sunny"}},
            ],
            "namespace": ""
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = PineconeSearchClient::connect(&cfg(server.uri())).expect("client connects");
    let hits = client
        .search(vec![0.5, 0.25], "text", 2)
        .await
        .expect("search succeeds");

    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0], ("1".to_string(), "nexusflow moves data fast".to_string()));
    assert_eq!(hits[1], ("2".to_string(), "the weather is sunny".to_string()));
}

#[tokio::test]
async fn search_skips_matches_missing_the_source_column() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "matches": [
                {"id": "1", "score": 0.98, "metadata": {"other_field": "irrelevant"}},
                {"id": "2", "score": 0.90, "metadata": {"text": "has the field"}},
            ]
        })))
        .mount(&server)
        .await;

    let client = PineconeSearchClient::connect(&cfg(server.uri())).expect("client connects");
    let hits = client
        .search(vec![0.1, 0.2], "text", 2)
        .await
        .expect("search succeeds");

    assert_eq!(hits, vec![("2".to_string(), "has the field".to_string())]);
}

#[tokio::test]
async fn search_includes_namespace_when_configured() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/query"))
        .and(body_json(serde_json::json!({
            "vector": [0.5, 0.25],
            "topK": 1,
            "includeMetadata": true,
            "namespace": "docs-ns"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"matches": []})))
        .expect(1)
        .mount(&server)
        .await;

    let mut config = cfg(server.uri());
    config.namespace = Some("docs-ns".to_string());
    let client = PineconeSearchClient::connect(&config).expect("client connects");
    client
        .search(vec![0.5, 0.25], "text", 1)
        .await
        .expect("search succeeds");
}
