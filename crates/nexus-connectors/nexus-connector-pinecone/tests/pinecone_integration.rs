//! Mockable-only test (no real Pinecone, no Docker option — see
//! `config.rs`): verifies the sink issues the right requests against a
//! `wiremock` server, same testing style as `nexus-connector-rest` (Marco 3).

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_connector_pinecone::{PineconeConnectorConfig, PineconeSink};
use nexus_core::Sink;
use std::sync::Arc;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn sample_batch() -> RecordBatch {
    let field = Arc::new(Field::new("item", DataType::Float32, false));
    let values: arrow_array::Float32Array = vec![0.5, 0.25, 0.75, 1.0].into();
    let embedding =
        arrow_array::FixedSizeListArray::try_new(field, 2, Arc::new(values), None).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("embedding", embedding.data_type().clone(), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(embedding),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn write_batch_upserts_vectors_with_metadata_and_values() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/vectors/upsert"))
        .and(header("Api-Key", "test-key"))
        .and(body_json(serde_json::json!({
            "vectors": [
                {"id": "1", "values": [0.5, 0.25], "metadata": {"id": 1, "text": "a"}},
                {"id": "2", "values": [0.75, 1.0], "metadata": {"id": 2, "text": "b"}},
            ]
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let cfg = PineconeConnectorConfig {
        host: server.uri(),
        api_key: "test-key".to_string(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 2,
        namespace: None,
        timeout_seconds: 30,
    };
    let mut sink = PineconeSink::connect(&cfg).expect("sink connects");
    sink.write_batch(sample_batch())
        .await
        .expect("writes batch");
}

#[tokio::test]
async fn write_batch_deletes_rows_with_delete_opcode() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/vectors/upsert"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/vectors/delete"))
        .and(header("Api-Key", "test-key"))
        .and(body_json(serde_json::json!({ "ids": ["1"] })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let cfg = PineconeConnectorConfig {
        host: server.uri(),
        api_key: "test-key".to_string(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 2,
        namespace: None,
        timeout_seconds: 30,
    };
    let mut sink = PineconeSink::connect(&cfg).expect("sink connects");

    let field = Arc::new(Field::new("item", DataType::Float32, false));
    let values: arrow_array::Float32Array = vec![0.1, 0.2].into();
    let embedding =
        arrow_array::FixedSizeListArray::try_new(field, 2, Arc::new(values), None).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("embedding", embedding.data_type().clone(), false),
        Field::new(nexus_core::OPCODE_COLUMN, DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(embedding),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();

    sink.write_batch(batch).await.expect("writes delete batch");
}

#[tokio::test]
async fn write_batch_includes_namespace_when_configured() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/vectors/upsert"))
        .and(body_json(serde_json::json!({
            "namespace": "docs-ns",
            "vectors": [
                {"id": "1", "values": [0.5, 0.25], "metadata": {"id": 1, "text": "a"}},
                {"id": "2", "values": [0.75, 1.0], "metadata": {"id": 2, "text": "b"}},
            ]
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let cfg = PineconeConnectorConfig {
        host: server.uri(),
        api_key: "test-key".to_string(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 2,
        namespace: Some("docs-ns".to_string()),
        timeout_seconds: 30,
    };
    let mut sink = PineconeSink::connect(&cfg).expect("sink connects");
    sink.write_batch(sample_batch())
        .await
        .expect("writes batch");
}
