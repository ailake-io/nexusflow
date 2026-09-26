//! End-to-end flow against a mocked Azure AI Search REST API
//! (`wiremock`) — covers upsert, item-level failure, and opcode-driven
//! delete, all through the single `docs/index` endpoint.

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_connector_azure_ai_search::{AzureAiSearchConnectorConfig, AzureAiSearchSink};
use nexus_core::Sink;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> AzureAiSearchConnectorConfig {
    AzureAiSearchConnectorConfig {
        endpoint: server.uri(),
        api_key: "test-admin-key".into(),
        index_name: "events".into(),
        primary_key: "id".into(),
        embedding_column: "embedding".into(),
        dimension: 2,
        api_version: "2024-07-01".into(),
        timeout_seconds: 10,
    }
}

fn batch_with_embedding(id: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
    ]));
    let mut embedding_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    embedding_builder.values().append_value(0.1);
    embedding_builder.values().append_value(0.2);
    embedding_builder.append(true);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![id])),
            Arc::new(embedding_builder.finish()),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn sink_writes_batch_upsert_successfully() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/indexes/events/docs/index"))
        .and(query_param("api-version", "2024-07-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [{"key": "doc-1", "status": true, "statusCode": 200}]
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = AzureAiSearchSink::connect(&cfg).await.unwrap();
    sink.write_batch(batch_with_embedding("doc-1"))
        .await
        .unwrap();
}

#[tokio::test]
async fn sink_surfaces_item_level_failures() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/indexes/events/docs/index"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [{"key": "doc-1", "status": false, "statusCode": 400, "errorMessage": "invalid vector length"}]
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = AzureAiSearchSink::connect(&cfg).await.unwrap();
    let err = sink
        .write_batch(batch_with_embedding("doc-1"))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("item-level failures"));
}

#[tokio::test]
async fn sink_deletes_via_opcode_split() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/indexes/events/docs/index"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [{"key": "doc-2", "status": true, "statusCode": 200}]
        })))
        .mount(&server)
        .await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
        Field::new("__opcode", DataType::Utf8, false),
    ]));
    let mut embedding_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    embedding_builder.values().append_value(0.1);
    embedding_builder.values().append_value(0.2);
    embedding_builder.append(true);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["doc-2"])),
            Arc::new(embedding_builder.finish()),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();

    let cfg = config(&server);
    let mut sink = AzureAiSearchSink::connect(&cfg).await.unwrap();
    sink.write_batch(batch).await.unwrap();
}
