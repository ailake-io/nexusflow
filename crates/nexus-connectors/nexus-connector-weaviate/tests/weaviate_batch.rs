//! End-to-end flow against a mocked Weaviate REST API (`wiremock`) —
//! covers batch upsert, batch delete via `where`/id-in filter, and
//! item-level batch failure.

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_connector_weaviate::{WeaviateConnectorConfig, WeaviateSink};
use nexus_core::Sink;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> WeaviateConnectorConfig {
    WeaviateConnectorConfig {
        host: server.uri(),
        api_key: Some("test-key".into()),
        class_name: "Event".into(),
        primary_key: "id".into(),
        embedding_column: "embedding".into(),
        dimension: 2,
        timeout_seconds: 10,
        retry: Default::default(),
        batch_delete_size: 5,
        max_concurrent_requests: 2,
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
        .and(path("/v1/batch/objects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"class": "Event", "id": "00000000-0000-0000-0000-000000000001", "result": {"status": "SUCCESS"}}
        ])))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = WeaviateSink::connect(&cfg).await.unwrap();
    sink.write_batch(batch_with_embedding("00000000-0000-0000-0000-000000000001"))
        .await
        .unwrap();
}

#[tokio::test]
async fn sink_surfaces_item_level_batch_failures() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/batch/objects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"class": "Event", "id": "00000000-0000-0000-0000-000000000001",
             "result": {"status": "FAILED", "errors": {"error": [{"message": "invalid vector length"}]}}}
        ])))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = WeaviateSink::connect(&cfg).await.unwrap();
    let err = sink
        .write_batch(batch_with_embedding("00000000-0000-0000-0000-000000000001"))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("item-level failures"));
}

#[tokio::test]
async fn sink_deletes_via_opcode_split() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v1/batch/objects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "matches": 1,
            "successful": 1,
            "failed": 0,
            "objects": [{"id": "00000000-0000-0000-0000-000000000002", "status": "SUCCESS"}]
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
            Arc::new(StringArray::from(vec![
                "00000000-0000-0000-0000-000000000002",
            ])),
            Arc::new(embedding_builder.finish()),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();

    let cfg = config(&server);
    let mut sink = WeaviateSink::connect(&cfg).await.unwrap();
    sink.write_batch(batch).await.unwrap();
}
