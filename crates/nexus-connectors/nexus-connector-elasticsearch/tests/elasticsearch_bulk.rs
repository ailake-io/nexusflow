//! End-to-end flow against a mocked Elasticsearch/OpenSearch Bulk API
//! (`wiremock`) — covers upsert, delete, and item-level bulk failure.

use arrow_array::builder::FixedSizeListBuilder;
use arrow_array::builder::Float32Builder;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_connector_elasticsearch::{ElasticsearchConnectorConfig, ElasticsearchSink};
use nexus_core::Sink;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> ElasticsearchConnectorConfig {
    ElasticsearchConnectorConfig {
        hosts: vec![server.uri()],
        api_key: None,
        username: Some("elastic".into()),
        password: Some("changeme".into()),
        index: "events".into(),
        primary_key: "id".into(),
        embedding_column: "embedding".into(),
        dimension: 2,
        timeout_seconds: 10,
    }
}

fn batch_with_embedding() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
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
        vec![Arc::new(Int64Array::from(vec![1])), Arc::new(embedding_builder.finish())],
    )
    .unwrap()
}

#[tokio::test]
async fn sink_writes_bulk_upsert_successfully() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "errors": false, "items": [] })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = ElasticsearchSink::connect(&cfg).await.unwrap();
    sink.write_batch(batch_with_embedding()).await.unwrap();
}

#[tokio::test]
async fn sink_surfaces_item_level_bulk_failures() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errors": true,
            "items": [{"index": {"_id": "1", "status": 400, "error": {"type": "mapper_parsing_exception"}}}]
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = ElasticsearchSink::connect(&cfg).await.unwrap();
    let err = sink.write_batch(batch_with_embedding()).await.unwrap_err();
    assert!(format!("{err}").contains("item-level failures"));
}

#[tokio::test]
async fn sink_deletes_via_opcode_split() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "errors": false, "items": [] })))
        .mount(&server)
        .await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
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
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(embedding_builder.finish()),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();

    let cfg = config(&server);
    let mut sink = ElasticsearchSink::connect(&cfg).await.unwrap();
    sink.write_batch(batch).await.unwrap();
}
