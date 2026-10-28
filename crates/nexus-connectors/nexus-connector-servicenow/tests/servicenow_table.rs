//! End-to-end flow against a mocked ServiceNow Table API (`wiremock`).

use futures::StreamExt;
use nexus_connector_servicenow::{ServicenowConnectorConfig, ServicenowSink, ServicenowSource};
use nexus_core::{Sink, Source};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> ServicenowConnectorConfig {
    ServicenowConnectorConfig {
        base_url: server.uri(),
        username: "admin".into(),
        password: "secret".into(),
        table: "incident".into(),
        fields: vec!["sys_id".into(), "short_description".into()],
        page_size: 2,
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_pages_via_offset_until_short_page() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/now/table/incident"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [
                {"sys_id": "1", "short_description": "Down"},
                {"sys_id": "2", "short_description": "Slow"},
            ],
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/incident"))
        .and(query_param("sysparm_offset", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{"sys_id": "3", "short_description": "Broken"}],
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = ServicenowSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn sink_creates_when_sys_id_absent_and_updates_when_present() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/now/table/incident"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"result": {"sys_id": "1"}})))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/api/now/table/incident/abc123"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"result": {"sys_id": "abc123"}})),
        )
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = ServicenowSink::connect(&cfg).await.unwrap();

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("sys_id", DataType::Utf8, true),
        Field::new("short_description", DataType::Utf8, false),
    ]));
    let create_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["New incident"])),
        ],
    )
    .unwrap();
    sink.write_batch(create_batch).await.unwrap();

    let update_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("abc123")])),
            Arc::new(StringArray::from(vec!["Updated incident"])),
        ],
    )
    .unwrap();
    sink.write_batch(update_batch).await.unwrap();
}
