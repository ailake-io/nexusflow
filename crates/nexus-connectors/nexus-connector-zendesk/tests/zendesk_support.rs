//! End-to-end flow against a mocked Zendesk Support API v2 (`wiremock`).

use futures::StreamExt;
use nexus_connector_zendesk::{
    ZendeskConnectorConfig, ZendeskResource, ZendeskSink, ZendeskSource,
};
use nexus_core::{Sink, Source};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> ZendeskConnectorConfig {
    ZendeskConnectorConfig {
        base_url: server.uri(),
        email: "agent@example.com".into(),
        api_token: "tok".into(),
        resource: ZendeskResource::Tickets,
        fields: vec!["id".into(), "subject".into()],
        page_size: 100,
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_paginates_via_after_cursor() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/v2/tickets.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tickets": [{"id": "1", "subject": "Help"}],
            "meta": {"has_more": true, "after_cursor": "cursor-1"},
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/tickets.json"))
        .and(query_param("page[after]", "cursor-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tickets": [{"id": "2", "subject": "Other"}],
            "meta": {"has_more": false},
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = ZendeskSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn sink_creates_when_id_absent_and_updates_when_present() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/tickets.json"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"ticket": {"id": 1}})))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/v2/tickets/42.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ticket": {"id": 42}})))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = ZendeskSink::connect(&cfg).await.unwrap();

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("subject", DataType::Utf8, false),
    ]));
    let create_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["New ticket"])),
        ],
    )
    .unwrap();
    sink.write_batch(create_batch).await.unwrap();

    let update_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("42")])),
            Arc::new(StringArray::from(vec!["Updated ticket"])),
        ],
    )
    .unwrap();
    sink.write_batch(update_batch).await.unwrap();
}
