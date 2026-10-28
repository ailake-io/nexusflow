//! End-to-end flow against a mocked HubSpot CRM v3 API (`wiremock`).

use futures::StreamExt;
use nexus_connector_hubspot::{HubspotConnectorConfig, HubspotSink, HubspotSource};
use nexus_core::{Sink, Source};
use serde_json::json;
use wiremock::matchers::{body_partial_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> HubspotConnectorConfig {
    HubspotConnectorConfig {
        access_token: "pat-test".into(),
        object_type: "contacts".into(),
        properties: vec!["email".into(), "firstname".into()],
        id_property: Some("email".into()),
        page_size: 100,
        base_url: server.uri(),
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_paginates_via_after_cursor() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/crm/v3/objects/contacts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id": "1", "properties": {"email": "a@b.com", "firstname": "Alice"}}],
            "paging": {"next": {"after": "cursor-1"}},
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/crm/v3/objects/contacts"))
        .and(query_param("after", "cursor-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id": "2", "properties": {"email": "c@d.com", "firstname": "Carl"}}],
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = HubspotSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn sink_upserts_via_batch_upsert_endpoint() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/crm/v3/objects/contacts/batch/upsert"))
        .and(body_partial_json(json!({
            "inputs": [{"idProperty": "email", "id": "a@b.com"}]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results": []})))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = HubspotSink::connect(&cfg).await.unwrap();

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("email", DataType::Utf8, false),
        Field::new("firstname", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a@b.com"])),
            Arc::new(StringArray::from(vec!["Alice"])),
        ],
    )
    .unwrap();

    sink.write_batch(batch).await.unwrap();
}
