//! End-to-end flow against a mocked Microsoft Graph API (`wiremock`).

use futures::StreamExt;
use nexus_connector_sharepoint::{SharepointConnectorConfig, SharepointSink, SharepointSource};
use nexus_core::{Sink, Source};
use serde_json::json;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> SharepointConnectorConfig {
    SharepointConnectorConfig {
        access_token: "eyJ.test".into(),
        site_id: "site123".into(),
        list_id: "list456".into(),
        fields: vec!["Title".into(), "Status".into()],
        page_size: 100,
        base_url: server.uri(),
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_follows_odata_next_link() {
    let server = MockServer::start().await;
    let next_link = format!("{}/sites/site123/lists/list456/items?page=2", server.uri());

    Mock::given(method("GET"))
        .and(path("/sites/site123/lists/list456/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [{"id": "1", "fields": {"Title": "Task A", "Status": "Open"}}],
            "@odata.nextLink": next_link,
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/sites/site123/lists/list456/items$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [{"id": "2", "fields": {"Title": "Task B", "Status": "Closed"}}],
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = SharepointSource::connect(&cfg).await.unwrap();
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
        .and(path("/sites/site123/lists/list456/items"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": "1"})))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/sites/site123/lists/list456/items/42/fields"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Title": "Updated"})))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = SharepointSink::connect(&cfg).await.unwrap();

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("Title", DataType::Utf8, false),
    ]));
    let create_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["New task"])),
        ],
    )
    .unwrap();
    sink.write_batch(create_batch).await.unwrap();

    let update_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("42")])),
            Arc::new(StringArray::from(vec!["Updated task"])),
        ],
    )
    .unwrap();
    sink.write_batch(update_batch).await.unwrap();
}
