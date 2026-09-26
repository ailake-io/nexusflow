//! End-to-end flow against a mocked Dropbox API v2 (`wiremock`).

use futures::StreamExt;
use nexus_connector_dropbox::{DropboxConnectorConfig, DropboxSink, DropboxSource};
use nexus_core::{Sink, Source};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> DropboxConnectorConfig {
    DropboxConnectorConfig {
        access_token: "sl.test".into(),
        folder_path: "/data".into(),
        delimiter: ',',
        has_header: true,
        quote: '"',
        escape: None,
        fields: Vec::new(),
        schema_sample_rows: 1000,
        batch_size: 50000,
        api_base_url: server.uri(),
        content_base_url: server.uri(),
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_lists_and_downloads_files_in_folder() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/2/files/list_folder"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "entries": [
                {".tag": "file", "name": "events.csv", "path_lower": "/data/events.csv"},
                {".tag": "folder", "name": "sub", "path_lower": "/data/sub"},
            ],
            "cursor": "c1",
            "has_more": false,
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/2/files/download"))
        .and(header("Dropbox-API-Arg", r#"{"path":"/data/events.csv"}"#))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,name\n1,alice\n2,bob\n"))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = DropboxSource::connect(&cfg).await.unwrap();
    assert_eq!(source.schema().fields().len(), 2);

    let mut stream = source.read_batches().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn sink_uploads_a_new_csv_file_per_batch() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/2/files/upload"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"name": "ok.csv"})))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = DropboxSink::connect(&cfg).await.unwrap();

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["carl"]))]).unwrap();

    sink.write_batch(batch).await.unwrap();
}
