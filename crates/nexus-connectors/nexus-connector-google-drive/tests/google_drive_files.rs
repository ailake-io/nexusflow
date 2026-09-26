//! End-to-end flow against a mocked Google Drive API v3 (`wiremock`).

use futures::StreamExt;
use nexus_connector_google_drive::{
    GoogleDriveConnectorConfig, GoogleDriveSink, GoogleDriveSource,
};
use nexus_core::{Sink, Source};
use serde_json::json;
use wiremock::matchers::{method, path, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> GoogleDriveConnectorConfig {
    GoogleDriveConnectorConfig {
        access_token: "ya29.test".into(),
        folder_id: "folder123".into(),
        delimiter: ',',
        has_header: true,
        quote: '"',
        escape: None,
        fields: Vec::new(),
        schema_sample_rows: 1000,
        batch_size: 50000,
        api_base_url: server.uri(),
        upload_base_url: server.uri(),
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_lists_and_downloads_files_in_folder() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/drive/v3/files"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "files": [{"id": "file1", "name": "events.csv"}],
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/drive/v3/files/file1"))
        .and(query_param("alt", "media"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,name\n1,alice\n2,bob\n"))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = GoogleDriveSource::connect(&cfg).await.unwrap();
    assert_eq!(source.schema().fields().len(), 2);

    let mut stream = source.read_batches().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn sink_creates_and_uploads_a_new_csv_file_per_batch() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/drive/v3/files"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "new-file-1"})))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path_regex(r"^/upload/drive/v3/files/new-file-1$"))
        .and(query_param("uploadType", "media"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "new-file-1"})))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = GoogleDriveSink::connect(&cfg).await.unwrap();

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["carl"]))]).unwrap();

    sink.write_batch(batch).await.unwrap();
}
