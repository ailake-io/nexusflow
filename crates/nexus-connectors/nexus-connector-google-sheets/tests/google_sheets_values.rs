//! End-to-end flow against a mocked Google Sheets API v4 (`wiremock`).

use futures::StreamExt;
use nexus_connector_google_sheets::{
    GoogleSheetsConnectorConfig, GoogleSheetsSink, GoogleSheetsSource,
};
use nexus_core::{Sink, Source};
use serde_json::json;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> GoogleSheetsConnectorConfig {
    GoogleSheetsConnectorConfig {
        access_token: "ya29.test".into(),
        spreadsheet_id: "abc123".into(),
        range: "Sheet1!A1:B3".into(),
        has_header_row: true,
        base_url: server.uri(),
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_reads_header_and_data_rows() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"^/v4/spreadsheets/abc123/values/.*$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "range": "Sheet1!A1:B3",
            "majorDimension": "ROWS",
            "values": [
                ["name", "age"],
                ["Alice", "30"],
                ["Bob", "25"],
            ],
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = GoogleSheetsSource::connect(&cfg).await.unwrap();
    assert_eq!(source.schema().fields().len(), 2);
    assert_eq!(source.schema().field(0).name(), "name");

    let mut stream = source.read_batches().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn sink_appends_rows() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path_regex(r"^/v4/spreadsheets/abc123/values/.*:append$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"updates": {"updatedRows": 1}})),
        )
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = GoogleSheetsSink::connect(&cfg).await.unwrap();

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["Carl"])),
            Arc::new(StringArray::from(vec!["40"])),
        ],
    )
    .unwrap();

    sink.write_batch(batch).await.unwrap();
}
