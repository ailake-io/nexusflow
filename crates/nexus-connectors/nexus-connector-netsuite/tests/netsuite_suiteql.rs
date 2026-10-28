//! End-to-end flow against a mocked NetSuite REST API (`wiremock`).

use futures::StreamExt;
use nexus_connector_netsuite::{NetsuiteConnectorConfig, NetsuiteSink, NetsuiteSource};
use nexus_core::{Sink, Source};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> NetsuiteConnectorConfig {
    NetsuiteConnectorConfig {
        account_url: server.uri(),
        access_token: "eyJ.test".into(),
        record_type: Some("customer".into()),
        query: Some("SELECT id, entityid FROM customer".into()),
        page_size: 2,
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_infers_schema_and_pages_via_has_more() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/services/rest/query/v1/suiteql"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {"id": "1", "entityid": "Acme"},
                {"id": "2", "entityid": "Globex"},
            ],
            "hasMore": true,
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/services/rest/query/v1/suiteql"))
        .and(query_param("offset", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": "3", "entityid": "Initech"}],
            "hasMore": false,
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = NetsuiteSource::connect(&cfg).await.unwrap();
    assert_eq!(source.schema().fields().len(), 2);

    let mut stream = source.read_batches().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn sink_creates_when_id_absent_and_updates_when_present() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/services/rest/record/v1/customer"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/services/rest/record/v1/customer/42"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = NetsuiteSink::connect(&cfg).await.unwrap();

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("entityid", DataType::Utf8, false),
    ]));
    let create_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["New customer"])),
        ],
    )
    .unwrap();
    sink.write_batch(create_batch).await.unwrap();

    let update_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("42")])),
            Arc::new(StringArray::from(vec!["Updated customer"])),
        ],
    )
    .unwrap();
    sink.write_batch(update_batch).await.unwrap();
}
