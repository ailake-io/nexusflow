//! End-to-end flow against a mocked Dataverse Web API (`wiremock`).

use futures::StreamExt;
use nexus_connector_dynamics365::{Dynamics365ConnectorConfig, Dynamics365Sink, Dynamics365Source};
use nexus_core::{Sink, Source};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> Dynamics365ConnectorConfig {
    Dynamics365ConnectorConfig {
        base_url: server.uri(),
        access_token: "eyJ.test".into(),
        entity_set: "accounts".into(),
        id_field: Some("accountid".into()),
        fields: vec!["accountid".into(), "name".into()],
        page_size: 100,
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_follows_odata_next_link() {
    let server = MockServer::start().await;
    let next_link = format!("{}/api/data/v9.2/accounts?page=2", server.uri());

    Mock::given(method("GET"))
        .and(path("/api/data/v9.2/accounts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [{"accountid": "1", "name": "Acme"}],
            "@odata.nextLink": next_link,
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/data/v9.2/accounts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [{"accountid": "2", "name": "Globex"}],
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = Dynamics365Source::connect(&cfg).await.unwrap();
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
        .and(path("/api/data/v9.2/accounts"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/api/data/v9.2/accounts(abc-123)"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = Dynamics365Sink::connect(&cfg).await.unwrap();

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("accountid", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
    ]));
    let create_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["New account"])),
        ],
    )
    .unwrap();
    sink.write_batch(create_batch).await.unwrap();

    let update_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("abc-123")])),
            Arc::new(StringArray::from(vec!["Updated account"])),
        ],
    )
    .unwrap();
    sink.write_batch(update_batch).await.unwrap();
}
