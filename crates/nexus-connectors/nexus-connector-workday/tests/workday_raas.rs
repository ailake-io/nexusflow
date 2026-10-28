//! End-to-end flow against a mocked Workday RaaS endpoint (`wiremock`).

use futures::StreamExt;
use nexus_connector_workday::{WorkdayConnectorConfig, WorkdaySource};
use nexus_core::Source;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> WorkdayConnectorConfig {
    WorkdayConnectorConfig {
        base_url: server.uri(),
        username: "isu_integration".into(),
        password: "secret".into(),
        tenant: "acme".into(),
        report_owner: "isu_integration@acme".into(),
        report_name: "Custom_Worker_Report".into(),
        timeout_seconds: 10,
        retry: Default::default(),
    }
}

#[tokio::test]
async fn source_reads_report_entry_and_infers_schema() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/ccx/service/customreport2/acme/isu_integration@acme/Custom_Worker_Report",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Report_Entry": [
                {"Worker": "John Doe", "Position": "Engineer"},
                {"Worker": "Jane Doe", "Position": "Manager"},
            ],
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = WorkdaySource::connect(&cfg).await.unwrap();
    assert_eq!(source.schema().fields().len(), 2);

    let mut stream = source.read_batches().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}
