//! End-to-end flow against a mocked Meta Marketing API (`wiremock`).

use futures::StreamExt;
use nexus_connector_meta_ads::{MetaAdsConnectorConfig, MetaAdsLevel, MetaAdsSource};
use nexus_core::Source;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> MetaAdsConnectorConfig {
    MetaAdsConnectorConfig {
        ad_account_id: "123".into(),
        access_token: "test-system-user-token".into(),
        fields: vec!["impressions".into(), "clicks".into()],
        level: MetaAdsLevel::Campaign,
        breakdowns: vec![],
        date_preset: Some("last_7d".into()),
        since: None,
        until: None,
        api_version: "v23.0".into(),
        graph_api_base_url: server.uri(),
        poll_interval_seconds: 0,
        timeout_seconds: 10,
    }
}

#[tokio::test]
async fn source_polls_async_job_and_paginates_results() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v23.0/act_123/insights"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "report_run_id": "789",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v23.0/789"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "async_status": "Job Completed",
        })))
        .mount(&server)
        .await;

    let next_url = format!("{}/v23.0/789/insights?after=cursor1", server.uri());
    Mock::given(method("GET"))
        .and(path("/v23.0/789/insights"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"impressions": "1000", "clicks": "10"}],
            "paging": {"next": next_url},
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v23.0/789/insights"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"impressions": "2000", "clicks": "20"}],
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = MetaAdsSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}
