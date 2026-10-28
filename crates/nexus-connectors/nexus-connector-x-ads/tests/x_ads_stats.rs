//! End-to-end flow against a mocked X Ads API (`wiremock`).

use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use nexus_connector_x_ads::{XAdsConnectorConfig, XAdsEntity, XAdsGranularity, XAdsSource};
use nexus_core::Source;
use serde_json::json;
use std::io::Write;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> XAdsConnectorConfig {
    XAdsConnectorConfig {
        consumer_key: "consumer-key".into(),
        consumer_secret: "consumer-secret".into(),
        access_token: "access-token".into(),
        access_token_secret: "access-token-secret".into(),
        account_id: "acct123".into(),
        entity: XAdsEntity::Campaign,
        entity_ids: vec!["camp1".into()],
        metric_groups: vec!["engagement".into()],
        granularity: XAdsGranularity::Day,
        start_time: "2026-08-01T00:00:00Z".into(),
        end_time: "2026-08-02T00:00:00Z".into(),
        placement: None,
        fields: vec!["impressions".into(), "clicks".into()],
        api_base_url: server.uri(),
        poll_interval_seconds: 0,
        timeout_seconds: 10,
    }
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn source_creates_job_polls_and_downloads_gzipped_results() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/12/stats/jobs/accounts/acct123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"id": "job789", "status": "PROCESSING"},
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/12/stats/jobs/accounts/acct123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "id": "job789",
                "status": "SUCCESS",
                "url": format!("{}/download/job789.json.gz", server.uri()),
            }],
        })))
        .mount(&server)
        .await;

    let result_body = json!({
        "data": [
            {"id": "camp1", "id_data": [{"impressions": "1000", "clicks": "10"}]}
        ],
        "data_type": "stats",
    })
    .to_string();
    let gzipped = gzip(result_body.as_bytes());

    Mock::given(method("GET"))
        .and(path("/download/job789.json.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(gzipped))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = XAdsSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 1);
}
