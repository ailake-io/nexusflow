//! End-to-end flow against a mocked TikTok Business API (`wiremock`).

use futures::StreamExt;
use nexus_connector_tiktok_ads::{TiktokAdsConnectorConfig, TiktokAdsSource};
use nexus_core::Source;
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> TiktokAdsConnectorConfig {
    TiktokAdsConnectorConfig {
        access_token: "test-access-token".into(),
        advertiser_id: "1234567890".into(),
        report_type: "BASIC".into(),
        data_level: "AUCTION_CAMPAIGN".into(),
        dimensions: vec!["campaign_id".into()],
        metrics: vec!["impressions".into()],
        start_date: "2026-01-01".into(),
        end_date: "2026-01-31".into(),
        page_size: 1,
        api_base_url: server.uri(),
        timeout_seconds: 10,
    }
}

#[tokio::test]
async fn source_paginates_by_page_until_total_page() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/open_api/v1.3/report/integrated/get/"))
        .and(header("Access-Token", "test-access-token"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "message": "OK",
            "data": {
                "list": [{"dimensions": {"campaign_id": "111"}, "metrics": {"impressions": "1000"}}],
                "page_info": {"page": 1, "page_size": 1, "total_number": 2, "total_page": 2},
            }
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/open_api/v1.3/report/integrated/get/"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": 0,
            "message": "OK",
            "data": {
                "list": [{"dimensions": {"campaign_id": "222"}, "metrics": {"impressions": "2000"}}],
                "page_info": {"page": 2, "page_size": 1, "total_number": 2, "total_page": 2},
            }
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = TiktokAdsSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}
