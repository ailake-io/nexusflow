//! End-to-end flow against a mocked Google Ads API (`wiremock`).

use futures::StreamExt;
use nexus_connector_google_ads::{GoogleAdsConnectorConfig, GoogleAdsSource};
use nexus_core::Source;
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> GoogleAdsConnectorConfig {
    GoogleAdsConnectorConfig {
        developer_token: "test-dev-token".into(),
        login_customer_id: "1112223333".into(),
        customer_id: "4445556666".into(),
        client_id: "test-client-id".into(),
        client_secret: "test-client-secret".into(),
        refresh_token: "test-refresh-token".into(),
        gaql_query: "SELECT campaign.name, metrics.clicks FROM campaign".into(),
        fields: vec!["campaign.name".into(), "metrics.clicks".into()],
        api_version: "v25".into(),
        token_uri: format!("{}/token", server.uri()),
        ads_api_base_url: server.uri(),
        timeout_seconds: 10,
    }
}

#[tokio::test]
async fn source_flattens_nested_search_stream_rows() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "test-access-token",
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/v25/customers/4445556666/googleAds:searchStream"))
        .and(header("developer-token", "test-dev-token"))
        .and(header("login-customer-id", "1112223333"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {
                "results": [
                    {"campaign": {"name": "Summer Sale"}, "metrics": {"clicks": "42"}},
                    {"campaign": {"name": "Winter Sale"}, "metrics": {"clicks": "7"}},
                ]
            }
        ])))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = GoogleAdsSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}
