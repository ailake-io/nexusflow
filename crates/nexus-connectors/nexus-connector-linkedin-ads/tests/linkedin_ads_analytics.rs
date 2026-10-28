//! End-to-end flow against a mocked LinkedIn Marketing API (`wiremock`).

use futures::StreamExt;
use nexus_connector_linkedin_ads::{LinkedinAdsConnectorConfig, LinkedinAdsSource};
use nexus_core::Source;
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn source_reads_ad_analytics_without_pagination() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/rest/adAnalytics"))
        .and(query_param("pivot", "CAMPAIGN"))
        .and(header("LinkedIn-Version", "202605"))
        .and(header("X-Restli-Protocol-Version", "2.0.0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "elements": [
                {"impressions": "1000", "clicks": "10"},
                {"impressions": "2000", "clicks": "20"},
            ]
        })))
        .mount(&server)
        .await;

    let cfg = LinkedinAdsConnectorConfig {
        access_token: "test-access-token".into(),
        refresh_token: None,
        client_id: None,
        client_secret: None,
        account_urns: vec!["urn:li:sponsoredAccount:123456".into()],
        pivot: "CAMPAIGN".into(),
        fields: vec!["impressions".into(), "clicks".into()],
        start_date: (2026, 1, 1),
        end_date: (2026, 1, 31),
        finder: "analytics".into(),
        linkedin_version: "202605".into(),
        token_uri: format!("{}/token", server.uri()),
        api_base_url: server.uri(),
        timeout_seconds: 10,
    };

    let mut source = LinkedinAdsSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn source_refreshes_token_when_mdp_credentials_configured() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "refreshed-token",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/rest/adAnalytics"))
        .and(header("Authorization", "Bearer refreshed-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "elements": [] })))
        .mount(&server)
        .await;

    let cfg = LinkedinAdsConnectorConfig {
        access_token: "stale-token".into(),
        refresh_token: Some("test-refresh-token".into()),
        client_id: Some("test-client-id".into()),
        client_secret: Some("test-client-secret".into()),
        account_urns: vec!["urn:li:sponsoredAccount:123456".into()],
        pivot: "CAMPAIGN".into(),
        fields: vec!["impressions".into()],
        start_date: (2026, 1, 1),
        end_date: (2026, 1, 31),
        finder: "analytics".into(),
        linkedin_version: "202605".into(),
        token_uri: format!("{}/token", server.uri()),
        api_base_url: server.uri(),
        timeout_seconds: 10,
    };

    let mut source = LinkedinAdsSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();
    while let Some(batch) = stream.next().await {
        batch.unwrap();
    }
}
