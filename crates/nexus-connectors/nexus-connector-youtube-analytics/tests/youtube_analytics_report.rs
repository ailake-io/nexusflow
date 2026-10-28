//! End-to-end flow against a mocked YouTube Analytics API (`wiremock`)
//! — covers index-based pagination and `columnHeaders`→`rows`
//! positional mapping.

use futures::StreamExt;
use nexus_connector_youtube_analytics::{YoutubeAnalyticsConnectorConfig, YoutubeAnalyticsSource};
use nexus_core::Source;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> YoutubeAnalyticsConnectorConfig {
    YoutubeAnalyticsConnectorConfig {
        access_token: "test-access-token".into(),
        refresh_token: None,
        client_id: None,
        client_secret: None,
        ids: "channel==MINE".into(),
        dimensions: vec!["day".into()],
        metrics: vec!["views".into()],
        start_date: "2026-01-01".into(),
        end_date: "2026-01-31".into(),
        filters: None,
        max_results: 1,
        token_uri: format!("{}/token", server.uri()),
        api_base_url: server.uri(),
        timeout_seconds: 10,
    }
}

#[tokio::test]
async fn source_paginates_by_index_and_maps_columns_positionally() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/reports"))
        .and(query_param("startIndex", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "columnHeaders": [{"name": "day"}, {"name": "views"}],
            "rows": [["2026-01-01", "100"]],
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/reports"))
        .and(query_param("startIndex", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "columnHeaders": [{"name": "day"}, {"name": "views"}],
            "rows": [],
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = YoutubeAnalyticsSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 1);
}
