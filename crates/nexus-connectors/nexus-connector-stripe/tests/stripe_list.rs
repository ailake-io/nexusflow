//! End-to-end flow against a mocked Stripe API (`wiremock`).

use futures::StreamExt;
use nexus_connector_stripe::{StripeConnectorConfig, StripeResource, StripeSource};
use nexus_core::Source;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> StripeConnectorConfig {
    StripeConnectorConfig {
        secret_key: "sk_test_123".into(),
        resource: StripeResource::Charges,
        fields: vec!["id".into(), "amount".into()],
        created_gte: None,
        created_lt: None,
        stripe_version: "2025-11-17".into(),
        api_base_url: server.uri(),
        timeout_seconds: 10,
        retry: Default::default(),
        page_limit: 100,
    }
}

#[tokio::test]
async fn source_paginates_via_starting_after_cursor() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/charges"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [{"id": "ch_1", "amount": 1000}],
            "has_more": true,
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/charges"))
        .and(query_param("starting_after", "ch_1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [{"id": "ch_2", "amount": 2000}],
            "has_more": false,
        })))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut source = StripeSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}
