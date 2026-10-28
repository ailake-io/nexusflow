//! End-to-end flow against a mocked Shopify Admin GraphQL API
//! (`wiremock`) — covers `bulkOperationRunQuery` → poll →
//! `currentBulkOperation` → JSONL download.

use futures::StreamExt;
use nexus_connector_shopify::{ShopifyConnectorConfig, ShopifySource};
use nexus_core::Source;
use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn source_runs_bulk_query_and_downloads_jsonl() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/admin/api/2026-01/graphql.json"))
        .and(body_string_contains("bulkOperationRunQuery"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "bulkOperationRunQuery": {
                    "bulkOperation": {"id": "gid://shopify/BulkOperation/1", "status": "CREATED"},
                    "userErrors": [],
                }
            }
        })))
        .mount(&server)
        .await;

    let jsonl_url = format!("{}/download/result.jsonl", server.uri());
    Mock::given(method("POST"))
        .and(path("/admin/api/2026-01/graphql.json"))
        .and(body_string_contains("currentBulkOperation"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "currentBulkOperation": {
                    "id": "gid://shopify/BulkOperation/1",
                    "status": "COMPLETED",
                    "url": jsonl_url,
                    "errorCode": null,
                }
            }
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/download/result.jsonl"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "{\"id\": \"gid://shopify/Product/1\", \"title\": \"Widget\"}\n\
             {\"id\": \"gid://shopify/Product/2\", \"title\": \"Gadget\"}\n",
        ))
        .mount(&server)
        .await;

    let cfg = ShopifyConnectorConfig {
        shop_domain: "test-shop.myshopify.com".into(),
        access_token: "test-token".into(),
        api_version: "2026-01".into(),
        api_base_url: Some(server.uri()),
        graphql_query: Some("{ products { edges { node { id title } } } }".into()),
        fields: vec!["id".into(), "title".into()],
        sink_mutation: None,
        poll_interval_seconds: 1,
        timeout_seconds: 10,
        retry: Default::default(),
    };

    let mut source = ShopifySource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}
