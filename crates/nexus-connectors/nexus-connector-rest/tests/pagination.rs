use futures::StreamExt;
use nexus_connector_rest::{
    RestConnectorConfig, RestDataType, RestFieldSpec, RestMethod, RestPagination, RestSource,
};
use nexus_core::Source;
use serde_json::json;
use std::collections::HashMap;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn fields() -> Vec<RestFieldSpec> {
    vec![
        RestFieldSpec {
            name: "id".into(),
            data_type: RestDataType::Int64,
            nullable: false,
        },
        RestFieldSpec {
            name: "name".into(),
            data_type: RestDataType::Utf8,
            nullable: true,
        },
    ]
}

#[tokio::test]
async fn no_pagination_reads_single_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ])))
        .mount(&server)
        .await;

    let config = RestConnectorConfig {
        uri: None,
        url: None,
        base_url: server.uri(),
        path: "/users".into(),
        method: RestMethod::Get,
        headers: HashMap::new(),
        fields: fields(),
        rows_path: None,
        pagination: RestPagination::None,
        max_pages: 10,
        timeout_seconds: 5,
        retries: 0,
        retry_backoff_seconds: 0,
        requests_per_second: 0,
    };

    let mut source = RestSource::connect(&config).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn legacy_url_takes_precedence() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/legacy/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 1, "name": "alice"},
        ])))
        .mount(&server)
        .await;

    let config = RestConnectorConfig {
        uri: None,
        url: Some(format!("{}/legacy/users", server.uri())),
        base_url: String::new(),
        path: String::new(),
        method: RestMethod::Get,
        headers: HashMap::new(),
        fields: fields(),
        rows_path: None,
        pagination: RestPagination::None,
        max_pages: 10,
        timeout_seconds: 5,
        retries: 0,
        retry_backoff_seconds: 0,
        requests_per_second: 0,
    };

    let mut source = RestSource::connect(&config).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn offset_pagination_stops_on_short_page() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/users"))
        .and(query_param("offset", "0"))
        .and(query_param("limit", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/users"))
        .and(query_param("offset", "2"))
        .and(query_param("limit", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 3, "name": "carol"}]
        })))
        .mount(&server)
        .await;

    let config = RestConnectorConfig {
        uri: None,
        url: None,
        base_url: server.uri(),
        path: "/users".into(),
        method: RestMethod::Get,
        headers: HashMap::new(),
        fields: fields(),
        rows_path: Some("items".into()),
        pagination: RestPagination::Offset {
            offset_param: "offset".into(),
            limit_param: "limit".into(),
            limit: 2,
        },
        max_pages: 10,
        timeout_seconds: 5,
        retries: 0,
        retry_backoff_seconds: 0,
        requests_per_second: 0,
    };

    let mut source = RestSource::connect(&config).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();
    let mut total_rows = 0;
    let mut page_count = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
        page_count += 1;
    }
    assert_eq!(total_rows, 3);
    assert_eq!(page_count, 2);
}

#[tokio::test]
async fn cursor_pagination_stops_when_next_cursor_absent() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1, "name": "alice"}],
            "next_cursor": "page-2"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/users"))
        .and(query_param("cursor", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 2, "name": "bob"}]
        })))
        .mount(&server)
        .await;

    let config = RestConnectorConfig {
        uri: None,
        url: None,
        base_url: server.uri(),
        path: "/users".into(),
        method: RestMethod::Get,
        headers: HashMap::new(),
        fields: fields(),
        rows_path: Some("items".into()),
        pagination: RestPagination::Cursor {
            cursor_param: "cursor".into(),
            next_cursor_path: "next_cursor".into(),
        },
        max_pages: 10,
        timeout_seconds: 5,
        retries: 0,
        retry_backoff_seconds: 0,
        requests_per_second: 0,
    };

    let mut source = RestSource::connect(&config).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}
