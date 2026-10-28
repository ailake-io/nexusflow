//! End-to-end flow against a mocked Salesforce org (`wiremock`) — same
//! tool `nexus-connector-rest` (public repo) already uses for its own
//! HTTP-mocked tests. Unlike Snowflake/BigQuery/Redshift (no way to
//! test without a paid trial), this exercises the *real* auth → job →
//! poll → result sequence end to end, just against a fake server
//! instead of a real Salesforce org.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_connector_salesforce::{SalesforceConnectorConfig, SalesforceSink, SalesforceSource};
use nexus_core::{Sink, Source};
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// Throwaway RSA key generated locally purely to satisfy
// `jsonwebtoken::encode`'s signing step — wiremock never checks JWT
// signatures, so this key is never validated against anything real and
// carries no secret of any kind.
const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCdaPvqQKt02+bG
JDpT3A6ATPPAtgmwA1lCAehC6yf3qXb+5nfHolYv8+VnzHo2oAGc0pmnZYAX94oW
IQxZT+HGuPjdieZLkmIw4oXhFajDIAjTyVsYMGMx+d8vPPs9NHhH8gdt3qE8WBqI
oLR9Zeo9Hc742olGxY4lau1UHDtzNqcKRaGuIxoNWug5HQE2ursSu5/4I8VtYqM6
UJ0SsX2ZyoAGWt/joBgRw3g8VEzHFr3V14hxF9ZUKIGXAnp6ZwcmfRDr/4ArVNXS
ik+YsSMivx4MGyyqBvMCnEW6b4L+rM5ugPtfEOOBQIvBsmgU9wDvjKPVOUCa9pHK
eDttLw7jAgMBAAECggEASxJ+0sH1A868yVMN3mDdKaOJvScUh7WRJEH0m7W7YgqY
jgkspzFtGYGgr1h+EP9OxZRLY+KsrMGKQfORCCdo7nXZew8Bnpk560adwzOpQSZO
D3PA1lB9fqBFKSpUSGR12Ro9INFE5JrATNkYO5YXmP5Wb6kKP46ItJ/CgJLWZ6Ox
mEvWtoDJMGslB7J3oFCzBjNHl6tI8y6sRnxrBxIvniknItu1huGcOqqjBo33msrI
LrJwMaDCBVPy+u6itJJbMwZSRkFdJni+J5E4le8ydsKIAZjU5+OdZer0sRPHHVz3
lBQ88B1ccSRUX7YF78pyz24Z5jpuCAdLw3nnaaBegQKBgQDLWskII3OsCAIDwvEr
6E4GHCwS45XaTdmJoP2G9avNFnef0k8q11yvnx8J7bXsZK9gdmi9bytVKuv/Gn7b
eOmKrcssmKGdBK+r30YxjgPRmAFOx/rii0tLZrfE4IdAvZjZ/CKMMa2Ogie7BSlG
rnYsg475VypN1uleQ00BelWRGwKBgQDGKUAQcpOmfgGBTShdtAx9vGPMrevO9e5n
VT1zhWyBqTChEkciQRUOthM/+OfoqrjZ/HiQqIrHe7859gv+pMxxnbQCwa2qvJR7
p8nIq+6TmzZiAThIiqVVXVCYeNz8uXn/+GYG1A+ORrUsssDHdGQb9UFGd90dmRcQ
c0TShhUd2QKBgQCgepGhWZDkVyF35HS8yMQiMENb2LyenccpxKGuytt8qtlWh/qv
/WsIsVMmW7Cw0DhSsL8xl7Sjro61MCyieMYdCdAH7p/DsToNMdNMMh2zXvjROiI0
e+a8p2Ao/2PdZIJmrIJ7Do0/pFlETutm+zEJKf0/qlkZOpvKJuRzYR57twKBgDPV
oHts7TB074HaI//20/mj6Nsmd3Noo1cGVg+8y/hSwHSxqkfMjGyPthNa0Zbr6XSj
9Qmp/LtXpFrOAK84fn4NyYObFAmAULrT1hWW285ioGQce5OGKN9ejHGF1BCLl90c
JdwNZpBJ8KRjkcfaq0Eg81Uyj3VpkT3tWQhUqHtpAoGBAKBKfMSdO2Y1zu+/nfow
49zS1bhXDbfaRB2pTplzo7aGxbcAnnitCJ46UnFYEvCGudpApO1PmOcYk67Vk/+E
YRQNNVahCCJ5ref46cGUTf+T+MPNi30VcKY6QNuaqFbcZUl2JTiOCKc6g4ggCvf8
imPgXwxWHf5D9y8hnIJbQYEI
-----END PRIVATE KEY-----";

fn config(server: &MockServer, external_id_field: Option<&str>) -> SalesforceConnectorConfig {
    SalesforceConnectorConfig {
        login_url: server.uri(),
        client_id: "test-consumer-key".into(),
        username: "integration@example.com".into(),
        private_key: TEST_PRIVATE_KEY_PEM.into(),
        api_version: "v62.0".into(),
        sobject: "Account".into(),
        soql: None,
        external_id_field: external_id_field.map(str::to_string),
        timeout_seconds: 10,
        poll_interval_seconds: 0,
    }
}

async fn mount_auth_and_describe(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/services/oauth2/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "test-access-token",
            "instance_url": server.uri(),
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/services/data/v62.0/sobjects/Account/describe"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "fields": [
                {"name": "Id", "type": "id"},
                {"name": "Name", "type": "string"},
            ]
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn source_reads_query_job_results() {
    let server = MockServer::start().await;
    mount_auth_and_describe(&server).await;

    Mock::given(method("POST"))
        .and(path("/services/data/v62.0/jobs/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "750JOB1" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/services/data/v62.0/jobs/query/750JOB1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "JobComplete" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/services/data/v62.0/jobs/query/750JOB1/results"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Id,Name\n001xx0000000001,Acme\n"))
        .mount(&server)
        .await;

    let cfg = config(&server, None);
    let mut source = SalesforceSource::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn sink_upserts_via_ingest_job() {
    let server = MockServer::start().await;
    mount_auth_and_describe(&server).await;

    Mock::given(method("POST"))
        .and(path("/services/data/v62.0/jobs/ingest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "750JOB2",
            "contentUrl": "services/data/v62.0/jobs/ingest/750JOB2/batches",
        })))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/services/data/v62.0/jobs/ingest/750JOB2/batches"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/services/data/v62.0/jobs/ingest/750JOB2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "750JOB2" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/services/data/v62.0/jobs/ingest/750JOB2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "JobComplete" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/services/data/v62.0/jobs/ingest/750JOB2/failedResults",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string("sf__Id,sf__Error\n"))
        .mount(&server)
        .await;

    let cfg = config(&server, Some("Id"));
    let mut sink = SalesforceSink::connect(&cfg).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("Id", DataType::Utf8, false),
        Field::new("Amount", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["001xx0000000001"])),
            Arc::new(Int64Array::from(vec![42])),
        ],
    )
    .unwrap();

    sink.write_batch(batch).await.unwrap();
}
