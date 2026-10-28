//! End-to-end flow against a mocked GA4 Data API (`wiremock`) — same
//! tool `nexus-connector-salesforce` (this repo) already uses for its
//! own HTTP-mocked tests.

use futures::StreamExt;
use nexus_connector_ga4::{Ga4ConnectorConfig, Ga4Source};
use nexus_core::Source;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// Throwaway RSA key generated locally purely to satisfy
// `jsonwebtoken::encode`'s signing step — wiremock never checks JWT
// signatures. Same key already used in
// nexus-connector-salesforce/tests/salesforce_bulk.rs (this repo).
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

#[tokio::test]
async fn source_reads_run_report_with_pagination() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "test-access-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        })))
        .mount(&server)
        .await;

    // First page: full page (page_size=1), signals more data via
    // rowCount > rows returned so far.
    Mock::given(method("POST"))
        .and(path("/v1beta/properties/12345:runReport"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "rows": [
                {"dimensionValues": [{"value": "20260101"}], "metricValues": [{"value": "10"}]},
            ],
            "rowCount": 2,
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1beta/properties/12345:runReport"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "rows": [
                {"dimensionValues": [{"value": "20260102"}], "metricValues": [{"value": "20"}]},
            ],
            "rowCount": 2,
        })))
        .mount(&server)
        .await;

    let cfg = Ga4ConnectorConfig {
        property_id: "12345".into(),
        client_email: "test@example.iam.gserviceaccount.com".into(),
        private_key: TEST_PRIVATE_KEY_PEM.into(),
        token_uri: format!("{}/token", server.uri()),
        data_api_base_url: server.uri(),
        dimensions: vec!["date".into()],
        metrics: vec!["activeUsers".into()],
        start_date: "7daysAgo".into(),
        end_date: "today".into(),
        page_size: 1,
        timeout_seconds: 10,
        retry: Default::default(),
    };

    let mut source = Ga4Source::connect(&cfg).await.unwrap();
    let mut stream = source.read_batches().await.unwrap();

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}
