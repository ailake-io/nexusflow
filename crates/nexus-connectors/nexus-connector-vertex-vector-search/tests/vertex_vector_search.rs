//! End-to-end flow against a mocked Vertex AI Vector Search API
//! (`wiremock`) — covers auth, upsert, and opcode-driven delete.

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_connector_vertex_vector_search::{
    VertexVectorSearchConnectorConfig, VertexVectorSearchSink,
};
use nexus_core::Sink;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// Throwaway RSA key, only used to exercise this connector's JWT-signing
// code path — wiremock never checks the signature. Allowlisted in
// .gitleaks.toml by exact key content.
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

fn config(server: &MockServer) -> VertexVectorSearchConnectorConfig {
    VertexVectorSearchConnectorConfig {
        project_id: "test-project".into(),
        region: "us-central1".into(),
        index_id: "1234567890".into(),
        client_email: "test@test-project.iam.gserviceaccount.com".into(),
        private_key: TEST_PRIVATE_KEY_PEM.into(),
        token_uri: format!("{}/token", server.uri()),
        api_base_url: Some(server.uri()),
        primary_key: "id".into(),
        embedding_column: "embedding".into(),
        dimension: 2,
        timeout_seconds: 10,
    }
}

fn batch_with_embedding() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
    ]));
    let mut embedding_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    embedding_builder.values().append_value(0.1);
    embedding_builder.values().append_value(0.2);
    embedding_builder.append(true);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(embedding_builder.finish()),
        ],
    )
    .unwrap()
}

async fn mount_auth(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "access_token": "test-access-token" })),
        )
        .mount(server)
        .await;
}

#[tokio::test]
async fn sink_upserts_datapoints() {
    let server = MockServer::start().await;
    mount_auth(&server).await;

    Mock::given(method("POST"))
        .and(path(
            "/v1/projects/test-project/locations/us-central1/indexes/1234567890:upsertDatapoints",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let cfg = config(&server);
    let mut sink = VertexVectorSearchSink::connect(&cfg).await.unwrap();
    sink.write_batch(batch_with_embedding()).await.unwrap();
}

#[tokio::test]
async fn sink_deletes_via_opcode_split() {
    let server = MockServer::start().await;
    mount_auth(&server).await;

    Mock::given(method("POST"))
        .and(path(
            "/v1/projects/test-project/locations/us-central1/indexes/1234567890:removeDatapoints",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
        Field::new("__opcode", DataType::Utf8, false),
    ]));
    let mut embedding_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    embedding_builder.values().append_value(0.1);
    embedding_builder.values().append_value(0.2);
    embedding_builder.append(true);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(embedding_builder.finish()),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();

    let cfg = config(&server);
    let mut sink = VertexVectorSearchSink::connect(&cfg).await.unwrap();
    sink.write_batch(batch).await.unwrap();
}
