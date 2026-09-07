use nexus_connector_redis::RedisKvClient;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// LLMOPS_IMPLEMENTATION_PLAN.md Marco L3's own acceptance criterion:
/// "testado contra Redis real via testcontainers" — same pattern every
/// stateful connector in this repo uses (see
/// `nexus-connector-mongodb/tests/mongo_integration.rs`).
#[tokio::test]
async fn set_ex_then_get_round_trips_the_value() {
    let container = Redis::default().start().await.expect("redis starts");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("container port");
    let url = format!("redis://127.0.0.1:{port}");

    let client = RedisKvClient::connect(&url).await.expect("client connects");

    assert_eq!(client.get("missing-key").await.unwrap(), None);

    client
        .set_ex("greeting", "hello", 60)
        .await
        .expect("set_ex succeeds");
    assert_eq!(
        client.get("greeting").await.unwrap(),
        Some("hello".to_string())
    );
}

#[tokio::test]
async fn set_ex_expires_after_the_ttl() {
    let container = Redis::default().start().await.expect("redis starts");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("container port");
    let url = format!("redis://127.0.0.1:{port}");

    let client = RedisKvClient::connect(&url).await.expect("client connects");
    client
        .set_ex("short-lived", "value", 1)
        .await
        .expect("set_ex succeeds");
    assert_eq!(
        client.get("short-lived").await.unwrap(),
        Some("value".to_string())
    );

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(client.get("short-lived").await.unwrap(), None);
}
