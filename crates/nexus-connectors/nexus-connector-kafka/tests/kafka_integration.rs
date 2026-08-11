#![cfg(feature = "consumer")]

use futures::StreamExt;
use nexus_connector_kafka::{KafkaConnectorConfig, KafkaDataType, KafkaFieldSpec, KafkaSource};
use nexus_core::Source;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::collections::HashMap;
use std::time::Duration;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn consumes_json_messages_as_record_batches() {
    let kafka_node = Kafka::default().start().await.expect("kafka starts");
    let bootstrap_servers = format!(
        "127.0.0.1:{}",
        kafka_node
            .get_host_port_ipv4(KAFKA_PORT)
            .await
            .expect("container port")
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer creates");

    let topic = "events";
    for (id, name) in [(1, "alice"), (2, "bob"), (3, "carol")] {
        let payload = format!(r#"{{"id": {id}, "name": "{name}"}}"#);
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(&payload)
                    .key(&id.to_string()),
                Duration::from_secs(0),
            )
            .await
            .expect("message sent");
    }

    let config = KafkaConnectorConfig {
        bootstrap_servers,
        topic: topic.to_string(),
        group_id: "nexus-test".to_string(),
        fields: vec![
            KafkaFieldSpec {
                name: "id".into(),
                data_type: KafkaDataType::Int64,
                nullable: false,
            },
            KafkaFieldSpec {
                name: "name".into(),
                data_type: KafkaDataType::Utf8,
                nullable: true,
            },
        ],
        batch_size: 500,
        // Generous idle cutoff — a fresh consumer group's first poll pays for
        // a full JoinGroup/SyncGroup rebalance before any message arrives.
        poll_timeout_ms: 15000,
        max_messages: 100,
        start_offsets: HashMap::new(),
    };

    let mut source = KafkaSource::connect(&config).expect("source connects");
    let mut stream = source.read_batches().await.expect("read_batches");
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 3);
    drop(stream);
    assert_eq!(source.last_offsets().get(&0), Some(&2));
}

/// Resume-by-partition: a source that consumed messages 0..3 must let a
/// fresh source pick up exactly where it left off via `start_offsets`, not
/// replay from the start of the topic. See ARCHITECTURE.md §5.
#[tokio::test]
async fn resumes_from_explicit_start_offset() {
    let kafka_node = Kafka::default().start().await.expect("kafka starts");
    let bootstrap_servers = format!(
        "127.0.0.1:{}",
        kafka_node
            .get_host_port_ipv4(KAFKA_PORT)
            .await
            .expect("container port")
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer creates");

    let topic = "resume-events";
    for id in 1..=5 {
        let payload = format!(r#"{{"id": {id}}}"#);
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(&payload)
                    .key(&id.to_string()),
                Duration::from_secs(0),
            )
            .await
            .expect("message sent");
    }

    let base_config = KafkaConnectorConfig {
        bootstrap_servers,
        topic: topic.to_string(),
        group_id: "nexus-resume-test".to_string(),
        fields: vec![KafkaFieldSpec {
            name: "id".into(),
            data_type: KafkaDataType::Int64,
            nullable: false,
        }],
        batch_size: 500,
        poll_timeout_ms: 15000,
        max_messages: 3,
        start_offsets: HashMap::new(),
    };

    let mut first = KafkaSource::connect(&base_config).expect("first source connects");
    let mut first_rows = 0;
    {
        let mut stream = first.read_batches().await.expect("read_batches");
        while let Some(batch) = stream.next().await {
            first_rows += batch.unwrap().num_rows();
        }
    }
    assert_eq!(first_rows, 3);
    let resume_offset = *first.last_offsets().get(&0).expect("partition 0 consumed") + 1;

    let resume_config = KafkaConnectorConfig {
        max_messages: 100,
        poll_timeout_ms: 5000,
        start_offsets: HashMap::from([(0, resume_offset)]),
        ..base_config
    };

    let mut second = KafkaSource::connect(&resume_config).expect("resumed source connects");
    let mut second_rows = 0;
    {
        let mut stream = second.read_batches().await.expect("read_batches");
        while let Some(batch) = stream.next().await {
            second_rows += batch.unwrap().num_rows();
        }
    }
    assert_eq!(
        second_rows, 2,
        "must consume only the remaining messages, not replay"
    );
}
