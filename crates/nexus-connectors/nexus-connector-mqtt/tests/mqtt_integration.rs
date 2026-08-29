#![cfg(feature = "client")]

use arrow_array::Array;
use futures::StreamExt;
use nexus_connector_mqtt::{MqttConnectorConfig, MqttDataType, MqttFieldSpec, MqttQos, MqttSource};
use nexus_core::Source;
use rumqttc::{AsyncClient, MqttOptions, PublishOptions, QoS};
use testcontainers_modules::mosquitto::Mosquitto;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn consumes_json_messages_as_record_batches_with_topic_column() {
    let node = Mosquitto::default()
        .start()
        .await
        .expect("mosquitto starts");
    let port = node.get_host_port_ipv4(1883).await.expect("container port");

    let mut publisher_options = MqttOptions::new("nexus-test-publisher", ("127.0.0.1", port));
    publisher_options.set_keep_alive(5);
    let (publisher, mut publisher_eventloop) = AsyncClient::builder(publisher_options)
        .capacity(10)
        .try_build()
        .expect("publisher client builds");
    tokio::spawn(async move {
        loop {
            if publisher_eventloop.poll().await.is_err() {
                break;
            }
        }
    });

    let config = MqttConnectorConfig {
        broker_url: format!("mqtt://127.0.0.1:{port}"),
        client_id: "nexus-mqtt-test".to_string(),
        topic_filter: "sensors/#".to_string(),
        qos: MqttQos::AtLeastOnce,
        username: None,
        password: None,
        ca_cert_path: None,
        client_cert_path: None,
        client_key_path: None,
        fields: vec![MqttFieldSpec {
            name: "valor".into(),
            data_type: MqttDataType::Float64,
            nullable: false,
        }],
        schema_sample_rows: 1000,
        batch_size: 500,
        poll_timeout_ms: 5000,
        max_messages: 3,
    };

    let mut source = MqttSource::connect(&config).await.expect("source connects");

    for (station, value) in [("est-01", 23.5), ("est-02", 18.2), ("est-01", 23.7)] {
        let payload = format!(r#"{{"valor": {value}}}"#);
        publisher
            .publish(
                format!("sensors/{station}/temperatura"),
                payload,
                PublishOptions::new(QoS::AtLeastOnce),
            )
            .await
            .expect("message published");
    }

    let mut stream = source.read_batches().await.expect("read_batches");
    let mut total_rows = 0;
    let mut topics = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
        let topic_col = batch
            .column_by_name("__mqtt_topic")
            .expect("topic column present")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("topic column is Utf8");
        for i in 0..topic_col.len() {
            topics.push(topic_col.value(i).to_string());
        }
    }
    assert_eq!(total_rows, 3);
    assert!(topics.contains(&"sensors/est-01/temperatura".to_string()));
    assert!(topics.contains(&"sensors/est-02/temperatura".to_string()));
}
