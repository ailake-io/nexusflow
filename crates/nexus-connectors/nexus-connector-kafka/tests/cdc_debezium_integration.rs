#![cfg(feature = "consumer")]

//! End-to-end CDC test: real Postgres -> Debezium -> Kafka -> `KafkaSource`
//! (`envelope: Debezium`). Validates the full Marco 4 critério de pronto —
//! see IMPLEMENTATION_PLAN.md Marco 4 and `docs/cdc-reference/README.md`
//! (same topology, just driven by testcontainers instead of `docker compose`).

use futures::StreamExt;
use nexus_connector_kafka::{
    KafkaConnectorConfig, KafkaDataType, KafkaEnvelope, KafkaFieldSpec, KafkaSource, OPCODE_COLUMN,
};
use nexus_core::Source;
use serde_json::json;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

fn unique_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string()
}

// This spins up three JVMs (Zookeeper, Kafka, Kafka Connect) and this host
// runs consistently memory-constrained (something external holds ~23-25GB
// of 31GB total) — the Connect container's distributed-worker rebalance +
// herder startup is the slowest and flakiest part, intermittently exceeding
// even a 300s `startup_timeout` on transient memory pressure rather than
// hanging outright (an identical run has passed in 44s right after one
// failed at 188s). Retrying the *entire* container topology from scratch —
// not just waiting longer — is the fix: each attempt gets fresh container
// names/network and the previous attempt's `ContainerAsync` guards drop
// (stopping/removing those containers) when `run_once` returns, so a retry
// never contends with a half-started previous attempt for RAM.
const MAX_ATTEMPTS: u32 = 3;

#[tokio::test]
async fn postgres_to_kafka_cdc_carries_correct_opcode_per_row() {
    let mut last_err = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match run_once().await {
            Ok(()) => return,
            Err(e) => {
                eprintln!(
                    "postgres_to_kafka_cdc_carries_correct_opcode_per_row: attempt {attempt}/{MAX_ATTEMPTS} failed: {e}"
                );
                last_err = Some(e);
            }
        }
    }
    panic!(
        "postgres_to_kafka_cdc_carries_correct_opcode_per_row failed after {MAX_ATTEMPTS} attempts: {}",
        last_err.unwrap()
    );
}

async fn run_once() -> Result<(), String> {
    let suffix = unique_suffix();
    let network = format!("nexus-cdc-{suffix}");
    let zk_name = format!("nexus-zk-{suffix}");
    let pg_name = format!("nexus-pg-{suffix}");
    let kafka_name = format!("nexus-kafka-{suffix}");
    let connect_name = format!("nexus-connect-{suffix}");
    // Fixed external host port for Kafka's EXTERNAL listener — must be known
    // before the container starts since it's baked into
    // KAFKA_ADVERTISED_LISTENERS. Only this one test in the crate binds it;
    // each retry attempt reuses the same port, which is fine since the
    // previous attempt's Kafka container is already stopped by the time a
    // retry starts (guards dropped at the end of the failed `run_once` call).
    let kafka_ext_port: u16 = 19094;

    let _zk = GenericImage::new("quay.io/debezium/zookeeper", "2.7")
        .with_wait_for(WaitFor::message_on_stdout("binding to port"))
        // Without an explicit cap, JVM heap ergonomics size off the host's
        // total visible RAM (not a cgroup limit) — three uncapped JVMs
        // (zookeeper+kafka+connect) can starve a memory-constrained host.
        .with_env_var("KAFKA_HEAP_OPTS", "-Xmx192M -Xms128M")
        .with_network(&network)
        .with_container_name(&zk_name)
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .map_err(|e| format!("zookeeper starts: {e}"))?;

    let postgres = GenericImage::new("quay.io/debezium/postgres", "16")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "nexus")
        .with_env_var("POSTGRES_PASSWORD", "nexus")
        .with_env_var("POSTGRES_DB", "nexus")
        .with_network(&network)
        .with_container_name(&pg_name)
        .start()
        .await
        .map_err(|e| format!("postgres starts: {e}"))?;
    let pg_host_port = postgres
        .get_host_port_ipv4(5432)
        .await
        .map_err(|e| format!("postgres host port: {e}"))?;

    let _kafka = GenericImage::new("quay.io/debezium/kafka", "2.7")
        .with_exposed_port(9093.tcp())
        .with_wait_for(WaitFor::message_on_stdout("[KafkaServer id=1] started"))
        .with_env_var("ZOOKEEPER_CONNECT", format!("{zk_name}:2181"))
        .with_env_var(
            "KAFKA_LISTENERS",
            "INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093",
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("INTERNAL://{kafka_name}:9092,EXTERNAL://localhost:{kafka_ext_port}"),
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
        )
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
        .with_env_var("KAFKA_HEAP_OPTS", "-Xmx256M -Xms128M")
        .with_mapped_port(kafka_ext_port, 9093.tcp())
        .with_network(&network)
        .with_container_name(&kafka_name)
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .map_err(|e| format!("kafka starts: {e}"))?;

    let _connect = GenericImage::new("quay.io/debezium/connect", "2.7")
        .with_exposed_port(8083.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/connectors").with_expected_status_code(200u16),
        ))
        .with_env_var("BOOTSTRAP_SERVERS", format!("{kafka_name}:9092"))
        .with_env_var("GROUP_ID", "1")
        .with_env_var("CONFIG_STORAGE_TOPIC", format!("connect-configs-{suffix}"))
        .with_env_var("OFFSET_STORAGE_TOPIC", format!("connect-offsets-{suffix}"))
        .with_env_var("STATUS_STORAGE_TOPIC", format!("connect-statuses-{suffix}"))
        .with_env_var("KAFKA_HEAP_OPTS", "-Xmx384M -Xms256M")
        .with_network(&network)
        .with_container_name(&connect_name)
        // The distributed worker's group rebalance + herder startup takes
        // well past testcontainers' default startup timeout, and can take
        // even longer on a loaded/shared CI runner — this is the slowest
        // and flakiest of the three containers in practice. When even this
        // is exceeded, the outer retry in the caller restarts the whole
        // topology rather than waiting longer still.
        .with_startup_timeout(Duration::from_secs(300))
        .start()
        .await
        .map_err(|e| format!("kafka connect starts: {e}"))?;
    let connect_host_port = _connect
        .get_host_port_ipv4(8083)
        .await
        .map_err(|e| format!("connect host port: {e}"))?;

    // --- Postgres side: create the table Debezium will capture ---
    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_host_port} user=nexus password=nexus dbname=nexus"),
        tokio_postgres::NoTls,
    )
    .await
    .map_err(|e| format!("connects to postgres: {e}"))?;
    tokio::spawn(async move {
        if let Err(e) = pg_conn.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    pg_client
        .batch_execute("CREATE TABLE public.orders (id INT PRIMARY KEY, status TEXT)")
        .await
        .map_err(|e| format!("creates table: {e}"))?;
    // Default REPLICA IDENTITY only carries primary-key columns in the old
    // tuple, so DELETE's "before" would be missing `status`. FULL captures
    // every column, matching the "before" contract our decoder relies on.
    pg_client
        .batch_execute("ALTER TABLE public.orders REPLICA IDENTITY FULL")
        .await
        .map_err(|e| format!("sets replica identity full: {e}"))?;

    // --- Register the Debezium Postgres connector against it ---
    let http = reqwest::Client::new();
    let connector_name = format!("nexus-postgres-connector-{suffix}");
    let register_body = json!({
        "name": connector_name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": pg_name,
            "database.port": "5432",
            "database.user": "nexus",
            "database.password": "nexus",
            "database.dbname": "nexus",
            "topic.prefix": "nexus",
            "plugin.name": "pgoutput",
            "table.include.list": "public.orders",
            "tombstones.on.delete": "false",
        }
    });
    let register_url = format!("http://127.0.0.1:{connect_host_port}/connectors");
    let response = http
        .post(&register_url)
        .json(&register_body)
        .send()
        .await
        .map_err(|e| format!("registers connector: {e}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "connector registration failed: {:?}",
            response.text().await
        ));
    }

    // Wait for the connector task to report RUNNING before generating
    // change events, or the initial snapshot could race the DML below.
    let status_url =
        format!("http://127.0.0.1:{connect_host_port}/connectors/{connector_name}/status");
    let mut running = false;
    for _ in 0..60 {
        if let Ok(resp) = http.get(&status_url).send().await {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                if body["connector"]["state"].as_str() == Some("RUNNING")
                    && body["tasks"].as_array().is_some_and(|tasks| {
                        tasks.iter().any(|t| t["state"].as_str() == Some("RUNNING"))
                    })
                {
                    running = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !running {
        return Err("debezium connector never reached RUNNING".to_string());
    }

    // --- Generate INSERT / UPDATE / DELETE on the captured table ---
    pg_client
        .batch_execute("INSERT INTO public.orders (id, status) VALUES (1, 'pending')")
        .await
        .map_err(|e| format!("inserts row: {e}"))?;
    pg_client
        .batch_execute("UPDATE public.orders SET status = 'paid' WHERE id = 1")
        .await
        .map_err(|e| format!("updates row: {e}"))?;
    pg_client
        .batch_execute("DELETE FROM public.orders WHERE id = 1")
        .await
        .map_err(|e| format!("deletes row: {e}"))?;

    // --- Consume the resulting CDC events via nexus-connector-kafka ---
    let config = KafkaConnectorConfig {
        bootstrap_servers: format!("localhost:{kafka_ext_port}"),
        topic: "nexus.public.orders".to_string(),
        group_id: format!("nexus-cdc-test-{suffix}"),
        fields: vec![
            KafkaFieldSpec {
                name: "id".into(),
                data_type: KafkaDataType::Int64,
                nullable: false,
            },
            KafkaFieldSpec {
                name: "status".into(),
                data_type: KafkaDataType::Utf8,
                nullable: true,
            },
        ],
        batch_size: 10,
        // Generous idle cutoff — first poll of a fresh consumer group pays
        // for a full rebalance, plus Debezium's own snapshot/stream latency.
        poll_timeout_ms: 20_000,
        max_messages: 10,
        envelope: KafkaEnvelope::Debezium,
        start_offsets: HashMap::new(),
    };

    let mut source = KafkaSource::connect(&config).map_err(|e| format!("source connects: {e}"))?;
    let mut stream = source
        .read_batches()
        .await
        .map_err(|e| format!("read_batches: {e}"))?;
    let mut opcodes = Vec::new();
    let mut statuses = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| format!("batch decodes: {e}"))?;
        let opcode_col = batch
            .column_by_name(OPCODE_COLUMN)
            .ok_or("opcode column present")?
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or("opcode column is Utf8")?;
        let status_col = batch
            .column_by_name("status")
            .ok_or("status column present")?
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or("status column is Utf8")?;
        for i in 0..batch.num_rows() {
            opcodes.push(opcode_col.value(i).to_string());
            statuses.push(status_col.value(i).to_string());
        }
    }

    if opcodes != vec!["I", "U", "D"] {
        return Err(format!(
            "opcode per row must reflect insert/update/delete in order, got {opcodes:?}"
        ));
    }
    if statuses != vec!["pending", "paid", "paid"] {
        return Err(format!(
            "delete row must carry the pre-delete ('before') values, got {statuses:?}"
        ));
    }
    Ok(())
}
