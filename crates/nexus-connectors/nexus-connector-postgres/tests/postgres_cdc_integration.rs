#![cfg(feature = "cdc")]

//! End-to-end native CDC: real Postgres logical replication (`pgoutput`),
//! no Debezium/Kafka in front — ARCHITECTURE.md §7,
//! `docs/cdc-reference/README.md`. Mirrors the assertions of
//! `nexus-connector-kafka`'s `cdc_debezium_integration.rs` (opcode sequence
//! + before/after values) but against a single plain Postgres container
//!   instead of a 3-JVM Debezium+Kafka+Zookeeper stack.

use arrow_array::{Int64Array, StringArray};
use futures::StreamExt;
use nexus_connector_postgres::{
    PostgresCdcConfig, PostgresCdcDataType, PostgresCdcFieldSpec, PostgresCdcSource,
};
use nexus_core::{Source, OPCODE_COLUMN};
use std::time::{SystemTime, UNIX_EPOCH};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

fn unique_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string()
}

#[tokio::test]
async fn postgres_native_cdc_carries_correct_opcode_per_row() {
    let postgres = GenericImage::new("postgres", "16")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "nexus")
        .with_env_var("POSTGRES_PASSWORD", "nexus")
        .with_env_var("POSTGRES_DB", "nexus")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .expect("postgres starts");

    let host = postgres.get_host().await.expect("container host");
    let port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres host port");
    let uri = format!("postgres://nexus:nexus@{host}:{port}/nexus");

    let (setup_client, setup_conn) = tokio_postgres::connect(&uri, tokio_postgres::NoTls)
        .await
        .expect("connects to postgres");
    tokio::spawn(async move {
        let _ = setup_conn.await;
    });

    let suffix = unique_suffix();
    let table = format!("cdc_test_{suffix}");
    let publication = format!("pub_{suffix}");
    let slot = format!("slot_{suffix}");

    setup_client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT); \
             ALTER TABLE {table} REPLICA IDENTITY FULL; \
             CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .expect("test table + publication created");

    let config = PostgresCdcConfig {
        uri: Some(uri.clone()),
        table: table.clone(),
        publication_name: publication.clone(),
        slot_name: slot.clone(),
        fields: vec![
            PostgresCdcFieldSpec {
                name: "id".to_string(),
                data_type: PostgresCdcDataType::Int64,
                nullable: false,
            },
            PostgresCdcFieldSpec {
                name: "name".to_string(),
                data_type: PostgresCdcDataType::Utf8,
                nullable: true,
            },
        ],
        timeout_seconds: 30,
    };

    // Connects and starts replication (creating the slot) *before* the DML
    // below runs, so those changes land in the WAL stream this source reads.
    let mut source = PostgresCdcSource::connect(&config)
        .await
        .expect("postgres-cdc connects and starts replication");

    setup_client
        .batch_execute(&format!(
            "INSERT INTO {table} (id, name) VALUES (1, 'alice'); \
             UPDATE {table} SET name = 'alice2' WHERE id = 1; \
             DELETE FROM {table} WHERE id = 1;"
        ))
        .await
        .expect("test DML");

    let mut stream = source.read_batches().await.expect("read_batches");
    let batch = stream
        .next()
        .await
        .expect("a batch arrives")
        .expect("batch is Ok");

    assert_eq!(batch.num_rows(), 3, "insert + update + delete = 3 rows");

    let opcodes = batch
        .column_by_name(OPCODE_COLUMN)
        .expect("opcode column present")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("opcode column is Utf8");
    assert_eq!(opcodes.value(0), "I");
    assert_eq!(opcodes.value(1), "U");
    assert_eq!(opcodes.value(2), "D");

    let names = batch
        .column_by_name("name")
        .expect("name column present")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column is Utf8");
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "alice2");
    // REPLICA IDENTITY FULL means the delete's old row carries every
    // column, not just the primary key.
    assert_eq!(names.value(2), "alice2");

    let ids = batch
        .column_by_name("id")
        .expect("id column present")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column is Int64");
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 1);
    assert_eq!(ids.value(2), 1);
}
