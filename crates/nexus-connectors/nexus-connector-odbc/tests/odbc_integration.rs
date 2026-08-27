//! End-to-end ODBC integration test using the SQLite ODBC driver.
//!
//! Requires the SQLite ODBC driver to be installed on the host:
//!   /usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so
//!
//! The test creates a temporary SQLite database, writes a batch through
//! `OdbcSink`, reads it back through `OdbcSource`, and verifies the schema
//! and data round-trip. This is intentionally a local, self-contained test
//! that needs no network credentials.

#![cfg(feature = "legacy")]

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_connector_odbc::{
    OdbcConnectorConfig, OdbcDataType, OdbcFieldSpec, OdbcSink, OdbcSource,
};
use nexus_core::{Sink, Source};
use std::sync::Arc;

const SQLITE_ODBC_DRIVER: &str = "/usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so";

fn test_cfg(db_path: &str) -> OdbcConnectorConfig {
    OdbcConnectorConfig {
        connection_string: Some(format!(
            "Driver={SQLITE_ODBC_DRIVER};Database={db_path};StepAPI=No;NoTXN=No;Timeout=100000;SyncPragma=NORMAL;ShortNames=No;LongNames=No;NoCreatetemp=No;NoWCHAR=No;FKSupport=Yes;JournalMode=;OEMCP=No;LoadExt=;BigInt=Yes;JDConv=No"
        )),
        driver: SQLITE_ODBC_DRIVER.to_string(),
        server: "localhost".to_string(),
        port: None,
        database: Some(db_path.to_string()),
        username: "".to_string(),
        password: "".to_string(),
        encrypt: None,
        trust_server_certificate: None,
        login_timeout_seconds: None,
        table: "orders".to_string(),
        primary_key: "id".to_string(),
        fields: vec![
            OdbcFieldSpec {
                name: "id".to_string(),
                data_type: OdbcDataType::Int64,
                nullable: false,
            },
            OdbcFieldSpec {
                name: "status".to_string(),
                data_type: OdbcDataType::Utf8,
                nullable: true,
            },
        ],
        batch_size: 1000,
        timeout_seconds: 30,
    }
}

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]))
}

type SchemaRef = std::sync::Arc<Schema>;

#[tokio::test]
async fn writes_and_reads_back_via_odbc_sqlite() {
    if !std::path::Path::new(SQLITE_ODBC_DRIVER).exists() {
        eprintln!("SQLite ODBC driver not installed; skipping integration test");
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir creates");
    let db_path = dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();

    // Create the table directly through SQLite so the source can discover it.
    let conn = rusqlite::Connection::open(db_path_str).expect("open sqlite db");
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)",
        [],
    )
    .expect("create orders table");
    drop(conn);

    let cfg = test_cfg(db_path_str);
    let schema = orders_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("pending"),
                Some("shipped"),
                None,
            ])),
        ],
    )
    .unwrap();

    let mut sink = OdbcSink::connect(&cfg).expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    let mut source = OdbcSource::connect(&cfg).expect("source connects");
    assert_eq!(source.schema(), schema);

    let mut stream = source.read_batches().await.expect("read batches");
    let read_batch = stream.next().await.expect("batch yielded").expect("batch ok");
    assert!(stream.next().await.is_none(), "only one batch expected");

    let id_col = read_batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let status_col = read_batch
        .column_by_name("status")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(id_col.values(), &[1, 2, 3]);
    assert_eq!(status_col.value(0), "pending");
    assert_eq!(status_col.value(1), "shipped");
    assert!(status_col.is_null(2));
}

#[tokio::test]
async fn upserts_existing_rows_via_odbc_sqlite() {
    if !std::path::Path::new(SQLITE_ODBC_DRIVER).exists() {
        eprintln!("SQLite ODBC driver not installed; skipping integration test");
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir creates");
    let db_path = dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();

    let conn = rusqlite::Connection::open(db_path_str).expect("open sqlite db");
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)",
        [],
    )
    .expect("create orders table");
    conn.execute(
        "INSERT INTO orders (id, status) VALUES (1, 'pending')",
        [],
    )
    .expect("seed row");
    drop(conn);

    let cfg = test_cfg(db_path_str);
    let schema = orders_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("shipped")])),
        ],
    )
    .unwrap();

    // Non-CDC batch: INSERT-only path. SQLite will fail with primary-key
    // conflict, so this test exercises the upsert path by using a CDC opcode
    // column, which forces UPDATE-then-INSERT.
    let mut cdc_batch = batch;
    let opcode_array = Arc::new(StringArray::from(vec![Some("U")]));
    let cdc_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("__opcode", DataType::Utf8, false),
    ]));
    cdc_batch = RecordBatch::try_new(
        cdc_schema,
        vec![
            cdc_batch.column(0).clone(),
            cdc_batch.column(1).clone(),
            opcode_array,
        ],
    )
    .unwrap();

    let mut sink = OdbcSink::connect(&cfg).expect("sink connects");
    sink.write_batch(cdc_batch).await.expect("upserts batch");

    let mut source = OdbcSource::connect(&cfg).expect("source connects");
    let mut stream = source.read_batches().await.expect("read batches");
    let read_batch = stream.next().await.unwrap().unwrap();
    let status_col = read_batch
        .column_by_name("status")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(status_col.value(0), "shipped");
}
