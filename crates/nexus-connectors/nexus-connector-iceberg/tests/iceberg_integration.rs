//! Real end-to-end: same pipeline shape as the other Marco 6 formats —
//! writes a batch, reads it back, validates schema/data. Embedded SQLite
//! catalog + local warehouse directory, no container. See
//! IMPLEMENTATION_PLAN.md Marco 6.
//!
//! CDC delete isn't exercised here (see `sink.rs`'s doc: not supported by
//! iceberg-rust 0.10.0's public Transaction API yet) — a separate test
//! confirms the sink rejects it explicitly instead of silently dropping it.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_connector_iceberg::{
    IcebergConnectorConfig, IcebergFormatVersion, IcebergSink, IcebergSource,
};
use nexus_core::{Sink, Source};
use std::sync::Arc;

fn test_cfg(dir: &std::path::Path, format_version: IcebergFormatVersion) -> IcebergConnectorConfig {
    IcebergConnectorConfig {
        catalog_uri: format!(
            "sqlite://{}?mode=rwc",
            dir.join("catalog.db").to_str().unwrap()
        ),
        warehouse_location: format!("file://{}", dir.join("warehouse").to_str().unwrap()),
        namespace: "ns".to_string(),
        table: "orders".to_string(),
        format_version,
    }
}

#[tokio::test]
async fn writes_and_reads_back_v2() {
    writes_and_reads_back(IcebergFormatVersion::V2).await;
}

#[tokio::test]
async fn writes_and_reads_back_v3() {
    writes_and_reads_back(IcebergFormatVersion::V3).await;
}

async fn writes_and_reads_back(format_version: IcebergFormatVersion) {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let cfg = test_cfg(dir.path(), format_version);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["pending", "pending", "pending"])),
        ],
    )
    .unwrap();

    let mut sink = IcebergSink::connect(&cfg).expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    // A second batch on the same table exercises the "table already
    // exists" branch of ensure_table, not just first-write create.
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let batch2 = RecordBatch::try_new(
        schema2,
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["pending"])),
        ],
    )
    .unwrap();
    sink.write_batch(batch2).await.expect("writes second batch");

    let mut source = IcebergSource::connect(&cfg).await.expect("source connects");
    let schema = source.schema();
    assert!(schema.field_with_name("id").is_ok());
    assert!(schema.field_with_name("status").is_ok());

    let mut stream = source.read_batches().await.expect("reads batches");
    let mut ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch reads ok");
        let id_col = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        ids.extend((0..id_col.len()).map(|i| id_col.value(i)));
    }
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn cdc_delete_batches_are_rejected_not_silently_dropped() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let cfg = test_cfg(dir.path(), IcebergFormatVersion::V2);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["pending"])),
        ],
    )
    .unwrap();
    let mut sink = IcebergSink::connect(&cfg).expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    let delete_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new(nexus_core::OPCODE_COLUMN, DataType::Utf8, false),
    ]));
    let delete_batch = RecordBatch::try_new(
        delete_schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec!["pending"])),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();

    let err = sink
        .write_batch(delete_batch)
        .await
        .expect_err("CDC delete must be rejected, not silently accepted");
    let msg = err.to_string();
    assert!(
        msg.contains("not supported"),
        "expected a clear 'not supported' error, got: {msg}"
    );
}
