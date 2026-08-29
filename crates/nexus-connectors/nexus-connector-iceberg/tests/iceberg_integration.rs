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
use nexus_core::{CheckpointCursor, Sink, Source};
use std::sync::Arc;

fn test_cfg(dir: &std::path::Path, format_version: IcebergFormatVersion) -> IcebergConnectorConfig {
    IcebergConnectorConfig {
        catalog_uri: Some(format!(
            "sqlite://{}?mode=rwc",
            dir.join("catalog.db").to_str().unwrap()
        )),
        catalog_path: None,
        warehouse_location: Some(format!(
            "file://{}",
            dir.join("warehouse").to_str().unwrap()
        )),
        warehouse_path: None,
        namespace: Some("ns".to_string()),
        namespace_name: None,
        table: Some("orders".to_string()),
        table_name: None,
        storage_options: Default::default(),
        format_version,
        primary_key: None,
        append_only: false,
        timeout_seconds: 30,
        flush_threshold_rows: 50_000,
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
    // Non-CDC batches are buffered (flush_threshold_rows) and only land on
    // disk once commit_checkpoint flushes them. Flushing here (instead of
    // letting both batches accumulate in one buffer) is also what actually
    // exercises the "table already exists" branch of ensure_table below —
    // without it, the second write below would just extend the same
    // unflushed buffer instead of hitting a real second append.
    sink.commit_checkpoint(CheckpointCursor::new("p0"))
        .await
        .expect("flushes first batch");

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
    sink.commit_checkpoint(CheckpointCursor::new("p0"))
        .await
        .expect("flushes second batch");

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

#[tokio::test]
async fn primary_key_dedup_prevents_duplicates_on_retry() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let mut cfg = test_cfg(dir.path(), IcebergFormatVersion::V2);
    cfg.primary_key = Some("id".to_string());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let mut sink = IcebergSink::connect(&cfg).expect("sink connects");
    sink.write_batch(batch).await.expect("writes first batch");
    // Flush before the "retry" write below — otherwise both writes just
    // accumulate in the same unflushed buffer and this never exercises
    // dedup_against_existing's snapshot scan (it would only exercise the
    // separate intra-batch dedupe, which isn't what this test is about).
    sink.commit_checkpoint(CheckpointCursor::new("p0"))
        .await
        .expect("flushes first batch");

    // Retry with overlapping keys plus one new row: only row 4 should be
    // appended because 1/2/3 already exist.
    let retry = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a2", "b2", "c2", "d"])),
        ],
    )
    .unwrap();
    sink.write_batch(retry).await.expect("writes retry batch");
    sink.commit_checkpoint(CheckpointCursor::new("p0"))
        .await
        .expect("flushes retry batch");

    let mut source = IcebergSource::connect(&cfg).await.expect("source connects");
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
    assert_eq!(ids, vec![1, 2, 3, 4], "duplicate PK rows must be dropped");
}
