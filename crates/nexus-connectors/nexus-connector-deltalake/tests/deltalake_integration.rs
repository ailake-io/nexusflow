//! Real end-to-end: same pipeline shape as the other Marco 6 formats —
//! writes a batch, reads it back, validates schema/data, then a CDC upsert
//! and delete confirm real Delta Lake semantics (transaction log versions).
//! Plain local table directory, no container. See IMPLEMENTATION_PLAN.md
//! Marco 6.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_connector_deltalake::{DeltaConnectorConfig, DeltaSink, DeltaSource};
use nexus_core::{CheckpointCursor, Sink, Source};
use std::sync::Arc;

#[tokio::test]
async fn writes_reads_back_and_deletes_via_delta_ops() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let table_uri = dir.path().join("orders").to_str().unwrap().to_string();

    let cfg = DeltaConnectorConfig {
        table_uri: table_uri.clone(),
        path: None,
        table_name: None,
        storage_options: nexus_connector_deltalake::StorageOptions::default(),
        primary_key: "id".to_string(),
        append_only: false,
        timeout_seconds: 30,
        flush_threshold_rows: 50_000,
    };

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

    let mut sink = DeltaSink::connect(&cfg).expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");
    // Non-CDC batches are buffered (flush_threshold_rows) and only land on
    // disk once commit_checkpoint flushes them — same contract the real
    // pipeline engine relies on (it always commits a checkpoint after a
    // partition finishes).
    sink.commit_checkpoint(CheckpointCursor::new("p0"))
        .await
        .expect("flushes buffered batch");

    // --- read back and validate schema/data ---
    let mut source = DeltaSource::connect(&cfg).await.expect("source connects");
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
    drop(stream);
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);

    // --- CDC upsert: update id=2's status ---
    let update_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let update_batch = RecordBatch::try_new(
        update_schema,
        vec![
            Arc::new(Int64Array::from(vec![2i64])),
            Arc::new(StringArray::from(vec!["paid"])),
        ],
    )
    .unwrap();
    sink.write_batch(update_batch)
        .await
        .expect("writes update batch");

    // --- CDC delete: id=1 must be removed, not kept ---
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
    sink.write_batch(delete_batch)
        .await
        .expect("writes delete batch");

    // --- re-read: id=1 gone, id=2 updated, id=3 unchanged ---
    let mut source = DeltaSource::connect(&cfg).await.expect("source reconnects");
    let mut stream = source.read_batches().await.expect("reads batches");
    let mut rows: Vec<(i64, String)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch reads ok");
        let id_col = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let status_col = batch
            .column(batch.schema().index_of("status").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for i in 0..id_col.len() {
            rows.push((id_col.value(i), status_col.value(i).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![(2, "paid".to_string()), (3, "pending".to_string())],
        "id=1 must have been deleted, id=2 updated, id=3 unchanged: {rows:?}"
    );
}
