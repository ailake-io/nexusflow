//! Real end-to-end: same shape as `nexus-connector-parquet`'s own
//! integration test — writes a batch, reads it back, validates schema/data,
//! then a CDC delete batch confirms row removal via the read-filter-rewrite
//! path. Plain local file (no cloud credentials in this sandbox); the
//! S3/GCS/Azure paths go through `object_store`'s own well-tested builders,
//! not something re-tested for connectivity here — only the local backend
//! and the delimiter/schema/CDC logic on top of it.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_connector_csv::{CsvConnectorConfig, CsvDataType, CsvFieldSpec, CsvSink, CsvSource};
use nexus_core::{Sink, Source};
use std::collections::HashMap;
use std::sync::Arc;

fn test_cfg(uri: String, delimiter: char) -> CsvConnectorConfig {
    CsvConnectorConfig {
        uri,
        delimiter,
        has_header: true,
        fields: vec![
            CsvFieldSpec {
                name: "id".to_string(),
                data_type: CsvDataType::Int64,
                nullable: false,
            },
            CsvFieldSpec {
                name: "status".to_string(),
                data_type: CsvDataType::Utf8,
                nullable: false,
            },
        ],
        primary_key: Some("id".to_string()),
        batch_size: 1000,
        storage_options: HashMap::new(),
        timeout_seconds: 30,
    }
}

#[tokio::test]
async fn writes_reads_back_and_deletes_via_rewrite() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let path = dir.path().join("orders.csv");
    let cfg = test_cfg(path.to_str().unwrap().to_string(), ';');

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

    let mut sink = CsvSink::connect(&cfg).expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    // File actually uses the configured delimiter, not a hardcoded comma.
    let raw = std::fs::read_to_string(&path).expect("file exists");
    assert!(
        raw.lines().next().unwrap().contains(';'),
        "header must use the configured ';' delimiter: {raw:?}"
    );

    let mut source = CsvSource::connect(&cfg).expect("source connects");
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
    let mut source = CsvSource::connect(&cfg).expect("source reconnects");
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
        vec![(2, "paid".to_string()), (3, "pending".to_string()),],
        "id=1 must have been deleted, id=2 updated, id=3 unchanged: {rows:?}"
    );
}

#[tokio::test]
async fn rejects_multi_byte_delimiter() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let path = dir.path().join("orders.csv");
    let cfg = test_cfg(path.to_str().unwrap().to_string(), '★');
    match CsvSink::connect(&cfg) {
        Ok(_) => panic!("multi-byte delimiter must be rejected"),
        Err(e) => assert!(matches!(e, nexus_core::NexusError::Schema(_))),
    }
}
