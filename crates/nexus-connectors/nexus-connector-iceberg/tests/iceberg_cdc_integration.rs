//! Real end-to-end: native CDC via manual manifest diffing, no built-in
//! incremental scan in `iceberg` 0.10.0 (see `cdc.rs`). Embedded SQLite
//! catalog + local warehouse directory, no container.
//!
//! Insert-only — `IcebergSink` only ever commits `fast_append` snapshots
//! (no committable row-delta/equality-delete action in this crate version,
//! see `iceberg_integration.rs`'s `cdc_delete_batches_are_rejected...`
//! test), so there is no `Update`/`Delete` to detect from data this system
//! wrote itself.

#![cfg(feature = "cdc")]

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_connector_iceberg::{
    IcebergCdcConfig, IcebergCdcSource, IcebergConnectorConfig, IcebergFormatVersion, IcebergSink,
};
use nexus_core::{Sink, Source, OPCODE_COLUMN};
use std::sync::Arc;

fn batch(ids: Vec<i64>, statuses: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(statuses)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn cdc_source_replays_every_append_as_insert() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let batch_cfg = IcebergConnectorConfig {
        catalog_uri: Some(format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("catalog.db").to_str().unwrap()
        )),
        catalog_path: None,
        warehouse_location: Some(format!(
            "file://{}",
            dir.path().join("warehouse").to_str().unwrap()
        )),
        warehouse_path: None,
        namespace: Some("ns".to_string()),
        namespace_name: None,
        table: Some("orders".to_string()),
        table_name: None,
        storage_options: Default::default(),
        format_version: IcebergFormatVersion::V2,
        primary_key: None,
        append_only: false,
        timeout_seconds: 30,
        flush_threshold_rows: 50_000,
    };

    let mut sink = IcebergSink::connect(&batch_cfg).expect("sink connects");
    sink.write_batch(batch(vec![1, 2, 3], vec!["pending", "pending", "pending"]))
        .await
        .expect("writes first batch");
    sink.write_batch(batch(vec![4], vec!["pending"]))
        .await
        .expect("writes second batch (separate snapshot)");

    let cdc_cfg = IcebergCdcConfig {
        catalog_uri: batch_cfg.catalog_uri,
        catalog_path: batch_cfg.catalog_path,
        warehouse_location: batch_cfg.warehouse_location,
        warehouse_path: batch_cfg.warehouse_path,
        namespace: batch_cfg.namespace,
        namespace_name: batch_cfg.namespace_name,
        table: batch_cfg.table,
        table_name: batch_cfg.table_name,
        storage_options: batch_cfg.storage_options,
        starting_snapshot_id: None,
        timeout_seconds: 30,
    };
    let mut source = IcebergCdcSource::connect(&cdc_cfg)
        .await
        .expect("cdc source connects");

    let mut stream = source.read_batches().await.expect("reads cdc batches");
    let mut rows: Vec<(i64, String, String)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch reads ok");
        let ids = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let statuses = batch
            .column(batch.schema().index_of("status").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        let opcodes = batch
            .column(batch.schema().index_of(OPCODE_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for i in 0..ids.len() {
            rows.push((
                ids.value(i),
                statuses.value(i).to_string(),
                opcodes.value(i).to_string(),
            ));
        }
    }
    drop(stream);

    rows.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        rows,
        vec![
            (1, "pending".to_string(), "I".to_string()),
            (2, "pending".to_string(), "I".to_string()),
            (3, "pending".to_string(), "I".to_string()),
            (4, "pending".to_string(), "I".to_string()),
        ],
        "every row from every snapshot must replay as an insert: {rows:?}"
    );
}
