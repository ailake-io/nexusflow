//! Real end-to-end: native CDC via `CatalogProvider`'s "as of snapshot"
//! `list_files`/`list_equality_deletes` — no container, embedded
//! `HadoopCatalog` + `LocalStore` in a temp directory.
//!
//! Only exercises `I`/`D` here, not `U`: this source doesn't yet distinguish
//! a genuine first-time insert from an update of a previously-live key (both
//! are tagged `I`) — see `AilakeCdcConfig`'s doc comment for why. `upsert()`
//! (`upsert_replaces_prior_row_instead_of_duplicating_it` in
//! `ailake_integration.rs`) does have real delete-then-insert semantics now,
//! it's this CDC source's opcode tagging that stops short of telling I/U
//! apart.

#![cfg(feature = "cdc")]

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_ai::embedding::append_embedding_column;
use nexus_connector_ailake::{AilakeCdcConfig, AilakeCdcSource, AilakeConnectorConfig, AilakeSink};
use nexus_core::{Sink, Source, OPCODE_COLUMN};
use std::sync::Arc;

const DIMENSION: usize = 2;

fn batch_with_embedding(ids: Vec<i64>, statuses: Vec<&str>) -> RecordBatch {
    let raw = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(statuses)),
        ],
    )
    .unwrap();
    let embeddings: Vec<Vec<f32>> = ids.iter().map(|_| vec![0.1, 0.2]).collect();
    append_embedding_column(&raw, &embeddings, DIMENSION, "embedding").unwrap()
}

#[tokio::test]
async fn cdc_source_replays_inserts_and_a_real_delete() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let batch_cfg = AilakeConnectorConfig {
        warehouse: dir.path().to_str().unwrap().to_string(),
        warehouse_path: None,
        namespace: "ns".to_string(),
        namespace_name: None,
        table: "docs".to_string(),
        table_name: None,
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: DIMENSION as u32,
        storage_options: nexus_connector_ailake::AilakeStorageOptions::default(),
        append_only: false,
        timeout_seconds: 30,
    };
    let mut sink = AilakeSink::connect(&batch_cfg).expect("sink connects");

    // insert 3 rows
    sink.write_batch(batch_with_embedding(
        vec![1, 2, 3],
        vec!["pending", "pending", "pending"],
    ))
    .await
    .expect("writes insert batch");

    // real CDC delete of id=1
    let delete_raw = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new(OPCODE_COLUMN, DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec!["pending"])),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();
    let delete_batch =
        append_embedding_column(&delete_raw, &[vec![0.1, 0.2]], DIMENSION, "embedding").unwrap();
    sink.write_batch(delete_batch)
        .await
        .expect("writes delete batch");

    let cdc_cfg = AilakeCdcConfig {
        warehouse: batch_cfg.warehouse,
        warehouse_path: batch_cfg.warehouse_path,
        namespace: batch_cfg.namespace,
        namespace_name: batch_cfg.namespace_name,
        table: batch_cfg.table,
        table_name: batch_cfg.table_name,
        primary_key: batch_cfg.primary_key,
        embedding_column: batch_cfg.embedding_column,
        dimension: batch_cfg.dimension,
        storage_options: batch_cfg.storage_options,
        starting_snapshot_id: None,
        timeout_seconds: 30,
    };
    let mut source = AilakeCdcSource::connect(&cdc_cfg)
        .await
        .expect("cdc source connects");
    let mut stream = source.read_batches().await.expect("reads cdc batches");

    let mut rows: Vec<(i64, String)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch reads ok");
        let ids = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let opcodes = batch
            .column(batch.schema().index_of(OPCODE_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for i in 0..ids.len() {
            rows.push((ids.value(i), opcodes.value(i).to_string()));
        }
    }
    drop(stream);

    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![
            (1, "D".to_string()),
            (2, "I".to_string()),
            (3, "I".to_string()),
        ],
        "id=1 deleted for real, id=2/3 untouched inserts: {rows:?}"
    );
}
