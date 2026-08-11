//! Real end-to-end: Delta Lake's built-in Change Data Feed, no Debezium/
//! Kafka, no polling loop of our own (`DeltaTable::scan_cdf`). Plain local
//! table directory, no container.
//!
//! `DeltaSink`'s upsert path is delete-then-append across two *separate*
//! Delta commits (see `sink.rs`), not a single SQL `UPDATE` — so an
//! upserted row shows up in the feed as a `delete` of the old row plus an
//! `insert` of the new one, never `update_preimage`/`update_postimage`
//! (those only come from a real `UPDATE` statement, which nothing in this
//! crate issues). This test asserts the real sequence that mechanism
//! produces, not an idealized one.

#![cfg(feature = "cdc")]

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use deltalake::operations::create::CreateBuilder;
use deltalake::TableProperty;
use futures::StreamExt;
use nexus_connector_deltalake::{DeltaCdcConfig, DeltaCdcSource, DeltaConnectorConfig, DeltaSink};
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
async fn cdc_source_replays_inserts_and_deletes_from_change_data_feed() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let table_uri = dir.path().join("orders").to_str().unwrap().to_string();

    // Create the table ourselves (not via `DeltaSink::ensure_table`, which
    // has no way to set table properties) so `delta.enableChangeDataFeed`
    // is on from version 0 — same pattern deltalake-core's own CDF tests
    // use (`operations/cdc.rs`).
    use deltalake::{DataType as DeltaDataType, PrimitiveType, StructField};
    CreateBuilder::new()
        .with_location(&table_uri)
        .with_columns(vec![
            StructField::new(
                "id",
                DeltaDataType::Primitive(PrimitiveType::Long),
                false,
            ),
            StructField::new(
                "status",
                DeltaDataType::Primitive(PrimitiveType::String),
                false,
            ),
        ])
        .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
        .await
        .expect("table creates with CDF enabled");

    let batch_cfg = DeltaConnectorConfig {
        table_uri: table_uri.clone(),
        primary_key: "id".to_string(),
        timeout_seconds: 30,
    };
    let mut sink = DeltaSink::connect(&batch_cfg).expect("sink connects");

    // version 1: insert 3 rows
    sink.write_batch(batch(vec![1, 2, 3], vec!["pending", "pending", "pending"]))
        .await
        .expect("writes insert batch");

    // versions 2+3: upsert id=2 (delete old row, append new one)
    sink.write_batch(batch(vec![2], vec!["paid"]))
        .await
        .expect("writes update batch");

    // version 4: real CDC delete of id=1
    let delete_batch = RecordBatch::try_new(
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
    sink.write_batch(delete_batch)
        .await
        .expect("writes delete batch");

    // --- read the whole feed from version 0 ---
    let cdc_cfg = DeltaCdcConfig {
        table_uri,
        starting_version: Some(0),
        timeout_seconds: 30,
    };
    let mut source = DeltaCdcSource::connect(&cdc_cfg)
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

    // Fold in commit order (already the order CDF returns them) into final
    // per-key state — the real assertion that matters: does replaying this
    // feed reconstruct the same state `DeltaSource` (the batch reader)
    // would show right now?
    let mut state: std::collections::BTreeMap<i64, Option<String>> = std::collections::BTreeMap::new();
    for (id, status, opcode) in &rows {
        match opcode.as_str() {
            "I" | "U" => {
                state.insert(*id, Some(status.clone()));
            }
            "D" => {
                state.insert(*id, None);
            }
            other => panic!("unexpected opcode {other}"),
        }
    }
    let final_state: Vec<(i64, String)> = state
        .into_iter()
        .filter_map(|(id, status)| status.map(|s| (id, s)))
        .collect();

    assert_eq!(
        final_state,
        vec![(2, "paid".to_string()), (3, "pending".to_string())],
        "replaying the CDC feed must reconstruct id=1 deleted, id=2 updated, id=3 unchanged: {rows:?}"
    );

    // Delta's delete-then-append upsert mechanic means this is I,I,I,D,I,D —
    // never "U" — see this test's module doc comment.
    let opcode_sequence: Vec<&str> = rows.iter().map(|(_, _, o)| o.as_str()).collect();
    assert_eq!(opcode_sequence, vec!["I", "I", "I", "D", "I", "D"]);
}
