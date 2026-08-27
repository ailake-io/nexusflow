//! Real end-to-end: chunk → embed(CPU) → AI-Lake sink, then read back via
//! AI-Lake source and validate schema/data, including a CDC delete. AI-Lake
//! is embedded (HadoopCatalog + LocalStore) — no container, just a temp
//! directory. See IMPLEMENTATION_PLAN.md Marco 6.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use nexus_ai::chunking::{chunk_recursive_character, RecursiveCharacterConfig};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_ailake::{AilakeConnectorConfig, AilakeSink, AilakeSource};
use nexus_core::{Sink, Source};
use std::sync::Arc;

#[tokio::test]
async fn text_chunk_embed_ailake_end_to_end() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let warehouse = dir.path().to_str().unwrap().to_string();

    // --- chunk ---
    let source_text = "NexusFlow moves data at high speed.\n\n\
        It builds an AI lakehouse from any source.\n\n\
        The weather today is sunny with a light breeze.";
    let chunks = chunk_recursive_character(
        source_text,
        &RecursiveCharacterConfig {
            chunk_size: 60,
            overlap: 0,
            ..RecursiveCharacterConfig::default()
        },
    );
    assert!(
        chunks.len() >= 2,
        "expected multiple chunks, got {chunks:?}"
    );

    // --- embed ---
    let embedding_cfg = EmbeddingModelConfig {
        model: ModelConfig {
            repo_id: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            revision: "main".to_string(),
            filename: "onnx/model.onnx".to_string(),
        },
        tokenizer_filename: "tokenizer.json".to_string(),
        dimension: 384,
        max_length: 128,
    };
    let model = EmbeddingModel::load(&embedding_cfg)
        .await
        .expect("embedding model loads");
    let embeddings = model.embed_batch(&chunks).expect("embeds chunks");

    // --- build the RecordBatch nexus-server would hand to the sink ---
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("chunk", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (1..=chunks.len() as i64).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(chunks.clone())),
        ],
    )
    .unwrap();
    let batch = append_embedding_column(&batch, &embeddings, 384, "embedding").unwrap();

    // --- write to ailake ---
    let sink_cfg = AilakeConnectorConfig {
        warehouse: warehouse.clone(),
        warehouse_path: None,
        namespace: "ns".to_string(),
        namespace_name: None,
        table: "docs".to_string(),
        table_name: None,
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
        storage_options: nexus_connector_ailake::AilakeStorageOptions::default(),
        append_only: false,
        timeout_seconds: 30,
        flush_threshold_rows: 50_000,
    };
    let mut sink = AilakeSink::connect(&sink_cfg).expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    // --- read back via the source and validate schema/data ---
    let mut source = AilakeSource::connect(&sink_cfg)
        .await
        .expect("source connects");
    let schema = source.schema();
    assert!(schema.field_with_name("id").is_ok());
    assert!(schema.field_with_name("chunk").is_ok());
    assert!(schema.field_with_name("embedding").is_ok());

    let mut stream = source.read_batches().await.expect("reads batches");
    let mut total_rows = 0usize;
    let mut seen_ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch reads ok");
        total_rows += batch.num_rows();
        let id_col = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        seen_ids.extend((0..id_col.len()).map(|i| id_col.value(i)));
    }
    drop(stream);
    assert_eq!(total_rows, chunks.len());
    seen_ids.sort();
    assert_eq!(seen_ids, ids);

    // --- CDC delete: a batch carrying __opcode = "D" for id 1 must remove it ---
    let delete_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("chunk", DataType::Utf8, false),
        Field::new(nexus_core::OPCODE_COLUMN, DataType::Utf8, false),
    ]));
    let delete_batch = RecordBatch::try_new(
        delete_schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec![chunks[0].clone()])),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();
    let delete_batch =
        append_embedding_column(&delete_batch, &embeddings[..1], 384, "embedding").unwrap();
    sink.write_batch(delete_batch)
        .await
        .expect("writes delete batch");

    // AI-Lake equality deletes mask rows at read time without rewriting data
    // files — a fresh source (fresh scan) must no longer see id=1.
    let mut source = AilakeSource::connect(&sink_cfg)
        .await
        .expect("source reconnects");
    let mut stream = source.read_batches().await.expect("reads batches");
    let mut seen_ids_after = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch reads ok");
        let id_col = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        seen_ids_after.extend((0..id_col.len()).map(|i| id_col.value(i)));
    }
    assert!(
        !seen_ids_after.contains(&1i64),
        "id=1 must have been deleted, not upserted: {seen_ids_after:?}"
    );
}

/// Real upsert semantics: a second write of the same primary key must
/// replace the row's data, not produce a second physical row alongside it.
/// `AilakeSink::upsert` commits an equality-delete for the batch's keys
/// immediately before appending — see its doc comment for why the new row
/// is never masked by its own delete (`ailake-catalog`/`ailake-query`
/// >=0.1.11's sequence-scoped equality deletes).
#[tokio::test]
async fn upsert_replaces_prior_row_instead_of_duplicating_it() {
    const DIMENSION: usize = 2;

    fn batch_with_status(id: i64, status: &str) -> RecordBatch {
        let raw = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("status", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![status])),
            ],
        )
        .unwrap();
        append_embedding_column(&raw, &[vec![0.1, 0.2]], DIMENSION, "embedding").unwrap()
    }

    let dir = tempfile::tempdir().expect("tempdir creates");
    let cfg = AilakeConnectorConfig {
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
        flush_threshold_rows: 50_000,
    };
    let mut sink = AilakeSink::connect(&cfg).expect("sink connects");

    sink.write_batch(batch_with_status(1, "version-a"))
        .await
        .expect("writes first version");
    sink.write_batch(batch_with_status(1, "version-b"))
        .await
        .expect("writes second version");

    let mut source = AilakeSource::connect(&cfg).await.expect("source connects");
    let mut stream = source.read_batches().await.expect("reads batches");
    let mut rows: Vec<(i64, String)> = Vec::new();
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
        for i in 0..ids.len() {
            rows.push((ids.value(i), statuses.value(i).to_string()));
        }
    }
    drop(stream);

    assert_eq!(
        rows,
        vec![(1, "version-b".to_string())],
        "second write must replace the row, not duplicate it: {rows:?}"
    );
}
