//! Real end-to-end: chunk → embed(CPU) → LanceDB, including CDC delete.
//! LanceDB is embedded — no container, just a temp directory. See
//! IMPLEMENTATION_PLAN.md Marco 5.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_ai::chunking::{chunk_recursive_character, RecursiveCharacterConfig};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_lancedb::{LanceDbConnectorConfig, LanceDbSink};
use nexus_core::Sink;
use std::sync::Arc;

#[tokio::test]
async fn text_chunk_embed_lancedb_end_to_end() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let uri = dir.path().to_str().unwrap().to_string();

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
    let mut model = EmbeddingModel::load(&embedding_cfg)
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

    // --- write to lancedb ---
    let sink_cfg = LanceDbConnectorConfig {
        uri: uri.clone(),
        table: "docs".to_string(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
    };
    let mut sink = LanceDbSink::connect(&sink_cfg)
        .await
        .expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    let connection = lancedb::connect(&uri).execute().await.expect("reconnects");
    let table = connection
        .open_table("docs")
        .execute()
        .await
        .expect("opens table");
    let count = table.count_rows(None).await.expect("counts rows");
    assert_eq!(count, chunks.len());

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

    // Re-open the table — the sink wrote the delete through its own
    // connection/table handle, and this test's `table` handle was opened
    // before that, so it won't see the new commit without reloading.
    let table = connection
        .open_table("docs")
        .execute()
        .await
        .expect("reopens table");
    let count_after = table
        .count_rows(Some("id = 1".to_string()))
        .await
        .expect("counts rows");
    assert_eq!(count_after, 0, "id=1 must have been deleted, not upserted");
}
