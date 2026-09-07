//! Real end-to-end: embed(CPU) chunks about two unrelated subjects into
//! LanceDB, then search with a query embedding and confirm the nearest
//! result is from the right subject. Same real-model/no-container shape as
//! `lancedb_integration.rs`. LLMOPS_IMPLEMENTATION_PLAN.md Marco L5's own
//! acceptance criterion for the search client this exercises.

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_lancedb::{LanceDbConnectorConfig, LanceDbSearchClient, LanceDbSink};
use nexus_core::Sink;
use std::sync::Arc;

#[tokio::test]
async fn nearest_to_finds_the_row_from_the_matching_subject() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let uri = dir.path().to_str().unwrap().to_string();

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

    let rows = [
        "NexusFlow moves data at high speed across many connectors.".to_string(),
        "The AI lakehouse builder embeds text and writes vectors.".to_string(),
        "The weather today is sunny with a light breeze outside.".to_string(),
    ];
    let embeddings = model.embed_batch(&rows).expect("embeds rows");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(StringArray::from(rows.to_vec())),
        ],
    )
    .unwrap();
    let batch = append_embedding_column(&batch, &embeddings, 384, "embedding").unwrap();

    let sink_cfg = LanceDbConnectorConfig {
        uri: Some(uri.clone()),
        path: None,
        storage_options: nexus_connector_lancedb::LanceDbStorageOptions::default(),
        table: None,
        table_name: Some("docs".to_string()),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
        timeout_seconds: 30,
    };
    let mut sink = LanceDbSink::connect(&sink_cfg)
        .await
        .expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    let query_embedding = model
        .embed_batch(&["How fast does NexusFlow move data?".to_string()])
        .expect("embeds query")
        .remove(0);

    let search_client = LanceDbSearchClient::connect(&sink_cfg)
        .await
        .expect("search client connects");
    let results = search_client
        .search(query_embedding, "embedding", 1)
        .await
        .expect("search succeeds");

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    let id_col = batch
        .column(batch.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        id_col.value(0),
        1,
        "expected the NexusFlow row (id=1) to be nearest, not the weather row"
    );
}

#[tokio::test]
async fn search_against_a_table_that_was_never_created_is_a_clear_error() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let uri = dir.path().to_str().unwrap().to_string();

    // `LanceDbSink::write_batch` on a 0-row batch is a no-op that never
    // creates the table (confirmed by reading sink.rs's `upsert` — an
    // empty batch returns `Ok(())` immediately) — so a pipeline whose
    // source produced zero rows leaves no table at all, not an empty one.
    // A RAG query against that pipeline must fail clearly, not panic.
    let sink_cfg = LanceDbConnectorConfig {
        uri: Some(uri.clone()),
        path: None,
        storage_options: nexus_connector_lancedb::LanceDbStorageOptions::default(),
        table: None,
        table_name: Some("never_created".to_string()),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
        timeout_seconds: 30,
    };

    let search_client = LanceDbSearchClient::connect(&sink_cfg)
        .await
        .expect("search client connects (connecting to the DB itself always succeeds)");
    let err = search_client
        .search(vec![0.0f32; 384], "embedding", 5)
        .await
        .expect_err("searching a table that doesn't exist must fail, not panic");
    assert!(matches!(err, nexus_core::NexusError::Connector(_)));
}
