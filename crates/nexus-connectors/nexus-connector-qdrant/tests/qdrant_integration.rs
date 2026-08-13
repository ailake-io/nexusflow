//! Real end-to-end: chunk → embed(CPU) → Qdrant, including CDC delete. See
//! IMPLEMENTATION_PLAN.md Marco 5.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_ai::chunking::{chunk_recursive_character, RecursiveCharacterConfig};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_qdrant::{QdrantConnectorConfig, QdrantSink};
use nexus_core::Sink;
use qdrant_client::qdrant::{CreateCollectionBuilder, Distance, VectorParamsBuilder};
use qdrant_client::Qdrant;
use std::sync::Arc;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;

#[tokio::test]
async fn text_chunk_embed_qdrant_end_to_end() {
    let qdrant_node = GenericImage::new("qdrant/qdrant", "latest")
        .with_exposed_port(6333.tcp())
        .with_exposed_port(6334.tcp())
        // We connect over gRPC (6334), so wait for that listener
        // specifically — it logs a moment after the HTTP one (6333).
        .with_wait_for(WaitFor::message_on_stdout("Qdrant gRPC listening"))
        .start()
        .await
        .expect("qdrant starts");
    let grpc_port = qdrant_node
        .get_host_port_ipv4(6334)
        .await
        .expect("qdrant grpc port");
    let url = format!("http://127.0.0.1:{grpc_port}");

    let setup_client = Qdrant::from_url(&url)
        .build()
        .expect("qdrant client builds");
    setup_client
        .create_collection(
            CreateCollectionBuilder::new("docs")
                .vectors_config(VectorParamsBuilder::new(384, Distance::Cosine)),
        )
        .await
        .expect("creates collection");

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

    // --- write to qdrant ---
    let sink_cfg = QdrantConnectorConfig {
        url: url.clone(),
        host: String::new(),
        port: 6334,
        grpc_url: String::new(),
        api_key: String::new(),
        collection: "docs".to_string(),
        collection_name: String::new(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
        timeout_seconds: 30,
    };
    let mut sink = QdrantSink::connect(&sink_cfg).expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    let count = setup_client
        .count(qdrant_client::qdrant::CountPointsBuilder::new("docs"))
        .await
        .expect("count succeeds")
        .result
        .expect("count result present")
        .count;
    assert_eq!(count, chunks.len() as u64);

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

    let count_after = setup_client
        .count(qdrant_client::qdrant::CountPointsBuilder::new("docs"))
        .await
        .expect("count succeeds")
        .result
        .expect("count result present")
        .count;
    assert_eq!(
        count_after,
        (chunks.len() - 1) as u64,
        "id=1 must have been deleted, not upserted"
    );
}
