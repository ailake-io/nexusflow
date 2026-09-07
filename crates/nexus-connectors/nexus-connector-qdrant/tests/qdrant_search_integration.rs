//! `QdrantSearchClient` (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up
//! — RAG multi-vetor) against a real Qdrant, same container setup as
//! `qdrant_integration.rs`'s own sink test.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_qdrant::{QdrantConnectorConfig, QdrantSearchClient, QdrantSink};
use nexus_core::Sink;
use qdrant_client::qdrant::{CreateCollectionBuilder, Distance, VectorParamsBuilder};
use qdrant_client::Qdrant;
use std::sync::Arc;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;

#[tokio::test]
async fn search_returns_the_most_similar_point_first() {
    let qdrant_node = GenericImage::new("qdrant/qdrant", "latest")
        .with_exposed_port(6333.tcp())
        .with_exposed_port(6334.tcp())
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

    let texts = vec![
        "NexusFlow moves data at high speed across many connectors.".to_string(),
        "The weather today is sunny with a light breeze.".to_string(),
    ];
    let embeddings = model.embed_batch(&texts).expect("embeds chunks");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("chunk", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2i64])),
            Arc::new(StringArray::from(texts.clone())),
        ],
    )
    .unwrap();
    let batch = append_embedding_column(&batch, &embeddings, 384, "embedding").unwrap();

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

    let question_vector = model
        .embed_batch(&["What does NexusFlow do with data?".to_string()])
        .expect("embeds question")
        .remove(0);

    let search_client = QdrantSearchClient::connect(&sink_cfg).expect("search client connects");
    let hits = search_client
        .search(question_vector, "chunk", 2)
        .await
        .expect("search succeeds");

    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].0, "1", "the NexusFlow point must rank first");
    assert_eq!(hits[0].1, texts[0]);
}
