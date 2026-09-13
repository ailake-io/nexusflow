//! `PgVectorSearchClient` (LLMOPS_IMPLEMENTATION_PLAN.md Marco L7 follow-up
//! — RAG multi-vetor) against a real pgvector-enabled Postgres, same
//! container setup as `pgvector_integration.rs`'s own sink test.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_pgvector::{PgVectorConnectorConfig, PgVectorSearchClient, PgVectorSink};
use nexus_core::Sink;
use std::sync::Arc;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

#[tokio::test]
async fn search_returns_the_most_similar_row_first() {
    let postgres = GenericImage::new("pgvector/pgvector", "pg16")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "nexus")
        .with_env_var("POSTGRES_PASSWORD", "nexus")
        .with_env_var("POSTGRES_DB", "nexus")
        .start()
        .await
        .expect("pgvector postgres starts");
    let host_port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres host port");
    let uri = format!("host=127.0.0.1 port={host_port} user=nexus password=nexus dbname=nexus");

    let (setup_client, setup_conn) = tokio_postgres::connect(&uri, tokio_postgres::NoTls)
        .await
        .expect("connects to postgres");
    tokio::spawn(async move {
        let _ = setup_conn.await;
    });
    setup_client
        .batch_execute(
            "CREATE EXTENSION IF NOT EXISTS vector; \
             CREATE TABLE docs (id BIGINT PRIMARY KEY, chunk TEXT, embedding VECTOR(384));",
        )
        .await
        .expect("creates schema");

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

    let sink_cfg = PgVectorConnectorConfig {
        uri: Some(uri.clone()),
        host: "localhost".to_string(),
        port: 5432,
        username: String::new(),
        password: String::new(),
        database: String::new(),
        schema: None,
        ssl_mode: nexus_connector_pgvector::PgVectorSslMode::Prefer,
        table: "docs".to_string(),
        primary_key: "id".to_string(),
        embedding_column: "embedding".to_string(),
        dimension: 384,
        timeout_seconds: 30,
    };
    let mut sink = PgVectorSink::connect(&sink_cfg, &["id".to_string(), "chunk".to_string()])
        .await
        .expect("sink connects");
    sink.write_batch(batch).await.expect("writes batch");

    let question_vector = model
        .embed_batch(&["What does NexusFlow do with data?".to_string()])
        .expect("embeds question")
        .remove(0);

    let search_client = PgVectorSearchClient::connect(&sink_cfg)
        .await
        .expect("search client connects");
    let hits = search_client
        .search(question_vector, "embedding", "chunk", 2)
        .await
        .expect("search succeeds");

    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].0, "1", "the NexusFlow row must rank first");
    assert_eq!(hits[0].1, texts[0]);
}
