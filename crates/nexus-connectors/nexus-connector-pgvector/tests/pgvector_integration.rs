//! Marco 5's critério de pronto: texto→chunk→embedding(CPU)→pgvector,
//! end-to-end against a real pgvector-enabled Postgres. See
//! IMPLEMENTATION_PLAN.md Marco 5.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nexus_ai::chunking::{chunk_recursive_character, RecursiveCharacterConfig};
use nexus_ai::embedding::{
    append_embedding_column, EmbeddingModel, EmbeddingModelConfig, ModelConfig,
};
use nexus_connector_pgvector::{PgVectorConnectorConfig, PgVectorSink};
use nexus_core::Sink;
use std::sync::Arc;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

#[tokio::test]
async fn text_chunk_embed_pgvector_end_to_end() {
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

    // --- write to pgvector ---
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

    let rows = setup_client
        .query(
            "SELECT id, chunk, embedding IS NOT NULL FROM docs ORDER BY id",
            &[],
        )
        .await
        .expect("query succeeds");
    assert_eq!(rows.len(), chunks.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i64 = row.get(0);
        let chunk: String = row.get(1);
        let has_embedding: bool = row.get(2);
        assert_eq!(id, ids[i]);
        assert_eq!(chunk, chunks[i]);
        assert!(has_embedding, "row {id} must have a non-null embedding");
    }

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

    let remaining: i64 = setup_client
        .query_one("SELECT count(*) FROM docs WHERE id = 1", &[])
        .await
        .expect("query succeeds")
        .get(0);
    assert_eq!(remaining, 0, "id=1 must have been deleted, not upserted");
}
