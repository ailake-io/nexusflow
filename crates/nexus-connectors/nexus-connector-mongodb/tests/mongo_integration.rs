use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::Client;
use nexus_connector_mongodb::{
    MongoConnectorConfig, MongoDataType, MongoFieldSpec, MongoSink, MongoSource,
};
use nexus_core::{CheckpointCursor, Sink, Source};
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn fields() -> Vec<MongoFieldSpec> {
    vec![
        MongoFieldSpec {
            name: "id".into(),
            data_type: MongoDataType::Int64,
            nullable: false,
        },
        MongoFieldSpec {
            name: "name".into(),
            data_type: MongoDataType::Utf8,
            nullable: true,
        },
    ]
}

#[tokio::test]
async fn source_reads_documents_as_record_batches() {
    let container = Mongo::default().start().await.expect("mongo starts");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("container port");
    let uri = format!("mongodb://127.0.0.1:{port}");

    let client = Client::with_uri_str(&uri).await.expect("client connects");
    let collection = client.database("nexus").collection("events");
    collection
        .insert_many(vec![
            doc! {"id": 1_i64, "name": "alice"},
            doc! {"id": 2_i64, "name": "bob"},
        ])
        .await
        .expect("seed insert");

    let config = MongoConnectorConfig {
        uri,
        database: "nexus".into(),
        collection: "events".into(),
        primary_key: "id".into(),
        fields: fields(),
        batch_size: 1000,
    };

    let mut source = MongoSource::connect(&config)
        .await
        .expect("source connects");
    let mut stream = source.read_batches().await.expect("read_batches");
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn sink_upsert_is_idempotent_on_replay() {
    let container = Mongo::default().start().await.expect("mongo starts");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("container port");
    let uri = format!("mongodb://127.0.0.1:{port}");

    let config = MongoConnectorConfig {
        uri: uri.clone(),
        database: "nexus".into(),
        collection: "events_copy".into(),
        primary_key: "id".into(),
        fields: fields(),
        batch_size: 1000,
    };

    let schema = arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
    ]);
    let batch = arrow_array::RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
            std::sync::Arc::new(arrow_array::StringArray::from(vec!["alice", "bob"])),
        ],
    )
    .unwrap();

    let mut sink = MongoSink::connect(&config).await.expect("sink connects");
    // Write the same batch twice — replay after a crash must not duplicate.
    sink.write_batch(batch.clone()).await.expect("first write");
    sink.write_batch(batch).await.expect("replayed write");
    sink.commit_checkpoint(CheckpointCursor::new("p0"))
        .await
        .expect("commit is a no-op");

    let client = Client::with_uri_str(&uri).await.expect("client connects");
    let count = client
        .database("nexus")
        .collection::<mongodb::bson::Document>("events_copy")
        .count_documents(doc! {})
        .await
        .expect("count");
    assert_eq!(count, 2);
}
