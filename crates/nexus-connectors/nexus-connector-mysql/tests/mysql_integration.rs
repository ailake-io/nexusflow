use futures::StreamExt;
use mysql_async::prelude::Queryable;
use nexus_connector_mysql::{
    MySqlCdcDataType, MySqlCdcFieldSpec, MySqlConnectorConfig, MySqlSink, MySqlSource,
};
use nexus_core::{CheckpointCursor, Sink, Source, OPCODE_COLUMN};
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn fields() -> Vec<MySqlCdcFieldSpec> {
    vec![
        MySqlCdcFieldSpec {
            name: "id".into(),
            data_type: MySqlCdcDataType::Int64,
            nullable: false,
        },
        MySqlCdcFieldSpec {
            name: "name".into(),
            data_type: MySqlCdcDataType::Utf8,
            nullable: true,
        },
    ]
}

#[tokio::test]
async fn source_reads_rows_as_record_batches() {
    let container = Mysql::default().start().await.expect("mysql starts");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("container port");

    let pool = mysql_async::Pool::new(format!("mysql://root@{host}:{port}/test").as_str());
    let mut setup_conn = pool.get_conn().await.expect("connects to mysql");
    setup_conn
        .query_drop("CREATE TABLE events (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .expect("table created");
    setup_conn
        .query_drop("INSERT INTO events (id, name) VALUES (1, 'alice'), (2, 'bob')")
        .await
        .expect("seed insert");

    let config = MySqlConnectorConfig {
        uri: None,
        host: host.to_string(),
        port,
        username: "root".to_string(),
        password: String::new(),
        database: "test".to_string(),
        table: "events".to_string(),
        primary_key: "id".to_string(),
        fields: fields(),
        batch_size: 1000,
        timeout_seconds: 30,
    };

    let mut source = MySqlSource::connect(&config)
        .await
        .expect("source connects");
    let mut stream = source.read_batches().await.expect("read_batches");
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        total_rows += batch.expect("batch is Ok").num_rows();
    }
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn sink_upsert_is_idempotent_on_replay() {
    let container = Mysql::default().start().await.expect("mysql starts");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("container port");

    let pool = mysql_async::Pool::new(format!("mysql://root@{host}:{port}/test").as_str());
    let mut setup_conn = pool.get_conn().await.expect("connects to mysql");
    setup_conn
        .query_drop("CREATE TABLE events_copy (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .expect("table created");

    let config = MySqlConnectorConfig {
        uri: None,
        host: host.to_string(),
        port,
        username: "root".to_string(),
        password: String::new(),
        database: "test".to_string(),
        table: "events_copy".to_string(),
        primary_key: "id".to_string(),
        fields: fields(),
        batch_size: 1000,
        timeout_seconds: 30,
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

    let schema = std::sync::Arc::new(schema);
    let mut sink = MySqlSink::connect(&config, &schema)
        .await
        .expect("sink connects");
    // Write the same batch twice — replay after a crash must not duplicate
    // or fail on the primary-key conflict.
    sink.write_batch(batch.clone()).await.expect("first write");
    sink.write_batch(batch).await.expect("replayed write");
    sink.commit_checkpoint(CheckpointCursor::new("p0"))
        .await
        .expect("commit is a no-op");

    let count: i64 = setup_conn
        .query_first("SELECT COUNT(*) FROM events_copy")
        .await
        .expect("count query")
        .expect("count row");
    assert_eq!(count, 2);
}

#[tokio::test]
async fn sink_deletes_rows_carrying_the_d_opcode() {
    let container = Mysql::default().start().await.expect("mysql starts");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("container port");

    let pool = mysql_async::Pool::new(format!("mysql://root@{host}:{port}/test").as_str());
    let mut setup_conn = pool.get_conn().await.expect("connects to mysql");
    setup_conn
        .query_drop("CREATE TABLE events_cdc (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .expect("table created");
    setup_conn
        .query_drop("INSERT INTO events_cdc (id, name) VALUES (1, 'alice'), (2, 'bob')")
        .await
        .expect("seed insert");

    let config = MySqlConnectorConfig {
        uri: None,
        host: host.to_string(),
        port,
        username: "root".to_string(),
        password: String::new(),
        database: "test".to_string(),
        table: "events_cdc".to_string(),
        primary_key: "id".to_string(),
        fields: fields(),
        batch_size: 1000,
        timeout_seconds: 30,
    };

    // CDC-shaped batch: id=1 upserted (name changes), id=2 deleted.
    let schema = arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        arrow_schema::Field::new(OPCODE_COLUMN, arrow_schema::DataType::Utf8, false),
    ]);
    let batch = arrow_array::RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
            std::sync::Arc::new(arrow_array::StringArray::from(vec![Some("alice2"), None])),
            std::sync::Arc::new(arrow_array::StringArray::from(vec!["U", "D"])),
        ],
    )
    .unwrap();

    let schema = std::sync::Arc::new(schema);
    let mut sink = MySqlSink::connect(&config, &schema)
        .await
        .expect("sink connects");
    sink.write_batch(batch).await.expect("cdc write");

    let remaining: Vec<(i64, String)> = setup_conn
        .query("SELECT id, name FROM events_cdc ORDER BY id")
        .await
        .expect("select remaining rows");
    assert_eq!(remaining, vec![(1, "alice2".to_string())]);
}
