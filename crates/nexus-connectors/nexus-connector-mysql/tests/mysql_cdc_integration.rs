//! End-to-end native CDC: real MySQL binlog replication (row-based), no
//! Debezium/Kafka in front — ARCHITECTURE.md §7.

use arrow_array::{Array, Int64Array, StringArray};
use futures::StreamExt;
use mysql_async::prelude::Queryable;
use nexus_connector_mysql::{MySqlCdcConfig, MySqlCdcDataType, MySqlCdcFieldSpec, MySqlCdcSource};
use nexus_core::{Source, OPCODE_COLUMN};
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;

#[tokio::test]
async fn mysql_native_cdc_carries_correct_opcode_per_row() {
    let container = Mysql::default()
        .with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
        ])
        .start()
        .await
        .expect("mysql starts");

    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("container port");
    let database_url = format!("mysql://root@{host}:{port}/test");

    let pool = mysql_async::Pool::new(database_url.as_str());
    let mut setup_conn = pool.get_conn().await.expect("connects to mysql");

    setup_conn
        .query_drop("CREATE TABLE cdc_test (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .expect("test table created");
    // A dedicated replication user rather than root+empty-password: MySQL
    // 8's default `caching_sha2_password` auth plugin doesn't reliably
    // accept an empty password on a fresh replication-mode connection
    // (`root`'s grants and normal-query auth still work fine — only the
    // replication handshake fails), and a real deployment would never
    // register a replica as root anyway.
    setup_conn
        .query_drop(
            "CREATE USER 'replicator'@'%' IDENTIFIED WITH mysql_native_password BY 'replpass'",
        )
        .await
        .expect("replication user created");
    setup_conn
        .query_drop("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%'")
        .await
        .expect("replication grants");

    let config = MySqlCdcConfig {
        host: host.to_string(),
        port,
        username: "replicator".to_string(),
        password: "replpass".to_string(),
        database: "test".to_string(),
        table: "cdc_test".to_string(),
        server_id: 65535,
        // Positional, matching the table's column order exactly (id, name)
        // — see MySqlCdcConfig's doc comment on why this isn't by name.
        fields: vec![
            MySqlCdcFieldSpec {
                name: "id".to_string(),
                data_type: MySqlCdcDataType::Int64,
                nullable: false,
            },
            MySqlCdcFieldSpec {
                name: "name".to_string(),
                data_type: MySqlCdcDataType::Utf8,
                nullable: true,
            },
        ],
        binlog_filename: None,
        binlog_position: None,
    };

    // `read_batches` spawns the replication thread, which starts streaming
    // from the current end of the binlog (`BinlogOptions::from_end`, no
    // explicit position configured) — must happen before the writes below,
    // or they'd be missed entirely.
    let mut source = MySqlCdcSource::connect(&config)
        .await
        .expect("mysql-cdc connects");
    let mut stream = source.read_batches().await.expect("read_batches");

    setup_conn
        .query_drop("INSERT INTO cdc_test (id, name) VALUES (1, 'alice')")
        .await
        .expect("test insert");
    setup_conn
        .query_drop("UPDATE cdc_test SET name = 'alice2' WHERE id = 1")
        .await
        .expect("test update");
    setup_conn
        .query_drop("DELETE FROM cdc_test WHERE id = 1")
        .await
        .expect("test delete");

    // Mirrors the MongoDB CDC test: the 3 events aren't guaranteed to land
    // in a single batch (idle flush can fire between them), so accumulate
    // across as many batches as it takes.
    let mut opcodes: Vec<String> = Vec::new();
    let mut ids: Vec<Option<i64>> = Vec::new();
    let mut names: Vec<Option<String>> = Vec::new();
    while opcodes.len() < 3 {
        let batch = stream
            .next()
            .await
            .expect("a batch arrives")
            .expect("batch is Ok");

        let opcode_col = batch
            .column_by_name(OPCODE_COLUMN)
            .expect("opcode column present")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("opcode column is Utf8");
        let id_col = batch
            .column_by_name("id")
            .expect("id column present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let name_col = batch
            .column_by_name("name")
            .expect("name column present")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column is Utf8");

        for i in 0..batch.num_rows() {
            opcodes.push(opcode_col.value(i).to_string());
            ids.push((!id_col.is_null(i)).then(|| id_col.value(i)));
            names.push((!name_col.is_null(i)).then(|| name_col.value(i).to_string()));
        }
    }

    assert_eq!(opcodes, vec!["I", "U", "D"]);
    assert_eq!(ids, vec![Some(1), Some(1), Some(1)]);
    assert_eq!(names[0].as_deref(), Some("alice"));
    assert_eq!(names[1].as_deref(), Some("alice2"));
    // `binlog-row-image=FULL` means the delete's row image carries every
    // column, not just the primary key — unlike MongoDB's `document_key`.
    assert_eq!(names[2].as_deref(), Some("alice2"));
}
