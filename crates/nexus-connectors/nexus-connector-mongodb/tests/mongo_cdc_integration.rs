//! End-to-end native CDC: real MongoDB Change Streams, no Debezium/Kafka in
//! front — ARCHITECTURE.md §7. Requires a replica set (`Mongo::repl_set()`,
//! self-initiating via testcontainers-modules) — Change Streams don't work
//! against a standalone server.

use arrow_array::{Array, Int64Array, StringArray};
use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::Client;
use nexus_connector_mongodb::{MongoCdcConfig, MongoCdcSource, MongoDataType, MongoFieldSpec};
use nexus_core::{Source, OPCODE_COLUMN};
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn fields() -> Vec<MongoFieldSpec> {
    vec![
        MongoFieldSpec {
            name: "_id".into(),
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
async fn mongo_native_cdc_carries_correct_opcode_per_row() {
    let container = Mongo::repl_set().start().await.expect("mongo starts");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("container port");
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");

    let client = Client::with_uri_str(&uri).await.expect("client connects");
    let collection = client
        .database("nexus")
        .collection::<mongodb::bson::Document>("events");

    let config = MongoCdcConfig {
        connection_string: Some(uri.clone()),
        hosts: Vec::new(),
        username: None,
        password: None,
        auth_database: None,
        database: "nexus".to_string(),
        collection: "events".to_string(),
        fields: fields(),
        resume_token: None,
        batch_size: 1000,
        timeout_seconds: 5,
    };

    let mut source = MongoCdcSource::connect(&config)
        .await
        .expect("mongodb-cdc connects");

    // `read_batches` is what actually opens the change stream (matching
    // `MongoSource`'s existing lazy-cursor pattern) — it must happen
    // *before* the writes below, since a Change Stream with no
    // `resume_token` starts watching from "now": any write before this
    // point would be missed entirely, not just delayed.
    let mut stream = source.read_batches().await.expect("read_batches");

    collection
        .insert_one(doc! {"_id": 1_i64, "name": "alice"})
        .await
        .expect("test insert");
    collection
        .update_one(doc! {"_id": 1_i64}, doc! {"$set": {"name": "alice2"}})
        .await
        .expect("test update");
    // A real gap between writes, not back-to-back in the same millisecond —
    // `full_document: UpdateLookup` fetches the update's current document
    // state via a *separate* query at decode time; deleting immediately
    // after (as a tight loop with no delay would) races that lookup against
    // the delete and can make it legitimately come back null per MongoDB's
    // own semantics (`event_to_row` falls back to `document_key` in that
    // case — the row still arrives, just without the field values). This
    // sleep keeps the assertions below deterministic without relying on
    // that fallback path.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    collection
        .delete_one(doc! {"_id": 1_i64})
        .await
        .expect("test delete");

    // The 3 events aren't guaranteed to land in a single batch — the idle
    // flush can fire between them depending on propagation timing — so a
    // real consumer (and this test) reads across as many batches as it
    // takes, exactly like it would against a live, indefinitely long stream.
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
            .column_by_name("_id")
            .expect("_id column present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_id column is Int64");
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
    // `_id` survives on every event, including delete — it's always part of
    // `document_key`, unlike every other configured field.
    assert_eq!(ids, vec![Some(1), Some(1), Some(1)]);
    assert_eq!(names[0].as_deref(), Some("alice"));
    // The update's `name` value depends on MongoDB's `full_document:
    // updateLookup` actually finding a fresh copy at decode time, which
    // isn't guaranteed (see cdc.rs's `event_to_row` doc comment) — this
    // connector's contract is that the row still arrives with the right
    // opcode and identity either way, not that the value is always present.
    assert!(
        names[1].as_deref() == Some("alice2") || names[1].is_none(),
        "update row should carry the new value or fall back to null, got {:?}",
        names[1]
    );
    // Documented limitation (see cdc.rs): a delete event carries only
    // `document_key` (just `_id`), not `full_document` — every other
    // configured field comes through null.
    assert_eq!(names[2], None);
}
