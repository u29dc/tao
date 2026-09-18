use crate::*;
use rusqlite::{Connection, params};

#[test]
fn fresh_schema_is_atomic_idempotent_and_integrity_checked() {
    let mut connection = Connection::open_in_memory().unwrap();
    assert_eq!(
        preflight_migrations(&connection)
            .unwrap()
            .pending_migrations,
        1
    );
    let first = run_migrations(&mut connection).unwrap();
    assert_eq!(first.applied, [CURRENT_SCHEMA_ID]);
    let second = run_migrations(&mut connection).unwrap();
    assert!(second.applied.is_empty());
    assert_eq!(second.skipped, [CURRENT_SCHEMA_ID]);
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |r| r.get::<_, u32>(0))
            .unwrap(),
        CURRENT_FORMAT_EPOCH
    );
    assert_eq!(
        connection
            .query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, u64>(
                0
            ))
            .unwrap(),
        0
    );
    assert_eq!(connection.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('search_index','search_index_fts','search_segments_fts_content')",[],|r|r.get::<_,u64>(0)).unwrap(),0);
    assert_eq!(
        connection
            .query_row(
                "SELECT bytes_total FROM extraction_stage_usage WHERE singleton=1",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
        0
    );
    SearchSegmentRepository::check_integrity(&connection).unwrap();
}

#[test]
fn old_and_future_epochs_refuse_before_any_mutation() {
    for epoch in [0, 2, 999] {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE old_index(value TEXT); INSERT INTO old_index VALUES('preserve');",
            )
            .unwrap();
        connection
            .pragma_update(None, "user_version", epoch)
            .unwrap();
        let changes = connection.total_changes();
        let error = run_migrations(&mut connection).unwrap_err();
        assert!(matches!(
            error,
            MigrationRunnerError::UnsupportedFormat { .. }
        ));
        assert!(error.to_string().contains("vault reindex"));
        assert_eq!(connection.total_changes(), changes);
        assert_eq!(
            connection
                .query_row("SELECT value FROM old_index", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "preserve"
        );
    }
}

#[test]
fn current_checksum_mismatch_refuses_without_repairing_metadata() {
    let mut connection = Connection::open_in_memory().unwrap();
    run_migrations(&mut connection).unwrap();
    connection
        .execute("UPDATE schema_migrations SET checksum='bad'", [])
        .unwrap();
    assert!(matches!(
        run_migrations(&mut connection),
        Err(MigrationRunnerError::ChecksumMismatch { .. })
    ));
    assert_eq!(
        connection
            .query_row("SELECT checksum FROM schema_migrations", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "bad"
    );
}

#[test]
fn aliases_are_indexed_by_owner_and_newer_schemas_are_rejected_before_mutation() {
    let mut connection = Connection::open_in_memory().unwrap();
    run_migrations(&mut connection).unwrap();
    let plan = connection
        .query_row(
            "EXPLAIN QUERY PLAN DELETE FROM search_aliases WHERE file_id='x'",
            [],
            |r| r.get::<_, String>(3),
        )
        .unwrap();
    assert!(plan.contains("idx_search_aliases_file"), "{plan}");
    connection
        .execute(
            "INSERT INTO schema_migrations(id,checksum) VALUES('9999_future','future')",
            [],
        )
        .unwrap();
    assert!(matches!(
        run_migrations(&mut connection),
        Err(MigrationRunnerError::UnsupportedSchema { .. })
    ));
}
#[test]
fn canonical_generation_and_diagnostics_are_transactional() {
    let mut connection = Connection::open_in_memory().unwrap();
    run_migrations(&mut connection).unwrap();
    let initial = IndexGenerationRepository::get(&connection).unwrap();
    {
        let tx = connection.transaction().unwrap();
        tx.execute("INSERT INTO files(file_id,normalized_path,match_key,absolute_path,size_bytes,modified_unix_ms,hash_blake3,is_markdown) VALUES('f','a.md','a.md','/a.md',1,1,'h',1)",[]).unwrap();
        DocumentsRepository::upsert(
            &tx,
            &DocumentRecordInput {
                file_id: "f".into(),
                source_hash: "h".into(),
                parser_version: 1,
                raw_text: "# A".into(),
                body_text: "# A".into(),
                title: "A".into(),
                structure_json: "{}".into(),
            },
        )
        .unwrap();
        assert!(
            IndexGenerationRepository::get(&tx)
                .unwrap()
                .canonical_generation
                > initial.canonical_generation
        );
        assert_eq!(IndexGenerationRepository::dirty_files(&tx).unwrap(), ["f"]);
        // Dropping the transaction must discard the entire revision and generation.
    }
    assert_eq!(
        IndexGenerationRepository::get(&connection).unwrap(),
        initial
    );
    assert!(
        DocumentsRepository::get_by_file_id(&connection, "f")
            .unwrap()
            .is_none()
    );
}

#[test]
fn external_content_preserves_rank_and_reduces_duplicate_storage() {
    fn fixture(external: bool) -> Connection {
        let mut c = Connection::open_in_memory().unwrap();
        run_migrations(&mut c).unwrap();
        if !external {
            c.execute_batch("DROP TRIGGER search_segments_fts_ai; DROP TRIGGER search_segments_fts_au; DROP TRIGGER search_segments_fts_ad; DROP TABLE search_segments_fts; CREATE VIRTUAL TABLE search_segments_fts USING fts5(path_text,title_text,alias_text,body_text,property_text,task_text,link_text,base_text,tokenize='unicode61');").unwrap();
        }
        for index in 0..128 {
            let key = format!("file-{index}");
            c.execute("INSERT INTO files(file_id,normalized_path,match_key,absolute_path,size_bytes,modified_unix_ms,hash_blake3,is_markdown) VALUES(?1,?1,?1,?1,1,1,'h',1)",[&key]).unwrap();
            let body = format!(
                "uniquetoken{index} café {}",
                "alpha beta corpus reference ".repeat(512)
            );
            c.execute("INSERT INTO search_segments(segment_id,surface,file_id,normalized_path,normalized_path_lc,extension,field,label,weight,payload_json,body_text,title_text) VALUES(?1,'docs',?1,?1,?1,'md','document',?1,80,'{}',?2,'Title')",params![key,body]).unwrap();
        }
        if !external {
            c.execute("INSERT INTO search_segments_fts(rowid,path_text,title_text,alias_text,body_text,property_text,task_text,link_text,base_text) SELECT rowid,path_text,title_text,alias_text,body_text,property_text,task_text,link_text,base_text FROM search_segments",[]).unwrap();
        }
        c.execute_batch("VACUUM").unwrap();
        c
    }
    let ordinary = fixture(false);
    let connection = fixture(true);
    let results = |c: &Connection, q: &str| {
        c.prepare("SELECT rowid,bm25(search_segments_fts) FROM search_segments_fts WHERE search_segments_fts MATCH ?1 ORDER BY bm25(search_segments_fts),rowid").unwrap().query_map([q],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,f64>(1)?))).unwrap().collect::<Result<Vec<_>,_>>().unwrap()
    };
    for query in ["alpha*", "cafe", "uniquetoken7", "title_text:Title"] {
        assert_eq!(results(&connection, query), results(&ordinary, query));
    }
    let size = |c: &Connection| {
        c.query_row(
            "SELECT page_count*page_size FROM pragma_page_count,pragma_page_size",
            [],
            |r| r.get::<_, u64>(0),
        )
        .unwrap()
    };
    assert!(size(&connection) < size(&ordinary));
    eprintln!(
        "FTS qualification: equivalent ranked results; ordinary={} bytes external={} bytes",
        size(&ordinary),
        size(&connection)
    );
    connection
        .execute(
            "UPDATE search_segments SET body_text='replacementword' WHERE segment_id='file-7'",
            [],
        )
        .unwrap();
    assert!(results(&connection, "uniquetoken7").is_empty());
    assert_eq!(results(&connection, "replacementword").len(), 1);
    connection
        .execute("DELETE FROM search_segments WHERE segment_id='file-7'", [])
        .unwrap();
    assert!(results(&connection, "replacementword").is_empty());
    SearchSegmentRepository::check_integrity(&connection).unwrap();
    connection.execute_batch("PRAGMA query_only=ON").unwrap();
    let changes = connection.total_changes();
    SearchSegmentRepository::check_integrity_read_only(&connection).unwrap();
    assert_eq!(connection.total_changes(), changes);
}
