use super::*;
use std::fs::{self, File, FileTimes};
use tao_sdk_storage::{IndexGenerationRepository, LinksRepository, run_migrations};
use tempfile::tempdir;

fn database() -> Connection {
    let mut connection = Connection::open_in_memory().unwrap();
    run_migrations(&mut connection).unwrap();
    connection
}

fn reindex(root: &Path, connection: &mut Connection) -> crate::IndexRefreshOutcome {
    crate::IndexRefreshService
        .refresh(
            root,
            connection,
            CasePolicy::Sensitive,
            crate::IndexRefreshOptions {
                scan_mode: ReconciliationScanMode::VerifyContentHashes,
                max_batch_size: 128,
            },
        )
        .unwrap()
}

#[test]
fn ignore_transitions_remove_and_restore_inventory_graph_and_corpus() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("a.md"), "# A\n[[hidden]]").unwrap();
    fs::write(root.join("hidden.md"), "# Hidden\nsecretterm").unwrap();
    reindex(root, &mut db);
    fs::write(root.join(".taoignore"), "hidden.md\n").unwrap();
    let result = reindex(root, &mut db);
    assert_eq!(result.removed_files, 1);
    assert_eq!(result.mode, crate::IndexRefreshMode::Reconcile);
    assert!(
        FilesRepository::get_by_normalized_path(&db, "hidden.md")
            .unwrap()
            .is_none()
    );
    let hidden_segments: u64 = db
        .query_row(
            "SELECT COUNT(*) FROM search_segments WHERE normalized_path='hidden.md'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(hidden_segments, 0);
    assert!(LinksRepository::list_all_with_paths(&db).unwrap()[0].is_unresolved);
    assert_eq!(reindex(root, &mut db).drift_paths, 0);
    fs::write(root.join(".taoignore"), "").unwrap();
    reindex(root, &mut db);
    assert!(!LinksRepository::list_all_with_paths(&db).unwrap()[0].is_unresolved);
}

#[test]
fn all_file_ids_exist_before_edges_for_both_event_orders() {
    for reverse in [false, true] {
        let temp = tempdir().unwrap();
        let root = temp.path();
        let mut db = database();
        FullIndexService::default()
            .rebuild(root, &mut db, CasePolicy::Sensitive)
            .unwrap();
        fs::write(root.join("a.md"), "# A\n![asset](z.png)").unwrap();
        fs::write(root.join("z.png"), b"opaque fixture").unwrap();
        let mut paths = vec![PathBuf::from("a.md"), PathBuf::from("z.png")];
        if reverse {
            paths.reverse();
        }
        CoalescedBatchIndexService::default()
            .apply_coalesced(root, &mut db, &paths, 1, CasePolicy::Sensitive)
            .unwrap();
        let links = LinksRepository::list_all_with_paths(&db).unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].resolved_path.as_deref(), Some("z.png"));
        assert!(
            FilesRepository::get_by_normalized_path(&db, "z.png")
                .unwrap()
                .unwrap()
                .hash_blake3
                .is_empty()
        );
        assert_eq!(reindex(root, &mut db).drift_paths, 0);
    }
}

#[test]
fn same_batch_update_and_removed_target_produce_unresolved_edge() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("a.md"), "# A\n[[b]]").unwrap();
    fs::write(root.join("b.md"), "# B").unwrap();
    reindex(root, &mut db);
    fs::write(root.join("a.md"), "# Updated A\n[[b]]").unwrap();
    fs::remove_file(root.join("b.md")).unwrap();
    let outcome = reindex(root, &mut db);
    assert_eq!(outcome.mode, crate::IndexRefreshMode::Reconcile);
    assert_eq!(outcome.removed_files, 1);
    let links = LinksRepository::list_all_with_paths(&db).unwrap();
    assert!(links[0].is_unresolved);
    assert_eq!(links[0].unresolved_reason.as_deref(), Some("missing-note"));
}

#[test]
fn rename_does_not_rebuild_or_read_large_inventory_asset() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("old.md"), "# Old").unwrap();
    let asset = File::create(root.join("movie.bin")).unwrap();
    asset.set_len(1024 * 1024 * 1024 * 1024).unwrap();
    drop(asset);
    reindex(root, &mut db);
    fs::rename(root.join("old.md"), root.join("new.md")).unwrap();
    let outcome = reindex(root, &mut db);
    assert_eq!(outcome.mode, crate::IndexRefreshMode::Reconcile);
    assert_eq!(outcome.upserted_files, 1);
    assert_eq!(outcome.removed_files, 1);
    let asset = FilesRepository::get_by_normalized_path(&db, "movie.bin")
        .unwrap()
        .unwrap();
    assert_eq!(asset.size_bytes, 1024 * 1024 * 1024 * 1024);
    assert!(asset.hash_blake3.is_empty());
}

#[test]
fn uppercase_extensions_and_unicode_case_keys_converge() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("Äpfel.MD"), "---\nstatus: active\n---\n# Apples").unwrap();
    fs::write(root.join("View.BASE"), "views: []").unwrap();
    FullIndexService::default()
        .rebuild(root, &mut db, CasePolicy::Insensitive)
        .unwrap();
    fs::write(
        root.join("Äpfel.MD"),
        "---\nstatus: done\n---\n# Apples changed",
    )
    .unwrap();
    let scanner = ReconciliationScannerService::default();
    scanner
        .scan_and_repair_with_mode(
            root,
            &mut db,
            CasePolicy::Insensitive,
            128,
            ReconciliationScanMode::VerifyContentHashes,
        )
        .unwrap();
    let row = FilesRepository::get_by_normalized_path(&db, "Äpfel.MD")
        .unwrap()
        .unwrap();
    assert!(row.is_markdown);
    assert_eq!(row.match_key, "äpfel.md");
    assert_eq!(
        scanner
            .scan(root, &db, CasePolicy::Insensitive)
            .unwrap()
            .drift_paths,
        0
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM bases", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn same_metadata_content_capture_drives_all_projections() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    let note = root.join("a.md");
    fs::write(&note, "---\nstatus: open\n---\n# A\n- [ ] before\n").unwrap();
    reindex(root, &mut db);
    let modified = fs::metadata(&note).unwrap().modified().unwrap();
    let revised = "---\nstatus: done\n---\n# A\n- [x] after!\n";
    fs::write(&note, revised).unwrap();
    File::options()
        .write(true)
        .open(&note)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();
    assert_eq!(reindex(root, &mut db).upserted_files, 1);
    let file = FilesRepository::get_by_normalized_path(&db, "a.md")
        .unwrap()
        .unwrap();
    let doc = DocumentsRepository::get_by_file_id(&db, &file.file_id)
        .unwrap()
        .unwrap();
    assert_eq!(doc.raw_text, revised);
    assert_eq!(
        doc.source_hash,
        blake3::hash(revised.as_bytes()).to_hex().to_string()
    );
    assert_eq!(doc.source_hash, file.hash_blake3);
    assert_eq!(
        db.query_row("SELECT state FROM tasks", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "done"
    );
}

#[test]
fn unchanged_structures_and_text_do_not_require_original_files() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("a.md"), "# A\n[[b#Anchor]]").unwrap();
    fs::write(root.join("b.md"), "# Anchor").unwrap();
    reindex(root, &mut db);
    // Simulate a missed event. A targeted update can still derive links/search from
    // the unchanged committed revision without reopening its unavailable source.
    fs::remove_file(root.join("b.md")).unwrap();
    fs::write(root.join("a.md"), "# Changed A\n[[b#Anchor]]").unwrap();
    IncrementalIndexService::default()
        .apply_changes(
            root,
            &mut db,
            &[PathBuf::from("a.md")],
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(
        LinksRepository::list_all_with_paths(&db).unwrap()[0]
            .resolved_path
            .as_deref(),
        Some("b.md")
    );
}

#[test]
fn invalid_utf8_retains_last_good_revision_and_indexes_healthy_changes() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("good.md"), "# Before").unwrap();
    fs::write(root.join("bad.md"), "# Last good").unwrap();
    reindex(root, &mut db);
    fs::write(root.join("good.md"), "# After").unwrap();
    fs::write(root.join("bad.md"), [0xff, 0xfe]).unwrap();
    reindex(root, &mut db);
    let docs = DocumentsRepository::list_all(&db).unwrap();
    assert!(docs.iter().any(|doc| doc.raw_text == "# After"));
    assert!(docs.iter().any(|doc| doc.raw_text == "# Last good"));
    assert_eq!(
        DiagnosticsRepository::list_all(&db).unwrap()[0].path,
        "bad.md"
    );
    reindex(root, &mut db);
    assert!(
        !crate::SearchCorpusService
            .status(&db)
            .unwrap()
            .search_index_stale
    );
}

#[test]
fn failed_derived_publication_rolls_back_all_logical_batches() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    for name in ["a.md", "b.md"] {
        fs::write(root.join(name), "# Before").unwrap();
    }
    reindex(root, &mut db);
    let generation = IndexGenerationRepository::get(&db).unwrap();
    let before = DocumentsRepository::list_all(&db).unwrap();
    db.execute_batch("CREATE TRIGGER fail_publication BEFORE INSERT ON search_segments BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    for name in ["a.md", "b.md"] {
        fs::write(root.join(name), "# After").unwrap();
    }
    assert!(
        CoalescedBatchIndexService::default()
            .apply_coalesced(
                root,
                &mut db,
                &[PathBuf::from("a.md"), PathBuf::from("b.md")],
                1,
                CasePolicy::Sensitive
            )
            .is_err()
    );
    assert_eq!(DocumentsRepository::list_all(&db).unwrap(), before);
    assert_eq!(IndexGenerationRepository::get(&db).unwrap(), generation);
}

#[test]
fn corrupted_structure_repairs_from_canonical_text_without_original_read() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("a.md"), "# A\n[[b]]").unwrap();
    fs::write(root.join("b.md"), "# B").unwrap();
    reindex(root, &mut db);
    db.execute("UPDATE canonical_documents SET structure_json='{}' WHERE file_id=(SELECT file_id FROM files WHERE normalized_path='b.md')",[]).unwrap();
    fs::remove_file(root.join("b.md")).unwrap();
    fs::write(root.join("a.md"), "# Changed\n[[b]]").unwrap();
    IncrementalIndexService::default()
        .apply_changes(
            root,
            &mut db,
            &[PathBuf::from("a.md")],
            CasePolicy::Sensitive,
        )
        .unwrap();
    for record in DocumentsRepository::list_structures(&db).unwrap() {
        assert!(serde_json::from_str::<CanonicalStructure>(&record.structure_json).is_ok());
    }
}

#[test]
fn invalid_checkpoint_is_rejected_before_any_publication() {
    let temp = tempdir().unwrap();
    let mut db = database();
    let state = json!({"version":1,"vault_root":fs::canonicalize(temp.path()).unwrap(),"pending_paths":["a.md"],"next_offset":2,"max_batch_size":0,"case_policy":"sensitive","created_unix_ms":0,"updated_unix_ms":0});
    IndexStateRepository::upsert(
        &db,
        &IndexStateRecordInput {
            key: CHECKPOINT_STATE_KEY.to_string(),
            value_json: state.to_string(),
        },
    )
    .unwrap();
    assert!(matches!(
        CheckpointedIndexService::default().apply_checkpointed(
            temp.path(),
            &mut db,
            &[],
            128,
            None,
            CasePolicy::Sensitive
        ),
        Err(CheckpointedIndexError::InvalidCheckpoint { .. })
    ));
}

#[test]
fn aliases_converge_when_added_changed_and_removed() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("a.md"), "# A\n[[Complete, Alias]]").unwrap();
    fs::write(
        root.join("b.md"),
        "---\naliases: ['Complete, Alias']\n---\n# B",
    )
    .unwrap();
    reindex(root, &mut db);
    assert_eq!(
        LinksRepository::list_all_with_paths(&db).unwrap()[0]
            .resolved_path
            .as_deref(),
        Some("b.md")
    );
    fs::write(
        root.join("b.md"),
        "---\naliases: ['Different Alias']\n---\n# B",
    )
    .unwrap();
    reindex(root, &mut db);
    assert!(LinksRepository::list_all_with_paths(&db).unwrap()[0].is_unresolved);
    fs::write(
        root.join("c.md"),
        "---\naliases: ['Complete, Alias']\n---\n# C",
    )
    .unwrap();
    reindex(root, &mut db);
    assert_eq!(
        LinksRepository::list_all_with_paths(&db).unwrap()[0]
            .resolved_path
            .as_deref(),
        Some("c.md")
    );
}

#[test]
fn unchanged_malformed_frontmatter_keeps_diagnostic() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("a.md"), "---\nbad: [\n---\n# A").unwrap();
    reindex(root, &mut db);
    IncrementalIndexService::default()
        .apply_changes(
            root,
            &mut db,
            &[PathBuf::from("a.md")],
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(
        DiagnosticsRepository::list_all(&db).unwrap()[0].kind,
        "frontmatter_invalid"
    );
}

#[test]
fn stale_cleanup_retargets_to_remaining_candidate_atomically() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::create_dir(root.join("near")).unwrap();
    fs::create_dir(root.join("far")).unwrap();
    fs::write(root.join("near/source.md"), "# A\n[[target]]").unwrap();
    fs::write(root.join("near/target.md"), "# Near").unwrap();
    fs::write(root.join("far/target.md"), "# Far").unwrap();
    reindex(root, &mut db);
    fs::remove_file(root.join("near/target.md")).unwrap();
    StaleCleanupService
        .cleanup(root, &mut db, CasePolicy::Sensitive)
        .unwrap();
    assert_eq!(
        LinksRepository::list_all_with_paths(&db).unwrap()[0]
            .resolved_path
            .as_deref(),
        Some("far/target.md")
    );
    assert!(
        !crate::SearchCorpusService
            .status(&db)
            .unwrap()
            .search_index_stale
    );
}

#[test]
fn verified_plan_rejects_concurrent_canonical_publication() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("a.md"), "# Before").unwrap();
    reindex(root, &mut db);
    fs::write(root.join("a.md"), "# First revision").unwrap();
    let plan = ReconciliationScannerService::default()
        .plan(
            root,
            &db,
            CasePolicy::Sensitive,
            ReconciliationScanMode::VerifyContentHashes,
        )
        .unwrap();
    fs::write(root.join("a.md"), "# Newer revision").unwrap();
    reindex(root, &mut db);
    let error = CoalescedBatchIndexService::default()
        .apply_plan(
            root,
            &mut db,
            plan.changes,
            Some(plan.generation),
            128,
            CasePolicy::Sensitive,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        FullIndexError::ConcurrentPublication { .. }
    ));
    assert_eq!(
        DocumentsRepository::list_all(&db).unwrap()[0].raw_text,
        "# Newer revision"
    );
}

#[test]
fn unchanged_deferred_pdf_capture_does_not_force_source_reconciliation() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(
        root.join("waiting.pdf"),
        b"%PDF-1.4 waiting for bounded capture space",
    )
    .unwrap();
    reindex(root, &mut db);
    db.execute("UPDATE content_documents SET availability='deferred',desired_revision='',coverage='pending'", []).unwrap();
    db.execute("UPDATE files SET hash_blake3=''", []).unwrap();
    for mode in [
        ReconciliationScanMode::MetadataOnly,
        ReconciliationScanMode::VerifyContentHashes,
    ] {
        let plan = ReconciliationScannerService::default()
            .plan(root, &db, CasePolicy::Sensitive, mode)
            .unwrap();
        assert_eq!(plan.drift_paths(), 0);
    }
    fs::write(
        root.join("waiting.pdf"),
        b"%PDF-1.4 replacement has a different source size",
    )
    .unwrap();
    let plan = ReconciliationScannerService::default()
        .plan(
            root,
            &db,
            CasePolicy::Sensitive,
            ReconciliationScanMode::VerifyContentHashes,
        )
        .unwrap();
    assert_eq!(plan.drift_paths(), 1);
}

fn semantic_projection(connection: &Connection) -> Vec<Vec<String>> {
    let queries = [
        "SELECT json_array(normalized_path,match_key,size_bytes,hash_blake3,is_markdown) FROM files",
        "SELECT json_array(f.normalized_path,d.source_hash,d.parser_version,d.raw_text,d.body_text,d.title,d.structure_json) FROM canonical_documents d JOIN files f USING(file_id)",
        "SELECT json_array(f.normalized_path,p.key,p.value_type,p.value_json) FROM properties p JOIN files f USING(file_id)",
        "SELECT json_array(f.normalized_path,t.line_number,t.state,t.text) FROM tasks t JOIN files f USING(file_id)",
        "SELECT json_array(s.normalized_path,l.raw_target,t.normalized_path,l.heading_slug,l.block_id,l.is_unresolved,l.unresolved_reason,l.source_field,e.raw_expression,e.source_start,e.source_end,e.line,e.end_line,e.syntax,e.fragment_json,e.fragment_status,e.resolution_rule,e.candidates_json) FROM links l JOIN files s ON s.file_id=l.source_file_id LEFT JOIN files t ON t.file_id=l.resolved_file_id LEFT JOIN link_evidence e USING(link_id)",
        "SELECT json_array(f.normalized_path,b.config_json) FROM bases b JOIN files f USING(file_id)",
        "SELECT json_array(f.normalized_path,d.format,d.file_group,d.desired_revision,d.served_revision,d.extractor_identity,d.coverage,d.availability,d.metadata_json,d.diagnostics_json) FROM content_documents d JOIN files f USING(file_id)",
        "SELECT json_array(f.normalized_path,s.ordinal,s.locator_kind,s.source_start,s.source_end,s.text,s.method,s.coverage) FROM content_segments s JOIN files f USING(file_id)",
        "SELECT json_array(surface,normalized_path,extension,field,label,weight,path_text,title_text,alias_text,body_text,property_text,task_text,link_text,base_text) FROM search_segments",
        "SELECT json_array(normalized_path,extension,surface,alias_norm,alias_compact,source,weight) FROM search_aliases",
        "SELECT json_array(path,kind,message) FROM file_diagnostics",
    ];
    queries
        .into_iter()
        .map(|sql| {
            let mut rows = connection
                .prepare(sql)
                .unwrap()
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            rows.sort();
            rows
        })
        .collect()
}

fn assert_fresh_full_equivalence(root: &Path, incremental: &Connection, phase: &str) {
    let mut fresh = database();
    FullIndexService::default()
        .rebuild(root, &mut fresh, CasePolicy::Sensitive)
        .unwrap();
    let expected = semantic_projection(&fresh);
    let actual = semantic_projection(incremental);
    for (table, (actual, expected)) in actual.iter().zip(expected.iter()).enumerate() {
        if actual != expected {
            let added = actual
                .iter()
                .filter(|row| !expected.contains(row))
                .take(3)
                .collect::<Vec<_>>();
            let missing = expected
                .iter()
                .filter(|row| !actual.contains(row))
                .take(3)
                .collect::<Vec<_>>();
            panic!(
                "semantic mismatch phase={phase} projection={table}; incremental-only={added:?}; fresh-only={missing:?}"
            );
        }
    }
    assert!(
        !crate::SearchCorpusService
            .status(incremental)
            .unwrap()
            .search_index_stale
    );
    tao_sdk_storage::SearchSegmentRepository::check_integrity(incremental).unwrap();
}

#[test]
fn fresh_full_oracle_matches_incremental_across_batch_boundaries_and_lifecycle() {
    for count in [127, 128, 129] {
        let temp = tempdir().unwrap();
        let root = temp.path();
        let mut db = database();
        fs::create_dir(root.join("notes")).unwrap();
        for index in 0..count {
            fs::write(root.join(format!("notes/entry{index}.md")),format!("---\nstatus: open\naliases: ['Alias {index}']\nnumber: 9007199254740993\n---\n# Section\n- [ ] initial {index}\n[[entry0#Section]]\n")).unwrap();
        }
        reindex(root, &mut db);
        assert_fresh_full_equivalence(root, &db, "initial");
        let mut paths = Vec::new();
        for index in 0..count {
            let path = PathBuf::from(format!("notes/entry{index}.md"));
            fs::write(root.join(&path),format!("---\nstatus: done\naliases: ['Revised Alias {index}']\n---\n# Updated\n- [x] revised {index}\n[[entry0#Updated]]\n")).unwrap();
            paths.push(path);
        }
        paths.reverse();
        paths.push(paths[0].clone());
        let result = CoalescedBatchIndexService::default()
            .apply_coalesced(root, &mut db, &paths, 128, CasePolicy::Sensitive)
            .unwrap();
        assert_eq!(result.upserted_files, count as u64);
        assert_eq!(result.batches_applied, (count as u64).div_ceil(128));
        assert_fresh_full_equivalence(root, &db, &format!("{count} updates"));
        fs::rename(root.join("notes/entry0.md"), root.join("notes/renamed.md")).unwrap();
        fs::remove_file(root.join("notes/entry2.md")).unwrap();
        fs::write(root.join(".taoignore"), "notes/entry1.md\n").unwrap();
        fs::write(root.join("notes/entry3.md"),"---\nmalformed: [\n---\n# Body retained\n[[renamed#Updated]]\n![image](../image.png)\n").unwrap();
        fs::write(root.join("image.png"), b"inventory-only").unwrap();
        fs::write(
            root.join("readme.TXT"),
            "searchable text source\nsecond line",
        )
        .unwrap();
        fs::write(root.join("all.BASE"), "views: []\n").unwrap();
        reindex(root, &mut db);
        assert_fresh_full_equivalence(root, &db, "mixed lifecycle");
        fs::write(root.join(".taoignore"), "").unwrap();
        fs::write(
            root.join("notes/entry2.md"),
            "# Restored\n[[Revised Alias 0]]",
        )
        .unwrap();
        fs::write(
            root.join("notes/entry3.md"),
            "---\nstatus: restored\n---\n# Repaired\n",
        )
        .unwrap();
        fs::write(root.join("readme.TXT"), "changed text source").unwrap();
        reindex(root, &mut db);
        assert_fresh_full_equivalence(root, &db, "restored lifecycle");
        assert_eq!(reindex(root, &mut db).drift_paths, 0);
    }
}

#[test]
fn cancelled_refresh_preserves_previous_publication_and_scope_restores() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Instant;
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("a.md"), "# Before").unwrap();
    reindex(root, &mut db);
    let before = semantic_projection(&db);
    let generation = IndexGenerationRepository::get(&db).unwrap();
    fs::write(root.join("a.md"), "# After").unwrap();
    {
        let _scope =
            crate::IndexCancellationScope::enter(Instant::now(), Arc::new(AtomicBool::new(false)));
        let result = crate::IndexRefreshService.refresh(
            root,
            &mut db,
            CasePolicy::Sensitive,
            crate::IndexRefreshOptions::default(),
        );
        assert!(result.is_err());
        assert_eq!(semantic_projection(&db), before);
        assert_eq!(IndexGenerationRepository::get(&db).unwrap(), generation);
    }
    reindex(root, &mut db);
    assert_eq!(
        DocumentsRepository::list_all(&db).unwrap()[0].raw_text,
        "# After"
    );
}

#[test]
fn aggregate_preparation_refusal_preserves_generation_and_recovers_for_markdown_and_text() {
    for extension in ["md", "txt"] {
        let temp = tempdir().unwrap();
        let root = temp.path();
        let mut db = database();
        fs::write(root.join("existing.md"), "# Existing\nprevious revision").unwrap();
        reindex(root, &mut db);
        let generation = IndexGenerationRepository::get(&db).unwrap();
        let before = semantic_projection(&db);
        let payload = format!("# Revision\n{}\n", "captured content ".repeat(512));
        let paths = (0..8)
            .map(|i| {
                let path = PathBuf::from(format!("new-{i}.{extension}"));
                fs::write(root.join(&path), &payload).unwrap();
                path
            })
            .collect::<Vec<_>>();
        let changes = apply::changes_for_paths(root, &paths, CasePolicy::Sensitive).unwrap();
        let error = apply::apply_changes_with_budget(
            root,
            &mut db,
            changes,
            CasePolicy::Sensitive,
            MarkdownParser,
            apply::PublicationOptions::default(),
            64 * 1024,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            FullIndexError::PreparationBudgetExceeded { .. }
        ));
        assert!(error.to_string().contains("existing index was preserved"));
        assert_eq!(IndexGenerationRepository::get(&db).unwrap(), generation);
        assert_eq!(semantic_projection(&db), before);
        assert_eq!(fs::read_to_string(root.join(&paths[0])).unwrap(), payload);
        // An ordinary retry with the supported budget publishes the entire work set.
        reindex(root, &mut db);
        assert_eq!(FilesRepository::list_all(&db).unwrap().len(), 9);
        assert!(
            !crate::SearchCorpusService
                .status(&db)
                .unwrap()
                .search_index_stale
        );
    }
}

#[test]
fn retained_reconcile_captures_are_admitted_before_any_publication() {
    let temp = tempdir().unwrap();
    let root = temp.path();
    let mut db = database();
    fs::write(root.join("a.md"), "# A\nold").unwrap();
    reindex(root, &mut db);
    fs::write(root.join("a.md"), format!("# A\n{}", "new ".repeat(4096))).unwrap();
    let plan = ReconciliationScannerService::default()
        .plan(
            root,
            &db,
            CasePolicy::Sensitive,
            ReconciliationScanMode::VerifyContentHashes,
        )
        .unwrap();
    assert!(matches!(
        &plan.changes[0],
        IndexChange::Upsert {
            captured: Some(_),
            ..
        }
    ));
    let generation = IndexGenerationRepository::get(&db).unwrap();
    let before = semantic_projection(&db);
    let error = apply::apply_changes_with_budget(
        root,
        &mut db,
        plan.changes,
        CasePolicy::Sensitive,
        MarkdownParser,
        apply::PublicationOptions::default(),
        1024,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        FullIndexError::PreparationBudgetExceeded { .. }
    ));
    assert_eq!(IndexGenerationRepository::get(&db).unwrap(), generation);
    assert_eq!(semantic_projection(&db), before);
}
