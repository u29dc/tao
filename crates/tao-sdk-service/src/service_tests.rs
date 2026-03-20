use std::fs;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::symlink;

use rusqlite::Connection;
use tao_sdk_bases::{
    BaseAggregateOp, BaseAggregateSpec, BaseColumnConfig, BaseDiagnosticSeverity, BaseDocument,
    BaseFilterClause, BaseFilterOp, BaseNullOrder, BaseRelationSpec, BaseRollupOp, BaseRollupSpec,
    BaseSortClause, BaseSortDirection, BaseViewDefinition, BaseViewKind, TableQueryPlan,
};
use tao_sdk_properties::TypedPropertyValue;
use tao_sdk_storage::{
    BaseRecordInput, BasesRepository, FileRecordInput, FilesRepository, LinkRecordInput,
    LinksRepository, PropertiesRepository, PropertyRecordInput, known_migrations, run_migrations,
};
use tao_sdk_vault::{CasePolicy, VaultScanService};
use tempfile::tempdir;

use super::{
    BacklinkGraphService, BaseColumnConfigPersistError, BaseColumnConfigPersistenceService,
    BaseTableCachedQueryService, BaseTableExecutorError, BaseTableExecutorService,
    BaseValidationError, BaseValidationService, GraphComponentMode, GraphScopedInboundRequest,
    HealthSnapshotService, MarkdownIngestPipeline, NoteCrudError, NoteCrudService,
    PropertyQueryRequest, PropertyQueryService, PropertyQuerySort, PropertyUpdateService,
    ReconcileService, SdkTransactionCoordinator, ServiceTraceContext, StorageWriteService,
    WatcherStatus,
};

fn file_record(
    file_id: &str,
    normalized_path: &str,
    match_key: &str,
    absolute_path: &str,
) -> FileRecordInput {
    FileRecordInput {
        file_id: file_id.to_string(),
        normalized_path: normalized_path.to_string(),
        match_key: match_key.to_string(),
        absolute_path: absolute_path.to_string(),
        size_bytes: 10,
        modified_unix_ms: 1_700_000_000_000,
        hash_blake3: format!("hash-{file_id}"),
        is_markdown: true,
    }
}

#[test]
fn ingest_vault_parses_markdown_and_skips_non_markdown() {
    let temp = tempdir().expect("tempdir");
    fs::write(temp.path().join("daily.md"), "# Daily\ncontent").expect("write markdown");
    fs::write(temp.path().join("image.png"), "png").expect("write non-markdown");

    let pipeline = MarkdownIngestPipeline::from_root(temp.path(), CasePolicy::Sensitive)
        .expect("create pipeline");
    let notes = pipeline.ingest_vault().expect("ingest vault");

    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].normalized_path, "daily.md");
    assert_eq!(notes[0].parsed.title, "Daily");
}

#[test]
fn ingest_entries_uses_pre_scanned_manifest() {
    let temp = tempdir().expect("tempdir");
    fs::write(temp.path().join("a.md"), "# A").expect("write markdown a");
    fs::write(temp.path().join("b.md"), "# B").expect("write markdown b");

    let pipeline = MarkdownIngestPipeline::from_root(temp.path(), CasePolicy::Sensitive)
        .expect("create pipeline");
    let manifest = VaultScanService::from_root(temp.path(), CasePolicy::Sensitive)
        .expect("create scanner")
        .scan()
        .expect("scan manifest");

    let notes = pipeline
        .ingest_entries(&manifest.entries)
        .expect("ingest entries");
    assert_eq!(notes.len(), 2);
    assert_eq!(notes[0].parsed.title, "A");
    assert_eq!(notes[1].parsed.title, "B");
}

#[test]
fn storage_write_service_uses_typed_transaction_wrapper() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = StorageWriteService;
    let record = file_record(
        "f1",
        "notes/typed.md",
        "notes/typed.md",
        "/vault/notes/typed.md",
    );

    service
        .create_file_record(&mut connection, &record)
        .expect("create file record through transaction wrapper");

    let persisted = FilesRepository::get_by_id(&connection, "f1")
        .expect("get persisted record")
        .expect("record should exist");
    assert_eq!(persisted.normalized_path, "notes/typed.md");
}

#[test]
fn sdk_transaction_coordinator_replaces_file_metadata_atomically() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let coordinator = SdkTransactionCoordinator;
    let original = file_record(
        "f1",
        "notes/original.md",
        "notes/original.md",
        "/vault/notes/original.md",
    );
    let replacement = file_record(
        "f1",
        "notes/replacement.md",
        "notes/replacement.md",
        "/vault/notes/replacement.md",
    );

    coordinator
        .insert_file_metadata(&mut connection, &original)
        .expect("insert original");
    coordinator
        .replace_file_metadata(&mut connection, "f1", &replacement)
        .expect("replace metadata");

    let persisted = FilesRepository::get_by_id(&connection, "f1")
        .expect("get replaced")
        .expect("row exists");
    assert_eq!(persisted.normalized_path, "notes/replacement.md");
}

#[test]
fn sdk_transaction_coordinator_rolls_back_failed_replacement() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let coordinator = SdkTransactionCoordinator;
    let first = file_record("f1", "notes/a.md", "notes/a.md", "/vault/notes/a.md");
    let second = file_record("f2", "notes/b.md", "notes/b.md", "/vault/notes/b.md");
    coordinator
        .insert_file_metadata(&mut connection, &first)
        .expect("insert first");
    coordinator
        .insert_file_metadata(&mut connection, &second)
        .expect("insert second");

    let conflicting = file_record("f1", "notes/b.md", "notes/b.md", "/vault/notes/b.md");
    let result = coordinator.replace_file_metadata(&mut connection, "f1", &conflicting);
    assert!(result.is_err());

    let first_after = FilesRepository::get_by_id(&connection, "f1")
        .expect("get first after failed replace")
        .expect("first should remain after rollback");
    assert_eq!(first_after.normalized_path, "notes/a.md");
}

#[test]
fn note_crud_service_create_update_delete_flow() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(&vault_root).expect("create vault root");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = NoteCrudService::default();
    let relative = Path::new("notes/today.md");

    let created = service
        .create_note(&vault_root, &mut connection, "f1", relative, "# first")
        .expect("create note");
    assert_eq!(created.normalized_path, "notes/today.md");
    assert_eq!(
        fs::read_to_string(vault_root.join(relative)).expect("read created note"),
        "# first"
    );

    let before_update = FilesRepository::get_by_id(&connection, "f1")
        .expect("get before update")
        .expect("row before update");

    let updated = service
        .update_note(&vault_root, &mut connection, "f1", relative, "# second")
        .expect("update note");
    assert_eq!(updated.normalized_path, "notes/today.md");
    assert_eq!(
        fs::read_to_string(vault_root.join(relative)).expect("read updated note"),
        "# second"
    );

    let after_update = FilesRepository::get_by_id(&connection, "f1")
        .expect("get after update")
        .expect("row after update");
    assert_ne!(before_update.hash_blake3, after_update.hash_blake3);

    let deleted = service
        .delete_note(&vault_root, &mut connection, "f1")
        .expect("delete note");
    assert!(deleted);
    assert!(!vault_root.join(relative).exists());
    assert!(
        FilesRepository::get_by_id(&connection, "f1")
            .expect("get deleted")
            .is_none()
    );
}

#[test]
fn note_crud_service_trace_context_wrapper_executes_operation() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(&vault_root).expect("create vault root");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = NoteCrudService::default();
    let trace_context = ServiceTraceContext::with_correlation("note_create", "cid-note-1");
    let created = service
        .create_note_with_trace_context(
            &trace_context,
            &vault_root,
            &mut connection,
            "f1",
            Path::new("notes/traced.md"),
            "# traced",
        )
        .expect("create traced note");

    assert_eq!(created.normalized_path, "notes/traced.md");
    assert_eq!(trace_context.correlation_id(), "cid-note-1");
}

#[test]
fn note_crud_service_rejects_escape_paths() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(&vault_root).expect("create vault root");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = NoteCrudService::default();
    let error = service
        .create_note(
            &vault_root,
            &mut connection,
            "f1",
            Path::new("../escape.md"),
            "nope",
        )
        .expect_err("path escaping should fail");

    assert!(matches!(error, NoteCrudError::InvalidPath { .. }));
}

#[cfg(unix)]
#[test]
fn note_crud_service_rejects_symlink_parent_escaping_vault_before_write() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    let outside_root = temp.path().join("outside");
    fs::create_dir_all(&vault_root).expect("create vault root");
    fs::create_dir_all(&outside_root).expect("create outside root");
    symlink(&outside_root, vault_root.join("notes")).expect("create notes symlink");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = NoteCrudService::default();
    let error = service
        .create_note(
            &vault_root,
            &mut connection,
            "f1",
            Path::new("notes/escape.md"),
            "# Escape",
        )
        .expect_err("symlink escape should fail");

    assert!(matches!(error, NoteCrudError::PathOutsideVault { .. }));
    assert!(!outside_root.join("escape.md").exists());
    assert!(
        FilesRepository::get_by_id(&connection, "f1")
            .expect("get metadata after failed create")
            .is_none()
    );
}

#[test]
fn note_crud_service_rename_keeps_link_resolution_consistent() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(&vault_root).expect("create vault root");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = NoteCrudService::default();
    service
        .create_note(
            &vault_root,
            &mut connection,
            "target",
            Path::new("notes/target.md"),
            "# target",
        )
        .expect("create target");
    service
        .create_note(
            &vault_root,
            &mut connection,
            "source",
            Path::new("notes/source.md"),
            "# source",
        )
        .expect("create source");

    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l1".to_string(),
            source_file_id: "source".to_string(),
            raw_target: "target".to_string(),
            resolved_file_id: Some("target".to_string()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        },
    )
    .expect("insert link");

    let renamed = service
        .rename_note(
            &vault_root,
            &mut connection,
            "target",
            Path::new("archive/renamed-target.md"),
        )
        .expect("rename note");
    assert_eq!(renamed.normalized_path, "archive/renamed-target.md");

    assert!(!vault_root.join("notes/target.md").exists());
    assert!(vault_root.join("archive/renamed-target.md").exists());

    let backlinks =
        LinksRepository::list_backlinks_with_paths(&connection, "target").expect("list backlinks");
    assert_eq!(backlinks.len(), 1);
    assert_eq!(
        backlinks[0].resolved_path.as_deref(),
        Some("archive/renamed-target.md")
    );
}

#[test]
fn backlink_graph_service_returns_stable_outgoing_and_backlink_order() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "source-a",
            "notes/source-a.md",
            "notes/source-a.md",
            "/vault/notes/source-a.md",
        ),
    )
    .expect("insert source a");
    FilesRepository::insert(
        &connection,
        &file_record(
            "source-b",
            "notes/source-b.md",
            "notes/source-b.md",
            "/vault/notes/source-b.md",
        ),
    )
    .expect("insert source b");
    FilesRepository::insert(
        &connection,
        &file_record(
            "target",
            "notes/target.md",
            "notes/target.md",
            "/vault/notes/target.md",
        ),
    )
    .expect("insert target");

    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l2".to_string(),
            source_file_id: "source-a".to_string(),
            raw_target: "target".to_string(),
            resolved_file_id: Some("target".to_string()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        },
    )
    .expect("insert outgoing l2");
    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l1".to_string(),
            source_file_id: "source-a".to_string(),
            raw_target: "target".to_string(),
            resolved_file_id: Some("target".to_string()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        },
    )
    .expect("insert outgoing l1");
    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l3".to_string(),
            source_file_id: "source-b".to_string(),
            raw_target: "target".to_string(),
            resolved_file_id: Some("target".to_string()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        },
    )
    .expect("insert outgoing l3");

    let service = BacklinkGraphService;
    let outgoing = service
        .outgoing_for_path(&connection, "notes/source-a.md")
        .expect("query outgoing");
    assert_eq!(outgoing.len(), 2);
    assert_eq!(outgoing[0].link_id, "l1");
    assert_eq!(outgoing[1].link_id, "l2");

    let backlinks = service
        .backlinks_for_path(&connection, "notes/target.md")
        .expect("query backlinks");
    assert_eq!(backlinks.len(), 3);
    assert_eq!(backlinks[0].source_path, "notes/source-a.md");
    assert_eq!(backlinks[1].source_path, "notes/source-a.md");
    assert_eq!(backlinks[2].source_path, "notes/source-b.md");
}

#[test]
fn backlink_graph_service_lists_unresolved_links() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "source-a",
            "notes/source-a.md",
            "notes/source-a.md",
            "/vault/notes/source-a.md",
        ),
    )
    .expect("insert source a");

    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l-unresolved".to_string(),
            source_file_id: "source-a".to_string(),
            raw_target: "missing".to_string(),
            resolved_file_id: None,
            heading_slug: None,
            block_id: None,
            is_unresolved: true,
            unresolved_reason: Some("missing-note".to_string()),
            source_field: "body".to_string(),
        },
    )
    .expect("insert unresolved link");
    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l-resolved".to_string(),
            source_file_id: "source-a".to_string(),
            raw_target: "missing".to_string(),
            resolved_file_id: None,
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        },
    )
    .expect("insert resolved marker link");

    let unresolved = BacklinkGraphService
        .unresolved_links(&connection)
        .expect("query unresolved");
    let (unresolved_total, unresolved_page) = BacklinkGraphService
        .unresolved_links_page(&connection, 1, 0)
        .expect("query unresolved page");
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved_total, 1);
    assert_eq!(unresolved_page.len(), 1);
    assert_eq!(unresolved_page[0].link_id, "l-unresolved");
    assert_eq!(unresolved[0].link_id, "l-unresolved");
    assert!(unresolved[0].is_unresolved);
    assert_eq!(
        unresolved[0].unresolved_reason.as_deref(),
        Some("missing-note")
    );
    assert_eq!(unresolved[0].source_field, "body");
}

#[test]
fn backlink_graph_service_scoped_inbound_audits_non_markdown_targets() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "source",
            "notes/source.md",
            "notes/source.md",
            "/vault/notes/source.md",
        ),
    )
    .expect("insert source");
    FilesRepository::insert(
        &connection,
        &FileRecordInput {
            file_id: "linked".to_string(),
            normalized_path: "notes/assets/linked.pdf".to_string(),
            match_key: "notes/assets/linked.pdf".to_string(),
            absolute_path: "/vault/notes/assets/linked.pdf".to_string(),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: "hash-linked".to_string(),
            is_markdown: false,
        },
    )
    .expect("insert linked asset");
    FilesRepository::insert(
        &connection,
        &FileRecordInput {
            file_id: "orphan".to_string(),
            normalized_path: "notes/assets/orphan.pdf".to_string(),
            match_key: "notes/assets/orphan.pdf".to_string(),
            absolute_path: "/vault/notes/assets/orphan.pdf".to_string(),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: "hash-orphan".to_string(),
            is_markdown: false,
        },
    )
    .expect("insert orphan asset");

    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l-attachment".to_string(),
            source_file_id: "source".to_string(),
            raw_target: "assets/linked.pdf".to_string(),
            resolved_file_id: Some("linked".to_string()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body:markdown".to_string(),
        },
    )
    .expect("insert attachment edge");

    let (summary, rows) = BacklinkGraphService
        .scoped_inbound_page(
            &connection,
            &GraphScopedInboundRequest {
                scope_prefix: "notes".to_string(),
                include_markdown: false,
                include_non_markdown: true,
                exclude_prefixes: Vec::new(),
                limit: 100,
                offset: 0,
            },
        )
        .expect("scoped inbound");
    assert_eq!(summary.total_files, 2);
    assert_eq!(summary.linked_files, 1);
    assert_eq!(summary.unlinked_files, 1);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].path, "notes/assets/linked.pdf");
    assert_eq!(rows[0].inbound_resolved, 1);
    assert_eq!(rows[1].path, "notes/assets/orphan.pdf");
    assert_eq!(rows[1].inbound_resolved, 0);
}

#[test]
fn backlink_graph_service_floating_returns_strict_disconnected_files() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "note-source",
            "notes/source.md",
            "notes/source.md",
            "/vault/notes/source.md",
        ),
    )
    .expect("insert source note");
    FilesRepository::insert(
        &connection,
        &file_record(
            "note-linked",
            "notes/linked.md",
            "notes/linked.md",
            "/vault/notes/linked.md",
        ),
    )
    .expect("insert linked note");
    FilesRepository::insert(
        &connection,
        &file_record(
            "note-floating",
            "notes/floating.md",
            "notes/floating.md",
            "/vault/notes/floating.md",
        ),
    )
    .expect("insert floating note");
    FilesRepository::insert(
        &connection,
        &FileRecordInput {
            file_id: "asset-floating".to_string(),
            normalized_path: "notes/assets/floating.pdf".to_string(),
            match_key: "notes/assets/floating.pdf".to_string(),
            absolute_path: "/vault/notes/assets/floating.pdf".to_string(),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: "hash-asset-floating".to_string(),
            is_markdown: false,
        },
    )
    .expect("insert floating asset");
    FilesRepository::insert(
        &connection,
        &FileRecordInput {
            file_id: "noise".to_string(),
            normalized_path: ".DS_Store".to_string(),
            match_key: ".ds_store".to_string(),
            absolute_path: "/vault/.DS_Store".to_string(),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: "hash-noise".to_string(),
            is_markdown: false,
        },
    )
    .expect("insert noise file");

    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l-note".to_string(),
            source_file_id: "note-source".to_string(),
            raw_target: "linked".to_string(),
            resolved_file_id: Some("note-linked".to_string()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        },
    )
    .expect("insert note edge");

    let (summary, rows) = BacklinkGraphService
        .floating_page(&connection, 100, 0)
        .expect("floating page");
    assert_eq!(summary.total_files, 2);
    assert_eq!(summary.markdown_files, 1);
    assert_eq!(summary.non_markdown_files, 1);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].path, "notes/assets/floating.pdf");
    assert_eq!(rows[0].incoming_resolved, 0);
    assert_eq!(rows[0].outgoing_resolved, 0);
    assert_eq!(rows[1].path, "notes/floating.md");
    assert_eq!(rows[1].incoming_resolved, 0);
    assert_eq!(rows[1].outgoing_resolved, 0);
}

#[test]
fn backlink_graph_components_supports_weak_and_strong_modes() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record("a", "notes/a.md", "notes/a.md", "/vault/notes/a.md"),
    )
    .expect("insert a");
    FilesRepository::insert(
        &connection,
        &file_record("b", "notes/b.md", "notes/b.md", "/vault/notes/b.md"),
    )
    .expect("insert b");
    FilesRepository::insert(
        &connection,
        &file_record("c", "notes/c.md", "notes/c.md", "/vault/notes/c.md"),
    )
    .expect("insert c");

    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l-a-b".to_string(),
            source_file_id: "a".to_string(),
            raw_target: "b".to_string(),
            resolved_file_id: Some("b".to_string()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        },
    )
    .expect("insert a->b");
    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l-b-a".to_string(),
            source_file_id: "b".to_string(),
            raw_target: "a".to_string(),
            resolved_file_id: Some("a".to_string()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        },
    )
    .expect("insert b->a");
    LinksRepository::insert(
        &connection,
        &LinkRecordInput {
            link_id: "l-b-c".to_string(),
            source_file_id: "b".to_string(),
            raw_target: "c".to_string(),
            resolved_file_id: Some("c".to_string()),
            heading_slug: None,
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        },
    )
    .expect("insert b->c");

    let (weak_total, weak_rows) = BacklinkGraphService
        .components_page(&connection, GraphComponentMode::Weak, 50, 0, true, 64)
        .expect("weak components");
    assert_eq!(weak_total, 1);
    assert_eq!(weak_rows.len(), 1);
    assert_eq!(weak_rows[0].size, 3);

    let (strong_total, strong_rows) = BacklinkGraphService
        .components_page(&connection, GraphComponentMode::Strong, 50, 0, true, 64)
        .expect("strong components");
    assert_eq!(strong_total, 2);
    let sizes = strong_rows.iter().map(|row| row.size).collect::<Vec<_>>();
    assert_eq!(sizes, vec![2, 1]);
}

#[test]
fn note_crud_service_rolls_back_created_file_when_metadata_insert_fails() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let coordinator = SdkTransactionCoordinator;
    coordinator
        .insert_file_metadata(
            &mut connection,
            &file_record(
                "conflict",
                "notes/conflict.md",
                "notes/conflict.md",
                "/ghost/conflict.md",
            ),
        )
        .expect("seed conflicting metadata");

    let service = NoteCrudService::default();
    let error = service
        .create_note(
            &vault_root,
            &mut connection,
            "new-file",
            Path::new("notes/conflict.md"),
            "# New",
        )
        .expect_err("create should fail on metadata conflict");

    assert!(matches!(error, NoteCrudError::Coordinator { .. }));
    assert!(!vault_root.join("notes/conflict.md").exists());
    assert!(
        FilesRepository::get_by_id(&connection, "new-file")
            .expect("get metadata for failed create")
            .is_none()
    );
}

#[test]
fn note_crud_service_rolls_back_rename_when_metadata_update_fails() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = NoteCrudService::default();
    service
        .create_note(
            &vault_root,
            &mut connection,
            "note-a",
            Path::new("notes/a.md"),
            "# A",
        )
        .expect("create note a");

    let coordinator = SdkTransactionCoordinator;
    coordinator
        .insert_file_metadata(
            &mut connection,
            &file_record(
                "conflict",
                "notes/conflict.md",
                "notes/conflict.md",
                "/ghost/conflict.md",
            ),
        )
        .expect("seed conflicting metadata");

    let error = service
        .rename_note(
            &vault_root,
            &mut connection,
            "note-a",
            Path::new("notes/conflict.md"),
        )
        .expect_err("rename should fail on metadata conflict");

    assert!(matches!(error, NoteCrudError::Coordinator { .. }));
    assert!(vault_root.join("notes/a.md").exists());
    assert!(!vault_root.join("notes/conflict.md").exists());

    let file_record = FilesRepository::get_by_id(&connection, "note-a")
        .expect("get file row")
        .expect("file row exists");
    assert_eq!(file_record.normalized_path, "notes/a.md");
}

#[test]
fn reconcile_service_is_idempotent_across_repeated_runs() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = ReconcileService;
    let first = service
        .reconcile_vault(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("first reconcile");
    assert_eq!(first.scanned_files, 2);
    assert_eq!(first.inserted_files, 2);
    assert_eq!(first.updated_files, 0);
    assert_eq!(first.removed_files, 0);
    assert_eq!(first.unchanged_files, 0);

    let second = service
        .reconcile_vault(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("second reconcile");
    assert_eq!(second.scanned_files, 2);
    assert_eq!(second.inserted_files, 0);
    assert_eq!(second.updated_files, 0);
    assert_eq!(second.removed_files, 0);
    assert_eq!(second.unchanged_files, 2);
}

#[test]
fn reconcile_service_updates_changed_files_and_removes_stale_rows() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = ReconcileService;
    service
        .reconcile_vault(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed reconcile");

    fs::remove_file(temp.path().join("notes/a.md")).expect("remove a");
    fs::write(temp.path().join("notes/b.md"), "# B changed").expect("update b");
    fs::write(temp.path().join("notes/c.md"), "# C").expect("write c");

    let result = service
        .reconcile_vault(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("reconcile drift");
    assert_eq!(result.scanned_files, 2);
    assert_eq!(result.inserted_files, 1);
    assert_eq!(result.updated_files, 1);
    assert_eq!(result.removed_files, 1);
    assert_eq!(result.unchanged_files, 0);

    let indexed = FilesRepository::list_all(&connection).expect("list indexed files");
    let indexed_paths: Vec<String> = indexed
        .iter()
        .map(|record| record.normalized_path.clone())
        .collect();
    assert_eq!(
        indexed_paths,
        vec!["notes/b.md".to_string(), "notes/c.md".to_string()]
    );
}

#[test]
fn reconcile_service_trace_context_wrapper_executes_operation() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = ReconcileService;
    let trace_context = ServiceTraceContext::with_correlation("reconcile", "cid-reconcile-1");
    let result = service
        .reconcile_vault_with_trace_context(
            &trace_context,
            temp.path(),
            &mut connection,
            CasePolicy::Sensitive,
        )
        .expect("traced reconcile");

    assert_eq!(result.scanned_files, 1);
    assert_eq!(trace_context.correlation_id(), "cid-reconcile-1");
}

#[test]
fn health_snapshot_reports_vault_db_and_watcher_status() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
    fs::write(vault_root.join("notes/a.md"), "# A").expect("write a");
    fs::write(vault_root.join("notes/b.md"), "# B").expect("write b");
    fs::write(vault_root.join("notes/c.png"), "png").expect("write c");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FilesRepository::insert(
        &connection,
        &file_record("f1", "notes/a.md", "notes/a.md", "/vault/notes/a.md"),
    )
    .expect("insert a");
    FilesRepository::insert(
        &connection,
        &file_record("f2", "notes/b.md", "notes/b.md", "/vault/notes/b.md"),
    )
    .expect("insert b");
    let mut non_markdown = file_record("f3", "notes/c.png", "notes/c.png", "/vault/notes/c.png");
    non_markdown.is_markdown = false;
    FilesRepository::insert(&connection, &non_markdown).expect("insert c");
    connection
        .execute(
            "INSERT INTO index_state (key, value_json) VALUES (?1, ?2)",
            rusqlite::params!["last_index_at", "\"2026-03-03T19:00:00Z\""],
        )
        .expect("seed index_state");

    let snapshot = HealthSnapshotService
        .snapshot(&vault_root, &connection, 3, WatcherStatus::Running)
        .expect("build health snapshot");

    assert!(snapshot.db_healthy);
    assert_eq!(snapshot.db_migrations, known_migrations().len() as u64);
    assert_eq!(snapshot.index_lag, 3);
    assert_eq!(snapshot.watcher_status, "running");
    assert_eq!(snapshot.files_total, 3);
    assert_eq!(snapshot.markdown_files, 2);
    assert!(snapshot.last_index_updated_at.is_some());
}

#[test]
fn property_update_service_persists_and_updates_markdown() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(&vault_root).expect("create vault root");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let note_service = NoteCrudService::default();
    note_service
        .create_note(
            &vault_root,
            &mut connection,
            "f1",
            Path::new("notes/property.md"),
            "# Body",
        )
        .expect("create note");

    let before = FilesRepository::get_by_id(&connection, "f1")
        .expect("get file before property set")
        .expect("file exists before property set");

    let update_service = PropertyUpdateService::default();
    let result = update_service
        .set_property(
            &vault_root,
            &mut connection,
            "f1",
            "status",
            TypedPropertyValue::String("draft".to_string()),
        )
        .expect("set typed property");

    assert_eq!(result.file_id, "f1");
    assert_eq!(result.key, "status");
    assert_eq!(
        result.value,
        TypedPropertyValue::String("draft".to_string())
    );

    let markdown =
        fs::read_to_string(vault_root.join("notes/property.md")).expect("read updated markdown");
    assert!(markdown.contains("---"));
    assert!(markdown.contains("status: draft"));

    let property = PropertiesRepository::get_by_file_and_key(&connection, "f1", "status")
        .expect("get stored property")
        .expect("property should exist");
    assert_eq!(property.value_type, "string");
    assert_eq!(property.value_json, "\"draft\"");

    let after = FilesRepository::get_by_id(&connection, "f1")
        .expect("get file after property set")
        .expect("file exists after property set");
    assert_ne!(before.hash_blake3, after.hash_blake3);
}

#[test]
fn property_update_service_trace_context_wrapper_executes_operation() {
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(&vault_root).expect("create vault root");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let note_service = NoteCrudService::default();
    note_service
        .create_note(
            &vault_root,
            &mut connection,
            "f1",
            Path::new("notes/property-traced.md"),
            "# Body",
        )
        .expect("create note");

    let update_service = PropertyUpdateService::default();
    let trace_context = ServiceTraceContext::with_correlation("property_set", "cid-property-1");
    let result = update_service
        .set_property_with_trace_context(
            &trace_context,
            &vault_root,
            &mut connection,
            "f1",
            "status",
            TypedPropertyValue::String("published".to_string()),
        )
        .expect("set typed property with trace");

    assert_eq!(result.key, "status");
    assert_eq!(trace_context.correlation_id(), "cid-property-1");
}

#[test]
fn property_query_service_filters_sorts_and_paginates_rows() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record("f1", "notes/a.md", "notes/a.md", "/vault/notes/a.md"),
    )
    .expect("insert f1");
    FilesRepository::insert(
        &connection,
        &file_record("f2", "notes/b.md", "notes/b.md", "/vault/notes/b.md"),
    )
    .expect("insert f2");
    FilesRepository::insert(
        &connection,
        &file_record("f3", "notes/c.md", "notes/c.md", "/vault/notes/c.md"),
    )
    .expect("insert f3");

    PropertiesRepository::upsert(
        &connection,
        &PropertyRecordInput {
            property_id: "p1".to_string(),
            file_id: "f1".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"draft\"".to_string(),
        },
    )
    .expect("insert p1");
    PropertiesRepository::upsert(
        &connection,
        &PropertyRecordInput {
            property_id: "p2".to_string(),
            file_id: "f2".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"published\"".to_string(),
        },
    )
    .expect("insert p2");
    PropertiesRepository::upsert(
        &connection,
        &PropertyRecordInput {
            property_id: "p3".to_string(),
            file_id: "f3".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"public\"".to_string(),
        },
    )
    .expect("insert p3");

    let service = PropertyQueryService;
    let first_page = service
        .query(
            &connection,
            &PropertyQueryRequest {
                key: " status ".to_string(),
                value_contains: Some("PUB".to_string()),
                limit: Some(1),
                offset: 0,
                sort: PropertyQuerySort::FilePathDesc,
            },
        )
        .expect("query first page");
    assert_eq!(first_page.total, 2);
    assert_eq!(first_page.rows.len(), 1);
    assert_eq!(first_page.rows[0].file_path, "notes/c.md");
    assert_eq!(first_page.rows[0].property_id, "p3");

    let second_page = service
        .query(
            &connection,
            &PropertyQueryRequest {
                key: "status".to_string(),
                value_contains: Some("pub".to_string()),
                limit: Some(1),
                offset: 1,
                sort: PropertyQuerySort::FilePathDesc,
            },
        )
        .expect("query second page");
    assert_eq!(second_page.total, 2);
    assert_eq!(second_page.rows.len(), 1);
    assert_eq!(second_page.rows[0].file_path, "notes/b.md");
    assert_eq!(second_page.rows[0].property_id, "p2");
}

#[test]
fn property_query_service_supports_updated_at_sorting() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record("f1", "notes/a.md", "notes/a.md", "/vault/notes/a.md"),
    )
    .expect("insert f1");
    FilesRepository::insert(
        &connection,
        &file_record("f2", "notes/b.md", "notes/b.md", "/vault/notes/b.md"),
    )
    .expect("insert f2");

    PropertiesRepository::upsert(
        &connection,
        &PropertyRecordInput {
            property_id: "p1".to_string(),
            file_id: "f1".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"draft\"".to_string(),
        },
    )
    .expect("insert p1");
    PropertiesRepository::upsert(
        &connection,
        &PropertyRecordInput {
            property_id: "p2".to_string(),
            file_id: "f2".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"published\"".to_string(),
        },
    )
    .expect("insert p2");

    connection
        .execute(
            "UPDATE properties SET updated_at = ?1 WHERE property_id = ?2",
            rusqlite::params!["2026-03-03T12:00:00.000Z", "p1"],
        )
        .expect("set p1 timestamp");
    connection
        .execute(
            "UPDATE properties SET updated_at = ?1 WHERE property_id = ?2",
            rusqlite::params!["2026-03-03T12:00:01.000Z", "p2"],
        )
        .expect("set p2 timestamp");

    let rows = PropertyQueryService
        .query(
            &connection,
            &PropertyQueryRequest {
                key: "status".to_string(),
                value_contains: None,
                limit: None,
                offset: 0,
                sort: PropertyQuerySort::UpdatedAtDesc,
            },
        )
        .expect("query by updated_at desc")
        .rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].property_id, "p2");
    assert_eq!(rows[1].property_id, "p1");
}

#[test]
fn property_query_service_rejects_invalid_requests() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let service = PropertyQueryService;
    let missing_key = service
        .query(
            &connection,
            &PropertyQueryRequest {
                key: "   ".to_string(),
                value_contains: None,
                limit: None,
                offset: 0,
                sort: PropertyQuerySort::FilePathAsc,
            },
        )
        .expect_err("empty key should fail");
    assert!(matches!(missing_key, super::PropertyQueryError::InvalidKey));

    let zero_limit = service
        .query(
            &connection,
            &PropertyQueryRequest {
                key: "status".to_string(),
                value_contains: None,
                limit: Some(0),
                offset: 0,
                sort: PropertyQuerySort::FilePathAsc,
            },
        )
        .expect_err("zero limit should fail");
    assert!(matches!(
        zero_limit,
        super::PropertyQueryError::InvalidLimit { limit: 0 }
    ));
}

#[test]
fn base_table_executor_filters_sorts_and_projects_rows() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f1",
            "notes/projects/alpha.md",
            "notes/projects/alpha.md",
            "/vault/notes/projects/alpha.md",
        ),
    )
    .expect("insert f1");
    FilesRepository::insert(
        &connection,
        &file_record(
            "f2",
            "notes/projects/beta.md",
            "notes/projects/beta.md",
            "/vault/notes/projects/beta.md",
        ),
    )
    .expect("insert f2");
    FilesRepository::insert(
        &connection,
        &file_record(
            "f3",
            "notes/archive/gamma.md",
            "notes/archive/gamma.md",
            "/vault/notes/archive/gamma.md",
        ),
    )
    .expect("insert f3");

    for (property_id, file_id, key, value_json) in [
        ("p1", "f1", "status", "\"active\""),
        ("p2", "f1", "due", "2"),
        ("p3", "f1", "assignee", "\"han\""),
        ("p4", "f2", "status", "\"active\""),
        ("p5", "f2", "due", "1"),
        ("p6", "f2", "assignee", "\"sam\""),
        ("p7", "f3", "status", "\"active\""),
        ("p8", "f3", "due", "3"),
        ("p9", "f3", "assignee", "\"han\""),
    ] {
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: property_id.to_string(),
                file_id: file_id.to_string(),
                key: key.to_string(),
                value_type: "string".to_string(),
                value_json: value_json.to_string(),
            },
        )
        .expect("upsert property");
    }

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: vec![
            "status".to_string(),
            "due".to_string(),
            "assignee".to_string(),
        ],
        filters: vec![
            BaseFilterClause {
                key: "status".to_string(),
                op: BaseFilterOp::Eq,
                value: serde_json::json!("active"),
            },
            BaseFilterClause {
                key: "assignee".to_string(),
                op: BaseFilterOp::Contains,
                value: serde_json::json!("ha"),
            },
        ],
        sorts: vec![BaseSortClause {
            key: "due".to_string(),
            direction: BaseSortDirection::Desc,
            null_order: BaseNullOrder::First,
        }],
        columns: vec![
            BaseColumnConfig {
                key: "title".to_string(),
                label: None,
                width: None,
                hidden: false,
            },
            BaseColumnConfig {
                key: "status".to_string(),
                label: None,
                width: None,
                hidden: false,
            },
            BaseColumnConfig {
                key: "due".to_string(),
                label: None,
                width: None,
                hidden: false,
            },
        ],
        group_by: Vec::new(),
        aggregates: Vec::new(),
        relations: Vec::new(),
        rollups: Vec::new(),
        limit: 25,
        offset: 0,
        property_queries: Vec::new(),
    };

    let page = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect("execute table plan");
    assert_eq!(page.total, 1);
    assert_eq!(page.summaries.len(), 3);
    assert_eq!(page.summaries[2].key, "due");
    assert_eq!(page.summaries[2].count, 1);
    assert_eq!(page.summaries[2].min, Some(serde_json::json!(2)));
    assert_eq!(page.summaries[2].max, Some(serde_json::json!(2)));
    assert_eq!(page.summaries[2].avg, Some(serde_json::json!(2.0)));
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].file_path, "notes/projects/alpha.md");
    assert_eq!(
        page.rows[0].values.get("title"),
        Some(&serde_json::json!("alpha"))
    );
    assert_eq!(
        page.rows[0].values.get("status"),
        Some(&serde_json::json!("active"))
    );
    assert_eq!(page.rows[0].values.get("due"), Some(&serde_json::json!(2)));
}

#[test]
fn base_table_executor_supports_grouped_aggregate_output() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    for (file_id, path) in [
        ("f1", "notes/projects/alpha.md"),
        ("f2", "notes/projects/beta.md"),
        ("f3", "notes/projects/gamma.md"),
    ] {
        FilesRepository::insert(
            &connection,
            &file_record(file_id, path, path, &format!("/vault/{path}")),
        )
        .expect("insert file");
    }

    for (property_id, file_id, key, value_type, value_json) in [
        ("p1", "f1", "status", "string", "\"active\""),
        ("p2", "f1", "priority", "number", "2"),
        ("p3", "f2", "status", "string", "\"active\""),
        ("p4", "f2", "priority", "number", "3"),
        ("p5", "f3", "status", "string", "\"paused\""),
        ("p6", "f3", "priority", "number", "5"),
    ] {
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: property_id.to_string(),
                file_id: file_id.to_string(),
                key: key.to_string(),
                value_type: value_type.to_string(),
                value_json: value_json.to_string(),
            },
        )
        .expect("upsert property");
    }

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: vec!["status".to_string(), "priority".to_string()],
        filters: Vec::new(),
        sorts: Vec::new(),
        columns: Vec::new(),
        group_by: vec!["status".to_string()],
        aggregates: vec![
            BaseAggregateSpec {
                alias: "count_all".to_string(),
                op: BaseAggregateOp::Count,
                key: None,
            },
            BaseAggregateSpec {
                alias: "priority_sum".to_string(),
                op: BaseAggregateOp::Sum,
                key: Some("priority".to_string()),
            },
        ],
        relations: Vec::new(),
        rollups: Vec::new(),
        limit: 50,
        offset: 0,
        property_queries: Vec::new(),
    };

    let page = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect("execute grouped table plan");
    assert_eq!(page.total, 2);
    assert!(page.summaries.is_empty());
    assert!(page.grouping.is_some());
    assert_eq!(
        page.grouping.as_ref().map(|value| value.group_by.clone()),
        Some(vec!["status".to_string()])
    );

    let active = page
        .rows
        .iter()
        .find(|row| row.values.get("status") == Some(&serde_json::json!("active")))
        .expect("active group");
    assert_eq!(active.values.get("count_all"), Some(&serde_json::json!(2)));
    assert_eq!(
        active.values.get("priority_sum"),
        Some(&serde_json::json!(5.0))
    );
}

#[test]
fn base_table_executor_resolves_relations_and_rollups() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    for (file_id, path) in [
        ("f-project", "notes/projects/alpha.md"),
        ("f-meeting-1", "notes/meetings/m1.md"),
        ("f-meeting-2", "notes/meetings/m2.md"),
    ] {
        FilesRepository::insert(
            &connection,
            &file_record(file_id, path, path, &format!("/vault/{path}")),
        )
        .expect("insert file");
    }

    for (property_id, file_id, key, value_type, value_json) in [
        (
            "p1",
            "f-project",
            "meetings",
            "json",
            r#"["notes/meetings/m1.md", "[[notes/meetings/m2]]", "[[notes/meetings/missing]]"]"#,
        ),
        ("p2", "f-meeting-1", "duration", "number", "30"),
        ("p3", "f-meeting-2", "duration", "number", "45"),
    ] {
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: property_id.to_string(),
                file_id: file_id.to_string(),
                key: key.to_string(),
                value_type: value_type.to_string(),
                value_json: value_json.to_string(),
            },
        )
        .expect("upsert property");
    }

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: vec!["meetings".to_string(), "duration".to_string()],
        filters: Vec::new(),
        sorts: Vec::new(),
        columns: vec![
            BaseColumnConfig {
                key: "meetings".to_string(),
                label: None,
                width: None,
                hidden: false,
            },
            BaseColumnConfig {
                key: "meeting_total".to_string(),
                label: None,
                width: None,
                hidden: false,
            },
        ],
        group_by: Vec::new(),
        aggregates: Vec::new(),
        relations: vec![BaseRelationSpec {
            key: "meetings".to_string(),
        }],
        rollups: vec![BaseRollupSpec {
            alias: "meeting_total".to_string(),
            relation_key: "meetings".to_string(),
            target_key: "duration".to_string(),
            op: BaseRollupOp::Sum,
        }],
        limit: 10,
        offset: 0,
        property_queries: Vec::new(),
    };

    let page = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect("execute relation/rollup plan");
    assert_eq!(page.total, 1);
    assert_eq!(page.relation_diagnostics.len(), 1);
    assert_eq!(
        page.relation_diagnostics[0].reason,
        "relation_target_not_found"
    );
    assert_eq!(
        page.rows[0].values.get("meeting_total"),
        Some(&serde_json::json!(75.0))
    );
    let meetings = page.rows[0]
        .values
        .get("meetings")
        .and_then(serde_json::Value::as_array)
        .expect("meetings relation array");
    assert_eq!(meetings.len(), 3);
    assert!(meetings.iter().any(|entry| {
        entry.get("resolved").and_then(serde_json::Value::as_bool) == Some(false)
    }));
}

#[test]
fn base_table_executor_resolves_short_wikilink_relation_tokens() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    for (file_id, path) in [
        ("f-project", "notes/projects/alpha.md"),
        ("f-meeting-1", "notes/meetings/m1.md"),
        ("f-meeting-2", "notes/meetings/m2.md"),
    ] {
        FilesRepository::insert(
            &connection,
            &file_record(file_id, path, path, &format!("/vault/{path}")),
        )
        .expect("insert file");
    }

    for (property_id, file_id, key, value_type, value_json) in [
        ("p1", "f-project", "meetings", "json", r#"["[[m1]]", "m2"]"#),
        ("p2", "f-meeting-1", "duration", "number", "30"),
        ("p3", "f-meeting-2", "duration", "number", "45"),
    ] {
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: property_id.to_string(),
                file_id: file_id.to_string(),
                key: key.to_string(),
                value_type: value_type.to_string(),
                value_json: value_json.to_string(),
            },
        )
        .expect("upsert property");
    }

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: vec!["meetings".to_string(), "duration".to_string()],
        filters: Vec::new(),
        sorts: Vec::new(),
        columns: vec![
            BaseColumnConfig {
                key: "meetings".to_string(),
                label: None,
                width: None,
                hidden: false,
            },
            BaseColumnConfig {
                key: "meeting_total".to_string(),
                label: None,
                width: None,
                hidden: false,
            },
        ],
        group_by: Vec::new(),
        aggregates: Vec::new(),
        relations: vec![BaseRelationSpec {
            key: "meetings".to_string(),
        }],
        rollups: vec![BaseRollupSpec {
            alias: "meeting_total".to_string(),
            relation_key: "meetings".to_string(),
            target_key: "duration".to_string(),
            op: BaseRollupOp::Sum,
        }],
        limit: 10,
        offset: 0,
        property_queries: Vec::new(),
    };

    let page = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect("execute relation/rollup plan");
    assert_eq!(page.total, 1);
    assert_eq!(page.relation_diagnostics.len(), 0);
    assert_eq!(
        page.rows[0].values.get("meeting_total"),
        Some(&serde_json::json!(75.0))
    );
    let meetings = page.rows[0]
        .values
        .get("meetings")
        .and_then(serde_json::Value::as_array)
        .expect("meetings relation array");
    assert_eq!(meetings.len(), 2);
    assert!(
        meetings.iter().all(|entry| {
            entry.get("resolved").and_then(serde_json::Value::as_bool) == Some(true)
        })
    );
}

#[test]
fn base_table_executor_applies_sort_and_pagination_offset() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f1",
            "notes/projects/alpha.md",
            "notes/projects/alpha.md",
            "/vault/notes/projects/alpha.md",
        ),
    )
    .expect("insert f1");
    FilesRepository::insert(
        &connection,
        &file_record(
            "f2",
            "notes/projects/beta.md",
            "notes/projects/beta.md",
            "/vault/notes/projects/beta.md",
        ),
    )
    .expect("insert f2");

    for (property_id, file_id, key, value_json) in
        [("p1", "f1", "due", "2"), ("p2", "f2", "due", "1")]
    {
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: property_id.to_string(),
                file_id: file_id.to_string(),
                key: key.to_string(),
                value_type: "number".to_string(),
                value_json: value_json.to_string(),
            },
        )
        .expect("upsert property");
    }

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: vec!["due".to_string()],
        filters: Vec::new(),
        sorts: vec![BaseSortClause {
            key: "due".to_string(),
            direction: BaseSortDirection::Asc,
            null_order: BaseNullOrder::First,
        }],
        columns: vec![BaseColumnConfig {
            key: "path".to_string(),
            label: None,
            width: None,
            hidden: false,
        }],
        group_by: Vec::new(),
        aggregates: Vec::new(),
        relations: Vec::new(),
        rollups: Vec::new(),
        limit: 1,
        offset: 1,
        property_queries: Vec::new(),
    };

    let page = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect("execute paged table");
    assert_eq!(page.total, 2);
    assert_eq!(page.summaries.len(), 1);
    assert_eq!(page.summaries[0].key, "path");
    assert_eq!(page.summaries[0].count, 2);
    assert_eq!(
        page.summaries[0].min,
        Some(serde_json::json!("notes/projects/alpha.md"))
    );
    assert_eq!(
        page.summaries[0].max,
        Some(serde_json::json!("notes/projects/beta.md"))
    );
    assert_eq!(page.summaries[0].avg, None);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].file_id, "f1");
    assert_eq!(
        page.rows[0].values.get("path"),
        Some(&serde_json::json!("notes/projects/alpha.md"))
    );
}

#[test]
fn base_table_executor_parallel_fast_path_is_deterministic() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    for index in 0..1_300 {
        let file_id = format!("f{index}");
        let path = format!("notes/projects/{index:04}.md");
        FilesRepository::insert(
            &connection,
            &file_record(&file_id, &path, &path, &format!("/vault/{path}")),
        )
        .expect("insert file");
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: format!("p{index}"),
                file_id: file_id.clone(),
                key: "due".to_string(),
                value_type: "number".to_string(),
                value_json: ((index % 17) as i64).to_string(),
            },
        )
        .expect("upsert property");
    }

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: vec!["due".to_string()],
        filters: vec![BaseFilterClause {
            key: "due".to_string(),
            op: BaseFilterOp::Gte,
            value: serde_json::json!(3),
        }],
        sorts: vec![BaseSortClause {
            key: "due".to_string(),
            direction: BaseSortDirection::Desc,
            null_order: BaseNullOrder::First,
        }],
        columns: vec![
            BaseColumnConfig {
                key: "path".to_string(),
                label: None,
                width: None,
                hidden: false,
            },
            BaseColumnConfig {
                key: "due".to_string(),
                label: None,
                width: None,
                hidden: false,
            },
        ],
        group_by: Vec::new(),
        aggregates: Vec::new(),
        relations: Vec::new(),
        rollups: Vec::new(),
        limit: 200,
        offset: 20,
        property_queries: Vec::new(),
    };

    let first = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect("execute first");
    let second = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect("execute second");

    assert_eq!(first.total, second.total);
    assert_eq!(first.summaries, second.summaries);
    assert_eq!(first.rows, second.rows);
}

#[test]
fn base_table_executor_excludes_non_markdown_files_from_candidates() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f1",
            "notes/projects/alpha.md",
            "notes/projects/alpha.md",
            "/vault/notes/projects/alpha.md",
        ),
    )
    .expect("insert markdown");
    let mut non_markdown = file_record(
        "f2",
        "notes/projects/readme.txt",
        "notes/projects/readme.txt",
        "/vault/notes/projects/readme.txt",
    );
    non_markdown.is_markdown = false;
    FilesRepository::insert(&connection, &non_markdown).expect("insert text");

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: Vec::new(),
        filters: Vec::new(),
        sorts: Vec::new(),
        columns: vec![BaseColumnConfig {
            key: "path".to_string(),
            label: None,
            width: None,
            hidden: false,
        }],
        group_by: Vec::new(),
        aggregates: Vec::new(),
        relations: Vec::new(),
        rollups: Vec::new(),
        limit: 10,
        offset: 0,
        property_queries: Vec::new(),
    };

    let page = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect("execute table plan");
    assert_eq!(page.total, 1);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].file_path, "notes/projects/alpha.md");
}

#[test]
fn base_table_executor_exposes_file_extension_builtin() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f1",
            "notes/projects/alpha.md",
            "notes/projects/alpha.md",
            "/vault/notes/projects/alpha.md",
        ),
    )
    .expect("insert markdown");

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: vec!["file_ext".to_string()],
        filters: vec![BaseFilterClause {
            key: "file_ext".to_string(),
            op: BaseFilterOp::Eq,
            value: serde_json::json!("md"),
        }],
        sorts: Vec::new(),
        columns: vec![BaseColumnConfig {
            key: "file_ext".to_string(),
            label: None,
            width: None,
            hidden: false,
        }],
        group_by: Vec::new(),
        aggregates: Vec::new(),
        relations: Vec::new(),
        rollups: Vec::new(),
        limit: 10,
        offset: 0,
        property_queries: Vec::new(),
    };

    let page = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect("execute table plan");
    assert_eq!(page.total, 1);
    assert_eq!(
        page.rows[0].values.get("file_ext"),
        Some(&serde_json::json!("md"))
    );
}

#[test]
fn base_table_executor_reports_invalid_property_json_payloads() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f1",
            "notes/projects/alpha.md",
            "notes/projects/alpha.md",
            "/vault/notes/projects/alpha.md",
        ),
    )
    .expect("insert f1");
    PropertiesRepository::upsert(
        &connection,
        &PropertyRecordInput {
            property_id: "p1".to_string(),
            file_id: "f1".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "{bad-json".to_string(),
        },
    )
    .expect("upsert malformed property");

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: vec!["status".to_string()],
        filters: Vec::new(),
        sorts: Vec::new(),
        columns: vec![BaseColumnConfig {
            key: "status".to_string(),
            label: None,
            width: None,
            hidden: false,
        }],
        group_by: Vec::new(),
        aggregates: Vec::new(),
        relations: Vec::new(),
        rollups: Vec::new(),
        limit: 10,
        offset: 0,
        property_queries: Vec::new(),
    };

    let error = BaseTableExecutorService
        .execute(&connection, &plan)
        .expect_err("malformed json should fail");
    assert!(matches!(
        error,
        BaseTableExecutorError::ParsePropertyValue { file_id, key, .. }
        if file_id == "f1" && key == "status"
    ));
}

#[test]
fn base_column_persistence_updates_column_order_and_visibility() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f-base",
            "views/projects.base",
            "views/projects.base",
            "/vault/views/projects.base",
        ),
    )
    .expect("insert base file");

    let document = BaseDocument {
        views: vec![BaseViewDefinition {
            name: "Projects".to_string(),
            kind: BaseViewKind::Table,
            source: Some("notes/projects".to_string()),
            filters: Vec::new(),
            sorts: Vec::new(),
            columns: vec![
                BaseColumnConfig {
                    key: "status".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
                BaseColumnConfig {
                    key: "due".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
            ],
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
            extras: serde_json::Map::new(),
        }],
    };
    let config_json = serde_json::to_string(&document).expect("serialize base config");
    BasesRepository::upsert(
        &connection,
        &BaseRecordInput {
            base_id: "b1".to_string(),
            file_id: "f-base".to_string(),
            config_json,
        },
    )
    .expect("insert base row");

    let result = BaseColumnConfigPersistenceService
        .persist_view_columns(
            &connection,
            "b1",
            "projects",
            vec![
                BaseColumnConfig {
                    key: "due".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
                BaseColumnConfig {
                    key: "status".to_string(),
                    label: Some("Status".to_string()),
                    width: Some(120),
                    hidden: true,
                },
            ],
        )
        .expect("persist column layout");
    assert_eq!(result.base_id, "b1");
    assert_eq!(result.view_name, "Projects");
    assert_eq!(result.columns_total, 2);

    let persisted = BasesRepository::get_by_id(&connection, "b1")
        .expect("load persisted base")
        .expect("base exists");
    let persisted_document =
        serde_json::from_str::<BaseDocument>(&persisted.config_json).expect("parse persisted");
    let columns = &persisted_document.views[0].columns;
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].key, "due");
    assert_eq!(columns[1].key, "status");
    assert!(columns[1].hidden);
    assert_eq!(columns[1].label.as_deref(), Some("Status"));
}

#[test]
fn base_column_persistence_reports_missing_view() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f-base",
            "views/projects.base",
            "views/projects.base",
            "/vault/views/projects.base",
        ),
    )
    .expect("insert base file");
    let config_json = serde_json::to_string(&BaseDocument {
        views: vec![BaseViewDefinition {
            name: "Projects".to_string(),
            kind: BaseViewKind::Table,
            source: None,
            filters: Vec::new(),
            sorts: Vec::new(),
            columns: Vec::new(),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
            extras: serde_json::Map::new(),
        }],
    })
    .expect("serialize base config");
    BasesRepository::upsert(
        &connection,
        &BaseRecordInput {
            base_id: "b1".to_string(),
            file_id: "f-base".to_string(),
            config_json,
        },
    )
    .expect("insert base row");

    let error = BaseColumnConfigPersistenceService
        .persist_view_columns(&connection, "b1", "missing", Vec::new())
        .expect_err("missing view should fail");
    assert!(matches!(
        error,
        BaseColumnConfigPersistError::ViewNotFound {
            base_id,
            view_name
        } if base_id == "b1" && view_name == "missing"
    ));
}

#[test]
fn base_column_persistence_reports_invalid_stored_config_payload() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f-base",
            "views/projects.base",
            "views/projects.base",
            "/vault/views/projects.base",
        ),
    )
    .expect("insert base file");
    BasesRepository::upsert(
        &connection,
        &BaseRecordInput {
            base_id: "b1".to_string(),
            file_id: "f-base".to_string(),
            config_json: "{\"raw\":\"legacy\"}".to_string(),
        },
    )
    .expect("insert legacy base row");

    let error = BaseColumnConfigPersistenceService
        .persist_view_columns(&connection, "b1", "projects", Vec::new())
        .expect_err("invalid config should fail");
    assert!(matches!(
        error,
        BaseColumnConfigPersistError::DeserializeConfig { base_id, .. } if base_id == "b1"
    ));
}

#[test]
fn base_validation_service_validates_by_id_and_path() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f-base",
            "views/projects.base",
            "views/projects.base",
            "/vault/views/projects.base",
        ),
    )
    .expect("insert base file");
    let config_json = serde_json::to_string(&BaseDocument {
        views: vec![BaseViewDefinition {
            name: "Projects".to_string(),
            kind: BaseViewKind::Table,
            source: None,
            filters: Vec::new(),
            sorts: Vec::new(),
            columns: vec![
                BaseColumnConfig {
                    key: "status".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
                BaseColumnConfig {
                    key: "status".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
            ],
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
            extras: serde_json::Map::new(),
        }],
    })
    .expect("serialize base config");
    BasesRepository::upsert(
        &connection,
        &BaseRecordInput {
            base_id: "b1".to_string(),
            file_id: "f-base".to_string(),
            config_json,
        },
    )
    .expect("insert base row");

    let by_id = BaseValidationService
        .validate(&connection, "b1")
        .expect("validate by id");
    assert_eq!(by_id.base_id, "b1");
    assert_eq!(by_id.file_path, "views/projects.base");
    assert!(by_id.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "bases.column.duplicate_key"
            && diagnostic.severity == BaseDiagnosticSeverity::Warning
    }));

    let by_path = BaseValidationService
        .validate(&connection, "views/projects.base")
        .expect("validate by path");
    assert_eq!(by_path.base_id, "b1");
    assert_eq!(by_path.file_id, "f-base");
    assert_eq!(by_path.diagnostics, by_id.diagnostics);
}

#[test]
fn base_validation_service_reports_invalid_input_and_missing_base() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let invalid_input = BaseValidationService
        .validate(&connection, "   ")
        .expect_err("empty lookup should fail");
    assert!(matches!(invalid_input, BaseValidationError::InvalidInput));

    let missing = BaseValidationService
        .validate(&connection, "missing")
        .expect_err("missing base should fail");
    assert!(matches!(
        missing,
        BaseValidationError::BaseNotFound { path_or_id } if path_or_id == "missing"
    ));
}

#[test]
fn base_table_cached_query_service_invalidates_on_metadata_change() {
    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    FilesRepository::insert(
        &connection,
        &file_record(
            "f1",
            "notes/projects/alpha.md",
            "notes/projects/alpha.md",
            "/vault/notes/projects/alpha.md",
        ),
    )
    .expect("insert file");
    PropertiesRepository::upsert(
        &connection,
        &PropertyRecordInput {
            property_id: "p1".to_string(),
            file_id: "f1".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"draft\"".to_string(),
        },
    )
    .expect("insert property");

    let plan = TableQueryPlan {
        view_name: "Projects".to_string(),
        source_prefix: Some("notes/projects".to_string()),
        required_property_keys: vec!["status".to_string()],
        filters: Vec::new(),
        sorts: Vec::new(),
        columns: vec![BaseColumnConfig {
            key: "status".to_string(),
            label: None,
            width: None,
            hidden: false,
        }],
        group_by: Vec::new(),
        aggregates: Vec::new(),
        relations: Vec::new(),
        rollups: Vec::new(),
        limit: 10,
        offset: 0,
        property_queries: Vec::new(),
    };

    let cache_service = BaseTableCachedQueryService::default();
    let first = cache_service
        .execute(&connection, &plan)
        .expect("first cached execute");
    assert_eq!(
        first.rows[0].values.get("status"),
        Some(&serde_json::json!("draft"))
    );

    let second = cache_service
        .execute(&connection, &plan)
        .expect("second cached execute");
    assert_eq!(
        second.rows[0].values.get("status"),
        Some(&serde_json::json!("draft"))
    );

    PropertiesRepository::upsert(
        &connection,
        &PropertyRecordInput {
            property_id: "p1".to_string(),
            file_id: "f1".to_string(),
            key: "status".to_string(),
            value_type: "string".to_string(),
            value_json: "\"published\"".to_string(),
        },
    )
    .expect("update property");

    let third = cache_service
        .execute(&connection, &plan)
        .expect("third cached execute");
    assert_eq!(
        third.rows[0].values.get("status"),
        Some(&serde_json::json!("published"))
    );
}
