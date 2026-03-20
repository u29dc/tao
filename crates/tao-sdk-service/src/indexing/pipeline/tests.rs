use std::fs;
use std::path::PathBuf;

use rusqlite::Connection;
use serde_json::Value as JsonValue;
use tao_sdk_storage::{
    BasesRepository, FileRecordInput, FilesRepository, IndexStateRepository, LinksRepository,
    PropertiesRepository, run_migrations,
};
use tempfile::tempdir;

use crate::BacklinkGraphService;

use super::{
    CURRENT_LINK_RESOLUTION_VERSION, CasePolicy, CheckpointedIndexService,
    CoalescedBatchIndexService, ConsistencyIssueKind, FullIndexService, IncrementalIndexService,
    IndexConsistencyChecker, IndexSelfHealService, LINK_RESOLUTION_VERSION_STATE_KEY,
    ReconciliationScannerService, StaleCleanupService,
};

#[test]
fn rebuild_populates_core_tables_for_files_links_properties_bases_and_state() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::create_dir_all(temp.path().join("views")).expect("create views dir");
    fs::create_dir_all(temp.path().join("assets")).expect("create assets dir");

    fs::write(
        temp.path().join("notes/a.md"),
        "---\nstatus: draft\n---\n# A\n[[b]]\n[[missing]]",
    )
    .expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");
    fs::write(temp.path().join("views/projects.base"), "views:\n  - table").expect("write base");
    fs::write(temp.path().join("assets/logo.png"), "png").expect("write asset");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let result = FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("full rebuild");

    assert_eq!(result.indexed_files, 4);
    assert_eq!(result.markdown_files, 2);
    assert_eq!(result.links_total, 2);
    assert_eq!(result.unresolved_links, 1);
    assert_eq!(result.properties_total, 1);
    assert_eq!(result.bases_total, 1);

    let all_files = FilesRepository::list_all(&connection).expect("list files");
    assert_eq!(all_files.len(), 4);

    let source = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get source")
        .expect("source exists");
    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing");
    assert_eq!(outgoing.len(), 2);
    assert_eq!(outgoing.iter().filter(|row| row.is_unresolved).count(), 1);

    let properties = PropertiesRepository::list_for_file_with_path(&connection, &source.file_id)
        .expect("list properties");
    assert_eq!(properties.len(), 1);
    assert_eq!(properties[0].key, "status");

    let base_file = FilesRepository::get_by_normalized_path(&connection, "views/projects.base")
        .expect("get base file")
        .expect("base file exists");
    let base = BasesRepository::get_by_file_id(&connection, &base_file.file_id)
        .expect("get base row")
        .expect("base exists");
    assert!(base.config_json.contains("views"));

    assert!(
        IndexStateRepository::get_by_key(&connection, "last_index_at")
            .expect("get index state")
            .is_some()
    );
    assert!(
        IndexStateRepository::get_by_key(&connection, "last_full_index_summary")
            .expect("get summary state")
            .is_some()
    );
    let version = IndexStateRepository::get_by_key(&connection, LINK_RESOLUTION_VERSION_STATE_KEY)
        .expect("get link resolution version")
        .expect("version state exists");
    assert_eq!(
        serde_json::from_str::<u32>(&version.value_json).expect("parse version"),
        CURRENT_LINK_RESOLUTION_VERSION
    );
}

#[test]
fn rebuild_produces_deterministic_link_rows_across_repeated_runs() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes/projects")).expect("create notes dir");
    fs::write(
            temp.path().join("notes/a.md"),
            "---\nrelated: [\"[[projects/b]]\", \"[[projects/c]]\"]\n---\n# A\n[[projects/b]]\n[[missing]]",
        )
        .expect("write a");
    fs::write(
        temp.path().join("notes/projects/b.md"),
        "# B\n[[../a]]\n[[c]]",
    )
    .expect("write b");
    fs::write(temp.path().join("notes/projects/c.md"), "# C").expect("write c");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let mut expected_link_ids: Option<Vec<String>> = None;
    for _ in 0..5 {
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("full rebuild");

        let mut statement = connection
            .prepare("SELECT link_id FROM links ORDER BY link_id ASC")
            .expect("prepare link id query");
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query link ids");
        let link_ids = rows
            .map(|row| row.expect("map link id row"))
            .collect::<Vec<_>>();
        match expected_link_ids.as_ref() {
            Some(expected) => assert_eq!(&link_ids, expected),
            None => {
                expected_link_ids = Some(link_ids);
            }
        }
    }
}

#[test]
fn heading_fragment_links_only_resolve_when_target_heading_exists() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(
        temp.path().join("notes/a.md"),
        "# A\n[[b#Project Plan]]\n[[b#Missing Heading]]",
    )
    .expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# Project Plan").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a")
        .expect("a exists");
    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
        .expect("list outgoing");
    assert_eq!(outgoing.len(), 2);

    let resolved_heading = outgoing
        .iter()
        .find(|row| row.heading_slug.as_deref() == Some("project-plan"))
        .expect("resolved heading link");
    assert!(!resolved_heading.is_unresolved);
    assert_eq!(
        resolved_heading.resolved_path.as_deref(),
        Some("notes/b.md")
    );

    let missing_heading = outgoing
        .iter()
        .find(|row| row.heading_slug.as_deref() == Some("missing-heading"))
        .expect("missing heading link");
    assert!(missing_heading.is_unresolved);
    assert_eq!(missing_heading.resolved_path, None);
    assert_eq!(
        missing_heading.unresolved_reason.as_deref(),
        Some("bad-anchor")
    );
}

#[test]
fn block_fragment_links_only_resolve_when_target_block_exists() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(
        temp.path().join("notes/a.md"),
        "# A\n[[b#^block-a]]\n[[b#^missing-block]]",
    )
    .expect("write a");
    fs::write(temp.path().join("notes/b.md"), "Paragraph ^block-a").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a")
        .expect("a exists");
    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
        .expect("list outgoing");
    assert_eq!(outgoing.len(), 2);

    let resolved_block = outgoing
        .iter()
        .find(|row| row.block_id.as_deref() == Some("block-a"))
        .expect("resolved block link");
    assert!(!resolved_block.is_unresolved);
    assert_eq!(resolved_block.resolved_path.as_deref(), Some("notes/b.md"));

    let missing_block = outgoing
        .iter()
        .find(|row| row.block_id.as_deref() == Some("missing-block"))
        .expect("missing block link");
    assert!(missing_block.is_unresolved);
    assert_eq!(missing_block.resolved_path, None);
    assert_eq!(
        missing_block.unresolved_reason.as_deref(),
        Some("bad-block")
    );
}

#[test]
fn unresolved_links_include_reason_codes_and_provenance() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(
            temp.path().join("notes/a.md"),
            "---\nup: \"[[frontmatter-missing]]\"\n---\n# A\n[[missing-note]]\n[[b#Missing Heading]]\n[[b#^missing-block]]\n[[bad??target]]",
        )
        .expect("write a");
    fs::write(
        temp.path().join("notes/b.md"),
        "# Known Heading\nParagraph ^known-block",
    )
    .expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a")
        .expect("a exists");
    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
        .expect("list outgoing");

    let unresolved = outgoing
        .iter()
        .filter(|row| row.is_unresolved)
        .collect::<Vec<_>>();
    assert_eq!(unresolved.len(), 5);
    assert!(unresolved.iter().any(|row| {
        row.unresolved_reason.as_deref() == Some("missing-note")
            && row.source_field == "body"
            && row.raw_target == "missing-note"
    }));
    assert!(unresolved.iter().any(|row| {
        row.unresolved_reason.as_deref() == Some("missing-note")
            && row.source_field.starts_with("frontmatter:")
            && row.raw_target == "frontmatter-missing"
    }));
    assert!(unresolved.iter().any(|row| {
        row.unresolved_reason.as_deref() == Some("bad-anchor") && row.raw_target == "b"
    }));
    assert!(unresolved.iter().any(|row| {
        row.unresolved_reason.as_deref() == Some("bad-block") && row.raw_target == "b"
    }));
    assert!(unresolved.iter().any(|row| {
        row.unresolved_reason.as_deref() == Some("malformed-target")
            && row.raw_target == "bad??target"
    }));
}

#[test]
fn malformed_front_matter_documents_do_not_break_indexing() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(
        temp.path().join("notes/a.md"),
        "---\nstatus: [broken\n# A\n[[b]]",
    )
    .expect("write malformed a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let result = FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("full index should tolerate malformed front matter");
    assert_eq!(result.indexed_files, 2);
    assert_eq!(result.markdown_files, 2);

    let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a")
        .expect("a exists");
    let properties = PropertiesRepository::list_for_file_with_path(&connection, &source_a.file_id)
        .expect("list properties");
    assert!(properties.is_empty());

    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
        .expect("list outgoing links");
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].resolved_path.as_deref(), Some("notes/b.md"));
    assert!(!outgoing[0].is_unresolved);
}

#[test]
fn frontmatter_only_wikilinks_are_indexed_for_outgoing_and_backlinks() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(
        temp.path().join("notes/a.md"),
        "---\nup: \"[[b]]\"\nchildren:\n  - \"[[c]]\"\n---\n# A\n",
    )
    .expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B\n").expect("write b");
    fs::write(temp.path().join("notes/c.md"), "# C\n").expect("write c");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    let result = FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    assert_eq!(result.links_total, 2);
    assert_eq!(result.unresolved_links, 0);

    let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a")
        .expect("a exists");
    let mut outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
        .expect("list outgoing");
    outgoing.sort_by(|left, right| left.raw_target.cmp(&right.raw_target));
    assert_eq!(outgoing.len(), 2);
    assert_eq!(outgoing[0].resolved_path.as_deref(), Some("notes/b.md"));
    assert_eq!(outgoing[1].resolved_path.as_deref(), Some("notes/c.md"));
    assert!(
        outgoing
            .iter()
            .all(|row| row.source_field.starts_with("frontmatter:"))
    );

    let target_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
        .expect("get b")
        .expect("b exists");
    let backlinks_b = LinksRepository::list_backlinks_with_paths(&connection, &target_b.file_id)
        .expect("list b backlinks");
    assert_eq!(backlinks_b.len(), 1);
    assert_eq!(backlinks_b[0].source_path, "notes/a.md");
}

#[test]
fn markdown_links_and_embeds_resolve_to_non_markdown_targets() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes/assets")).expect("create notes/assets");
    fs::write(
        temp.path().join("notes/index.md"),
        "# Index\n[Deck](assets/company%20deck.pdf#page=2)\n![Photo](assets/image.png)",
    )
    .expect("write index");
    fs::write(temp.path().join("notes/assets/company deck.pdf"), "pdf").expect("write pdf");
    fs::write(temp.path().join("notes/assets/image.png"), "png").expect("write png");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("full index");

    let source = FilesRepository::get_by_normalized_path(&connection, "notes/index.md")
        .expect("get source")
        .expect("source exists");
    let outgoing =
        LinksRepository::list_outgoing_with_paths(&connection, &source.file_id).expect("outgoing");
    assert_eq!(outgoing.len(), 2);
    assert!(outgoing.iter().all(|row| !row.is_unresolved));
    assert!(outgoing.iter().any(|row| {
        row.source_field == "body:markdown"
            && row.resolved_path.as_deref() == Some("notes/assets/company deck.pdf")
    }));
    assert!(outgoing.iter().any(|row| {
        row.source_field == "body:embed"
            && row.resolved_path.as_deref() == Some("notes/assets/image.png")
    }));

    let linked_pdf =
        FilesRepository::get_by_normalized_path(&connection, "notes/assets/company deck.pdf")
            .expect("get pdf")
            .expect("pdf exists");
    let backlinks_pdf =
        LinksRepository::list_backlinks_with_paths(&connection, &linked_pdf.file_id)
            .expect("pdf backlinks");
    assert_eq!(backlinks_pdf.len(), 1);
    assert_eq!(backlinks_pdf[0].source_path, "notes/index.md");
}

#[test]
fn wikilink_attachments_resolve_from_frontmatter_and_body_with_ancestor_relative_paths() {
    let temp = tempdir().expect("tempdir");
    let contents_root = temp.path().join("WORK/13-RELATIONS/Contents");
    fs::create_dir_all(contents_root.join("Media")).expect("create media dir");
    fs::write(
            contents_root.join("post.md"),
            "---\nassets:\n  - \"[[Contents/Media/foo.jpg]]\"\n---\n# Post\n![[Contents/Media/foo.jpg]]",
        )
        .expect("write post");
    fs::write(contents_root.join("Media/foo.jpg"), "jpg").expect("write image");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("full index");

    let source =
        FilesRepository::get_by_normalized_path(&connection, "WORK/13-RELATIONS/Contents/post.md")
            .expect("get source")
            .expect("source exists");
    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing");

    assert_eq!(outgoing.len(), 2);
    assert!(outgoing.iter().all(|row| !row.is_unresolved));
    assert!(outgoing.iter().all(|row| {
        row.resolved_path.as_deref() == Some("WORK/13-RELATIONS/Contents/Media/foo.jpg")
    }));
    assert!(
        outgoing
            .iter()
            .any(|row| row.source_field == "body" && row.raw_target == "Contents/Media/foo.jpg")
    );
    assert!(outgoing.iter().any(|row| {
        row.source_field.starts_with("frontmatter:assets")
            && row.raw_target == "Contents/Media/foo.jpg"
    }));
}

#[test]
fn incremental_reindex_refreshes_markdown_attachment_links() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes/assets")).expect("create notes/assets");
    fs::write(
        temp.path().join("notes/index.md"),
        "# Index\n[Deck](assets/company-deck.pdf)",
    )
    .expect("write index");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let source = FilesRepository::get_by_normalized_path(&connection, "notes/index.md")
        .expect("get source")
        .expect("source exists");
    let outgoing_before = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing before");
    assert_eq!(outgoing_before.len(), 1);
    assert!(outgoing_before[0].is_unresolved);

    fs::write(temp.path().join("notes/assets/company-deck.pdf"), "pdf").expect("write pdf");
    IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/assets/company-deck.pdf")],
            CasePolicy::Sensitive,
        )
        .expect("reindex attachment");

    let outgoing_after = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing after");
    assert_eq!(outgoing_after.len(), 1);
    assert!(!outgoing_after[0].is_unresolved);
    assert_eq!(
        outgoing_after[0].resolved_path.as_deref(),
        Some("notes/assets/company-deck.pdf")
    );
    assert_eq!(outgoing_after[0].source_field, "body:markdown");
}

#[test]
fn incremental_reindex_updates_frontmatter_only_wikilinks() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(
        temp.path().join("notes/a.md"),
        "---\nup: \"[[b]]\"\n---\n# A\n",
    )
    .expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B\n").expect("write b");
    fs::write(temp.path().join("notes/c.md"), "# C\n").expect("write c");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    fs::write(
        temp.path().join("notes/a.md"),
        "---\nup: \"[[c]]\"\n---\n# A updated\n",
    )
    .expect("update a");

    IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/a.md")],
            CasePolicy::Sensitive,
        )
        .expect("incremental update");

    let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a")
        .expect("a exists");
    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
        .expect("list outgoing");
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].resolved_path.as_deref(), Some("notes/c.md"));
}

#[test]
fn incremental_apply_changes_reindexes_only_changed_markdown_file() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A\n[[b]]").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let before_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
        .expect("get b before")
        .expect("b exists before");

    fs::write(
        temp.path().join("notes/a.md"),
        "---\nstatus: done\n---\n# A updated\n[[b]]\n[[missing]]",
    )
    .expect("update a");

    let result = IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/a.md")],
            CasePolicy::Sensitive,
        )
        .expect("incremental update");

    assert_eq!(result.processed_paths, 1);
    assert_eq!(result.upserted_files, 1);
    assert_eq!(result.removed_files, 0);

    let after_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
        .expect("get b after")
        .expect("b exists after");
    assert_eq!(before_b.file_id, after_b.file_id);
    assert_eq!(before_b.hash_blake3, after_b.hash_blake3);

    let source = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get source")
        .expect("source exists");
    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing");
    assert_eq!(outgoing.len(), 2);
    assert_eq!(outgoing.iter().filter(|row| row.is_unresolved).count(), 1);

    let properties = PropertiesRepository::list_for_file_with_path(&connection, &source.file_id)
        .expect("list properties");
    assert_eq!(properties.len(), 1);
    assert_eq!(properties[0].key, "status");
}

#[test]
fn incremental_apply_changes_resolves_forward_links_within_same_batch() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A\n[[b]]").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B\n[[c]]").expect("write b");
    fs::write(temp.path().join("notes/c.md"), "# C").expect("write c");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[
                PathBuf::from("notes/a.md"),
                PathBuf::from("notes/b.md"),
                PathBuf::from("notes/c.md"),
            ],
            CasePolicy::Sensitive,
        )
        .expect("incremental apply on fresh db");

    let file_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a")
        .expect("a exists");
    let file_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
        .expect("get b")
        .expect("b exists");
    let outgoing_a = LinksRepository::list_outgoing_with_paths(&connection, &file_a.file_id)
        .expect("list outgoing a");
    let outgoing_b = LinksRepository::list_outgoing_with_paths(&connection, &file_b.file_id)
        .expect("list outgoing b");
    assert_eq!(outgoing_a.len(), 1);
    assert_eq!(outgoing_a[0].resolved_path.as_deref(), Some("notes/b.md"));
    assert!(!outgoing_a[0].is_unresolved);
    assert_eq!(outgoing_b.len(), 1);
    assert_eq!(outgoing_b[0].resolved_path.as_deref(), Some("notes/c.md"));
    assert!(!outgoing_b[0].is_unresolved);
}

#[test]
fn incremental_apply_changes_skips_unchanged_paths_using_metadata_prefilter() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A\n[[b]]").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let before_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a before")
        .expect("a exists before");

    let result = IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/a.md")],
            CasePolicy::Sensitive,
        )
        .expect("incremental unchanged path");

    assert_eq!(result.processed_paths, 1);
    assert_eq!(result.upserted_files, 0);
    assert_eq!(result.links_reindexed, 0);
    assert_eq!(result.properties_reindexed, 0);

    let after_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a after")
        .expect("a exists after");
    assert_eq!(before_a.hash_blake3, after_a.hash_blake3);
    assert_eq!(before_a.modified_unix_ms, after_a.modified_unix_ms);
}

#[test]
fn incremental_apply_changes_removes_deleted_file_metadata() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    fs::remove_file(temp.path().join("notes/a.md")).expect("remove a");

    let result = IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/a.md")],
            CasePolicy::Sensitive,
        )
        .expect("incremental delete");

    assert_eq!(result.processed_paths, 1);
    assert_eq!(result.upserted_files, 0);
    assert_eq!(result.removed_files, 1);
    assert!(
        FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get deleted file")
            .is_none()
    );
}

#[test]
fn incremental_apply_changes_refreshes_links_when_target_note_is_created() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/source.md"), "# Source\n[[target]]").expect("write source");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let source = FilesRepository::get_by_normalized_path(&connection, "notes/source.md")
        .expect("get source")
        .expect("source exists");
    let before = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing before");
    assert_eq!(before.len(), 1);
    assert!(before[0].is_unresolved);

    fs::write(temp.path().join("notes/target.md"), "# Target").expect("write target");
    IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/target.md")],
            CasePolicy::Sensitive,
        )
        .expect("incremental create target");

    let after = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing after");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].resolved_path.as_deref(), Some("notes/target.md"));
    assert!(!after[0].is_unresolved);
}

#[test]
fn incremental_apply_changes_re_resolves_links_when_new_same_folder_match_is_added() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes/project")).expect("create project dir");
    fs::write(
        temp.path().join("notes/project/today.md"),
        "# Today\n[[alpha]]",
    )
    .expect("write source");
    fs::write(temp.path().join("notes/alpha.md"), "# Alpha").expect("write root alpha");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let source = FilesRepository::get_by_normalized_path(&connection, "notes/project/today.md")
        .expect("get source")
        .expect("source exists");
    let before = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing before");
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].resolved_path.as_deref(), Some("notes/alpha.md"));

    fs::write(
        temp.path().join("notes/project/alpha.md"),
        "# Project Alpha",
    )
    .expect("write project alpha");
    IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/project/alpha.md")],
            CasePolicy::Sensitive,
        )
        .expect("incremental add same-folder match");

    let after = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing after");
    assert_eq!(after.len(), 1);
    assert_eq!(
        after[0].resolved_path.as_deref(),
        Some("notes/project/alpha.md")
    );
    assert!(!after[0].is_unresolved);
}

#[test]
fn incremental_apply_changes_re_resolves_links_when_new_nearer_match_is_added() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes/project")).expect("create project dir");
    fs::create_dir_all(temp.path().join("archive")).expect("create archive dir");
    fs::write(
        temp.path().join("notes/project/today.md"),
        "# Today\n[[alpha]]",
    )
    .expect("write source");
    fs::write(temp.path().join("archive/alpha.md"), "# Archive Alpha")
        .expect("write archive alpha");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let source = FilesRepository::get_by_normalized_path(&connection, "notes/project/today.md")
        .expect("get source")
        .expect("source exists");
    let before = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing before");
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].resolved_path.as_deref(), Some("archive/alpha.md"));

    fs::write(temp.path().join("notes/alpha.md"), "# Notes Alpha").expect("write notes alpha");
    IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/alpha.md")],
            CasePolicy::Sensitive,
        )
        .expect("incremental add nearer match");

    let after = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing after");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].resolved_path.as_deref(), Some("notes/alpha.md"));
    assert!(!after[0].is_unresolved);
}

#[test]
fn incremental_apply_changes_marks_backlinks_unresolved_when_target_is_deleted() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/source.md"), "# Source\n[[target]]").expect("write source");
    fs::write(temp.path().join("notes/target.md"), "# Target").expect("write target");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let source = FilesRepository::get_by_normalized_path(&connection, "notes/source.md")
        .expect("get source")
        .expect("source exists");
    fs::remove_file(temp.path().join("notes/target.md")).expect("remove target");

    IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/target.md")],
            CasePolicy::Sensitive,
        )
        .expect("incremental delete target");

    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing after delete");
    assert_eq!(outgoing.len(), 1);
    assert!(outgoing[0].is_unresolved);
    assert_eq!(outgoing[0].resolved_path, None);
    assert_eq!(
        outgoing[0].unresolved_reason.as_deref(),
        Some("missing-note")
    );
    assert_eq!(
        LinksRepository::count_unresolved(&connection).expect("count unresolved"),
        1
    );
}

#[test]
fn incremental_apply_changes_refreshes_anchor_links_when_target_heading_changes() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(
        temp.path().join("notes/source.md"),
        "# Source\n[[target#Known Heading]]",
    )
    .expect("write source");
    fs::write(temp.path().join("notes/target.md"), "# Known Heading\nbody").expect("write target");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let source = FilesRepository::get_by_normalized_path(&connection, "notes/source.md")
        .expect("get source")
        .expect("source exists");
    fs::write(
        temp.path().join("notes/target.md"),
        "# Renamed Heading\nbody",
    )
    .expect("rewrite target");

    IncrementalIndexService::default()
        .apply_changes(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/target.md")],
            CasePolicy::Sensitive,
        )
        .expect("incremental heading change");

    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
        .expect("list outgoing after heading change");
    assert_eq!(outgoing.len(), 1);
    assert!(outgoing[0].is_unresolved);
    assert_eq!(outgoing[0].resolved_path, None);
    assert_eq!(outgoing[0].heading_slug.as_deref(), Some("known-heading"));
    assert_eq!(outgoing[0].unresolved_reason.as_deref(), Some("bad-anchor"));
}

#[test]
fn reconciliation_scan_detects_malformed_normalized_rows_without_mutating_and_repair_removes_them()
{
    let temp = tempdir().expect("tempdir");
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(vault_root.join("notes")).expect("create notes");
    fs::write(vault_root.join("notes/a.md"), "# A\n[[b]]\n").expect("write a");
    fs::write(vault_root.join("notes/b.md"), "# B\n").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(&vault_root, &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let bogus_absolute = vault_root
        .join("notes/a.md")
        .canonicalize()
        .expect("canonicalize note");
    let bogus_path = bogus_absolute
        .to_string_lossy()
        .trim_start_matches('/')
        .to_string();
    let metadata = fs::metadata(&bogus_absolute).expect("read metadata");

    FilesRepository::insert(
        &connection,
        &FileRecordInput {
            file_id: "file-bogus-a".to_string(),
            normalized_path: bogus_path.clone(),
            match_key: bogus_path.to_lowercase(),
            absolute_path: bogus_absolute.to_string_lossy().to_string(),
            size_bytes: metadata.len(),
            modified_unix_ms: metadata
                .modified()
                .expect("modified time")
                .duration_since(std::time::UNIX_EPOCH)
                .expect("modified after epoch")
                .as_millis()
                .try_into()
                .expect("modified fits in i64"),
            hash_blake3: "hash-bogus".to_string(),
            is_markdown: true,
        },
    )
    .expect("insert bogus row");

    let (orphans_total_before, orphans_before) = BacklinkGraphService
        .orphans_page(&connection, 50, 0)
        .expect("orphans before repair");
    assert_eq!(orphans_total_before, 1);
    assert_eq!(orphans_before[0].path, bogus_path);

    let scan = ReconciliationScannerService::default()
        .scan(&vault_root, &connection, CasePolicy::Sensitive)
        .expect("scan drift");
    assert_eq!(scan.removed_paths, 1);
    assert_eq!(scan.drift_paths, 1);
    assert!(
        FilesRepository::get_by_normalized_path(&connection, &bogus_path)
            .expect("lookup bogus before repair")
            .is_some()
    );

    let repair = ReconciliationScannerService::default()
        .scan_and_repair(&vault_root, &mut connection, CasePolicy::Sensitive, 128)
        .expect("repair drift");
    assert_eq!(repair.removed_paths, 1);
    assert_eq!(repair.drift_paths, 1);
    assert_eq!(repair.removed_files, 1);
    assert!(
        FilesRepository::get_by_normalized_path(&connection, &bogus_path)
            .expect("lookup bogus after repair")
            .is_none()
    );

    let (orphans_total_after, orphans_after) = BacklinkGraphService
        .orphans_page(&connection, 50, 0)
        .expect("orphans after repair");
    assert_eq!(orphans_total_after, 0);
    assert!(orphans_after.is_empty());
}

#[test]
fn coalesced_batch_apply_deduplicates_events_and_respects_batch_size() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    fs::write(temp.path().join("notes/a.md"), "# A changed").expect("update a");
    fs::write(temp.path().join("notes/b.md"), "# B changed").expect("update b");

    let result = CoalescedBatchIndexService::default()
        .apply_coalesced(
            temp.path(),
            &mut connection,
            &[
                PathBuf::from("notes/a.md"),
                PathBuf::from("notes/a.md"),
                PathBuf::from("notes/b.md"),
            ],
            1,
            CasePolicy::Sensitive,
        )
        .expect("apply coalesced batches");

    assert_eq!(result.input_events, 3);
    assert_eq!(result.unique_paths, 2);
    assert_eq!(result.batches_applied, 2);
    assert_eq!(result.upserted_files, 2);
}

#[test]
fn coalesced_batch_apply_rejects_zero_batch_size() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let error = CoalescedBatchIndexService::default()
        .apply_coalesced(
            temp.path(),
            &mut connection,
            &[PathBuf::from("notes/a.md")],
            0,
            CasePolicy::Sensitive,
        )
        .expect_err("zero batch size should fail");

    assert!(matches!(
        error,
        super::FullIndexError::InvalidBatchSize { .. }
    ));
}

#[test]
fn checkpointed_apply_persists_progress_and_resume_finishes_remaining_paths() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");
    fs::write(temp.path().join("notes/c.md"), "# C").expect("write c");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let before_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
        .expect("get b before")
        .expect("b exists before");
    let before_c = FilesRepository::get_by_normalized_path(&connection, "notes/c.md")
        .expect("get c before")
        .expect("c exists before");

    fs::write(temp.path().join("notes/a.md"), "# A changed").expect("update a");
    fs::write(temp.path().join("notes/b.md"), "# B changed").expect("update b");
    fs::write(temp.path().join("notes/c.md"), "# C changed").expect("update c");

    let first = CheckpointedIndexService::default()
        .apply_checkpointed(
            temp.path(),
            &mut connection,
            &[
                PathBuf::from("notes/a.md"),
                PathBuf::from("notes/b.md"),
                PathBuf::from("notes/c.md"),
            ],
            1,
            Some(1),
            CasePolicy::Sensitive,
        )
        .expect("first checkpointed run");

    assert!(!first.started_from_checkpoint);
    assert_eq!(first.total_paths, 3);
    assert_eq!(first.processed_paths, 1);
    assert_eq!(first.remaining_paths, 2);
    assert_eq!(first.batches_applied, 1);
    assert!(!first.checkpoint_completed);
    assert!(
        IndexStateRepository::get_by_key(&connection, "checkpoint.incremental_index")
            .expect("get checkpoint state")
            .is_some()
    );

    let mid_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
        .expect("get b mid")
        .expect("b exists mid");
    assert_eq!(mid_b.hash_blake3, before_b.hash_blake3);

    let resumed = CheckpointedIndexService::default()
        .apply_checkpointed(
            temp.path(),
            &mut connection,
            &[],
            8,
            None,
            CasePolicy::Insensitive,
        )
        .expect("resume checkpointed run");

    assert!(resumed.started_from_checkpoint);
    assert_eq!(resumed.total_paths, 3);
    assert_eq!(resumed.processed_paths, 2);
    assert_eq!(resumed.remaining_paths, 0);
    assert_eq!(resumed.batches_applied, 2);
    assert!(resumed.checkpoint_completed);
    assert!(
        IndexStateRepository::get_by_key(&connection, "checkpoint.incremental_index")
            .expect("get consumed checkpoint state")
            .is_none()
    );

    let after_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
        .expect("get b after")
        .expect("b exists after");
    let after_c = FilesRepository::get_by_normalized_path(&connection, "notes/c.md")
        .expect("get c after")
        .expect("c exists after");
    assert_ne!(after_b.hash_blake3, before_b.hash_blake3);
    assert_ne!(after_c.hash_blake3, before_c.hash_blake3);

    let summary = IndexStateRepository::get_by_key(&connection, "last_checkpointed_index_summary")
        .expect("get checkpoint summary")
        .expect("checkpoint summary exists");
    let summary_json: JsonValue =
        serde_json::from_str(&summary.value_json).expect("parse checkpoint summary");
    assert_eq!(
        summary_json
            .get("checkpoint_completed")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
}

#[test]
fn checkpointed_apply_returns_noop_when_no_checkpoint_exists() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");

    let result = CheckpointedIndexService::default()
        .apply_checkpointed(
            temp.path(),
            &mut connection,
            &[],
            32,
            None,
            CasePolicy::Sensitive,
        )
        .expect("resume with no checkpoint");

    assert!(result.started_from_checkpoint);
    assert_eq!(result.total_paths, 0);
    assert_eq!(result.processed_paths, 0);
    assert_eq!(result.remaining_paths, 0);
    assert!(result.checkpoint_completed);
}

#[test]
fn reconciliation_scanner_repairs_missed_add_update_delete_events() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A\n[[b]]").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    fs::write(temp.path().join("notes/a.md"), "# A updated\n[[c]]").expect("update a");
    fs::remove_file(temp.path().join("notes/b.md")).expect("remove b");
    fs::write(temp.path().join("notes/c.md"), "# C").expect("write c");

    let result = ReconciliationScannerService::default()
        .scan_and_repair(temp.path(), &mut connection, CasePolicy::Sensitive, 2)
        .expect("scan and repair");

    assert_eq!(result.scanned_files, 2);
    assert_eq!(result.inserted_paths, 1);
    assert_eq!(result.updated_paths, 1);
    assert_eq!(result.removed_paths, 1);
    assert_eq!(result.drift_paths, 3);

    assert!(
        FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
            .expect("get removed b")
            .is_none()
    );
    let c_file = FilesRepository::get_by_normalized_path(&connection, "notes/c.md")
        .expect("get c")
        .expect("c exists");
    assert_eq!(c_file.normalized_path, "notes/c.md");

    let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a")
        .expect("a exists");
    let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
        .expect("list outgoing links");
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].raw_target, "c");
    assert_eq!(outgoing[0].resolved_path.as_deref(), Some("notes/c.md"));
    assert!(!outgoing[0].is_unresolved);
}

#[test]
fn reconciliation_scanner_returns_noop_when_no_drift_detected() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let result = ReconciliationScannerService::default()
        .scan_and_repair(temp.path(), &mut connection, CasePolicy::Sensitive, 4)
        .expect("scan without drift");

    assert_eq!(result.scanned_files, 1);
    assert_eq!(result.inserted_paths, 0);
    assert_eq!(result.updated_paths, 0);
    assert_eq!(result.removed_paths, 0);
    assert_eq!(result.drift_paths, 0);
    assert_eq!(result.batches_applied, 0);
}

#[test]
fn reconciliation_scanner_handles_burst_changes_consistently() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");

    for index in 0..40_u64 {
        let path = temp.path().join(format!("notes/n{index:02}.md"));
        let next = (index + 1) % 40;
        fs::write(path, format!("# Note {index}\n[[n{next:02}]]")).expect("write seed note");
    }

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    for index in 0..10_u64 {
        let path = temp.path().join(format!("notes/n{index:02}.md"));
        fs::write(path, format!("# Note {index} updated\n[[n{index:02}]]"))
            .expect("update existing note");
    }
    for index in 10..20_u64 {
        let path = temp.path().join(format!("notes/n{index:02}.md"));
        fs::remove_file(path).expect("remove existing note");
    }
    for index in 40..55_u64 {
        let path = temp.path().join(format!("notes/n{index:02}.md"));
        fs::write(path, format!("# New Note {index}\n[[n00]]")).expect("write inserted note");
    }

    let result = ReconciliationScannerService::default()
        .scan_and_repair(temp.path(), &mut connection, CasePolicy::Sensitive, 4)
        .expect("run reconciliation repair for burst changes");

    assert_eq!(result.inserted_paths, 15);
    assert_eq!(result.updated_paths, 10);
    assert_eq!(result.removed_paths, 10);
    assert_eq!(result.drift_paths, 35);
    assert_eq!(result.batches_applied, 9);

    let files = FilesRepository::list_all(&connection).expect("list reconciled files");
    assert_eq!(files.len(), 45);

    let report = IndexConsistencyChecker
        .check(temp.path(), &connection)
        .expect("run consistency checker");
    assert!(report.issues.is_empty());

    let second = ReconciliationScannerService::default()
        .scan_and_repair(temp.path(), &mut connection, CasePolicy::Sensitive, 4)
        .expect("run reconciliation after stabilization");
    assert_eq!(second.drift_paths, 0);
    assert_eq!(second.inserted_paths, 0);
    assert_eq!(second.updated_paths, 0);
    assert_eq!(second.removed_paths, 0);
}

#[test]
fn consistency_checker_reports_orphans_and_broken_references() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    fs::remove_file(temp.path().join("notes/b.md")).expect("remove b from disk");

    let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a file row")
        .expect("a file exists");

    connection
        .execute_batch("PRAGMA foreign_keys = OFF;")
        .expect("disable foreign key checks for injected corruption");
    connection
            .execute(
                "INSERT INTO properties (property_id, file_id, key, value_type, value_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params!["prop_orphan_1", "file_missing_1", "status", "string", "\"draft\""],
            )
            .expect("insert orphan property");
    connection
            .execute(
                "INSERT INTO render_cache (cache_key, file_id, html, content_hash) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params!["cache_orphan_1", "file_missing_2", "<p>x</p>", "abc123"],
            )
            .expect("insert orphan render cache");
    connection
            .execute(
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved, unresolved_reason, source_field) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, NULL, ?6)",
                rusqlite::params!["link_broken_target", source_a.file_id, "missing-target", "file_missing_3", 0_i64, "body"],
            )
            .expect("insert broken target link");
    connection
            .execute(
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved, unresolved_reason, source_field) VALUES (?1, ?2, ?3, NULL, NULL, NULL, ?4, NULL, ?5)",
                rusqlite::params!["link_resolution_mismatch", source_a.file_id, "mismatch", 0_i64, "body"],
            )
            .expect("insert resolution mismatch link");

    let report = IndexConsistencyChecker
        .check(temp.path(), &connection)
        .expect("run consistency checker");

    assert!(report.checked_at_unix_ms > 0);
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == ConsistencyIssueKind::OrphanProperty)
    );
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == ConsistencyIssueKind::OrphanRenderCache)
    );
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == ConsistencyIssueKind::BrokenLinkTarget)
    );
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == ConsistencyIssueKind::LinkResolutionMismatch)
    );
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == ConsistencyIssueKind::MissingOnDiskFile)
    );
}

#[test]
fn consistency_checker_returns_empty_report_for_healthy_index() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let report = IndexConsistencyChecker
        .check(temp.path(), &connection)
        .expect("run consistency checker");
    assert!(report.issues.is_empty());
}

#[test]
fn self_heal_repairs_common_consistency_issues() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
    fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    fs::remove_file(temp.path().join("notes/b.md")).expect("remove b from disk");

    let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
        .expect("get a file row")
        .expect("a file exists");

    connection
        .execute_batch("PRAGMA foreign_keys = OFF;")
        .expect("disable foreign key checks for injected corruption");
    connection
            .execute(
                "INSERT INTO properties (property_id, file_id, key, value_type, value_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params!["prop_orphan_2", "file_missing_x", "status", "string", "\"draft\""],
            )
            .expect("insert orphan property");
    connection
            .execute(
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved, unresolved_reason, source_field) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, NULL, ?6)",
                rusqlite::params!["link_broken_target_2", source_a.file_id, "missing-target", "file_missing_y", 0_i64, "body"],
            )
            .expect("insert broken target link");
    connection
            .execute(
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved, unresolved_reason, source_field) VALUES (?1, ?2, ?3, NULL, NULL, NULL, ?4, NULL, ?5)",
                rusqlite::params!["link_resolution_mismatch_2", source_a.file_id, "mismatch", 0_i64, "body"],
            )
            .expect("insert resolution mismatch link");

    let heal_result = IndexSelfHealService::default()
        .heal(temp.path(), &mut connection)
        .expect("run self-heal");

    assert!(heal_result.issues_detected > 0);
    assert!(heal_result.rows_deleted > 0);
    assert!(heal_result.rows_updated > 0);
    assert_eq!(heal_result.remaining_issues, 0);

    let report_after = IndexConsistencyChecker
        .check(temp.path(), &connection)
        .expect("run consistency checker after heal");
    assert!(report_after.issues.is_empty());
}

#[test]
fn self_heal_is_noop_for_consistent_index() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let heal_result = IndexSelfHealService::default()
        .heal(temp.path(), &mut connection)
        .expect("run self-heal");

    assert_eq!(heal_result.issues_detected, 0);
    assert_eq!(heal_result.rows_deleted, 0);
    assert_eq!(heal_result.rows_updated, 0);
    assert_eq!(heal_result.remaining_issues, 0);
}

#[test]
fn stale_cleanup_removes_rows_for_files_no_longer_in_vault() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::create_dir_all(temp.path().join("views")).expect("create views dir");
    fs::write(temp.path().join("notes/live.md"), "# Live").expect("write live note");
    fs::write(temp.path().join("notes/stale.md"), "# Stale").expect("write stale note");
    fs::write(temp.path().join("views/old.base"), "views:\n  - table").expect("write base");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    fs::remove_file(temp.path().join("notes/stale.md")).expect("remove stale note");
    fs::remove_file(temp.path().join("views/old.base")).expect("remove stale base");

    let result = StaleCleanupService
        .cleanup(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("run stale cleanup");

    assert_eq!(result.scanned_files, 1);
    assert_eq!(result.stale_files_removed, 2);

    let files = FilesRepository::list_all(&connection).expect("list files");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].normalized_path, "notes/live.md");

    let base_rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM bases", [], |row| row.get(0))
        .expect("count bases");
    assert_eq!(base_rows, 0);

    let summary_state = IndexStateRepository::get_by_key(&connection, "last_stale_cleanup_summary")
        .expect("get stale cleanup summary")
        .expect("summary exists");
    let summary_json: JsonValue =
        serde_json::from_str(&summary_state.value_json).expect("parse summary json");
    assert_eq!(
        summary_json.get("mode").and_then(JsonValue::as_str),
        Some("stale_cleanup")
    );
    assert_eq!(
        summary_json
            .get("scanned_files")
            .and_then(JsonValue::as_u64),
        Some(1)
    );
    assert_eq!(
        summary_json
            .get("stale_files_removed")
            .and_then(JsonValue::as_u64),
        Some(2)
    );
}

#[test]
fn stale_cleanup_is_noop_when_index_and_vault_match() {
    let temp = tempdir().expect("tempdir");
    fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
    fs::write(temp.path().join("notes/a.md"), "# A").expect("write note");

    let mut connection = Connection::open_in_memory().expect("open db");
    run_migrations(&mut connection).expect("run migrations");
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("seed full index");

    let result = StaleCleanupService
        .cleanup(temp.path(), &mut connection, CasePolicy::Sensitive)
        .expect("run stale cleanup");

    assert_eq!(result.scanned_files, 1);
    assert_eq!(result.stale_files_removed, 0);
    assert!(
        FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get note")
            .is_some()
    );
}
