//! Incremental Base invalidation retains independent projections and matches a clean rebuild.
use std::{fs, path::Path};

use rusqlite::Connection;
use tao_sdk_service::{FullIndexService, SearchCorpusService};
use tao_sdk_storage::{FilesRepository, run_migrations};
use tao_sdk_vault::CasePolicy;

fn fixture(bases: &[(&str, &str)]) -> (tempfile::TempDir, Connection) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/base-invalidation-tests");
    fs::create_dir_all(&root).unwrap();
    let temporary = tempfile::tempdir_in(root).unwrap();
    for (path, text) in [
        (
            "left/a.md",
            "---\nlabel: Left evidence\nparent: '[[/right/b]]'\n---\nLeft body\n",
        ),
        (
            "right/b.md",
            "---\nlabel: Right evidence\n---\nRight body\n",
        ),
        ("outside.md", "Outside body\n"),
    ]
    .into_iter()
    .chain(bases.iter().copied())
    {
        let path = temporary.path().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }
    let mut connection = Connection::open_in_memory().unwrap();
    connection.execute_batch("PRAGMA foreign_keys=ON").unwrap();
    run_migrations(&mut connection).unwrap();
    FullIndexService::default()
        .rebuild(temporary.path(), &mut connection, CasePolicy::Sensitive)
        .unwrap();
    (temporary, connection)
}

fn id(connection: &Connection, path: &str) -> String {
    FilesRepository::get_by_normalized_path(connection, path)
        .unwrap()
        .unwrap()
        .file_id
}

fn touch(connection: &Connection, file_id: &str) {
    connection.execute("UPDATE canonical_documents SET raw_text=raw_text||' changed',body_text=body_text||' changed' WHERE file_id=?1", [file_id]).unwrap();
}

fn base_snapshot(connection: &Connection) -> Vec<(String, String)> {
    let mut result = connection.prepare("SELECT segment_id,payload_json FROM search_segments WHERE surface='bases' ORDER BY segment_id").unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap().collect::<Result<Vec<(String, String)>, _>>().unwrap();
    result.extend(connection.prepare("SELECT alias_id,json_array(file_id,normalized_path,alias_norm,alias_compact,source,weight) FROM search_aliases WHERE surface='bases' ORDER BY alias_id").unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap().collect::<Result<Vec<(String, String)>, _>>().unwrap());
    result
}

fn assert_rebuild_parity(connection: &mut Connection) {
    let incremental = base_snapshot(connection);
    SearchCorpusService
        .rebuild_atomic(connection, CasePolicy::Sensitive)
        .unwrap();
    assert_eq!(incremental, base_snapshot(connection));
}

#[test]
fn disjoint_scopes_skip_unrelated_views_and_preserve_their_rows_and_aliases() {
    let (_temporary, mut connection) = fixture(&[
        (
            "left.base",
            "views:\n - name: Left\n   source: left\n   columns: [path, label]\n",
        ),
        (
            "right.base",
            "views:\n - name: Right\n   source: right\n   columns: [path, label]\n",
        ),
    ]);
    let before = base_snapshot(&connection);
    let outside = id(&connection, "outside.md");
    touch(&connection, &outside);
    let report = SearchCorpusService
        .refresh_files_atomic(&mut connection, &[outside], CasePolicy::Sensitive)
        .unwrap();
    assert_eq!(report.base_views_evaluated, 0);
    assert_eq!(before, base_snapshot(&connection));

    let left = id(&connection, "left/a.md");
    connection.execute("UPDATE properties SET value_json='\"Revised left evidence\"' WHERE file_id=?1 AND key='label'", [&left]).unwrap();
    let report = SearchCorpusService
        .refresh_files_atomic(&mut connection, &[left], CasePolicy::Sensitive)
        .unwrap();
    assert_eq!(report.base_views_evaluated, 1);
    assert!(
        base_snapshot(&connection)
            .iter()
            .any(|(_, value)| value.contains("Right evidence"))
    );
    assert!(
        base_snapshot(&connection)
            .iter()
            .any(|(_, value)| value.contains("Revised left evidence"))
    );
    assert_rebuild_parity(&mut connection);
}

#[test]
fn shared_aliases_keep_view_ownership_and_definition_changes_remove_obsolete_views() {
    let (_temporary, mut connection) = fixture(&[
        (
            "first.base",
            "views:\n - name: Shared\n   source: left\n   columns: [label]\n - name: Right\n   source: right\n   columns: [label]\n",
        ),
        (
            "second.base",
            "views:\n - name: Shared\n   source: left\n   columns: [label]\n",
        ),
    ]);
    let first = id(&connection, "first.base");
    let document = tao_sdk_bases::parse_base_document(
        "views:\n - name: Right\n   source: right\n   columns: [label]\n",
    )
    .unwrap();
    connection
        .execute(
            "UPDATE bases SET config_json=?1 WHERE file_id=?2",
            rusqlite::params![serde_json::to_string(&document).unwrap(), first],
        )
        .unwrap();
    let report = SearchCorpusService
        .refresh_files_atomic(&mut connection, &[first], CasePolicy::Sensitive)
        .unwrap();
    assert_eq!(report.base_views_evaluated, 1);
    let aliases: u64 = connection.query_row("SELECT COUNT(*) FROM search_aliases WHERE surface='bases' AND alias_norm='left evidence'", [], |row| row.get(0)).unwrap();
    assert_eq!(
        aliases, 1,
        "the independent second view still owns this alias"
    );
    assert_rebuild_parity(&mut connection);

    let second = id(&connection, "second.base");
    FilesRepository::delete_by_id(&connection, &second).unwrap();
    SearchCorpusService
        .refresh_files_atomic(&mut connection, &[second], CasePolicy::Sensitive)
        .unwrap();
    let aliases: u64 = connection.query_row("SELECT COUNT(*) FROM search_aliases WHERE surface='bases' AND alias_norm='left evidence'", [], |row| row.get(0)).unwrap();
    assert_eq!(
        aliases, 0,
        "deleting a Base removes aliases owned by its source rows"
    );
    assert_rebuild_parity(&mut connection);
}

#[test]
fn views_are_selected_individually_and_old_paths_invalidate_renamed_sources() {
    let (_temporary, mut connection) = fixture(&[(
        "both.base",
        "views:\n - name: Left\n   source: left\n   columns: [path, label]\n - name: Right\n   source: right\n   columns: [path, label]\n",
    )]);
    let left = id(&connection, "left/a.md");
    touch(&connection, &left);
    let report = SearchCorpusService
        .refresh_files_atomic(
            &mut connection,
            std::slice::from_ref(&left),
            CasePolicy::Sensitive,
        )
        .unwrap();
    assert_eq!(report.base_views_evaluated, 1);
    assert_rebuild_parity(&mut connection);
    connection.execute("UPDATE files SET normalized_path='right/renamed.md',match_key='right/renamed.md' WHERE file_id=?1", [&left]).unwrap();
    let report = SearchCorpusService
        .refresh_files_atomic(&mut connection, &[left], CasePolicy::Sensitive)
        .unwrap();
    assert_eq!(report.base_views_evaluated, 2);
    let old_rows: u64 = connection.query_row("SELECT COUNT(*) FROM search_segments WHERE field='base_row' AND json_extract(payload_json,'$.view_name')='Left'", [], |row| row.get(0)).unwrap();
    assert_eq!(old_rows, 0);
    assert_rebuild_parity(&mut connection);
}

#[test]
fn global_and_external_relation_views_remain_conservative() {
    let (_temporary, mut connection) = fixture(&[
        (
            "global.base",
            "views:\n - name: Global\n   columns: [label]\n",
        ),
        (
            "relation.base",
            "views:\n - name: Relation\n   source: left\n   columns: [label, parent]\n   relations: [parent]\n",
        ),
        (
            "scoped.base",
            "views:\n - name: Right\n   source: right\n   columns: [label]\n",
        ),
    ]);
    let outside = id(&connection, "outside.md");
    touch(&connection, &outside);
    let report = SearchCorpusService
        .refresh_files_atomic(&mut connection, &[outside], CasePolicy::Sensitive)
        .unwrap();
    assert_eq!(report.base_views_evaluated, 2);
    assert_rebuild_parity(&mut connection);
}

#[test]
fn changed_corpus_version_forces_complete_rebuild_before_partial_publication() {
    let (_temporary, mut connection) = fixture(&[
        (
            "left.base",
            "views:\n - name: Left\n   source: left\n   columns: [label]\n",
        ),
        (
            "right.base",
            "views:\n - name: Right\n   source: right\n   columns: [label]\n",
        ),
    ]);
    connection
        .execute(
            "UPDATE index_state SET value_json='2' WHERE key='search_corpus_schema_version'",
            [],
        )
        .unwrap();
    connection.execute("UPDATE search_aliases SET source='base_row' WHERE surface='bases' AND json_valid(source)", []).unwrap();
    let outside = id(&connection, "outside.md");
    touch(&connection, &outside);
    let report = SearchCorpusService
        .refresh_files_atomic(&mut connection, &[outside], CasePolicy::Sensitive)
        .unwrap();
    assert_eq!(report.base_views_evaluated, 2);
    let old_aliases: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM search_aliases WHERE surface='bases' AND source='base_row'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(old_aliases, 0);
    assert_rebuild_parity(&mut connection);
}
