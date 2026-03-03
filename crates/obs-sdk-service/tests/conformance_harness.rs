use std::fs;
use std::path::{Path, PathBuf};

use obs_sdk_service::{
    BacklinkGraphService, FullIndexService, HealthSnapshotService, WatcherStatus,
};
use obs_sdk_storage::{FilesRepository, run_migrations};
use obs_sdk_vault::CasePolicy;
use rusqlite::Connection;

fn fixture_vault_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../qa/fixtures/conformance-vault")
        .canonicalize()
        .expect("canonicalize conformance fixture vault")
}

fn copy_fixture_vault() -> tempfile::TempDir {
    let temp = tempfile::tempdir().expect("create tempdir");
    copy_dir_recursive(&fixture_vault_root(), temp.path()).expect("copy fixture vault");
    temp
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

fn collect_link_resolution_snapshot(
    connection: &Connection,
) -> Vec<(String, String, Option<String>)> {
    let mut statement = connection
        .prepare(
            r#"
SELECT
  sf.normalized_path AS source_path,
  l.raw_target,
  tf.normalized_path AS resolved_path
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
ORDER BY sf.normalized_path ASC, l.raw_target ASC, resolved_path ASC
"#,
        )
        .expect("prepare link resolution snapshot query");
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>("source_path")?,
                row.get::<_, String>("raw_target")?,
                row.get::<_, Option<String>>("resolved_path")?,
            ))
        })
        .expect("query link resolution snapshot rows");
    rows.map(|row| row.expect("decode link resolution snapshot row"))
        .collect()
}

#[test]
fn integration_harness_indexes_fixture_vault_end_to_end() {
    let fixture = copy_fixture_vault();
    let db_path = fixture.path().join("obs.sqlite");

    let mut connection = Connection::open(db_path).expect("open sqlite");
    run_migrations(&mut connection).expect("run migrations");

    let index_result = FullIndexService::default()
        .rebuild(fixture.path(), &mut connection, CasePolicy::Sensitive)
        .expect("rebuild fixture vault");

    assert!(index_result.indexed_files >= 9);
    assert!(index_result.markdown_files >= 7);
    assert!(index_result.links_total >= 4);
    assert!(index_result.unresolved_links >= 1);
    assert!(index_result.bases_total >= 2);

    let snapshot = HealthSnapshotService
        .snapshot(fixture.path(), &connection, 0, WatcherStatus::Stopped)
        .expect("snapshot fixture vault");
    assert!(snapshot.db_healthy);
    assert_eq!(snapshot.watcher_status, "stopped");
    assert!(snapshot.files_total >= 10);
    assert!(snapshot.markdown_files >= 7);

    let alpha = FilesRepository::get_by_normalized_path(&connection, "notes/alpha.md")
        .expect("lookup alpha")
        .expect("alpha indexed");
    let beta = FilesRepository::get_by_normalized_path(&connection, "notes/beta.md")
        .expect("lookup beta")
        .expect("beta indexed");
    assert_ne!(alpha.file_id, beta.file_id);

    let graph = BacklinkGraphService;
    let outgoing = graph
        .outgoing_for_path(&connection, "notes/alpha.md")
        .expect("query outgoing links");
    assert!(
        outgoing
            .iter()
            .any(|row| row.raw_target.contains("missing-note"))
    );
    assert!(
        outgoing
            .iter()
            .any(|row| row.resolved_path.as_deref() == Some("notes/beta.md"))
    );

    let backlinks = graph
        .backlinks_for_path(&connection, "notes/beta.md")
        .expect("query backlinks");
    assert!(
        backlinks
            .iter()
            .any(|row| row.source_path == "notes/alpha.md")
    );
}

#[test]
fn resolver_outputs_are_deterministic_across_repeated_rebuilds() {
    let fixture = copy_fixture_vault();
    let db_path = fixture.path().join("obs.sqlite");

    let mut connection = Connection::open(db_path).expect("open sqlite");
    run_migrations(&mut connection).expect("run migrations");

    FullIndexService::default()
        .rebuild(fixture.path(), &mut connection, CasePolicy::Sensitive)
        .expect("rebuild fixture vault first pass");
    let first_snapshot = collect_link_resolution_snapshot(&connection);

    FullIndexService::default()
        .rebuild(fixture.path(), &mut connection, CasePolicy::Sensitive)
        .expect("rebuild fixture vault second pass");
    let second_snapshot = collect_link_resolution_snapshot(&connection);

    assert_eq!(first_snapshot, second_snapshot);
    assert!(
        first_snapshot
            .iter()
            .any(|(source, raw, resolved)| source == "notes/alpha.md"
                && raw == "apple"
                && resolved.as_deref() == Some("notes/apple.md"))
    );
}
