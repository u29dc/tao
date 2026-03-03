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
