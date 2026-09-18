//! Structural work regressions and an opt-in measured SDK refresh qualification.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rusqlite::Connection;
use serde_json::Value;
use tao_sdk_storage::{IndexStateRepository, run_migrations};
use tao_sdk_vault::CasePolicy;
use tempfile::{TempDir, tempdir};

use crate::{
    IndexRefreshMode, IndexRefreshOptions, IndexRefreshOutcome, IndexRefreshService,
    ReconciliationScanMode,
};

const NOTE_COUNT: usize = 1_000;
const NOTE_BYTES: usize = 3_776;
const CHANGE_COUNTS: [usize; 4] = [1, 127, 128, 129];

struct Fixture {
    _temporary: TempDir,
    root: PathBuf,
    connection: Connection,
    revision: usize,
}

impl Fixture {
    fn new() -> (Self, f64) {
        let temporary = tempdir().expect("isolated repository-local test directory");
        let root = temporary.path().join("vault");
        fs::create_dir(&root).expect("create fixture vault");
        for index in 0..NOTE_COUNT {
            write_note(&root, index, 0);
        }
        // Keep SQLite outside the fixture vault so its writes never enter the inventory.
        // Normal production pragmas (WAL, NORMAL sync and bounded cache) are applied.
        let mut connection = Connection::open(temporary.path().join("index.sqlite"))
            .expect("open isolated on-disk database");
        run_migrations(&mut connection).expect("initialize production database settings");
        let started = Instant::now();
        let initial = refresh(&root, &mut connection);
        let initial_index_ms = started.elapsed().as_secs_f64() * 1_000.0;
        assert_eq!(initial.mode, IndexRefreshMode::FullRebuild);
        assert_eq!(initial.upserted_files, NOTE_COUNT as u64);
        (
            Self {
                _temporary: temporary,
                root,
                connection,
                revision: 0,
            },
            initial_index_ms,
        )
    }

    fn update(&mut self, changed_files: usize) -> (f64, IndexRefreshOutcome, Value) {
        self.revision += 1;
        for index in 0..changed_files {
            write_note(&self.root, index, self.revision);
        }
        // Timed work includes scan/hash verification, preparation, graph/search
        // maintenance and publication. Fixture edits and reading counters are excluded.
        let started = Instant::now();
        let outcome = refresh(&self.root, &mut self.connection);
        let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let summary =
            IndexStateRepository::get_by_key(&self.connection, "last_incremental_index_summary")
                .expect("read publication summary")
                .expect("published incremental summary");
        let summary: Value = serde_json::from_str(&summary.value_json).expect("summary JSON");
        (elapsed_ms, outcome, summary)
    }
}

fn write_note(root: &Path, index: usize, revision: usize) {
    // Stable-length real Markdown carries metadata, an alias, a task, headings and
    // a ring link. Updating a contiguous prefix also affects its incoming neighbor.
    let mut content = format!(
        "---\ntitle: Work fixture {index:04}\nstatus: active\ntags: [work-qualification]\naliases: [Work fixture {index:04}]\n---\n# Work fixture {index:04}\nRevision {revision:08}\n\n[[note-{:04}]]\n\n- [ ] Verify the indexed source revision.\n\n## Findings\n",
        (index + 1) % NOTE_COUNT,
    );
    const PARAGRAPH: &str = "Stable vault knowledge connects document evidence, explicit metadata, tasks, headings, aliases, and graph relationships.\n\n";
    while content.len() + PARAGRAPH.len() < NOTE_BYTES {
        content.push_str(PARAGRAPH);
    }
    content.extend(std::iter::repeat_n('x', NOTE_BYTES - content.len() - 1));
    content.push('\n');
    assert_eq!(content.len(), NOTE_BYTES);
    fs::write(root.join(format!("note-{index:04}.md")), content).expect("write source revision");
}

fn refresh(root: &Path, connection: &mut Connection) -> IndexRefreshOutcome {
    IndexRefreshService
        .refresh(
            root,
            connection,
            CasePolicy::Sensitive,
            IndexRefreshOptions {
                scan_mode: ReconciliationScanMode::VerifyContentHashes,
                max_batch_size: 128,
            },
        )
        .expect("refresh stable fixture")
}

fn assert_changed_work(changed_files: usize, outcome: &IndexRefreshOutcome, summary: &Value) {
    assert_eq!(outcome.mode, IndexRefreshMode::Reconcile);
    assert_eq!(outcome.drift_paths, changed_files as u64);
    assert_eq!(outcome.upserted_files, changed_files as u64);
    assert_eq!(outcome.removed_files, 0);
    assert_eq!(summary["source_errors"], 0);
    let work = &summary["work"];
    assert_eq!(work["markdown_parses"], changed_files as u64);
    assert_eq!(work["source_captures"], changed_files as u64);
    assert_eq!(
        work["source_bytes_captured"],
        (changed_files * NOTE_BYTES) as u64
    );
    assert_eq!(work["canonical_structures_loaded"], NOTE_COUNT as u64);
    assert_eq!(work["canonical_structures_reparsed"], 0);
    assert_eq!(work["publication_transactions"], 1);
    assert!(work["graph_sources_resolved"].as_u64().expect("graph work") <= NOTE_COUNT as u64);
    assert!(work["prepared_bytes"].as_u64().expect("preparation charge") <= 256 * 1024 * 1024);
}

#[test]
fn changed_source_work_does_not_repeat_at_the_128_path_boundary() {
    let (mut fixture, _) = Fixture::new();
    for changed_files in CHANGE_COUNTS {
        let (_, outcome, summary) = fixture.update(changed_files);
        assert_changed_work(changed_files, &outcome, &summary);
    }
    // A subsequent verified scan must converge, with no phantom follow-up edits.
    let clean = refresh(&fixture.root, &mut fixture.connection);
    assert_eq!(clean.drift_paths, 0);
    assert_eq!(clean.upserted_files, 0);
}

#[test]
#[cfg(not(debug_assertions))]
#[ignore = "opt-in release timing qualification; writes a local .benchmarks JSON report"]
fn qualify_incremental_work_at_the_128_path_boundary() {
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    let (mut fixture, initial_index_ms) = Fixture::new();
    let mut warmups = Vec::new();
    for changed_files in CHANGE_COUNTS {
        let (elapsed_ms, outcome, summary) = fixture.update(changed_files);
        assert_changed_work(changed_files, &outcome, &summary);
        warmups.push(json!({"changed_files": changed_files, "elapsed_ms": elapsed_ms}));
    }
    let mut samples = Vec::new();
    // Rotate condition order between rounds to reduce systematic cache/time bias.
    for round in 0..5 {
        for position in 0..CHANGE_COUNTS.len() {
            let changed_files = CHANGE_COUNTS[(position + round) % CHANGE_COUNTS.len()];
            let (elapsed_ms, outcome, summary) = fixture.update(changed_files);
            assert_changed_work(changed_files, &outcome, &summary);
            samples.push(json!({
                "round": round + 1,
                "position": position + 1,
                "changed_files": changed_files,
                "elapsed_ms": elapsed_ms,
                "batches_applied": outcome.batches_applied,
                "search_corpus_refresh": outcome.search_corpus_refresh.as_str(),
                "work": summary["work"],
            }));
        }
    }
    let medians = CHANGE_COUNTS.map(|changed_files| {
        let mut values = samples.iter()
            .filter(|sample| sample["changed_files"] == changed_files)
            .map(|sample| sample["elapsed_ms"].as_f64().expect("elapsed milliseconds"))
            .collect::<Vec<_>>();
        values.sort_by(f64::total_cmp);
        json!({"changed_files": changed_files, "runs": values.len(), "median_ms": values[values.len() / 2], "min_ms": values[0], "max_ms": values[values.len() - 1]})
    });
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory")
        .parent()
        .expect("repository root");
    let output = repository.join(".benchmarks/indexing-qualification");
    fs::create_dir_all(&output).expect("create report directory");
    let report = json!({
        "schema_version": 1,
        "measured_unix_seconds": SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_secs(),
        "build_profile": "release",
        "execution": "direct SDK refresh with persistent on-disk SQLite connection; no daemon/output cache",
        "fixture": {"markdown_files": NOTE_COUNT, "bytes_per_file": NOTE_BYTES, "total_source_bytes": NOTE_COUNT * NOTE_BYTES, "graph": "directed ring; one link per source", "metadata": "title, status, tags and alias", "source_locations": "headings and one task per source"},
        "scan_mode": "verify_content_hashes",
        "max_batch_size": 128,
        "initial_full_index_ms": initial_index_ms,
        "warmup_updates_per_condition": 1,
        "measured_rounds_per_condition": 5,
        "measurement_scope": "source edits excluded; includes root scan, content verification, preparation, dependency resolution, search refresh and SQLite publication",
        "counter_scope": "apply-stage authoritative captures consumed for publication only; scanner verification reads of unchanged sources are excluded from source_captures/source_bytes_captured. Memory counters are retained preparation accounting, not process RSS.",
        "interpretation": "Structural assertions verify changed-source parse/capture counts and one canonical-structure load/publication across 127/128/129. Latencies are observations, not pass/fail thresholds or a historical-baseline comparison.",
        "warmups": warmups,
        "medians": medians,
        "samples": samples,
    });
    let path = output.join("incremental-work.json");
    fs::write(
        &path,
        serde_json::to_vec_pretty(&report).expect("serialize report"),
    )
    .expect("write report");
    println!("Qualification report: {}", path.display());
    println!(
        "{}",
        serde_json::to_string_pretty(&report["medians"]).expect("median JSON")
    );
}
