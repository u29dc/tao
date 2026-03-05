//! Reconciliation-backed watch adapter for filesystem drift repair.

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tao_sdk_service::ReconciliationScannerService;
use tao_sdk_vault::CasePolicy;
use thiserror::Error;

/// Result payload for one reconciliation-backed watch pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchReconcileResult {
    /// Number of files scanned on disk.
    pub scanned_files: u64,
    /// Number of inserted paths detected.
    pub inserted_paths: u64,
    /// Number of updated paths detected.
    pub updated_paths: u64,
    /// Number of drift paths repaired.
    pub drift_paths: u64,
    /// Number of coalesced batches applied.
    pub batches_applied: u64,
    /// Number of files upserted by repair.
    pub upserted_files: u64,
    /// Number of files removed from index.
    pub removed_files: u64,
    /// Number of links reindexed.
    pub links_reindexed: u64,
    /// Number of properties reindexed.
    pub properties_reindexed: u64,
    /// Number of bases reindexed.
    pub bases_reindexed: u64,
}

/// Adapter exposing reconciliation scan as a watch-compatible primitive.
#[derive(Debug, Default, Clone, Copy)]
pub struct WatchReconcileService {
    scanner: ReconciliationScannerService,
}

impl WatchReconcileService {
    /// Run one drift-scan/repair pass for the provided vault root.
    pub fn reconcile_once(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
    ) -> Result<WatchReconcileResult, WatchReconcileError> {
        const DEFAULT_MAX_BATCH_SIZE: usize = 128;
        let result = self
            .scanner
            .scan_and_repair(vault_root, connection, case_policy, DEFAULT_MAX_BATCH_SIZE)
            .map_err(|source| WatchReconcileError::ScanAndRepair { source })?;
        Ok(WatchReconcileResult {
            scanned_files: result.scanned_files,
            inserted_paths: result.inserted_paths,
            updated_paths: result.updated_paths,
            drift_paths: result.drift_paths,
            batches_applied: result.batches_applied,
            upserted_files: result.upserted_files,
            removed_files: result.removed_files,
            links_reindexed: result.links_reindexed,
            properties_reindexed: result.properties_reindexed,
            bases_reindexed: result.bases_reindexed,
        })
    }
}

/// Filesystem monitor that increments one generation counter when vault content changes.
pub struct VaultChangeMonitor {
    generation: Arc<AtomicU64>,
    _watcher: RecommendedWatcher,
}

impl std::fmt::Debug for VaultChangeMonitor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VaultChangeMonitor")
            .field("generation", &self.generation())
            .finish()
    }
}

impl VaultChangeMonitor {
    /// Start monitoring one vault root recursively for external content changes.
    pub fn start(vault_root: &Path) -> Result<Self, VaultChangeMonitorError> {
        let canonical_root = std::fs::canonicalize(vault_root).map_err(|source| {
            VaultChangeMonitorError::CanonicalizeRoot {
                path: vault_root.to_path_buf(),
                source,
            }
        })?;
        let ignored_runtime_root = canonical_root.join(".tao");
        let generation = Arc::new(AtomicU64::new(0));
        let generation_ref = Arc::clone(&generation);
        let ignored_runtime_root_ref = ignored_runtime_root.clone();

        let mut watcher =
            notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
                let should_mark_dirty = match result {
                    Ok(event) => event
                        .paths
                        .iter()
                        .any(|path| !path.starts_with(&ignored_runtime_root_ref)),
                    Err(_) => true,
                };
                if should_mark_dirty {
                    generation_ref.fetch_add(1, Ordering::Relaxed);
                }
            })
            .map_err(|source| VaultChangeMonitorError::CreateWatcher { source })?;
        watcher
            .watch(&canonical_root, RecursiveMode::Recursive)
            .map_err(|source| VaultChangeMonitorError::WatchRoot {
                path: canonical_root.clone(),
                source,
            })?;

        Ok(Self {
            generation,
            _watcher: watcher,
        })
    }

    /// Return the current change generation.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

/// Watch adapter failures.
#[derive(Debug, Error)]
pub enum WatchReconcileError {
    /// Reconciliation scanner failed.
    #[error("watch reconcile scan failed: {source}")]
    ScanAndRepair {
        /// Underlying reconciliation error.
        #[source]
        source: tao_sdk_service::ReconciliationScanError,
    },
}

/// Vault change monitor failures.
#[derive(Debug, Error)]
pub enum VaultChangeMonitorError {
    /// Vault root canonicalization failed.
    #[error("failed to canonicalize vault root '{path}': {source}")]
    CanonicalizeRoot {
        /// Input vault path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Watcher creation failed.
    #[error("failed to create filesystem watcher: {source}")]
    CreateWatcher {
        /// Watcher backend error.
        #[source]
        source: notify::Error,
    },
    /// Registering the vault root with the watcher failed.
    #[error("failed to watch vault root '{path}': {source}")]
    WatchRoot {
        /// Canonical watched root.
        path: PathBuf,
        /// Watcher backend error.
        #[source]
        source: notify::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::thread;
    use std::time::{Duration, Instant};

    use rusqlite::Connection;
    use tao_sdk_service::FullIndexService;
    use tao_sdk_storage::run_migrations;
    use tao_sdk_vault::CasePolicy;
    use tempfile::tempdir;

    use super::{VaultChangeMonitor, WatchReconcileService};

    #[test]
    fn reconcile_once_detects_changed_paths() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(vault.join("notes")).expect("create notes");
        fs::write(vault.join("notes/a.md"), "# A").expect("write seed");

        let mut connection = Connection::open(temp.path().join("index.sqlite")).expect("open db");
        run_migrations(&mut connection).expect("migrate");
        FullIndexService::default()
            .rebuild(&vault, &mut connection, CasePolicy::Sensitive)
            .expect("seed index");

        fs::write(vault.join("notes/a.md"), "# A\nupdated").expect("update note");

        let result = WatchReconcileService::default()
            .reconcile_once(&vault, &mut connection, CasePolicy::Sensitive)
            .expect("reconcile");
        assert!(result.updated_paths >= 1);
        assert!(result.upserted_files >= 1);
    }

    #[test]
    fn change_monitor_marks_generation_when_vault_content_changes() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(vault.join("notes")).expect("create notes");
        fs::write(vault.join("notes/a.md"), "# A").expect("write seed");

        let monitor = VaultChangeMonitor::start(&vault).expect("start monitor");
        let before = monitor.generation();
        fs::write(vault.join("notes/a.md"), "# A\nupdated").expect("update note");

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if monitor.generation() > before {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }

        panic!("expected watcher generation to advance after note update");
    }
}
