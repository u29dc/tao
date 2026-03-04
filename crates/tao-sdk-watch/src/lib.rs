//! Reconciliation-backed watch adapter for filesystem drift repair.

use std::path::Path;

use rusqlite::Connection;
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

#[cfg(test)]
mod tests {
    use std::fs;

    use rusqlite::Connection;
    use tao_sdk_service::FullIndexService;
    use tao_sdk_storage::run_migrations;
    use tao_sdk_vault::CasePolicy;
    use tempfile::tempdir;

    use super::WatchReconcileService;

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
}
