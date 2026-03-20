use super::*;

/// Result payload for drift reconciliation scanner workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationScanResult {
    /// Number of files discovered in current vault scan.
    pub scanned_files: u64,
    /// Number of paths detected as inserted.
    pub inserted_paths: u64,
    /// Number of paths detected as updated.
    pub updated_paths: u64,
    /// Number of paths detected as removed.
    pub removed_paths: u64,
    /// Total drift path count submitted for repair.
    pub drift_paths: u64,
    /// Number of incremental repair batches applied.
    pub batches_applied: u64,
    /// Number of file rows upserted by repair batches.
    pub upserted_files: u64,
    /// Number of file rows removed by repair batches.
    pub removed_files: u64,
    /// Number of links rebuilt by repair batches.
    pub links_reindexed: u64,
    /// Number of properties rebuilt by repair batches.
    pub properties_reindexed: u64,
    /// Number of bases rebuilt by repair batches.
    pub bases_reindexed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReconciliationDrift {
    scanned_files: u64,
    inserted_changed_paths: Vec<PathBuf>,
    updated_changed_paths: Vec<PathBuf>,
    removed_changed_paths: Vec<PathBuf>,
    inserted_paths: u64,
    updated_paths: u64,
    removed_paths: u64,
}

impl ReconciliationDrift {
    fn drift_paths(&self) -> u64 {
        (self.inserted_changed_paths.len()
            + self.updated_changed_paths.len()
            + self.removed_changed_paths.len()) as u64
    }

    fn into_changed_paths(self) -> Vec<PathBuf> {
        let mut changed_paths = Vec::with_capacity(
            self.inserted_changed_paths.len()
                + self.updated_changed_paths.len()
                + self.removed_changed_paths.len(),
        );
        changed_paths.extend(self.inserted_changed_paths);
        changed_paths.extend(self.updated_changed_paths);
        changed_paths.extend(self.removed_changed_paths);
        changed_paths
    }
}

/// Scanner that detects drift and repairs it via bounded incremental index batches.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReconciliationScannerService {
    coalesced: CoalescedBatchIndexService,
}

impl ReconciliationScannerService {
    /// Scan vault vs index metadata and return drift counts without mutating the index.
    pub fn scan(
        &self,
        vault_root: &Path,
        connection: &Connection,
        case_policy: CasePolicy,
    ) -> Result<ReconciliationScanResult, ReconciliationScanError> {
        let drift = collect_reconciliation_drift(vault_root, connection, case_policy)?;

        Ok(ReconciliationScanResult {
            scanned_files: drift.scanned_files,
            inserted_paths: drift.inserted_paths,
            updated_paths: drift.updated_paths,
            removed_paths: drift.removed_paths,
            drift_paths: drift.drift_paths(),
            batches_applied: 0,
            upserted_files: 0,
            removed_files: 0,
            links_reindexed: 0,
            properties_reindexed: 0,
            bases_reindexed: 0,
        })
    }

    /// Scan vault vs index metadata and repair missed watcher events.
    pub fn scan_and_repair(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
        max_batch_size: usize,
    ) -> Result<ReconciliationScanResult, ReconciliationScanError> {
        if max_batch_size == 0 {
            return Err(ReconciliationScanError::InvalidBatchSize { value: 0 });
        }

        let drift = collect_reconciliation_drift(vault_root, connection, case_policy)?;
        let drift_paths = drift.drift_paths();
        let scanned_files = drift.scanned_files;
        let inserted_paths = drift.inserted_paths;
        let updated_paths = drift.updated_paths;
        let removed_paths = drift.removed_paths;

        if drift_paths == 0 {
            return Ok(ReconciliationScanResult {
                scanned_files,
                inserted_paths,
                updated_paths,
                removed_paths,
                drift_paths: 0,
                batches_applied: 0,
                upserted_files: 0,
                removed_files: 0,
                links_reindexed: 0,
                properties_reindexed: 0,
                bases_reindexed: 0,
            });
        }

        let changed_paths = drift.into_changed_paths();
        let batch_result = self
            .coalesced
            .apply_coalesced(
                vault_root,
                connection,
                &changed_paths,
                max_batch_size,
                case_policy,
            )
            .map_err(|source| ReconciliationScanError::RepairBatch {
                source: Box::new(source),
            })?;

        Ok(ReconciliationScanResult {
            scanned_files,
            inserted_paths,
            updated_paths,
            removed_paths,
            drift_paths,
            batches_applied: batch_result.batches_applied,
            upserted_files: batch_result.upserted_files,
            removed_files: batch_result.removed_files,
            links_reindexed: batch_result.links_reindexed,
            properties_reindexed: batch_result.properties_reindexed,
            bases_reindexed: batch_result.bases_reindexed,
        })
    }
}

fn collect_reconciliation_drift(
    vault_root: &Path,
    connection: &Connection,
    case_policy: CasePolicy,
) -> Result<ReconciliationDrift, ReconciliationScanError> {
    let scanner = VaultScanService::from_root(vault_root, case_policy).map_err(|source| {
        ReconciliationScanError::CreateScanner {
            source: Box::new(source),
        }
    })?;
    let manifest = scanner
        .scan()
        .map_err(|source| ReconciliationScanError::Scan {
            source: Box::new(source),
        })?;

    let existing = FilesRepository::list_reconcile(connection).map_err(|source| {
        ReconciliationScanError::ListIndexedFiles {
            source: Box::new(source),
        }
    })?;

    let mut inserted_changed_paths = Vec::new();
    let mut updated_changed_paths = Vec::new();
    let mut removed_changed_paths = Vec::new();
    let mut inserted_paths = 0_u64;
    let mut updated_paths = 0_u64;
    let mut removed_paths = 0_u64;
    let mut scan_index = 0_usize;
    let mut existing_index = 0_usize;

    while scan_index < manifest.entries.len() && existing_index < existing.len() {
        let scanned = &manifest.entries[scan_index];
        let indexed = &existing[existing_index];
        let order = scanned
            .match_key
            .cmp(&indexed.match_key)
            .then(scanned.normalized.cmp(&indexed.normalized_path));

        match order {
            std::cmp::Ordering::Less => {
                inserted_changed_paths.push(PathBuf::from(&scanned.normalized));
                inserted_paths += 1;
                scan_index += 1;
            }
            std::cmp::Ordering::Greater => {
                removed_changed_paths.push(PathBuf::from(&indexed.normalized_path));
                removed_paths += 1;
                existing_index += 1;
            }
            std::cmp::Ordering::Equal => {
                if !indexed_record_matches_manifest_entry(indexed, scanned) {
                    updated_changed_paths.push(PathBuf::from(&scanned.normalized));
                    updated_paths += 1;
                }
                scan_index += 1;
                existing_index += 1;
            }
        }
    }

    for scanned in &manifest.entries[scan_index..] {
        inserted_changed_paths.push(PathBuf::from(&scanned.normalized));
        inserted_paths += 1;
    }

    for indexed in &existing[existing_index..] {
        removed_changed_paths.push(PathBuf::from(&indexed.normalized_path));
        removed_paths += 1;
    }

    Ok(ReconciliationDrift {
        scanned_files: manifest.entries.len() as u64,
        inserted_changed_paths,
        updated_changed_paths,
        removed_changed_paths,
        inserted_paths,
        updated_paths,
        removed_paths,
    })
}

fn indexed_record_matches_manifest_entry(
    indexed: &tao_sdk_storage::FileReconcileRecord,
    entry: &tao_sdk_vault::VaultManifestEntry,
) -> bool {
    indexed.normalized_path == entry.normalized
        && indexed.match_key == entry.match_key
        && entry
            .absolute
            .to_str()
            .is_some_and(|absolute| indexed.absolute_path == absolute)
        && indexed.size_bytes == entry.size_bytes
        && indexed.modified_unix_ms == entry.modified_unix_ms
}
