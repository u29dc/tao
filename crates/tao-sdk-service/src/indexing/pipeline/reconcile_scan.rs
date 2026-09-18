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
    /// Derived search corpus refresh mode applied by repair batches.
    pub search_corpus_refresh: SearchCorpusRefreshMode,
}

/// An authoritative inventory operation. Removal is independent of physical existence.
#[derive(Debug, Clone)]
pub(crate) enum IndexChange {
    Upsert {
        entry: Box<VaultManifestEntry>,
        captured: Option<CapturedFile>,
    },
    Remove {
        normalized_path: String,
    },
}

#[derive(Debug)]
pub(crate) struct ReconciliationPlan {
    pub(crate) scanned_files: u64,
    pub(crate) generation: i64,
    pub(crate) changes: Vec<IndexChange>,
    pub(crate) inserted_paths: u64,
    pub(crate) updated_paths: u64,
    pub(crate) removed_paths: u64,
}

impl ReconciliationPlan {
    pub(crate) fn drift_paths(&self) -> u64 {
        self.changes.len() as u64
    }
}

/// Drift detection policy for reconciliation scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationScanMode {
    /// Compare file identity, path, size, and modified time only.
    MetadataOnly,
    /// Also verify persisted hashes for supported Markdown, Base, TXT and PDF content.
    VerifyContentHashes,
}

impl ReconciliationScanMode {
    fn verifies_content_hashes(self) -> bool {
        matches!(self, Self::VerifyContentHashes)
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
        self.scan_with_mode(
            vault_root,
            connection,
            case_policy,
            ReconciliationScanMode::MetadataOnly,
        )
    }

    /// Scan vault vs index metadata and return drift counts with an explicit scan mode.
    pub fn scan_with_mode(
        &self,
        vault_root: &Path,
        connection: &Connection,
        case_policy: CasePolicy,
        scan_mode: ReconciliationScanMode,
    ) -> Result<ReconciliationScanResult, ReconciliationScanError> {
        let drift = self.plan(vault_root, connection, case_policy, scan_mode)?;

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
            search_corpus_refresh: SearchCorpusRefreshMode::None,
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
        self.scan_and_repair_with_mode(
            vault_root,
            connection,
            case_policy,
            max_batch_size,
            ReconciliationScanMode::MetadataOnly,
        )
    }

    /// Scan vault vs index with an explicit mode and repair missed watcher events.
    pub fn scan_and_repair_with_mode(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
        max_batch_size: usize,
        scan_mode: ReconciliationScanMode,
    ) -> Result<ReconciliationScanResult, ReconciliationScanError> {
        let _publication =
            crate::publication_lock::PublicationGuard::acquire(connection).map_err(|source| {
                ReconciliationScanError::Content {
                    source: Box::new(crate::ContentError::Io(source)),
                }
            })?;
        if max_batch_size == 0 {
            return Err(ReconciliationScanError::InvalidBatchSize { value: 0 });
        }

        let drift = self.plan(vault_root, connection, case_policy, scan_mode)?;
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
                search_corpus_refresh: SearchCorpusRefreshMode::None,
            });
        }

        let batch_result = self
            .coalesced
            .apply_plan(
                vault_root,
                connection,
                drift.changes,
                Some(drift.generation),
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
            search_corpus_refresh: batch_result.search_corpus_refresh,
        })
    }
}

impl ReconciliationScannerService {
    pub(crate) fn plan(
        &self,
        vault_root: &Path,
        connection: &Connection,
        case_policy: CasePolicy,
        scan_mode: ReconciliationScanMode,
    ) -> Result<ReconciliationPlan, ReconciliationScanError> {
        let generation = tao_sdk_storage::IndexGenerationRepository::get(connection)
            .map_err(|source| ReconciliationScanError::Content {
                source: Box::new(crate::ContentError::Storage(source)),
            })?
            .canonical_generation;
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
        let fingerprints =
            FileFingerprintService::from_root(vault_root, case_policy).map_err(|source| {
                ReconciliationScanError::CreateScanner {
                    source: Box::new(source),
                }
            })?;
        let existing = FilesRepository::list_all(connection).map_err(|source| {
            ReconciliationScanError::ListIndexedFiles {
                source: Box::new(source),
            }
        })?;
        let mut existing = existing
            .into_iter()
            .map(|record| (record.normalized_path.clone(), record))
            .collect::<HashMap<_, _>>();
        let mut plan = ReconciliationPlan {
            generation,
            scanned_files: manifest.entries.len() as u64,
            changes: Vec::new(),
            inserted_paths: 0,
            updated_paths: 0,
            removed_paths: 0,
        };
        let mut retained_capture_bytes = 0_usize;
        // Deferred PDF capture belongs to the resumable worker queue. Re-reading
        // unchanged sources here just competes for the same bounded spool space.
        let deferred = connection
            .prepare("SELECT file_id FROM content_documents WHERE format='pdf' AND availability='deferred'")
            .and_then(|mut statement| statement.query_map([], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<std::collections::HashSet<_>>>())
            .map_err(|source| ReconciliationScanError::Content { source: Box::new(crate::ContentError::Storage(source)) })?;
        for entry in manifest.entries {
            let Some(indexed) = existing.remove(&entry.normalized) else {
                plan.inserted_paths += 1;
                plan.changes.push(IndexChange::Upsert {
                    entry: Box::new(entry),
                    captured: None,
                });
                continue;
            };
            let kind = file_kind(&entry.relative);
            let content_bearing = matches!(
                kind,
                FileKind::Markdown | FileKind::Base | FileKind::PlainText | FileKind::Pdf
            );
            let mut changed = indexed.match_key != entry.match_key
                || entry.absolute.to_str() != Some(indexed.absolute_path.as_str())
                || indexed.size_bytes != entry.size_bytes
                || indexed.modified_unix_ms != entry.modified_unix_ms
                || indexed.is_markdown != (kind == FileKind::Markdown)
                || (!content_bearing && !indexed.hash_blake3.is_empty());
            if matches!(kind, FileKind::PlainText | FileKind::Pdf) {
                changed |= crate::ContentIndexService
                    .needs_refresh(connection, &indexed.file_id, &entry.normalized)
                    .map_err(|source| ReconciliationScanError::Content {
                        source: Box::new(source),
                    })?;
            }
            let capture_limit = if kind == FileKind::Pdf {
                64 * 1024 * 1024
            } else {
                32 * 1024 * 1024
            };
            let captured = if content_bearing
                && entry.size_bytes <= capture_limit
                && scan_mode.verifies_content_hashes()
                && (changed || !deferred.contains(&indexed.file_id))
            {
                match fingerprints.capture_with_limit(&entry.absolute, capture_limit) {
                    Ok(capture) => {
                        changed |= capture.fingerprint.hash_blake3 != indexed.hash_blake3;
                        Some(capture)
                    }
                    // The apply stage records a precise per-file diagnostic without discarding
                    // unrelated healthy revisions. Failed capture is never evidence of removal.
                    Err(_) => {
                        changed = true;
                        None
                    }
                }
            } else {
                None
            };
            if changed {
                plan.updated_paths += 1;
                let captured = captured.filter(|capture| {
                    if retained_capture_bytes.saturating_add(capture.bytes.capacity())
                        > 64 * 1024 * 1024
                    {
                        return false;
                    }
                    retained_capture_bytes += capture.bytes.capacity();
                    true
                });
                plan.changes.push(IndexChange::Upsert {
                    entry: Box::new(entry),
                    captured,
                });
            }
        }
        let mut removed = existing.into_keys().collect::<Vec<_>>();
        removed.sort();
        for normalized_path in removed {
            plan.removed_paths += 1;
            plan.changes.push(IndexChange::Remove { normalized_path });
        }
        Ok(plan)
    }
}
