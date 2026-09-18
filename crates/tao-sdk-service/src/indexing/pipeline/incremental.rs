use super::*;

/// Result payload for incremental indexing workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalIndexResult {
    /// Number of changed paths processed.
    pub processed_paths: u64,
    /// Number of files inserted or updated.
    pub upserted_files: u64,
    /// Number of files removed from index.
    pub removed_files: u64,
    /// Number of links reindexed.
    pub links_reindexed: u64,
    /// Number of properties reindexed.
    pub properties_reindexed: u64,
    /// Number of bases reindexed.
    pub bases_reindexed: u64,
    /// Derived search corpus refresh mode applied by this run.
    pub search_corpus_refresh: SearchCorpusRefreshMode,
    /// File ids that require a deferred search corpus refresh.
    #[doc(hidden)]
    pub search_corpus_refresh_file_ids: Vec<String>,
    /// Whether deferred refresh must rebuild the full search corpus.
    #[doc(hidden)]
    pub requires_full_search_corpus_refresh: bool,
}

/// Result payload for coalesced batch indexing workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalescedBatchIndexResult {
    /// Number of raw path change events received.
    pub input_events: u64,
    /// Number of unique normalized paths after coalescing.
    pub unique_paths: u64,
    /// Number of batches applied.
    pub batches_applied: u64,
    /// Number of files inserted or updated.
    pub upserted_files: u64,
    /// Number of files removed from index.
    pub removed_files: u64,
    /// Number of links reindexed.
    pub links_reindexed: u64,
    /// Number of properties reindexed.
    pub properties_reindexed: u64,
    /// Number of bases reindexed.
    pub bases_reindexed: u64,
    /// Whether the derived search corpus was rebuilt after coalesced batches.
    pub search_segments_rebuilt: bool,
    /// Derived search corpus refresh mode applied after coalesced batches.
    pub search_corpus_refresh: SearchCorpusRefreshMode,
}

/// Derived search corpus refresh mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchCorpusRefreshMode {
    /// No derived corpus rows were refreshed.
    None,
    /// Only impacted file rows were refreshed.
    Partial,
    /// The full derived corpus was rebuilt.
    Full,
}

impl SearchCorpusRefreshMode {
    /// Return the stable JSON label for this refresh mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Partial => "partial",
            Self::Full => "full",
        }
    }
}

/// Incremental indexing service for targeted path updates.
#[derive(Debug, Default, Clone, Copy)]
pub struct IncrementalIndexService {
    parser: MarkdownParser,
}

impl IncrementalIndexService {
    /// Apply updates using authoritative scan membership, not filesystem existence.
    pub fn apply_changes(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changed_paths: &[PathBuf],
        case_policy: CasePolicy,
    ) -> Result<IncrementalIndexResult, FullIndexError> {
        let changes = apply::changes_for_paths(vault_root, changed_paths, case_policy)?;
        apply::apply_changes(
            vault_root,
            connection,
            changes,
            case_policy,
            self.parser,
            apply::PublicationOptions {
                force: false,
                force_full_corpus: false,
                expected_generation: None,
            },
        )
    }
    /// Rebuild the canonical projections for each supplied path.
    pub fn apply_changes_force(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changed_paths: &[PathBuf],
        case_policy: CasePolicy,
    ) -> Result<IncrementalIndexResult, FullIndexError> {
        let changes = apply::changes_for_paths(vault_root, changed_paths, case_policy)?;
        apply::apply_changes(
            vault_root,
            connection,
            changes,
            case_policy,
            self.parser,
            apply::PublicationOptions {
                force: true,
                force_full_corpus: false,
                expected_generation: None,
            },
        )
    }
}

/// Coalescing batch service for burst filesystem change events.
#[derive(Debug, Default, Clone, Copy)]
pub struct CoalescedBatchIndexService {
    incremental: IncrementalIndexService,
}

impl CoalescedBatchIndexService {
    /// Deduplicate events and publish the complete final inventory in one transaction.
    /// Batch size bounds logical work accounting; it never publishes partial generations.
    pub fn apply_coalesced(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changed_paths: &[PathBuf],
        max_batch_size: usize,
        case_policy: CasePolicy,
    ) -> Result<CoalescedBatchIndexResult, FullIndexError> {
        if max_batch_size == 0 {
            return Err(FullIndexError::InvalidBatchSize { value: 0 });
        }
        let changes = apply::changes_for_paths(vault_root, changed_paths, case_policy)?;
        let mut result = self.apply_plan(
            vault_root,
            connection,
            changes,
            None,
            max_batch_size,
            case_policy,
        )?;
        result.input_events = changed_paths.len() as u64;
        Ok(result)
    }
    pub(crate) fn apply_plan(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changes: Vec<IndexChange>,
        expected_generation: Option<i64>,
        max_batch_size: usize,
        case_policy: CasePolicy,
    ) -> Result<CoalescedBatchIndexResult, FullIndexError> {
        if max_batch_size == 0 {
            return Err(FullIndexError::InvalidBatchSize { value: 0 });
        }
        let unique_paths = changes.len();
        let result = apply::apply_changes(
            vault_root,
            connection,
            changes,
            case_policy,
            self.incremental.parser,
            apply::PublicationOptions {
                force: false,
                force_full_corpus: false,
                expected_generation,
            },
        )?;
        Ok(CoalescedBatchIndexResult {
            input_events: unique_paths as u64,
            unique_paths: unique_paths as u64,
            batches_applied: unique_paths.div_ceil(max_batch_size) as u64,
            upserted_files: result.upserted_files,
            removed_files: result.removed_files,
            links_reindexed: result.links_reindexed,
            properties_reindexed: result.properties_reindexed,
            bases_reindexed: result.bases_reindexed,
            search_segments_rebuilt: result.search_corpus_refresh != SearchCorpusRefreshMode::None,
            search_corpus_refresh: result.search_corpus_refresh,
        })
    }
}

pub(super) fn add_link_with_paths_corpus_file_ids(
    file_ids: &mut std::collections::BTreeSet<String>,
    link: &LinkWithPaths,
) {
    file_ids.insert(link.source_file_id.clone());
    if let Some(file_id) = &link.resolved_file_id {
        file_ids.insert(file_id.clone());
    }
}

pub(super) fn add_link_input_corpus_file_ids(
    file_ids: &mut std::collections::BTreeSet<String>,
    links: &[LinkRecordInput],
) {
    for link in links {
        file_ids.insert(link.source_file_id.clone());
        if let Some(file_id) = &link.resolved_file_id {
            file_ids.insert(file_id.clone());
        }
    }
}

/// Result payload for stale metadata cleanup workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleCleanupResult {
    /// Number of files discovered in current vault scan.
    pub scanned_files: u64,
    /// Number of stale file rows removed.
    pub stale_files_removed: u64,
}

/// Service for removing stale file metadata rows not present in the vault scan.
#[derive(Debug, Default, Clone, Copy)]
pub struct StaleCleanupService;

impl StaleCleanupService {
    /// Remove stale file rows and dependent records for files no longer present on disk.
    pub fn cleanup(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
    ) -> Result<StaleCleanupResult, StaleCleanupError> {
        let scanner = VaultScanService::from_root(vault_root, case_policy).map_err(|source| {
            StaleCleanupError::CreateScanner {
                source: Box::new(source),
            }
        })?;
        let manifest = scanner.scan().map_err(|source| StaleCleanupError::Scan {
            source: Box::new(source),
        })?;

        let live_paths = manifest
            .entries
            .iter()
            .map(|entry| entry.normalized.clone())
            .collect::<std::collections::HashSet<_>>();
        let existing = FilesRepository::list_all(connection).map_err(|source| {
            StaleCleanupError::ListFiles {
                source: Box::new(source),
            }
        })?;
        let changes = existing
            .into_iter()
            .filter(|record| !live_paths.contains(&record.normalized_path))
            .map(|record| IndexChange::Remove {
                normalized_path: record.normalized_path,
            })
            .collect::<Vec<_>>();
        let stale_files_removed = changes.len() as u64;
        apply::apply_changes(
            vault_root,
            connection,
            changes,
            case_policy,
            MarkdownParser,
            apply::PublicationOptions {
                force: false,
                force_full_corpus: false,
                expected_generation: None,
            },
        )
        .map_err(|source| StaleCleanupError::Publish {
            source: Box::new(source),
        })?;
        let summary_json = serde_json::to_string(&json!({
            "mode": "stale_cleanup",
            "scanned_files": manifest.entries.len(),
            "stale_files_removed": stale_files_removed,
            "completed_unix_ms": current_unix_ms_raw().map_err(|source| StaleCleanupError::Clock {
                source: Box::new(source),
            })?,
        }))
        .map_err(|source| StaleCleanupError::SerializeSummary {
            source: Box::new(source),
        })?;

        IndexStateRepository::upsert(
            connection,
            &IndexStateRecordInput {
                key: "last_stale_cleanup_summary".to_string(),
                value_json: summary_json,
            },
        )
        .map_err(|source| StaleCleanupError::UpsertIndexState {
            source: Box::new(source),
        })?;

        Ok(StaleCleanupResult {
            scanned_files: manifest.entries.len() as u64,
            stale_files_removed,
        })
    }
}
