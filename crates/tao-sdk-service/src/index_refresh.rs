//! Shared index freshness inspection and repair policy.

use std::path::Path;

use rusqlite::Connection;
use tao_sdk_storage::{FilesRepository, IndexStateRepository};
use tao_sdk_vault::{CasePolicy, PathCanonicalizationService};
use thiserror::Error;

use crate::{
    CURRENT_LINK_RESOLUTION_VERSION, FullIndexService, LINK_RESOLUTION_VERSION_STATE_KEY,
    ReconciliationScanMode, ReconciliationScannerService, SearchCorpusRefreshMode,
    SearchCorpusService,
};

const DEFAULT_MAX_BATCH_SIZE: usize = 128;
const REASON_LINK_RESOLUTION_VERSION_MISMATCH: &str = "link_resolution_version_mismatch";
const REASON_FILE_PATH_MISMATCH: &str = "file_path_mismatch";

/// Index freshness summary across canonical tables and the derived search corpus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRefreshStatus {
    /// Number of vault-relative paths that would be reconciled.
    pub drift_paths: u64,
    /// Reason a full rebuild is required, when incremental repair is insufficient.
    pub rebuild_reason: Option<&'static str>,
    /// Whether the derived search corpus is missing or stale.
    pub search_index_stale: bool,
    /// Whether a search corpus rebuild would be performed.
    pub would_rebuild_search_index: bool,
}

/// Refresh execution options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexRefreshOptions {
    /// Drift detection policy.
    pub scan_mode: ReconciliationScanMode,
    /// Maximum incremental batch size.
    pub max_batch_size: usize,
}

impl Default for IndexRefreshOptions {
    fn default() -> Self {
        Self {
            scan_mode: ReconciliationScanMode::MetadataOnly,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
        }
    }
}

/// Refresh action taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRefreshMode {
    /// No index maintenance was needed.
    Current,
    /// Canonical index was rebuilt from scratch.
    FullRebuild,
    /// Canonical index drift was repaired incrementally.
    Reconcile,
    /// Only the derived search corpus was rebuilt.
    CorpusOnly,
}

impl IndexRefreshMode {
    /// Stable label for command output.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::FullRebuild => "full_rebuild",
            Self::Reconcile => "reconcile",
            Self::CorpusOnly => "corpus_only",
        }
    }
}

/// Result from applying needed index refresh work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRefreshOutcome {
    /// Refresh action taken.
    pub mode: IndexRefreshMode,
    /// Full rebuild reason, when applicable.
    pub reason: Option<&'static str>,
    /// Number of drift paths found before repair.
    pub drift_paths: u64,
    /// Number of index batches applied.
    pub batches_applied: u64,
    /// Number of file rows upserted.
    pub upserted_files: u64,
    /// Number of file rows removed.
    pub removed_files: u64,
    /// Whether the derived search corpus was rebuilt.
    pub search_segments_rebuilt: bool,
    /// Derived search corpus refresh mode.
    pub search_corpus_refresh: SearchCorpusRefreshMode,
}

/// Service-level owner for index freshness policy.
#[derive(Debug, Default, Clone, Copy)]
pub struct IndexRefreshService;

impl IndexRefreshService {
    /// Inspect cached index health without scanning the vault filesystem.
    pub fn inspect_cached(
        &self,
        connection: &Connection,
    ) -> Result<IndexRefreshStatus, IndexRefreshError> {
        let search_status = SearchCorpusService.status(connection).map_err(|source| {
            IndexRefreshError::SearchCorpus {
                source: Box::new(source),
            }
        })?;
        let rebuild_reason = if index_requires_full_rebuild(connection)? {
            Some(REASON_LINK_RESOLUTION_VERSION_MISMATCH)
        } else {
            None
        };
        let drift_paths = u64::from(rebuild_reason.is_some() || search_status.search_index_stale);

        Ok(IndexRefreshStatus {
            drift_paths,
            rebuild_reason,
            search_index_stale: search_status.search_index_stale,
            would_rebuild_search_index: search_status.would_rebuild_search_index,
        })
    }

    /// Inspect index freshness without mutating state.
    pub fn inspect(
        &self,
        vault_root: &Path,
        connection: &Connection,
        case_policy: CasePolicy,
        scan_mode: ReconciliationScanMode,
    ) -> Result<IndexRefreshStatus, IndexRefreshError> {
        let drift = ReconciliationScannerService::default()
            .scan_with_mode(vault_root, connection, case_policy, scan_mode)
            .map_err(|source| IndexRefreshError::ScanDrift {
                source: Box::new(source),
            })?;
        let inconsistent_paths = count_inconsistent_file_rows(vault_root, connection, case_policy)?;
        let search_status = SearchCorpusService.status(connection).map_err(|source| {
            IndexRefreshError::SearchCorpus {
                source: Box::new(source),
            }
        })?;
        let rebuild_reason = if index_requires_full_rebuild(connection)? {
            Some(REASON_LINK_RESOLUTION_VERSION_MISMATCH)
        } else if inconsistent_paths > 0 {
            Some(REASON_FILE_PATH_MISMATCH)
        } else {
            None
        };

        Ok(IndexRefreshStatus {
            drift_paths: if rebuild_reason.is_some() {
                drift.drift_paths.max(inconsistent_paths).max(1)
            } else {
                drift.drift_paths
            },
            rebuild_reason,
            search_index_stale: search_status.search_index_stale,
            would_rebuild_search_index: search_status.would_rebuild_search_index,
        })
    }

    /// Execute the required refresh action.
    pub fn refresh(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
        options: IndexRefreshOptions,
    ) -> Result<IndexRefreshOutcome, IndexRefreshError> {
        let status = self.inspect(vault_root, connection, case_policy, options.scan_mode)?;

        if let Some(reason) = status.rebuild_reason {
            let rebuild = FullIndexService::default()
                .rebuild(vault_root, connection, case_policy)
                .map_err(|source| IndexRefreshError::FullRebuild {
                    source: Box::new(source),
                })?;
            return Ok(IndexRefreshOutcome {
                mode: IndexRefreshMode::FullRebuild,
                reason: Some(reason),
                drift_paths: status.drift_paths,
                batches_applied: 1,
                upserted_files: rebuild.indexed_files,
                removed_files: 0,
                search_segments_rebuilt: true,
                search_corpus_refresh: SearchCorpusRefreshMode::Full,
            });
        }

        if status.drift_paths > 0 {
            let reconcile = ReconciliationScannerService::default()
                .scan_and_repair_with_mode(
                    vault_root,
                    connection,
                    case_policy,
                    options.max_batch_size,
                    options.scan_mode,
                )
                .map_err(|source| IndexRefreshError::ScanDrift {
                    source: Box::new(source),
                })?;
            return Ok(IndexRefreshOutcome {
                mode: IndexRefreshMode::Reconcile,
                reason: None,
                drift_paths: reconcile.drift_paths,
                batches_applied: reconcile.batches_applied,
                upserted_files: reconcile.upserted_files,
                removed_files: reconcile.removed_files,
                search_segments_rebuilt: reconcile.search_corpus_refresh
                    != SearchCorpusRefreshMode::None,
                search_corpus_refresh: reconcile.search_corpus_refresh,
            });
        }

        if status.search_index_stale {
            SearchCorpusService
                .rebuild_atomic(connection, case_policy)
                .map_err(|source| IndexRefreshError::SearchCorpus {
                    source: Box::new(source),
                })?;
            return Ok(IndexRefreshOutcome {
                mode: IndexRefreshMode::CorpusOnly,
                reason: None,
                drift_paths: 0,
                batches_applied: 0,
                upserted_files: 0,
                removed_files: 0,
                search_segments_rebuilt: true,
                search_corpus_refresh: SearchCorpusRefreshMode::Full,
            });
        }

        Ok(IndexRefreshOutcome {
            mode: IndexRefreshMode::Current,
            reason: None,
            drift_paths: 0,
            batches_applied: 0,
            upserted_files: 0,
            removed_files: 0,
            search_segments_rebuilt: false,
            search_corpus_refresh: SearchCorpusRefreshMode::None,
        })
    }
}

fn index_requires_full_rebuild(connection: &Connection) -> Result<bool, IndexRefreshError> {
    let Some(record) =
        IndexStateRepository::get_by_key(connection, LINK_RESOLUTION_VERSION_STATE_KEY).map_err(
            |source| IndexRefreshError::IndexState {
                source: Box::new(source),
            },
        )?
    else {
        return Ok(true);
    };

    let stored_version = serde_json::from_str::<u32>(&record.value_json).unwrap_or_default();
    Ok(stored_version != CURRENT_LINK_RESOLUTION_VERSION)
}

fn count_inconsistent_file_rows(
    vault_root: &Path,
    connection: &Connection,
    case_policy: CasePolicy,
) -> Result<u64, IndexRefreshError> {
    let canonicalizer =
        PathCanonicalizationService::new(vault_root, case_policy).map_err(|source| {
            IndexRefreshError::CreateCanonicalizer {
                source: Box::new(source),
            }
        })?;
    let files =
        FilesRepository::list_all(connection).map_err(|source| IndexRefreshError::Files {
            source: Box::new(source),
        })?;

    let mut mismatches = 0_u64;
    for file in files {
        let absolute = Path::new(&file.absolute_path);
        let Ok(canonical) = canonicalizer.canonicalize(absolute) else {
            mismatches = mismatches.saturating_add(1);
            continue;
        };
        if canonical.normalized != file.normalized_path {
            mismatches = mismatches.saturating_add(1);
        }
    }

    Ok(mismatches)
}

/// Index refresh failures.
#[derive(Debug, Error)]
pub enum IndexRefreshError {
    /// Drift scan or repair failed.
    #[error("scan index drift failed: {source}")]
    ScanDrift {
        /// Source error.
        #[source]
        source: Box<crate::ReconciliationScanError>,
    },
    /// Full rebuild failed.
    #[error("full index rebuild failed: {source}")]
    FullRebuild {
        /// Source error.
        #[source]
        source: Box<crate::FullIndexError>,
    },
    /// Search corpus inspection or rebuild failed.
    #[error("search corpus refresh failed: {source}")]
    SearchCorpus {
        /// Source error.
        #[source]
        source: Box<crate::SearchCorpusError>,
    },
    /// File repository query failed.
    #[error("list indexed files failed: {source}")]
    Files {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Index state query failed.
    #[error("read index state failed: {source}")]
    IndexState {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Vault path canonicalizer could not be created.
    #[error("create vault canonicalizer failed: {source}")]
    CreateCanonicalizer {
        /// Source error.
        #[source]
        source: Box<tao_sdk_vault::PathCanonicalizationError>,
    },
}
