//! File metadata reconciliation service.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use tao_sdk_storage::{
    FileRecordInput, FilesRepository, StorageTransactionError, with_transaction,
};
use tao_sdk_vault::{
    CasePolicy, FileFingerprintError, FileFingerprintService, PathCanonicalizationError,
    VaultScanError, VaultScanService,
};
use thiserror::Error;

use super::ServiceTraceContext;

/// Reconcile run result over files metadata and on-disk vault contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileResult {
    /// Number of files discovered in current vault scan.
    pub scanned_files: u64,
    /// Number of file rows newly inserted.
    pub inserted_files: u64,
    /// Number of existing file rows refreshed from changed content.
    pub updated_files: u64,
    /// Number of stale file rows removed.
    pub removed_files: u64,
    /// Number of files unchanged between disk and index metadata.
    pub unchanged_files: u64,
}

/// Idempotent reconcile service for repairing files metadata drift.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReconcileService;

impl ReconcileService {
    /// Reconcile indexed file metadata against the current vault filesystem state.
    pub fn reconcile_vault(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
    ) -> Result<ReconcileResult, ReconcileError> {
        let scanner = VaultScanService::from_root(vault_root, case_policy).map_err(|source| {
            ReconcileError::CreateScanner {
                source: Box::new(source),
            }
        })?;
        let manifest = scanner.scan().map_err(|source| ReconcileError::Scan {
            source: Box::new(source),
        })?;

        let fingerprint_service = FileFingerprintService::from_root(vault_root, case_policy)
            .map_err(|source| ReconcileError::CreateFingerprintService {
                source: Box::new(source),
            })?;

        let existing = FilesRepository::list_all(connection).map_err(|source| {
            ReconcileError::ListExistingMetadata {
                source: Box::new(source),
            }
        })?;

        let mut existing_by_normalized = HashMap::new();
        for record in existing {
            existing_by_normalized.insert(record.normalized_path.clone(), record);
        }

        let mut seen_paths = HashSet::new();
        let mut upserts = Vec::new();
        let mut inserted_files = 0_u64;
        let mut updated_files = 0_u64;
        let mut unchanged_files = 0_u64;

        for entry in &manifest.entries {
            let fingerprint =
                fingerprint_service
                    .fingerprint(&entry.relative)
                    .map_err(|source| ReconcileError::Fingerprint {
                        path: entry.absolute.clone(),
                        source: Box::new(source),
                    })?;
            let modified_unix_ms = i64::try_from(fingerprint.modified_unix_ms).map_err(|_| {
                ReconcileError::TimestampOverflow {
                    value: fingerprint.modified_unix_ms,
                }
            })?;

            let previous = existing_by_normalized.get(&fingerprint.normalized);
            let file_id = previous.map_or_else(
                || deterministic_file_id(&fingerprint.normalized),
                |record| record.file_id.clone(),
            );

            let record = FileRecordInput {
                file_id,
                normalized_path: fingerprint.normalized.clone(),
                match_key: fingerprint.match_key,
                absolute_path: fingerprint.absolute.to_string_lossy().to_string(),
                size_bytes: fingerprint.size_bytes,
                modified_unix_ms,
                hash_blake3: fingerprint.hash_blake3,
                is_markdown: fingerprint.normalized.ends_with(".md"),
            };

            seen_paths.insert(record.normalized_path.clone());

            if let Some(previous) = previous {
                if files_metadata_matches(previous, &record) {
                    unchanged_files += 1;
                    continue;
                }
                updated_files += 1;
            } else {
                inserted_files += 1;
            }

            upserts.push(record);
        }

        let stale_file_ids: Vec<String> = existing_by_normalized
            .values()
            .filter(|record| !seen_paths.contains(&record.normalized_path))
            .map(|record| record.file_id.clone())
            .collect();

        let removed_files = stale_file_ids.len() as u64;
        with_transaction(connection, move |transaction| {
            for record in &upserts {
                transaction.files_upsert(record)?;
            }
            for file_id in &stale_file_ids {
                transaction.files_delete_by_id(file_id)?;
            }
            Ok(())
        })
        .map_err(|source| ReconcileError::Transaction {
            source: Box::new(source),
        })?;

        Ok(ReconcileResult {
            scanned_files: manifest.entries.len() as u64,
            inserted_files,
            updated_files,
            removed_files,
            unchanged_files,
        })
    }

    /// Tracing hook wrapper for `reconcile_vault` with explicit correlation context.
    pub fn reconcile_vault_with_trace_context(
        &self,
        trace_context: &ServiceTraceContext,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
    ) -> Result<ReconcileResult, ReconcileError> {
        let span = trace_context.span();
        let _entered = span.enter();
        trace_context.emit_start();

        let result = self.reconcile_vault(vault_root, connection, case_policy);
        match &result {
            Ok(_) => trace_context.emit_success(),
            Err(error) => trace_context.emit_failure(error),
        }
        result
    }
}

fn deterministic_file_id(normalized_path: &str) -> String {
    let hash = blake3::hash(normalized_path.as_bytes()).to_hex();
    format!("f_{}", &hash[..16])
}

fn files_metadata_matches(previous: &tao_sdk_storage::FileRecord, next: &FileRecordInput) -> bool {
    previous.normalized_path == next.normalized_path
        && previous.match_key == next.match_key
        && previous.absolute_path == next.absolute_path
        && previous.size_bytes == next.size_bytes
        && previous.modified_unix_ms == next.modified_unix_ms
        && previous.hash_blake3 == next.hash_blake3
        && previous.is_markdown == next.is_markdown
}

/// Reconcile service failures.
#[derive(Debug, Error)]
pub enum ReconcileError {
    /// Creating scan service failed.
    #[error("failed to initialize reconcile scanner: {source}")]
    CreateScanner {
        /// Scanner initialization error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Scanning vault failed.
    #[error("failed to scan vault during reconcile: {source}")]
    Scan {
        /// Scan error.
        #[source]
        source: Box<VaultScanError>,
    },
    /// Creating fingerprint service failed.
    #[error("failed to initialize reconcile fingerprint service: {source}")]
    CreateFingerprintService {
        /// Fingerprint service initialization error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Fingerprinting one scanned file failed.
    #[error("failed to fingerprint scanned file '{path}': {source}")]
    Fingerprint {
        /// Absolute file path.
        path: PathBuf,
        /// Fingerprint error.
        #[source]
        source: Box<FileFingerprintError>,
    },
    /// Fingerprint modified timestamp overflows storage integer type.
    #[error("fingerprint modified timestamp overflows i64: {value}")]
    TimestampOverflow {
        /// Raw timestamp value.
        value: u128,
    },
    /// Listing existing metadata rows failed.
    #[error("failed to list existing file metadata during reconcile: {source}")]
    ListExistingMetadata {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Applying transactional reconcile changes failed.
    #[error("failed to apply reconcile transaction: {source}")]
    Transaction {
        /// Transaction error.
        #[source]
        source: Box<StorageTransactionError>,
    },
}
