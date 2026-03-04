//! Service-layer orchestration entrypoints over SDK subsystem crates.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

mod config;
mod feature_flags;
mod import_export;
mod indexing;
mod tracing_hooks;

pub use config::{
    SdkBootstrapError, SdkBootstrapService, SdkBootstrapSnapshot, SdkConfig, SdkConfigError,
    SdkConfigLoader, SdkConfigOverrides,
};
pub use feature_flags::{FeatureFlagParseError, FeatureFlagRegistry, SdkFeature};
pub use import_export::{
    FilesystemImportExportService, ImportExportBoundaryError, ImportExportServiceBoundary,
    TransferExecutionRequest, TransferExecutionResult, TransferFailure, TransferItem,
    TransferItemKind, TransferJobKind, TransferMode, TransferPlan, TransferSummary,
};
pub use indexing::{
    CheckpointedIndexError, CheckpointedIndexResult, CheckpointedIndexService,
    CoalescedBatchIndexResult, CoalescedBatchIndexService, ConsistencyIssueKind, FullIndexError,
    FullIndexResult, FullIndexService, IncrementalIndexResult, IncrementalIndexService,
    IndexConsistencyChecker, IndexConsistencyError, IndexConsistencyIssue, IndexConsistencyReport,
    IndexSelfHealError, IndexSelfHealResult, IndexSelfHealService, ReconciliationScanError,
    ReconciliationScanResult, ReconciliationScannerService, StaleCleanupError, StaleCleanupResult,
    StaleCleanupService,
};
pub use tracing_hooks::ServiceTraceContext;

use rusqlite::{Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use tao_sdk_bases::{
    BaseColumnConfig, BaseDiagnostic, BaseDocument, BaseFilterClause, BaseFilterOp, BaseSortClause,
    BaseSortDirection, TableQueryPlan, validate_base_config_json,
};
use tao_sdk_core::{DomainEvent, DomainEventBus, NoteChangeKind};
use tao_sdk_markdown::{
    MarkdownParseError, MarkdownParseRequest, MarkdownParseResult, MarkdownParser,
};
use tao_sdk_properties::{FrontMatterStatus, TypedPropertyValue, extract_front_matter};
use tao_sdk_storage::{
    BaseRecordInput, BasesRepository, FileRecordInput, FilesRepository, LinksRepository,
    PropertiesRepository, PropertyRecordInput, StorageTransactionError, with_transaction,
};
use tao_sdk_vault::{
    CasePolicy, FileFingerprintError, FileFingerprintService, PathCanonicalizationError,
    VaultManifestEntry, VaultScanError, VaultScanService,
};
use thiserror::Error;

/// Parsed markdown note produced by the ingest pipeline shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestedMarkdownNote {
    /// Canonical absolute path in the active vault.
    pub absolute_path: PathBuf,
    /// Canonical normalized path used by index layers.
    pub normalized_path: String,
    /// Parsed markdown content.
    pub parsed: MarkdownParseResult,
}

/// Pipeline shell that scans vault files and parses markdown notes.
#[derive(Debug, Clone)]
pub struct MarkdownIngestPipeline {
    scanner: VaultScanService,
    parser: MarkdownParser,
}

impl MarkdownIngestPipeline {
    /// Create a pipeline from an already-configured scanner.
    #[must_use]
    pub fn new(scanner: VaultScanService) -> Self {
        Self {
            scanner,
            parser: MarkdownParser,
        }
    }

    /// Create a pipeline from vault root path and case policy.
    pub fn from_root(
        root: impl AsRef<Path>,
        case_policy: CasePolicy,
    ) -> Result<Self, MarkdownIngestError> {
        let scanner = VaultScanService::from_root(root, case_policy)
            .map_err(|source| MarkdownIngestError::CreateScanner { source })?;
        Ok(Self::new(scanner))
    }

    /// Run full vault scan and parse all markdown notes.
    pub fn ingest_vault(&self) -> Result<Vec<IngestedMarkdownNote>, MarkdownIngestError> {
        let manifest = self
            .scanner
            .scan()
            .map_err(|source| MarkdownIngestError::Scan { source })?;
        self.ingest_entries(&manifest.entries)
    }

    /// Parse markdown notes from a pre-scanned manifest.
    pub fn ingest_entries(
        &self,
        entries: &[VaultManifestEntry],
    ) -> Result<Vec<IngestedMarkdownNote>, MarkdownIngestError> {
        let mut notes = Vec::new();
        for entry in entries {
            if !is_markdown_file(&entry.relative) {
                continue;
            }

            let bytes =
                fs::read(&entry.absolute).map_err(|source| MarkdownIngestError::ReadFile {
                    path: entry.absolute.clone(),
                    source,
                })?;
            let raw =
                String::from_utf8(bytes).map_err(|source| MarkdownIngestError::DecodeUtf8 {
                    path: entry.absolute.clone(),
                    source,
                })?;

            let parsed = self
                .parser
                .parse(MarkdownParseRequest {
                    normalized_path: entry.normalized.clone(),
                    raw,
                })
                .map_err(|source| MarkdownIngestError::Parse {
                    path: entry.absolute.clone(),
                    source,
                })?;

            notes.push(IngestedMarkdownNote {
                absolute_path: entry.absolute.clone(),
                normalized_path: entry.normalized.clone(),
                parsed,
            });
        }

        Ok(notes)
    }
}

fn is_markdown_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("md"))
}

/// Coordinates write operations through typed SDK storage transactions.
#[derive(Debug, Default, Clone, Copy)]
pub struct SdkTransactionCoordinator;

impl SdkTransactionCoordinator {
    /// Insert one file metadata row in a typed transaction.
    pub fn insert_file_metadata(
        &self,
        connection: &mut Connection,
        record: &FileRecordInput,
    ) -> Result<(), SdkTransactionError> {
        with_transaction(connection, |transaction| {
            transaction.files_insert(record)?;
            Ok(())
        })
        .map_err(|source| SdkTransactionError::Transaction { source })
    }

    /// Delete one file metadata row in a typed transaction.
    pub fn delete_file_metadata(
        &self,
        connection: &mut Connection,
        file_id: &str,
    ) -> Result<bool, SdkTransactionError> {
        with_transaction(connection, |transaction| {
            transaction.files_delete_by_id(file_id)
        })
        .map_err(|source| SdkTransactionError::Transaction { source })
    }

    /// Replace one file metadata row atomically in a typed transaction.
    pub fn replace_file_metadata(
        &self,
        connection: &mut Connection,
        _file_id: &str,
        replacement: &FileRecordInput,
    ) -> Result<(), SdkTransactionError> {
        with_transaction(connection, |transaction| {
            transaction.files_upsert(replacement)?;
            Ok(())
        })
        .map_err(|source| SdkTransactionError::Transaction { source })
    }
}

/// Errors returned by SDK transaction coordination.
#[derive(Debug, Error)]
pub enum SdkTransactionError {
    /// Typed storage transaction failed.
    #[error("sdk transaction coordination failed: {source}")]
    Transaction {
        /// Transaction error details.
        #[source]
        source: StorageTransactionError,
    },
}

/// Service wrapper for storage writes executed inside typed transactions.
#[derive(Debug, Default, Clone, Copy)]
pub struct StorageWriteService;

impl StorageWriteService {
    /// Insert one file record using the typed storage transaction API.
    pub fn create_file_record(
        &self,
        connection: &mut Connection,
        record: &FileRecordInput,
    ) -> Result<(), StorageWriteError> {
        SdkTransactionCoordinator
            .insert_file_metadata(connection, record)
            .map_err(|source| StorageWriteError::Coordinator { source })
    }
}

/// Errors returned by service-layer storage writes.
#[derive(Debug, Error)]
pub enum StorageWriteError {
    /// SDK transaction coordinator failed.
    #[error("storage write coordinator failed: {source}")]
    Coordinator {
        /// Coordinator error details.
        #[source]
        source: SdkTransactionError,
    },
}

/// Result model for note write operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoteCrudResult {
    /// Stable file id.
    pub file_id: String,
    /// Canonical normalized path.
    pub normalized_path: String,
}

/// Service for note create/update/delete flows backed by SDK storage metadata writes.
#[derive(Clone)]
pub struct NoteCrudService {
    coordinator: SdkTransactionCoordinator,
    events: DomainEventBus,
}

impl Default for NoteCrudService {
    fn default() -> Self {
        Self {
            coordinator: SdkTransactionCoordinator,
            events: DomainEventBus::new(),
        }
    }
}

impl NoteCrudService {
    /// Create a note file and persist corresponding file metadata.
    pub fn create_note(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        relative_path: &Path,
        content: &str,
    ) -> Result<NoteCrudResult, NoteCrudError> {
        validate_relative_note_path(relative_path)?;
        let absolute = prepare_note_create_path(vault_root, relative_path)?;

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&absolute)
            .map_err(|source| NoteCrudError::WriteFile {
                path: absolute.clone(),
                source,
            })?;
        file.write_all(content.as_bytes())
            .map_err(|source| NoteCrudError::WriteFile {
                path: absolute.clone(),
                source,
            })?;

        let record = fingerprint_to_file_record(file_id, vault_root, relative_path)?;
        if let Err(source) = self.coordinator.insert_file_metadata(connection, &record) {
            match fs::remove_file(&absolute) {
                Ok(_) => {
                    return Err(NoteCrudError::Coordinator {
                        source: Box::new(source),
                    });
                }
                Err(rollback_source) => {
                    return Err(NoteCrudError::CoordinatorRollback {
                        source: Box::new(source),
                        details: Box::new(RollbackFailure {
                            step: "delete_created_file",
                            path: absolute,
                            error: rollback_source.to_string(),
                        }),
                    });
                }
            }
        }

        self.events.publish(DomainEvent::NoteChanged {
            file_id: file_id.to_string(),
            normalized_path: record.normalized_path.clone(),
            kind: NoteChangeKind::Created,
        });

        Ok(NoteCrudResult {
            file_id: file_id.to_string(),
            normalized_path: record.normalized_path,
        })
    }

    /// Update note content and replace corresponding file metadata atomically.
    pub fn update_note(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        relative_path: &Path,
        content: &str,
    ) -> Result<NoteCrudResult, NoteCrudError> {
        validate_relative_note_path(relative_path)?;
        let absolute = ensure_note_path_within_vault(vault_root, &vault_root.join(relative_path))?;
        let previous_content =
            fs::read(&absolute).map_err(|source| NoteCrudError::ReadFileForRollback {
                path: absolute.clone(),
                source,
            })?;
        fs::write(&absolute, content).map_err(|source| NoteCrudError::WriteFile {
            path: absolute.clone(),
            source,
        })?;

        let record = fingerprint_to_file_record(file_id, vault_root, relative_path)?;
        if let Err(source) = self
            .coordinator
            .replace_file_metadata(connection, file_id, &record)
        {
            match fs::write(&absolute, &previous_content) {
                Ok(_) => {
                    return Err(NoteCrudError::Coordinator {
                        source: Box::new(source),
                    });
                }
                Err(rollback_source) => {
                    return Err(NoteCrudError::CoordinatorRollback {
                        source: Box::new(source),
                        details: Box::new(RollbackFailure {
                            step: "restore_previous_content",
                            path: absolute,
                            error: rollback_source.to_string(),
                        }),
                    });
                }
            }
        }

        self.events.publish(DomainEvent::NoteChanged {
            file_id: file_id.to_string(),
            normalized_path: record.normalized_path.clone(),
            kind: NoteChangeKind::Updated,
        });

        Ok(NoteCrudResult {
            file_id: file_id.to_string(),
            normalized_path: record.normalized_path,
        })
    }

    /// Delete note file and remove corresponding metadata.
    pub fn delete_note(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
    ) -> Result<bool, NoteCrudError> {
        let existing = FilesRepository::get_by_id(connection, file_id)
            .map_err(|source| NoteCrudError::Repository { source })?;
        let Some(existing) = existing else {
            return Ok(false);
        };

        let absolute =
            ensure_note_path_within_vault(vault_root, &vault_root.join(&existing.normalized_path))?;
        let deleted_file_bytes = if absolute.exists() {
            Some(
                fs::read(&absolute).map_err(|source| NoteCrudError::ReadFileForRollback {
                    path: absolute.clone(),
                    source,
                })?,
            )
        } else {
            None
        };
        if absolute.exists() {
            fs::remove_file(&absolute).map_err(|source| NoteCrudError::DeleteFile {
                path: absolute.clone(),
                source,
            })?;
        }

        let removed = match self.coordinator.delete_file_metadata(connection, file_id) {
            Ok(removed) => removed,
            Err(source) => {
                if let Some(bytes) = deleted_file_bytes {
                    match fs::write(&absolute, bytes) {
                        Ok(_) => {
                            return Err(NoteCrudError::Coordinator {
                                source: Box::new(source),
                            });
                        }
                        Err(rollback_source) => {
                            return Err(NoteCrudError::CoordinatorRollback {
                                source: Box::new(source),
                                details: Box::new(RollbackFailure {
                                    step: "restore_deleted_file",
                                    path: absolute,
                                    error: rollback_source.to_string(),
                                }),
                            });
                        }
                    }
                }
                return Err(NoteCrudError::Coordinator {
                    source: Box::new(source),
                });
            }
        };

        if removed {
            self.events.publish(DomainEvent::NoteChanged {
                file_id: file_id.to_string(),
                normalized_path: existing.normalized_path,
                kind: NoteChangeKind::Deleted,
            });
        }

        Ok(removed)
    }

    /// Rename or move a note to a new relative path and refresh metadata.
    pub fn rename_note(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        new_relative_path: &Path,
    ) -> Result<NoteCrudResult, NoteCrudError> {
        validate_relative_note_path(new_relative_path)?;

        let existing = FilesRepository::get_by_id(connection, file_id)
            .map_err(|source| NoteCrudError::Repository { source })?;
        let Some(existing) = existing else {
            return Err(NoteCrudError::MissingFileRecord {
                file_id: file_id.to_string(),
            });
        };

        let old_absolute =
            ensure_note_path_within_vault(vault_root, &vault_root.join(&existing.normalized_path))?;
        let new_absolute = prepare_note_create_path(vault_root, new_relative_path)?;

        fs::rename(&old_absolute, &new_absolute).map_err(|source| NoteCrudError::RenameFile {
            from: old_absolute.clone(),
            to: new_absolute.clone(),
            source,
        })?;

        let record = fingerprint_to_file_record(file_id, vault_root, new_relative_path)?;
        if let Err(source) = self
            .coordinator
            .replace_file_metadata(connection, file_id, &record)
        {
            match fs::rename(&new_absolute, &old_absolute) {
                Ok(_) => {
                    return Err(NoteCrudError::Coordinator {
                        source: Box::new(source),
                    });
                }
                Err(rollback_source) => {
                    return Err(NoteCrudError::CoordinatorRollback {
                        source: Box::new(source),
                        details: Box::new(RollbackFailure {
                            step: "rename_back_to_original_path",
                            path: old_absolute,
                            error: rollback_source.to_string(),
                        }),
                    });
                }
            }
        }

        self.events.publish(DomainEvent::NoteChanged {
            file_id: file_id.to_string(),
            normalized_path: record.normalized_path.clone(),
            kind: NoteChangeKind::Renamed,
        });

        Ok(NoteCrudResult {
            file_id: file_id.to_string(),
            normalized_path: record.normalized_path,
        })
    }

    /// Move note convenience wrapper over `rename_note`.
    pub fn move_note(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        destination_relative_path: &Path,
    ) -> Result<NoteCrudResult, NoteCrudError> {
        self.rename_note(vault_root, connection, file_id, destination_relative_path)
    }

    /// Tracing hook wrapper for `create_note` with explicit correlation context.
    pub fn create_note_with_trace_context(
        &self,
        trace_context: &ServiceTraceContext,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        relative_path: &Path,
        content: &str,
    ) -> Result<NoteCrudResult, NoteCrudError> {
        let span = trace_context.span();
        let _entered = span.enter();
        trace_context.emit_start();

        let result = self.create_note(vault_root, connection, file_id, relative_path, content);
        match &result {
            Ok(_) => trace_context.emit_success(),
            Err(error) => trace_context.emit_failure(error),
        }
        result
    }

    /// Tracing hook wrapper for `update_note` with explicit correlation context.
    pub fn update_note_with_trace_context(
        &self,
        trace_context: &ServiceTraceContext,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        relative_path: &Path,
        content: &str,
    ) -> Result<NoteCrudResult, NoteCrudError> {
        let span = trace_context.span();
        let _entered = span.enter();
        trace_context.emit_start();

        let result = self.update_note(vault_root, connection, file_id, relative_path, content);
        match &result {
            Ok(_) => trace_context.emit_success(),
            Err(error) => trace_context.emit_failure(error),
        }
        result
    }

    /// Tracing hook wrapper for `delete_note` with explicit correlation context.
    pub fn delete_note_with_trace_context(
        &self,
        trace_context: &ServiceTraceContext,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
    ) -> Result<bool, NoteCrudError> {
        let span = trace_context.span();
        let _entered = span.enter();
        trace_context.emit_start();

        let result = self.delete_note(vault_root, connection, file_id);
        match &result {
            Ok(_) => trace_context.emit_success(),
            Err(error) => trace_context.emit_failure(error),
        }
        result
    }

    /// Tracing hook wrapper for `rename_note` with explicit correlation context.
    pub fn rename_note_with_trace_context(
        &self,
        trace_context: &ServiceTraceContext,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        new_relative_path: &Path,
    ) -> Result<NoteCrudResult, NoteCrudError> {
        let span = trace_context.span();
        let _entered = span.enter();
        trace_context.emit_start();

        let result = self.rename_note(vault_root, connection, file_id, new_relative_path);
        match &result {
            Ok(_) => trace_context.emit_success(),
            Err(error) => trace_context.emit_failure(error),
        }
        result
    }
}

fn validate_relative_note_path(relative_path: &Path) -> Result<(), NoteCrudError> {
    if relative_path.is_absolute() {
        return Err(NoteCrudError::InvalidPath {
            path: relative_path.to_path_buf(),
        });
    }

    if relative_path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(NoteCrudError::InvalidPath {
            path: relative_path.to_path_buf(),
        });
    }

    Ok(())
}

fn prepare_note_create_path(
    vault_root: &Path,
    relative_path: &Path,
) -> Result<PathBuf, NoteCrudError> {
    let absolute = vault_root.join(relative_path);
    let parent = absolute
        .parent()
        .ok_or_else(|| NoteCrudError::InvalidPath {
            path: relative_path.to_path_buf(),
        })?;

    validate_parent_ancestor_within_vault(vault_root, parent)?;
    fs::create_dir_all(parent).map_err(|source| NoteCrudError::CreateDir {
        path: parent.to_path_buf(),
        source,
    })?;
    ensure_note_parent_within_vault(vault_root, &absolute)?;
    Ok(absolute)
}

fn validate_parent_ancestor_within_vault(
    vault_root: &Path,
    parent: &Path,
) -> Result<(), NoteCrudError> {
    let existing_ancestor =
        nearest_existing_ancestor(parent).ok_or_else(|| NoteCrudError::InvalidPath {
            path: parent.to_path_buf(),
        })?;
    let canonical_vault = canonicalize_vault_root(vault_root)?;
    let canonical_ancestor =
        fs::canonicalize(existing_ancestor).map_err(|source| NoteCrudError::CanonicalizePath {
            path: existing_ancestor.to_path_buf(),
            source,
        })?;

    if !canonical_ancestor.starts_with(&canonical_vault) {
        return Err(NoteCrudError::PathOutsideVault {
            vault_root: canonical_vault,
            path: canonical_ancestor,
        });
    }

    Ok(())
}

fn nearest_existing_ancestor(path: &Path) -> Option<&Path> {
    let mut cursor = Some(path);
    while let Some(candidate) = cursor {
        if candidate.exists() {
            return Some(candidate);
        }
        cursor = candidate.parent();
    }
    None
}

fn ensure_note_path_within_vault(
    vault_root: &Path,
    absolute: &Path,
) -> Result<PathBuf, NoteCrudError> {
    let canonical_vault = canonicalize_vault_root(vault_root)?;
    let canonical_path =
        fs::canonicalize(absolute).map_err(|source| NoteCrudError::CanonicalizePath {
            path: absolute.to_path_buf(),
            source,
        })?;

    if !canonical_path.starts_with(&canonical_vault) {
        return Err(NoteCrudError::PathOutsideVault {
            vault_root: canonical_vault,
            path: canonical_path,
        });
    }

    Ok(absolute.to_path_buf())
}

fn ensure_note_parent_within_vault(
    vault_root: &Path,
    absolute: &Path,
) -> Result<(), NoteCrudError> {
    let parent = absolute
        .parent()
        .ok_or_else(|| NoteCrudError::InvalidPath {
            path: absolute.to_path_buf(),
        })?;
    let canonical_vault = canonicalize_vault_root(vault_root)?;
    let canonical_parent =
        fs::canonicalize(parent).map_err(|source| NoteCrudError::CanonicalizePath {
            path: parent.to_path_buf(),
            source,
        })?;

    if !canonical_parent.starts_with(&canonical_vault) {
        return Err(NoteCrudError::PathOutsideVault {
            vault_root: canonical_vault,
            path: canonical_parent,
        });
    }

    Ok(())
}

fn canonicalize_vault_root(vault_root: &Path) -> Result<PathBuf, NoteCrudError> {
    fs::canonicalize(vault_root).map_err(|source| NoteCrudError::CanonicalizeVaultRoot {
        path: vault_root.to_path_buf(),
        source,
    })
}

fn fingerprint_to_file_record(
    file_id: &str,
    vault_root: &Path,
    relative_path: &Path,
) -> Result<FileRecordInput, NoteCrudError> {
    let fingerprint_service = FileFingerprintService::from_root(vault_root, CasePolicy::Sensitive)
        .map_err(|source| NoteCrudError::FingerprintPath { source })?;
    let fingerprint = fingerprint_service
        .fingerprint(relative_path)
        .map_err(|source| NoteCrudError::Fingerprint { source })?;

    let modified_unix_ms = i64::try_from(fingerprint.modified_unix_ms).map_err(|_| {
        NoteCrudError::TimestampOverflow {
            value: fingerprint.modified_unix_ms,
        }
    })?;

    Ok(FileRecordInput {
        file_id: file_id.to_string(),
        normalized_path: fingerprint.normalized,
        match_key: fingerprint.match_key,
        absolute_path: fingerprint.absolute.to_string_lossy().to_string(),
        size_bytes: fingerprint.size_bytes,
        modified_unix_ms,
        hash_blake3: fingerprint.hash_blake3,
        is_markdown: true,
    })
}

/// Errors returned by note create/update/delete operations.
#[derive(Debug, Error)]
pub enum NoteCrudError {
    /// File metadata row does not exist for requested file id.
    #[error("no file metadata found for file id '{file_id}'")]
    MissingFileRecord {
        /// Missing file id.
        file_id: String,
    },
    /// Provided relative note path is invalid.
    #[error("invalid note path '{path}'")]
    InvalidPath {
        /// Invalid path.
        path: PathBuf,
    },
    /// Creating parent directories failed.
    #[error("failed to create directory '{path}': {source}")]
    CreateDir {
        /// Directory path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Writing note file content failed.
    #[error("failed to write note file '{path}': {source}")]
    WriteFile {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Reading note file content for rollback failed.
    #[error("failed to read note file '{path}' for rollback safety: {source}")]
    ReadFileForRollback {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Renaming note file failed.
    #[error("failed to rename note file from '{from}' to '{to}': {source}")]
    RenameFile {
        /// Previous file path.
        from: PathBuf,
        /// New file path.
        to: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Deleting note file failed.
    #[error("failed to delete note file '{path}': {source}")]
    DeleteFile {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Canonicalizing vault root for boundary checks failed.
    #[error("failed to canonicalize vault root '{path}': {source}")]
    CanonicalizeVaultRoot {
        /// Vault root path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Canonicalizing path for boundary checks failed.
    #[error("failed to canonicalize note path '{path}': {source}")]
    CanonicalizePath {
        /// Path being canonicalized.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Note path resolves outside the configured vault root.
    #[error("note path '{path}' resolves outside vault root '{vault_root}'")]
    PathOutsideVault {
        /// Canonical vault root.
        vault_root: PathBuf,
        /// Canonical note path.
        path: PathBuf,
    },
    /// Building fingerprint service path context failed.
    #[error("failed to initialize fingerprint path service: {source}")]
    FingerprintPath {
        /// Path canonicalization error.
        #[source]
        source: PathCanonicalizationError,
    },
    /// File fingerprint operation failed.
    #[error("failed to fingerprint note file: {source}")]
    Fingerprint {
        /// Fingerprint error.
        #[source]
        source: FileFingerprintError,
    },
    /// Fingerprint modified timestamp overflows storage integer type.
    #[error("fingerprint modified timestamp overflows i64: {value}")]
    TimestampOverflow {
        /// Raw timestamp value.
        value: u128,
    },
    /// Coordinator transaction failed.
    #[error("note coordinator failed: {source}")]
    Coordinator {
        /// Coordinator error.
        #[source]
        source: Box<SdkTransactionError>,
    },
    /// Coordinator transaction failed and filesystem rollback also failed.
    #[error(
        "note coordinator failed and filesystem rollback failed: {details}; coordinator: {source}"
    )]
    CoordinatorRollback {
        /// Coordinator error.
        #[source]
        source: Box<SdkTransactionError>,
        /// Rollback failure details.
        details: Box<RollbackFailure>,
    },
    /// Repository query failed.
    #[error("note repository query failed: {source}")]
    Repository {
        /// Files repository error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
}

/// Filesystem rollback failure details for note write paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackFailure {
    /// Rollback step identifier.
    pub step: &'static str,
    /// Filesystem path that failed rollback.
    pub path: PathBuf,
    /// Rollback failure details.
    pub error: String,
}

impl fmt::Display for RollbackFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "step '{}' failed for '{}': {}",
            self.step,
            self.path.display(),
            self.error
        )
    }
}

/// Result payload for typed property update operations.
#[derive(Debug, Clone, PartialEq)]
pub struct PropertyUpdateResult {
    /// File id that was updated.
    pub file_id: String,
    /// Property key that was set.
    pub key: String,
    /// Typed value persisted.
    pub value: TypedPropertyValue,
    /// Parsed markdown result after update.
    pub parsed: MarkdownParseResult,
}

/// Service that applies typed property updates into note front matter and storage.
#[derive(Clone)]
pub struct PropertyUpdateService {
    note_crud: NoteCrudService,
    parser: MarkdownParser,
}

impl Default for PropertyUpdateService {
    fn default() -> Self {
        Self {
            note_crud: NoteCrudService::default(),
            parser: MarkdownParser,
        }
    }
}

impl PropertyUpdateService {
    /// Set one typed property on a note, persist metadata, and parse updated markdown.
    pub fn set_property(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        key: &str,
        value: TypedPropertyValue,
    ) -> Result<PropertyUpdateResult, PropertyUpdateError> {
        let existing = FilesRepository::get_by_id(connection, file_id)
            .map_err(|source| PropertyUpdateError::Repository { source })?;
        let Some(existing) = existing else {
            return Err(PropertyUpdateError::MissingFileRecord {
                file_id: file_id.to_string(),
            });
        };

        let absolute = vault_root.join(&existing.normalized_path);
        let markdown =
            fs::read_to_string(&absolute).map_err(|source| PropertyUpdateError::ReadFile {
                path: absolute.clone(),
                source,
            })?;

        let extraction = extract_front_matter(&markdown);
        let body = extraction.body;
        let mut mapping = match extraction.status {
            FrontMatterStatus::Parsed { value } => match value {
                serde_yaml::Value::Mapping(mapping) => mapping,
                _ => serde_yaml::Mapping::new(),
            },
            FrontMatterStatus::Malformed { .. } | FrontMatterStatus::Missing => {
                serde_yaml::Mapping::new()
            }
        };

        mapping.insert(
            serde_yaml::Value::String(key.to_string()),
            typed_value_to_yaml(&value),
        );
        let yaml = serde_yaml::to_string(&serde_yaml::Value::Mapping(mapping))
            .map_err(|source| PropertyUpdateError::SerializeYaml { source })?;

        let mut updated_markdown = String::new();
        updated_markdown.push_str("---\n");
        updated_markdown.push_str(&yaml);
        updated_markdown.push_str("---\n");
        if !body.is_empty() {
            updated_markdown.push_str(&body);
        }

        self.note_crud
            .update_note(
                vault_root,
                connection,
                file_id,
                Path::new(&existing.normalized_path),
                &updated_markdown,
            )
            .map_err(|source| PropertyUpdateError::NoteUpdate {
                source: Box::new(source),
            })?;

        let parsed = self
            .parser
            .parse(MarkdownParseRequest {
                normalized_path: existing.normalized_path.clone(),
                raw: updated_markdown,
            })
            .map_err(|source| PropertyUpdateError::Parse { source })?;

        let property_input = PropertyRecordInput {
            property_id: format!("{file_id}:{key}"),
            file_id: file_id.to_string(),
            key: key.to_string(),
            value_type: typed_value_kind(&value).to_string(),
            value_json: serde_json::to_string(&typed_value_to_json(&value))
                .map_err(|source| PropertyUpdateError::SerializeJson { source })?,
        };
        PropertiesRepository::upsert(connection, &property_input)
            .map_err(|source| PropertyUpdateError::PropertyRepository { source })?;

        Ok(PropertyUpdateResult {
            file_id: file_id.to_string(),
            key: key.to_string(),
            value,
            parsed,
        })
    }

    /// Tracing hook wrapper for `set_property` with explicit correlation context.
    pub fn set_property_with_trace_context(
        &self,
        trace_context: &ServiceTraceContext,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        key: &str,
        value: TypedPropertyValue,
    ) -> Result<PropertyUpdateResult, PropertyUpdateError> {
        let span = trace_context.span();
        let _entered = span.enter();
        trace_context.emit_start();

        let result = self.set_property(vault_root, connection, file_id, key, value);
        match &result {
            Ok(_) => trace_context.emit_success(),
            Err(error) => trace_context.emit_failure(error),
        }
        result
    }
}

fn typed_value_kind(value: &TypedPropertyValue) -> &'static str {
    match value {
        TypedPropertyValue::Bool(_) => "bool",
        TypedPropertyValue::Number(_) => "number",
        TypedPropertyValue::Date(_) => "date",
        TypedPropertyValue::String(_) => "string",
        TypedPropertyValue::List(_) => "list",
        TypedPropertyValue::Null => "null",
    }
}

fn typed_value_to_yaml(value: &TypedPropertyValue) -> serde_yaml::Value {
    match value {
        TypedPropertyValue::Bool(value) => serde_yaml::Value::Bool(*value),
        TypedPropertyValue::Number(value) => {
            serde_yaml::to_value(*value).unwrap_or(serde_yaml::Value::Null)
        }
        TypedPropertyValue::Date(value) | TypedPropertyValue::String(value) => {
            serde_yaml::Value::String(value.clone())
        }
        TypedPropertyValue::List(values) => {
            serde_yaml::Value::Sequence(values.iter().map(typed_value_to_yaml).collect())
        }
        TypedPropertyValue::Null => serde_yaml::Value::Null,
    }
}

fn typed_value_to_json(value: &TypedPropertyValue) -> JsonValue {
    match value {
        TypedPropertyValue::Bool(value) => JsonValue::Bool(*value),
        TypedPropertyValue::Number(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        TypedPropertyValue::Date(value) | TypedPropertyValue::String(value) => {
            JsonValue::String(value.clone())
        }
        TypedPropertyValue::List(values) => {
            JsonValue::Array(values.iter().map(typed_value_to_json).collect())
        }
        TypedPropertyValue::Null => JsonValue::Null,
    }
}

/// Errors returned by typed property update operations.
#[derive(Debug, Error)]
pub enum PropertyUpdateError {
    /// File metadata row missing for requested file id.
    #[error("no file metadata found for file id '{file_id}'")]
    MissingFileRecord {
        /// Missing file id.
        file_id: String,
    },
    /// Reading note file failed.
    #[error("failed to read note file '{path}': {source}")]
    ReadFile {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Note update flow failed.
    #[error("note update failed while setting property: {source}")]
    NoteUpdate {
        /// Note update error.
        #[source]
        source: Box<NoteCrudError>,
    },
    /// Parsing updated markdown failed.
    #[error("failed to parse updated markdown after property set: {source}")]
    Parse {
        /// Markdown parser error.
        #[source]
        source: MarkdownParseError,
    },
    /// YAML serialization failed.
    #[error("failed to serialize front matter yaml: {source}")]
    SerializeYaml {
        /// YAML serializer error.
        #[source]
        source: serde_yaml::Error,
    },
    /// JSON serialization failed.
    #[error("failed to serialize property json payload: {source}")]
    SerializeJson {
        /// JSON serializer error.
        #[source]
        source: serde_json::Error,
    },
    /// Files repository query failed.
    #[error("file repository operation failed: {source}")]
    Repository {
        /// Files repository error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
    /// Properties repository update failed.
    #[error("property repository operation failed: {source}")]
    PropertyRepository {
        /// Properties repository error.
        #[source]
        source: tao_sdk_storage::PropertiesRepositoryError,
    },
}

/// Sorting strategies supported by property query APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyQuerySort {
    /// Sort by file path ascending.
    FilePathAsc,
    /// Sort by file path descending.
    FilePathDesc,
    /// Sort by update timestamp ascending.
    UpdatedAtAsc,
    /// Sort by update timestamp descending.
    UpdatedAtDesc,
    /// Sort by raw JSON value ascending.
    ValueAsc,
    /// Sort by raw JSON value descending.
    ValueDesc,
}

/// Request payload for property query APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyQueryRequest {
    /// Property key to query across files.
    pub key: String,
    /// Optional substring filter applied to JSON value payload.
    pub value_contains: Option<String>,
    /// Optional max rows to return.
    pub limit: Option<usize>,
    /// Row offset for pagination.
    pub offset: usize,
    /// Sort strategy.
    pub sort: PropertyQuerySort,
}

/// Property query row returned by query APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyQueryRow {
    /// Stable property id.
    pub property_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning file normalized path.
    pub file_path: String,
    /// Property key.
    pub key: String,
    /// Property value type.
    pub value_type: String,
    /// Property value payload JSON.
    pub value_json: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Property query result page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyQueryResult {
    /// Total rows matching filters before pagination.
    pub total: u64,
    /// Page rows after sort/pagination.
    pub rows: Vec<PropertyQueryRow>,
}

/// Query service for filtering and sorting property rows across files.
#[derive(Debug, Default, Clone, Copy)]
pub struct PropertyQueryService;

impl PropertyQueryService {
    /// Query property rows by key with filter/sort/pagination controls.
    pub fn query(
        &self,
        connection: &Connection,
        request: &PropertyQueryRequest,
    ) -> Result<PropertyQueryResult, PropertyQueryError> {
        let key = request.key.trim();
        if key.is_empty() {
            return Err(PropertyQueryError::InvalidKey);
        }
        if matches!(request.limit, Some(0)) {
            return Err(PropertyQueryError::InvalidLimit { limit: 0 });
        }

        let mut rows = PropertiesRepository::list_by_key_with_paths(connection, key)
            .map_err(|source| PropertyQueryError::Repository { source })?;

        if let Some(filter) = request
            .value_contains
            .as_deref()
            .map(str::trim)
            .filter(|filter| !filter.is_empty())
        {
            let filter = filter.to_lowercase();
            rows.retain(|row| row.value_json.to_lowercase().contains(&filter));
        }

        rows.sort_by(|left, right| compare_property_rows(left, right, request.sort));

        let total = rows.len() as u64;
        let iter = rows.into_iter().skip(request.offset);
        let paged_rows = match request.limit {
            Some(limit) => iter.take(limit).collect::<Vec<_>>(),
            None => iter.collect::<Vec<_>>(),
        };

        let rows = paged_rows
            .into_iter()
            .map(|row| PropertyQueryRow {
                property_id: row.property_id,
                file_id: row.file_id,
                file_path: row.file_path,
                key: row.key,
                value_type: row.value_type,
                value_json: row.value_json,
                updated_at: row.updated_at,
            })
            .collect();

        Ok(PropertyQueryResult { total, rows })
    }
}

fn compare_property_rows(
    left: &tao_sdk_storage::PropertyWithPath,
    right: &tao_sdk_storage::PropertyWithPath,
    sort: PropertyQuerySort,
) -> std::cmp::Ordering {
    match sort {
        PropertyQuerySort::FilePathAsc => left
            .file_path
            .cmp(&right.file_path)
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::FilePathDesc => right
            .file_path
            .cmp(&left.file_path)
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::UpdatedAtAsc => left
            .updated_at
            .cmp(&right.updated_at)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::UpdatedAtDesc => right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::ValueAsc => left
            .value_json
            .cmp(&right.value_json)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::ValueDesc => right
            .value_json
            .cmp(&left.value_json)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.property_id.cmp(&right.property_id)),
    }
}

/// Property query failures.
#[derive(Debug, Error)]
pub enum PropertyQueryError {
    /// Query key was empty.
    #[error("property query key must not be empty")]
    InvalidKey,
    /// Query limit was invalid.
    #[error("property query limit must be greater than zero")]
    InvalidLimit {
        /// Invalid limit value.
        limit: usize,
    },
    /// Properties repository query failed.
    #[error("property query repository operation failed: {source}")]
    Repository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::PropertiesRepositoryError,
    },
}

/// One row returned from base table execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseTableRow {
    /// Stable file id.
    pub file_id: String,
    /// Normalized file path.
    pub file_path: String,
    /// Projected column values keyed by column key.
    pub values: serde_json::Map<String, JsonValue>,
}

/// Paged table result from executing one base query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseTableSummary {
    /// Column key.
    pub key: String,
    /// Non-null value count.
    pub count: u64,
    /// Minimum value across matching rows.
    pub min: Option<JsonValue>,
    /// Maximum value across matching rows.
    pub max: Option<JsonValue>,
    /// Average value for numeric cells only.
    pub avg: Option<JsonValue>,
}

/// Paged table result from executing one base query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseTablePage {
    /// Total rows that matched filters before pagination.
    pub total: u64,
    /// Summary rows for configured columns over the filtered result set.
    pub summaries: Vec<BaseTableSummary>,
    /// Rows in this page.
    pub rows: Vec<BaseTableRow>,
}

/// Executor service that runs compiled base table plans against SQLite metadata.
#[derive(Debug, Default, Clone, Copy)]
pub struct BaseTableExecutorService;

impl BaseTableExecutorService {
    /// Execute one compiled table query plan and return a paged result.
    pub fn execute(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
    ) -> Result<BaseTablePage, BaseTableExecutorError> {
        if plan.limit == 0 {
            return Err(BaseTableExecutorError::InvalidPlan {
                reason: "limit must be greater than zero".to_string(),
            });
        }

        let files = FilesRepository::list_all(connection)
            .map_err(|source| BaseTableExecutorError::FilesRepository { source })?;
        let mut candidates = files
            .into_iter()
            .filter(|file| {
                matches_source_prefix(&file.normalized_path, plan.source_prefix.as_deref())
            })
            .map(|file| TableRowCandidate {
                file_id: file.file_id,
                file_path: file.normalized_path,
                properties: HashMap::new(),
            })
            .collect::<Vec<_>>();
        let candidate_indices = candidates
            .iter()
            .enumerate()
            .map(|(index, row)| (row.file_id.clone(), index))
            .collect::<HashMap<_, _>>();

        for key in &plan.required_property_keys {
            let rows = PropertiesRepository::list_by_key_with_paths(connection, key).map_err(
                |source| BaseTableExecutorError::PropertiesRepository {
                    key: key.clone(),
                    source,
                },
            )?;
            for row in rows {
                let Some(candidate_index) = candidate_indices.get(&row.file_id).copied() else {
                    continue;
                };
                let value =
                    serde_json::from_str::<JsonValue>(&row.value_json).map_err(|source| {
                        BaseTableExecutorError::ParsePropertyValue {
                            file_id: row.file_id.clone(),
                            key: key.clone(),
                            source,
                        }
                    })?;
                candidates[candidate_index]
                    .properties
                    .insert(key.clone(), value);
            }
        }

        candidates.retain(|row| row_matches_filters(row, &plan.filters));
        candidates.sort_by(|left, right| compare_table_rows(left, right, &plan.sorts));

        let total = candidates.len() as u64;
        let summaries = compute_table_summaries(&candidates, &plan.columns);
        let rows = candidates
            .into_iter()
            .skip(plan.offset)
            .take(plan.limit)
            .map(|row| project_table_row(row, &plan.columns))
            .collect::<Vec<_>>();

        Ok(BaseTablePage {
            total,
            summaries,
            rows,
        })
    }
}

#[derive(Debug, Clone)]
struct TableRowCandidate {
    file_id: String,
    file_path: String,
    properties: HashMap<String, JsonValue>,
}

impl TableRowCandidate {
    fn lookup_value(&self, key: &str) -> Option<JsonValue> {
        if key.eq_ignore_ascii_case("path") || key.eq_ignore_ascii_case("file_path") {
            return Some(JsonValue::String(self.file_path.clone()));
        }
        if key.eq_ignore_ascii_case("title") {
            return Some(JsonValue::String(note_title_from_path(&self.file_path)));
        }

        self.properties.get(key).cloned()
    }
}

fn note_title_from_path(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map_or_else(|| path.to_string(), |stem| stem.to_string())
}

fn matches_source_prefix(path: &str, source_prefix: Option<&str>) -> bool {
    let Some(source_prefix) = source_prefix else {
        return true;
    };
    if path == source_prefix {
        return true;
    }

    path.strip_prefix(source_prefix)
        .is_some_and(|remainder| remainder.starts_with('/'))
}

fn row_matches_filters(row: &TableRowCandidate, filters: &[BaseFilterClause]) -> bool {
    filters.iter().all(|filter| row_matches_filter(row, filter))
}

fn row_matches_filter(row: &TableRowCandidate, filter: &BaseFilterClause) -> bool {
    let row_value = row.lookup_value(&filter.key);

    match filter.op {
        BaseFilterOp::Eq => row_value
            .as_ref()
            .is_some_and(|value| value == &filter.value),
        BaseFilterOp::NotEq => row_value
            .as_ref()
            .is_none_or(|value| value != &filter.value),
        BaseFilterOp::Gt => row_value
            .as_ref()
            .is_some_and(|value| compare_json_values(value, &filter.value).is_gt()),
        BaseFilterOp::Gte => row_value
            .as_ref()
            .is_some_and(|value| compare_json_values(value, &filter.value).is_ge()),
        BaseFilterOp::Lt => row_value
            .as_ref()
            .is_some_and(|value| compare_json_values(value, &filter.value).is_lt()),
        BaseFilterOp::Lte => row_value
            .as_ref()
            .is_some_and(|value| compare_json_values(value, &filter.value).is_le()),
        BaseFilterOp::Contains => row_value
            .as_ref()
            .is_some_and(|value| value_contains(value, &filter.value)),
        BaseFilterOp::In => row_value
            .as_ref()
            .is_some_and(|value| filter_contains_value(&filter.value, value)),
        BaseFilterOp::NotIn => row_value
            .as_ref()
            .is_none_or(|value| !filter_contains_value(&filter.value, value)),
        BaseFilterOp::Exists => {
            let expected_exists = filter.value.as_bool().unwrap_or(true);
            row_value.is_some() == expected_exists
        }
    }
}

fn value_contains(value: &JsonValue, filter_value: &JsonValue) -> bool {
    let Some(needle) = json_scalar_to_string(filter_value) else {
        return false;
    };
    let needle = needle.to_lowercase();

    match value {
        JsonValue::Array(values) => values
            .iter()
            .any(|entry| value_contains(entry, filter_value)),
        _ => json_scalar_to_string(value)
            .unwrap_or_else(|| value.to_string())
            .to_lowercase()
            .contains(&needle),
    }
}

fn filter_contains_value(filter_value: &JsonValue, row_value: &JsonValue) -> bool {
    match filter_value {
        JsonValue::Array(values) => values.iter().any(|value| value == row_value),
        _ => false,
    }
}

fn compare_table_rows(
    left: &TableRowCandidate,
    right: &TableRowCandidate,
    sorts: &[BaseSortClause],
) -> Ordering {
    for sort in sorts {
        let ordering = compare_optional_json_values(
            left.lookup_value(&sort.key).as_ref(),
            right.lookup_value(&sort.key).as_ref(),
        );
        let ordering = match sort.direction {
            BaseSortDirection::Asc => ordering,
            BaseSortDirection::Desc => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    left.file_path
        .cmp(&right.file_path)
        .then_with(|| left.file_id.cmp(&right.file_id))
}

fn compare_optional_json_values(left: Option<&JsonValue>, right: Option<&JsonValue>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => compare_json_values(left, right),
    }
}

fn compare_json_values(left: &JsonValue, right: &JsonValue) -> Ordering {
    let left_rank = json_type_rank(left);
    let right_rank = json_type_rank(right);
    if left_rank != right_rank {
        return left_rank.cmp(&right_rank);
    }

    match (left, right) {
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            let left = left.as_f64().unwrap_or(0.0);
            let right = right.as_f64().unwrap_or(0.0);
            left.partial_cmp(&right).unwrap_or(Ordering::Equal)
        }
        (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
        (JsonValue::Array(left), JsonValue::Array(right)) => left.len().cmp(&right.len()),
        (JsonValue::Object(left), JsonValue::Object(right)) => left.len().cmp(&right.len()),
        _ => left.to_string().cmp(&right.to_string()),
    }
}

fn json_type_rank(value: &JsonValue) -> u8 {
    match value {
        JsonValue::Null => 0,
        JsonValue::Bool(_) => 1,
        JsonValue::Number(_) => 2,
        JsonValue::String(_) => 3,
        JsonValue::Array(_) => 4,
        JsonValue::Object(_) => 5,
    }
}

fn project_table_row(row: TableRowCandidate, columns: &[BaseColumnConfig]) -> BaseTableRow {
    let mut values = serde_json::Map::new();
    for column in columns {
        values.insert(
            column.key.clone(),
            row.lookup_value(&column.key).unwrap_or(JsonValue::Null),
        );
    }

    BaseTableRow {
        file_id: row.file_id,
        file_path: row.file_path,
        values,
    }
}

fn compute_table_summaries(
    rows: &[TableRowCandidate],
    columns: &[BaseColumnConfig],
) -> Vec<BaseTableSummary> {
    columns
        .iter()
        .map(|column| {
            let mut count = 0_u64;
            let mut min: Option<JsonValue> = None;
            let mut max: Option<JsonValue> = None;
            let mut numeric_sum = 0_f64;
            let mut numeric_count = 0_u64;

            for row in rows {
                let Some(value) = row.lookup_value(&column.key) else {
                    continue;
                };
                if value.is_null() {
                    continue;
                }

                count += 1;
                if min
                    .as_ref()
                    .is_none_or(|current| compare_json_values(&value, current).is_lt())
                {
                    min = Some(value.clone());
                }
                if max
                    .as_ref()
                    .is_none_or(|current| compare_json_values(&value, current).is_gt())
                {
                    max = Some(value.clone());
                }
                if let Some(number) = value.as_f64() {
                    numeric_sum += number;
                    numeric_count += 1;
                }
            }

            let avg = if numeric_count > 0 {
                serde_json::Number::from_f64(numeric_sum / (numeric_count as f64))
                    .map(JsonValue::Number)
            } else {
                None
            };

            BaseTableSummary {
                key: column.key.clone(),
                count,
                min,
                max,
                avg,
            }
        })
        .collect()
}

fn json_scalar_to_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

/// Base table execution failures.
#[derive(Debug, Error)]
pub enum BaseTableExecutorError {
    /// Plan payload was invalid for execution.
    #[error("invalid base table plan: {reason}")]
    InvalidPlan {
        /// Validation message.
        reason: String,
    },
    /// Listing file rows failed.
    #[error("failed to list file metadata for base table execution: {source}")]
    FilesRepository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
    /// Listing property rows by key failed.
    #[error("failed to list property rows for key '{key}' during base table execution: {source}")]
    PropertiesRepository {
        /// Property key.
        key: String,
        /// Repository error.
        #[source]
        source: tao_sdk_storage::PropertiesRepositoryError,
    },
    /// Stored property JSON payload could not be decoded.
    #[error("failed to parse property json for file '{file_id}' key '{key}': {source}")]
    ParsePropertyValue {
        /// File id.
        file_id: String,
        /// Property key.
        key: String,
        /// JSON parse error.
        #[source]
        source: serde_json::Error,
    },
}

/// Column persistence result for one base view update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseColumnConfigPersistResult {
    /// Base identifier.
    pub base_id: String,
    /// View name that was updated.
    pub view_name: String,
    /// Number of persisted columns.
    pub columns_total: u64,
}

/// Persistence service for updating base view column order/visibility config.
#[derive(Debug, Default, Clone, Copy)]
pub struct BaseColumnConfigPersistenceService;

impl BaseColumnConfigPersistenceService {
    /// Persist the full ordered column configuration for one base view.
    pub fn persist_view_columns(
        &self,
        connection: &Connection,
        base_id: &str,
        view_name: &str,
        columns: Vec<BaseColumnConfig>,
    ) -> Result<BaseColumnConfigPersistResult, BaseColumnConfigPersistError> {
        if base_id.trim().is_empty() {
            return Err(BaseColumnConfigPersistError::InvalidInput {
                field: "base_id".to_string(),
            });
        }
        if view_name.trim().is_empty() {
            return Err(BaseColumnConfigPersistError::InvalidInput {
                field: "view_name".to_string(),
            });
        }

        let Some(base) = BasesRepository::get_by_id(connection, base_id)
            .map_err(|source| BaseColumnConfigPersistError::Repository { source })?
        else {
            return Err(BaseColumnConfigPersistError::BaseNotFound {
                base_id: base_id.to_string(),
            });
        };

        let mut document =
            serde_json::from_str::<BaseDocument>(&base.config_json).map_err(|source| {
                BaseColumnConfigPersistError::DeserializeConfig {
                    base_id: base.base_id.clone(),
                    source,
                }
            })?;

        let (resolved_view_name, columns_total) = {
            let Some(view) = document
                .views
                .iter_mut()
                .find(|view| view.name.eq_ignore_ascii_case(view_name))
            else {
                return Err(BaseColumnConfigPersistError::ViewNotFound {
                    base_id: base.base_id.clone(),
                    view_name: view_name.to_string(),
                });
            };
            view.columns = columns;
            (view.name.clone(), view.columns.len() as u64)
        };

        let config_json = serde_json::to_string(&document).map_err(|source| {
            BaseColumnConfigPersistError::SerializeConfig {
                base_id: base.base_id.clone(),
                source,
            }
        })?;
        BasesRepository::upsert(
            connection,
            &BaseRecordInput {
                base_id: base.base_id.clone(),
                file_id: base.file_id.clone(),
                config_json,
            },
        )
        .map_err(|source| BaseColumnConfigPersistError::Repository { source })?;

        Ok(BaseColumnConfigPersistResult {
            base_id: base.base_id,
            view_name: resolved_view_name,
            columns_total,
        })
    }
}

/// Base column configuration persistence failures.
#[derive(Debug, Error)]
pub enum BaseColumnConfigPersistError {
    /// Required input field was empty.
    #[error("base column persistence input '{field}' must not be empty")]
    InvalidInput {
        /// Field name.
        field: String,
    },
    /// Requested base row was not found.
    #[error("base row '{base_id}' not found")]
    BaseNotFound {
        /// Base id.
        base_id: String,
    },
    /// Stored config JSON failed to decode into a base document.
    #[error("failed to decode base config json for '{base_id}': {source}")]
    DeserializeConfig {
        /// Base id.
        base_id: String,
        /// JSON parse error.
        #[source]
        source: serde_json::Error,
    },
    /// Requested view was not present in base config.
    #[error("view '{view_name}' not found in base '{base_id}'")]
    ViewNotFound {
        /// Base id.
        base_id: String,
        /// View name.
        view_name: String,
    },
    /// Updated base config failed to serialize.
    #[error("failed to serialize updated base config for '{base_id}': {source}")]
    SerializeConfig {
        /// Base id.
        base_id: String,
        /// JSON serialization error.
        #[source]
        source: serde_json::Error,
    },
    /// Repository operation failed.
    #[error("base repository operation failed while persisting columns: {source}")]
    Repository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::BasesRepositoryError,
    },
}

/// Base validation API result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseValidationResult {
    /// Base identifier.
    pub base_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning normalized file path.
    pub file_path: String,
    /// Validation diagnostics.
    pub diagnostics: Vec<BaseDiagnostic>,
}

/// Validation service for base config diagnostics.
#[derive(Debug, Default, Clone, Copy)]
pub struct BaseValidationService;

impl BaseValidationService {
    /// Validate one base config by base id or normalized base file path.
    pub fn validate(
        &self,
        connection: &Connection,
        path_or_id: &str,
    ) -> Result<BaseValidationResult, BaseValidationError> {
        let path_or_id = path_or_id.trim();
        if path_or_id.is_empty() {
            return Err(BaseValidationError::InvalidInput);
        }

        if let Some(base) = BasesRepository::get_by_id(connection, path_or_id)
            .map_err(|source| BaseValidationError::Repository { source })?
        {
            let file = FilesRepository::get_by_id(connection, &base.file_id)
                .map_err(|source| BaseValidationError::FilesRepository { source })?;
            let file_path = file.map(|file| file.normalized_path).unwrap_or_default();

            return Ok(BaseValidationResult {
                base_id: base.base_id,
                file_id: base.file_id,
                file_path,
                diagnostics: validate_base_config_json(&base.config_json),
            });
        }

        let Some(base) = BasesRepository::list_with_paths(connection)
            .map_err(|source| BaseValidationError::Repository { source })?
            .into_iter()
            .find(|base| base.file_path == path_or_id)
        else {
            return Err(BaseValidationError::BaseNotFound {
                path_or_id: path_or_id.to_string(),
            });
        };

        Ok(BaseValidationResult {
            base_id: base.base_id,
            file_id: base.file_id,
            file_path: base.file_path,
            diagnostics: validate_base_config_json(&base.config_json),
        })
    }
}

/// Base validation API failures.
#[derive(Debug, Error)]
pub enum BaseValidationError {
    /// Input was empty.
    #[error("base validation input must not be empty")]
    InvalidInput,
    /// Base id/path lookup failed.
    #[error("base '{path_or_id}' not found for validation")]
    BaseNotFound {
        /// Input value used for lookup.
        path_or_id: String,
    },
    /// Bases repository operation failed.
    #[error("base repository operation failed during validation: {source}")]
    Repository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::BasesRepositoryError,
    },
    /// Files repository operation failed.
    #[error("files repository operation failed while resolving base path: {source}")]
    FilesRepository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
}

#[derive(Debug, Default)]
struct BaseTableCacheState {
    metadata_digest: Option<String>,
    entries: HashMap<String, BaseTablePage>,
}

/// Cached base table query service with automatic invalidation on metadata changes.
#[derive(Debug, Default)]
pub struct BaseTableCachedQueryService {
    executor: BaseTableExecutorService,
    state: Mutex<BaseTableCacheState>,
}

impl BaseTableCachedQueryService {
    /// Execute one table plan using cache when the metadata digest is unchanged.
    pub fn execute(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
    ) -> Result<BaseTablePage, BaseTableCacheError> {
        let metadata_digest = compute_base_table_metadata_digest(connection)?;
        let cache_key = serde_json::to_string(plan)
            .map_err(|source| BaseTableCacheError::SerializePlan { source })?;

        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| BaseTableCacheError::LockPoisoned)?;
            if state.metadata_digest.as_deref() != Some(&metadata_digest) {
                state.entries.clear();
                state.metadata_digest = Some(metadata_digest.clone());
            }
            if let Some(cached) = state.entries.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        let computed = self
            .executor
            .execute(connection, plan)
            .map_err(|source| BaseTableCacheError::Execute { source })?;

        let mut state = self
            .state
            .lock()
            .map_err(|_| BaseTableCacheError::LockPoisoned)?;
        if state.metadata_digest.as_deref() != Some(&metadata_digest) {
            state.entries.clear();
            state.metadata_digest = Some(metadata_digest);
        }
        state.entries.insert(cache_key, computed.clone());
        Ok(computed)
    }

    /// Explicitly clear all cached table pages.
    pub fn invalidate_all(&self) -> Result<(), BaseTableCacheError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BaseTableCacheError::LockPoisoned)?;
        state.entries.clear();
        state.metadata_digest = None;
        Ok(())
    }
}

fn compute_base_table_metadata_digest(
    connection: &Connection,
) -> Result<String, BaseTableCacheError> {
    let mut hasher = blake3::Hasher::new();
    hash_table_rows_into_digest(
        connection,
        &mut hasher,
        "files",
        r#"
SELECT file_id, normalized_path, indexed_at, hash_blake3
FROM files
ORDER BY file_id ASC
"#,
    )?;
    hash_table_rows_into_digest(
        connection,
        &mut hasher,
        "properties",
        r#"
SELECT property_id, file_id, key, value_json, updated_at
FROM properties
ORDER BY property_id ASC
"#,
    )?;
    hash_table_rows_into_digest(
        connection,
        &mut hasher,
        "bases",
        r#"
SELECT base_id, file_id, config_json, updated_at
FROM bases
ORDER BY base_id ASC
"#,
    )?;

    Ok(hasher.finalize().to_hex().to_string())
}

fn hash_table_rows_into_digest(
    connection: &Connection,
    hasher: &mut blake3::Hasher,
    table_name: &'static str,
    query: &'static str,
) -> Result<(), BaseTableCacheError> {
    hasher.update(table_name.as_bytes());
    hasher.update(&[0x1d]);

    let mut statement =
        connection
            .prepare(query)
            .map_err(|source| BaseTableCacheError::DigestQuery {
                operation: "prepare_digest_query",
                source,
            })?;
    let mut rows = statement
        .query([])
        .map_err(|source| BaseTableCacheError::DigestQuery {
            operation: "run_digest_query",
            source,
        })?;

    while let Some(row) = rows
        .next()
        .map_err(|source| BaseTableCacheError::DigestQuery {
            operation: "iterate_digest_rows",
            source,
        })?
    {
        for column_index in 0..row.as_ref().column_count() {
            let value =
                row.get_ref(column_index)
                    .map_err(|source| BaseTableCacheError::DigestQuery {
                        operation: "read_digest_row_value",
                        source,
                    })?;
            match value {
                rusqlite::types::ValueRef::Null => {
                    hasher.update(b"<null>");
                }
                rusqlite::types::ValueRef::Integer(value) => {
                    hasher.update(value.to_string().as_bytes());
                }
                rusqlite::types::ValueRef::Real(value) => {
                    hasher.update(value.to_string().as_bytes());
                }
                rusqlite::types::ValueRef::Text(bytes) => {
                    hasher.update(bytes);
                }
                rusqlite::types::ValueRef::Blob(bytes) => {
                    hasher.update(bytes);
                }
            }
            hasher.update(&[0x1f]);
        }
        hasher.update(&[0x1e]);
    }

    Ok(())
}

/// Cached base table query failures.
#[derive(Debug, Error)]
pub enum BaseTableCacheError {
    /// Cache lock was poisoned.
    #[error("base table cache lock poisoned")]
    LockPoisoned,
    /// Plan serialization failed while creating cache key.
    #[error("failed to serialize table plan for cache key: {source}")]
    SerializePlan {
        /// JSON serialization error.
        #[source]
        source: serde_json::Error,
    },
    /// Metadata digest query failed.
    #[error("failed to compute base metadata digest during '{operation}': {source}")]
    DigestQuery {
        /// Operation name.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Underlying table execution failed.
    #[error("failed to execute table plan while populating cache: {source}")]
    Execute {
        /// Execution error.
        #[source]
        source: BaseTableExecutorError,
    },
}

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

/// Health snapshot status for watcher subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatcherStatus {
    /// Filesystem watcher is active.
    Running,
    /// Filesystem watcher is disabled.
    Stopped,
    /// Filesystem watcher is active with degraded guarantees.
    Degraded { reason: String },
}

impl WatcherStatus {
    fn as_label(&self) -> &'static str {
        match self {
            WatcherStatus::Running => "running",
            WatcherStatus::Stopped => "stopped",
            WatcherStatus::Degraded { .. } => "degraded",
        }
    }
}

/// Consolidated SDK health snapshot payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    /// Canonical vault root used for snapshot.
    pub vault_root: String,
    /// Database status flag.
    pub db_healthy: bool,
    /// Applied migration row count.
    pub db_migrations: u64,
    /// Current index lag count.
    pub index_lag: u64,
    /// Watcher status label.
    pub watcher_status: String,
    /// Total scanned files.
    pub files_total: u64,
    /// Total markdown files from latest scan.
    pub markdown_files: u64,
    /// Last index update timestamp when present.
    pub last_index_updated_at: Option<String>,
}

/// Service that builds SDK health snapshot payloads.
#[derive(Debug, Default, Clone, Copy)]
pub struct HealthSnapshotService;

impl HealthSnapshotService {
    /// Build one health snapshot from vault scan + sqlite status + watcher state.
    pub fn snapshot(
        &self,
        vault_root: &Path,
        connection: &Connection,
        index_lag: u64,
        watcher_status: WatcherStatus,
    ) -> Result<HealthSnapshot, HealthSnapshotError> {
        let scanner = VaultScanService::from_root(vault_root, CasePolicy::Sensitive)
            .map_err(|source| HealthSnapshotError::CreateScanner { source })?;
        let manifest = scanner
            .scan()
            .map_err(|source| HealthSnapshotError::Scan { source })?;

        let files_total = manifest.entries.len() as u64;
        let markdown_files = manifest
            .entries
            .iter()
            .filter(|entry| entry.normalized.ends_with(".md"))
            .count() as u64;

        let db_migrations = connection
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .map_err(|source| HealthSnapshotError::DatabaseStatus { source })?;

        let last_index_updated_at = connection
            .query_row(
                "SELECT updated_at FROM index_state WHERE key = 'last_index_at'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| HealthSnapshotError::DatabaseStatus { source })?;

        Ok(HealthSnapshot {
            vault_root: manifest.root.to_string_lossy().to_string(),
            db_healthy: true,
            db_migrations,
            index_lag,
            watcher_status: watcher_status.as_label().to_string(),
            files_total,
            markdown_files,
            last_index_updated_at,
        })
    }
}

/// Errors returned by health snapshot service operations.
#[derive(Debug, Error)]
pub enum HealthSnapshotError {
    /// Scanner initialization failed.
    #[error("failed to initialize vault scanner for health snapshot: {source}")]
    CreateScanner {
        /// Scanner path initialization error.
        #[source]
        source: PathCanonicalizationError,
    },
    /// Vault scan failed.
    #[error("failed to scan vault for health snapshot: {source}")]
    Scan {
        /// Vault scan error.
        #[source]
        source: VaultScanError,
    },
    /// Database status query failed.
    #[error("failed to query database status for health snapshot: {source}")]
    DatabaseStatus {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
}

/// One link graph edge enriched with source/target path metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkGraphEdge {
    /// Stable link row identifier.
    pub link_id: String,
    /// Source file id.
    pub source_file_id: String,
    /// Source normalized path.
    pub source_path: String,
    /// Raw link target payload.
    pub raw_target: String,
    /// Resolved target file id when available.
    pub resolved_file_id: Option<String>,
    /// Resolved target normalized path when available.
    pub resolved_path: Option<String>,
    /// Optional heading fragment slug.
    pub heading_slug: Option<String>,
    /// Optional block fragment id.
    pub block_id: Option<String>,
    /// Unresolved marker.
    pub is_unresolved: bool,
}

/// Link graph query service for outgoing, backlink, and unresolved edges.
#[derive(Debug, Default, Clone, Copy)]
pub struct BacklinkGraphService;

impl BacklinkGraphService {
    /// List outgoing edges for one source note path.
    pub fn outgoing_for_path(
        &self,
        connection: &Connection,
        source_path: &str,
    ) -> Result<Vec<LinkGraphEdge>, LinkGraphServiceError> {
        let Some(source_file) = FilesRepository::get_by_normalized_path(connection, source_path)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
        else {
            return Ok(Vec::new());
        };

        let rows = LinksRepository::list_outgoing_with_paths(connection, &source_file.file_id)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok(map_link_edges(rows))
    }

    /// List backlinks for one target note path.
    pub fn backlinks_for_path(
        &self,
        connection: &Connection,
        target_path: &str,
    ) -> Result<Vec<LinkGraphEdge>, LinkGraphServiceError> {
        let Some(target_file) = FilesRepository::get_by_normalized_path(connection, target_path)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
        else {
            return Ok(Vec::new());
        };

        let rows = LinksRepository::list_backlinks_with_paths(connection, &target_file.file_id)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok(map_link_edges(rows))
    }

    /// List unresolved edges across vault.
    pub fn unresolved_links(
        &self,
        connection: &Connection,
    ) -> Result<Vec<LinkGraphEdge>, LinkGraphServiceError> {
        let rows = LinksRepository::list_unresolved_with_paths(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok(map_link_edges(rows))
    }
}

fn map_link_edges(rows: Vec<tao_sdk_storage::LinkWithPaths>) -> Vec<LinkGraphEdge> {
    rows.into_iter()
        .map(|row| LinkGraphEdge {
            link_id: row.link_id,
            source_file_id: row.source_file_id,
            source_path: row.source_path,
            raw_target: row.raw_target,
            resolved_file_id: row.resolved_file_id,
            resolved_path: row.resolved_path,
            heading_slug: row.heading_slug,
            block_id: row.block_id,
            is_unresolved: row.is_unresolved,
        })
        .collect()
}

/// Link graph query failures.
#[derive(Debug, Error)]
pub enum LinkGraphServiceError {
    /// File lookup by normalized path failed.
    #[error("failed to query file metadata for link graph: {source}")]
    FilesRepository {
        /// Files repository error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
    /// Link graph query failed.
    #[error("failed to query link graph rows: {source}")]
    LinksRepository {
        /// Links repository error.
        #[source]
        source: tao_sdk_storage::LinksRepositoryError,
    },
}

/// Errors returned by markdown ingest pipeline shell operations.
#[derive(Debug, Error)]
pub enum MarkdownIngestError {
    /// Scanner construction failed.
    #[error("failed to create vault scanner: {source}")]
    CreateScanner {
        /// Scanner initialization error.
        #[source]
        source: PathCanonicalizationError,
    },
    /// Vault scan failed.
    #[error("failed to scan vault files: {source}")]
    Scan {
        /// Scan service error.
        #[source]
        source: VaultScanError,
    },
    /// Reading markdown file bytes failed.
    #[error("failed to read markdown file '{path}': {source}")]
    ReadFile {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// UTF-8 decoding failed.
    #[error("failed to decode markdown file '{path}' as utf-8: {source}")]
    DecodeUtf8 {
        /// File path.
        path: PathBuf,
        /// UTF-8 conversion error.
        #[source]
        source: std::string::FromUtf8Error,
    },
    /// Markdown parsing failed.
    #[error("failed to parse markdown file '{path}': {source}")]
    Parse {
        /// File path.
        path: PathBuf,
        /// Markdown parser error.
        #[source]
        source: MarkdownParseError,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    use rusqlite::Connection;
    use tao_sdk_bases::{
        BaseColumnConfig, BaseDiagnosticSeverity, BaseDocument, BaseFilterClause, BaseFilterOp,
        BaseSortClause, BaseSortDirection, BaseViewDefinition, BaseViewKind, TableQueryPlan,
    };
    use tao_sdk_properties::TypedPropertyValue;
    use tao_sdk_storage::{
        BaseRecordInput, BasesRepository, FileRecordInput, FilesRepository, LinkRecordInput,
        LinksRepository, PropertiesRepository, PropertyRecordInput, run_migrations,
    };
    use tempfile::tempdir;

    use super::{
        BacklinkGraphService, BaseColumnConfigPersistError, BaseColumnConfigPersistenceService,
        BaseTableCachedQueryService, BaseTableExecutorError, BaseTableExecutorService,
        BaseValidationError, BaseValidationService, CasePolicy, HealthSnapshotService,
        MarkdownIngestPipeline, NoteCrudError, NoteCrudService, PropertyQueryRequest,
        PropertyQueryService, PropertyQuerySort, PropertyUpdateService, ReconcileService,
        SdkTransactionCoordinator, ServiceTraceContext, StorageWriteService, WatcherStatus,
    };

    fn file_record(
        file_id: &str,
        normalized_path: &str,
        match_key: &str,
        absolute_path: &str,
    ) -> FileRecordInput {
        FileRecordInput {
            file_id: file_id.to_string(),
            normalized_path: normalized_path.to_string(),
            match_key: match_key.to_string(),
            absolute_path: absolute_path.to_string(),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: format!("hash-{file_id}"),
            is_markdown: true,
        }
    }

    #[test]
    fn ingest_vault_parses_markdown_and_skips_non_markdown() {
        let temp = tempdir().expect("tempdir");
        fs::write(temp.path().join("daily.md"), "# Daily\ncontent").expect("write markdown");
        fs::write(temp.path().join("image.png"), "png").expect("write non-markdown");

        let pipeline = MarkdownIngestPipeline::from_root(temp.path(), CasePolicy::Sensitive)
            .expect("create pipeline");
        let notes = pipeline.ingest_vault().expect("ingest vault");

        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].normalized_path, "daily.md");
        assert_eq!(notes[0].parsed.title, "Daily");
    }

    #[test]
    fn ingest_entries_uses_pre_scanned_manifest() {
        let temp = tempdir().expect("tempdir");
        fs::write(temp.path().join("a.md"), "# A").expect("write markdown a");
        fs::write(temp.path().join("b.md"), "# B").expect("write markdown b");

        let pipeline = MarkdownIngestPipeline::from_root(temp.path(), CasePolicy::Sensitive)
            .expect("create pipeline");
        let manifest = pipeline.scanner.scan().expect("scan manifest");

        let notes = pipeline
            .ingest_entries(&manifest.entries)
            .expect("ingest entries");
        assert_eq!(notes.len(), 2);
        assert_eq!(notes[0].parsed.title, "A");
        assert_eq!(notes[1].parsed.title, "B");
    }

    #[test]
    fn storage_write_service_uses_typed_transaction_wrapper() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = StorageWriteService;
        let record = file_record(
            "f1",
            "notes/typed.md",
            "notes/typed.md",
            "/vault/notes/typed.md",
        );

        service
            .create_file_record(&mut connection, &record)
            .expect("create file record through transaction wrapper");

        let persisted = FilesRepository::get_by_id(&connection, "f1")
            .expect("get persisted record")
            .expect("record should exist");
        assert_eq!(persisted.normalized_path, "notes/typed.md");
    }

    #[test]
    fn sdk_transaction_coordinator_replaces_file_metadata_atomically() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let coordinator = SdkTransactionCoordinator;
        let original = file_record(
            "f1",
            "notes/original.md",
            "notes/original.md",
            "/vault/notes/original.md",
        );
        let replacement = file_record(
            "f1",
            "notes/replacement.md",
            "notes/replacement.md",
            "/vault/notes/replacement.md",
        );

        coordinator
            .insert_file_metadata(&mut connection, &original)
            .expect("insert original");
        coordinator
            .replace_file_metadata(&mut connection, "f1", &replacement)
            .expect("replace metadata");

        let persisted = FilesRepository::get_by_id(&connection, "f1")
            .expect("get replaced")
            .expect("row exists");
        assert_eq!(persisted.normalized_path, "notes/replacement.md");
    }

    #[test]
    fn sdk_transaction_coordinator_rolls_back_failed_replacement() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let coordinator = SdkTransactionCoordinator;
        let first = file_record("f1", "notes/a.md", "notes/a.md", "/vault/notes/a.md");
        let second = file_record("f2", "notes/b.md", "notes/b.md", "/vault/notes/b.md");
        coordinator
            .insert_file_metadata(&mut connection, &first)
            .expect("insert first");
        coordinator
            .insert_file_metadata(&mut connection, &second)
            .expect("insert second");

        let conflicting = file_record("f1", "notes/b.md", "notes/b.md", "/vault/notes/b.md");
        let result = coordinator.replace_file_metadata(&mut connection, "f1", &conflicting);
        assert!(result.is_err());

        let first_after = FilesRepository::get_by_id(&connection, "f1")
            .expect("get first after failed replace")
            .expect("first should remain after rollback");
        assert_eq!(first_after.normalized_path, "notes/a.md");
    }

    #[test]
    fn note_crud_service_create_update_delete_flow() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault root");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = NoteCrudService::default();
        let relative = Path::new("notes/today.md");

        let created = service
            .create_note(&vault_root, &mut connection, "f1", relative, "# first")
            .expect("create note");
        assert_eq!(created.normalized_path, "notes/today.md");
        assert_eq!(
            fs::read_to_string(vault_root.join(relative)).expect("read created note"),
            "# first"
        );

        let before_update = FilesRepository::get_by_id(&connection, "f1")
            .expect("get before update")
            .expect("row before update");

        let updated = service
            .update_note(&vault_root, &mut connection, "f1", relative, "# second")
            .expect("update note");
        assert_eq!(updated.normalized_path, "notes/today.md");
        assert_eq!(
            fs::read_to_string(vault_root.join(relative)).expect("read updated note"),
            "# second"
        );

        let after_update = FilesRepository::get_by_id(&connection, "f1")
            .expect("get after update")
            .expect("row after update");
        assert_ne!(before_update.hash_blake3, after_update.hash_blake3);

        let deleted = service
            .delete_note(&vault_root, &mut connection, "f1")
            .expect("delete note");
        assert!(deleted);
        assert!(!vault_root.join(relative).exists());
        assert!(
            FilesRepository::get_by_id(&connection, "f1")
                .expect("get deleted")
                .is_none()
        );
    }

    #[test]
    fn note_crud_service_trace_context_wrapper_executes_operation() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault root");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = NoteCrudService::default();
        let trace_context = ServiceTraceContext::with_correlation("note_create", "cid-note-1");
        let created = service
            .create_note_with_trace_context(
                &trace_context,
                &vault_root,
                &mut connection,
                "f1",
                Path::new("notes/traced.md"),
                "# traced",
            )
            .expect("create traced note");

        assert_eq!(created.normalized_path, "notes/traced.md");
        assert_eq!(trace_context.correlation_id(), "cid-note-1");
    }

    #[test]
    fn note_crud_service_rejects_escape_paths() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault root");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = NoteCrudService::default();
        let error = service
            .create_note(
                &vault_root,
                &mut connection,
                "f1",
                Path::new("../escape.md"),
                "nope",
            )
            .expect_err("path escaping should fail");

        assert!(matches!(error, NoteCrudError::InvalidPath { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn note_crud_service_rejects_symlink_parent_escaping_vault_before_write() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        let outside_root = temp.path().join("outside");
        fs::create_dir_all(&vault_root).expect("create vault root");
        fs::create_dir_all(&outside_root).expect("create outside root");
        symlink(&outside_root, vault_root.join("notes")).expect("create notes symlink");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = NoteCrudService::default();
        let error = service
            .create_note(
                &vault_root,
                &mut connection,
                "f1",
                Path::new("notes/escape.md"),
                "# Escape",
            )
            .expect_err("symlink escape should fail");

        assert!(matches!(error, NoteCrudError::PathOutsideVault { .. }));
        assert!(!outside_root.join("escape.md").exists());
        assert!(
            FilesRepository::get_by_id(&connection, "f1")
                .expect("get metadata after failed create")
                .is_none()
        );
    }

    #[test]
    fn note_crud_service_rename_keeps_link_resolution_consistent() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault root");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = NoteCrudService::default();
        service
            .create_note(
                &vault_root,
                &mut connection,
                "target",
                Path::new("notes/target.md"),
                "# target",
            )
            .expect("create target");
        service
            .create_note(
                &vault_root,
                &mut connection,
                "source",
                Path::new("notes/source.md"),
                "# source",
            )
            .expect("create source");

        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l1".to_string(),
                source_file_id: "source".to_string(),
                raw_target: "target".to_string(),
                resolved_file_id: Some("target".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
            },
        )
        .expect("insert link");

        let renamed = service
            .rename_note(
                &vault_root,
                &mut connection,
                "target",
                Path::new("archive/renamed-target.md"),
            )
            .expect("rename note");
        assert_eq!(renamed.normalized_path, "archive/renamed-target.md");

        assert!(!vault_root.join("notes/target.md").exists());
        assert!(vault_root.join("archive/renamed-target.md").exists());

        let backlinks = LinksRepository::list_backlinks_with_paths(&connection, "target")
            .expect("list backlinks");
        assert_eq!(backlinks.len(), 1);
        assert_eq!(
            backlinks[0].resolved_path.as_deref(),
            Some("archive/renamed-target.md")
        );
    }

    #[test]
    fn backlink_graph_service_returns_stable_outgoing_and_backlink_order() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "source-a",
                "notes/source-a.md",
                "notes/source-a.md",
                "/vault/notes/source-a.md",
            ),
        )
        .expect("insert source a");
        FilesRepository::insert(
            &connection,
            &file_record(
                "source-b",
                "notes/source-b.md",
                "notes/source-b.md",
                "/vault/notes/source-b.md",
            ),
        )
        .expect("insert source b");
        FilesRepository::insert(
            &connection,
            &file_record(
                "target",
                "notes/target.md",
                "notes/target.md",
                "/vault/notes/target.md",
            ),
        )
        .expect("insert target");

        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l2".to_string(),
                source_file_id: "source-a".to_string(),
                raw_target: "target".to_string(),
                resolved_file_id: Some("target".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
            },
        )
        .expect("insert outgoing l2");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l1".to_string(),
                source_file_id: "source-a".to_string(),
                raw_target: "target".to_string(),
                resolved_file_id: Some("target".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
            },
        )
        .expect("insert outgoing l1");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l3".to_string(),
                source_file_id: "source-b".to_string(),
                raw_target: "target".to_string(),
                resolved_file_id: Some("target".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
            },
        )
        .expect("insert outgoing l3");

        let service = BacklinkGraphService;
        let outgoing = service
            .outgoing_for_path(&connection, "notes/source-a.md")
            .expect("query outgoing");
        assert_eq!(outgoing.len(), 2);
        assert_eq!(outgoing[0].link_id, "l1");
        assert_eq!(outgoing[1].link_id, "l2");

        let backlinks = service
            .backlinks_for_path(&connection, "notes/target.md")
            .expect("query backlinks");
        assert_eq!(backlinks.len(), 3);
        assert_eq!(backlinks[0].source_path, "notes/source-a.md");
        assert_eq!(backlinks[1].source_path, "notes/source-a.md");
        assert_eq!(backlinks[2].source_path, "notes/source-b.md");
    }

    #[test]
    fn backlink_graph_service_lists_unresolved_links() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "source-a",
                "notes/source-a.md",
                "notes/source-a.md",
                "/vault/notes/source-a.md",
            ),
        )
        .expect("insert source a");

        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-unresolved".to_string(),
                source_file_id: "source-a".to_string(),
                raw_target: "missing".to_string(),
                resolved_file_id: None,
                heading_slug: None,
                block_id: None,
                is_unresolved: true,
            },
        )
        .expect("insert unresolved link");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-resolved".to_string(),
                source_file_id: "source-a".to_string(),
                raw_target: "missing".to_string(),
                resolved_file_id: None,
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
            },
        )
        .expect("insert resolved marker link");

        let unresolved = BacklinkGraphService
            .unresolved_links(&connection)
            .expect("query unresolved");
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].link_id, "l-unresolved");
        assert!(unresolved[0].is_unresolved);
    }

    #[test]
    fn note_crud_service_rolls_back_created_file_when_metadata_insert_fails() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let coordinator = SdkTransactionCoordinator;
        coordinator
            .insert_file_metadata(
                &mut connection,
                &file_record(
                    "conflict",
                    "notes/conflict.md",
                    "notes/conflict.md",
                    "/ghost/conflict.md",
                ),
            )
            .expect("seed conflicting metadata");

        let service = NoteCrudService::default();
        let error = service
            .create_note(
                &vault_root,
                &mut connection,
                "new-file",
                Path::new("notes/conflict.md"),
                "# New",
            )
            .expect_err("create should fail on metadata conflict");

        assert!(matches!(error, NoteCrudError::Coordinator { .. }));
        assert!(!vault_root.join("notes/conflict.md").exists());
        assert!(
            FilesRepository::get_by_id(&connection, "new-file")
                .expect("get metadata for failed create")
                .is_none()
        );
    }

    #[test]
    fn note_crud_service_rolls_back_rename_when_metadata_update_fails() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = NoteCrudService::default();
        service
            .create_note(
                &vault_root,
                &mut connection,
                "note-a",
                Path::new("notes/a.md"),
                "# A",
            )
            .expect("create note a");

        let coordinator = SdkTransactionCoordinator;
        coordinator
            .insert_file_metadata(
                &mut connection,
                &file_record(
                    "conflict",
                    "notes/conflict.md",
                    "notes/conflict.md",
                    "/ghost/conflict.md",
                ),
            )
            .expect("seed conflicting metadata");

        let error = service
            .rename_note(
                &vault_root,
                &mut connection,
                "note-a",
                Path::new("notes/conflict.md"),
            )
            .expect_err("rename should fail on metadata conflict");

        assert!(matches!(error, NoteCrudError::Coordinator { .. }));
        assert!(vault_root.join("notes/a.md").exists());
        assert!(!vault_root.join("notes/conflict.md").exists());

        let file_record = FilesRepository::get_by_id(&connection, "note-a")
            .expect("get file row")
            .expect("file row exists");
        assert_eq!(file_record.normalized_path, "notes/a.md");
    }

    #[test]
    fn reconcile_service_is_idempotent_across_repeated_runs() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = ReconcileService;
        let first = service
            .reconcile_vault(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("first reconcile");
        assert_eq!(first.scanned_files, 2);
        assert_eq!(first.inserted_files, 2);
        assert_eq!(first.updated_files, 0);
        assert_eq!(first.removed_files, 0);
        assert_eq!(first.unchanged_files, 0);

        let second = service
            .reconcile_vault(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("second reconcile");
        assert_eq!(second.scanned_files, 2);
        assert_eq!(second.inserted_files, 0);
        assert_eq!(second.updated_files, 0);
        assert_eq!(second.removed_files, 0);
        assert_eq!(second.unchanged_files, 2);
    }

    #[test]
    fn reconcile_service_updates_changed_files_and_removes_stale_rows() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = ReconcileService;
        service
            .reconcile_vault(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed reconcile");

        fs::remove_file(temp.path().join("notes/a.md")).expect("remove a");
        fs::write(temp.path().join("notes/b.md"), "# B changed").expect("update b");
        fs::write(temp.path().join("notes/c.md"), "# C").expect("write c");

        let result = service
            .reconcile_vault(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("reconcile drift");
        assert_eq!(result.scanned_files, 2);
        assert_eq!(result.inserted_files, 1);
        assert_eq!(result.updated_files, 1);
        assert_eq!(result.removed_files, 1);
        assert_eq!(result.unchanged_files, 0);

        let indexed = FilesRepository::list_all(&connection).expect("list indexed files");
        let indexed_paths: Vec<String> = indexed
            .iter()
            .map(|record| record.normalized_path.clone())
            .collect();
        assert_eq!(
            indexed_paths,
            vec!["notes/b.md".to_string(), "notes/c.md".to_string()]
        );
    }

    #[test]
    fn reconcile_service_trace_context_wrapper_executes_operation() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = ReconcileService;
        let trace_context = ServiceTraceContext::with_correlation("reconcile", "cid-reconcile-1");
        let result = service
            .reconcile_vault_with_trace_context(
                &trace_context,
                temp.path(),
                &mut connection,
                CasePolicy::Sensitive,
            )
            .expect("traced reconcile");

        assert_eq!(result.scanned_files, 1);
        assert_eq!(trace_context.correlation_id(), "cid-reconcile-1");
    }

    #[test]
    fn health_snapshot_reports_vault_db_and_watcher_status() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes dir");
        fs::write(vault_root.join("notes/a.md"), "# A").expect("write a");
        fs::write(vault_root.join("notes/b.md"), "# B").expect("write b");
        fs::write(vault_root.join("notes/c.png"), "png").expect("write c");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        connection
            .execute(
                "INSERT INTO index_state (key, value_json) VALUES (?1, ?2)",
                rusqlite::params!["last_index_at", "\"2026-03-03T19:00:00Z\""],
            )
            .expect("seed index_state");

        let snapshot = HealthSnapshotService
            .snapshot(&vault_root, &connection, 3, WatcherStatus::Running)
            .expect("build health snapshot");

        assert!(snapshot.db_healthy);
        assert_eq!(snapshot.db_migrations, 1);
        assert_eq!(snapshot.index_lag, 3);
        assert_eq!(snapshot.watcher_status, "running");
        assert_eq!(snapshot.files_total, 3);
        assert_eq!(snapshot.markdown_files, 2);
        assert!(snapshot.last_index_updated_at.is_some());
    }

    #[test]
    fn property_update_service_persists_and_updates_markdown() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault root");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let note_service = NoteCrudService::default();
        note_service
            .create_note(
                &vault_root,
                &mut connection,
                "f1",
                Path::new("notes/property.md"),
                "# Body",
            )
            .expect("create note");

        let before = FilesRepository::get_by_id(&connection, "f1")
            .expect("get file before property set")
            .expect("file exists before property set");

        let update_service = PropertyUpdateService::default();
        let result = update_service
            .set_property(
                &vault_root,
                &mut connection,
                "f1",
                "status",
                TypedPropertyValue::String("draft".to_string()),
            )
            .expect("set typed property");

        assert_eq!(result.file_id, "f1");
        assert_eq!(result.key, "status");
        assert_eq!(
            result.value,
            TypedPropertyValue::String("draft".to_string())
        );

        let markdown = fs::read_to_string(vault_root.join("notes/property.md"))
            .expect("read updated markdown");
        assert!(markdown.contains("---"));
        assert!(markdown.contains("status: draft"));

        let property = PropertiesRepository::get_by_file_and_key(&connection, "f1", "status")
            .expect("get stored property")
            .expect("property should exist");
        assert_eq!(property.value_type, "string");
        assert_eq!(property.value_json, "\"draft\"");

        let after = FilesRepository::get_by_id(&connection, "f1")
            .expect("get file after property set")
            .expect("file exists after property set");
        assert_ne!(before.hash_blake3, after.hash_blake3);
    }

    #[test]
    fn property_update_service_trace_context_wrapper_executes_operation() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault root");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let note_service = NoteCrudService::default();
        note_service
            .create_note(
                &vault_root,
                &mut connection,
                "f1",
                Path::new("notes/property-traced.md"),
                "# Body",
            )
            .expect("create note");

        let update_service = PropertyUpdateService::default();
        let trace_context = ServiceTraceContext::with_correlation("property_set", "cid-property-1");
        let result = update_service
            .set_property_with_trace_context(
                &trace_context,
                &vault_root,
                &mut connection,
                "f1",
                "status",
                TypedPropertyValue::String("published".to_string()),
            )
            .expect("set typed property with trace");

        assert_eq!(result.key, "status");
        assert_eq!(trace_context.correlation_id(), "cid-property-1");
    }

    #[test]
    fn property_query_service_filters_sorts_and_paginates_rows() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record("f1", "notes/a.md", "notes/a.md", "/vault/notes/a.md"),
        )
        .expect("insert f1");
        FilesRepository::insert(
            &connection,
            &file_record("f2", "notes/b.md", "notes/b.md", "/vault/notes/b.md"),
        )
        .expect("insert f2");
        FilesRepository::insert(
            &connection,
            &file_record("f3", "notes/c.md", "notes/c.md", "/vault/notes/c.md"),
        )
        .expect("insert f3");

        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: "p1".to_string(),
                file_id: "f1".to_string(),
                key: "status".to_string(),
                value_type: "string".to_string(),
                value_json: "\"draft\"".to_string(),
            },
        )
        .expect("insert p1");
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: "p2".to_string(),
                file_id: "f2".to_string(),
                key: "status".to_string(),
                value_type: "string".to_string(),
                value_json: "\"published\"".to_string(),
            },
        )
        .expect("insert p2");
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: "p3".to_string(),
                file_id: "f3".to_string(),
                key: "status".to_string(),
                value_type: "string".to_string(),
                value_json: "\"public\"".to_string(),
            },
        )
        .expect("insert p3");

        let service = PropertyQueryService;
        let first_page = service
            .query(
                &connection,
                &PropertyQueryRequest {
                    key: " status ".to_string(),
                    value_contains: Some("PUB".to_string()),
                    limit: Some(1),
                    offset: 0,
                    sort: PropertyQuerySort::FilePathDesc,
                },
            )
            .expect("query first page");
        assert_eq!(first_page.total, 2);
        assert_eq!(first_page.rows.len(), 1);
        assert_eq!(first_page.rows[0].file_path, "notes/c.md");
        assert_eq!(first_page.rows[0].property_id, "p3");

        let second_page = service
            .query(
                &connection,
                &PropertyQueryRequest {
                    key: "status".to_string(),
                    value_contains: Some("pub".to_string()),
                    limit: Some(1),
                    offset: 1,
                    sort: PropertyQuerySort::FilePathDesc,
                },
            )
            .expect("query second page");
        assert_eq!(second_page.total, 2);
        assert_eq!(second_page.rows.len(), 1);
        assert_eq!(second_page.rows[0].file_path, "notes/b.md");
        assert_eq!(second_page.rows[0].property_id, "p2");
    }

    #[test]
    fn property_query_service_supports_updated_at_sorting() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record("f1", "notes/a.md", "notes/a.md", "/vault/notes/a.md"),
        )
        .expect("insert f1");
        FilesRepository::insert(
            &connection,
            &file_record("f2", "notes/b.md", "notes/b.md", "/vault/notes/b.md"),
        )
        .expect("insert f2");

        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: "p1".to_string(),
                file_id: "f1".to_string(),
                key: "status".to_string(),
                value_type: "string".to_string(),
                value_json: "\"draft\"".to_string(),
            },
        )
        .expect("insert p1");
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: "p2".to_string(),
                file_id: "f2".to_string(),
                key: "status".to_string(),
                value_type: "string".to_string(),
                value_json: "\"published\"".to_string(),
            },
        )
        .expect("insert p2");

        connection
            .execute(
                "UPDATE properties SET updated_at = ?1 WHERE property_id = ?2",
                rusqlite::params!["2026-03-03T12:00:00.000Z", "p1"],
            )
            .expect("set p1 timestamp");
        connection
            .execute(
                "UPDATE properties SET updated_at = ?1 WHERE property_id = ?2",
                rusqlite::params!["2026-03-03T12:00:01.000Z", "p2"],
            )
            .expect("set p2 timestamp");

        let rows = PropertyQueryService
            .query(
                &connection,
                &PropertyQueryRequest {
                    key: "status".to_string(),
                    value_contains: None,
                    limit: None,
                    offset: 0,
                    sort: PropertyQuerySort::UpdatedAtDesc,
                },
            )
            .expect("query by updated_at desc")
            .rows;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].property_id, "p2");
        assert_eq!(rows[1].property_id, "p1");
    }

    #[test]
    fn property_query_service_rejects_invalid_requests() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let service = PropertyQueryService;
        let missing_key = service
            .query(
                &connection,
                &PropertyQueryRequest {
                    key: "   ".to_string(),
                    value_contains: None,
                    limit: None,
                    offset: 0,
                    sort: PropertyQuerySort::FilePathAsc,
                },
            )
            .expect_err("empty key should fail");
        assert!(matches!(missing_key, super::PropertyQueryError::InvalidKey));

        let zero_limit = service
            .query(
                &connection,
                &PropertyQueryRequest {
                    key: "status".to_string(),
                    value_contains: None,
                    limit: Some(0),
                    offset: 0,
                    sort: PropertyQuerySort::FilePathAsc,
                },
            )
            .expect_err("zero limit should fail");
        assert!(matches!(
            zero_limit,
            super::PropertyQueryError::InvalidLimit { limit: 0 }
        ));
    }

    #[test]
    fn base_table_executor_filters_sorts_and_projects_rows() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "f1",
                "notes/projects/alpha.md",
                "notes/projects/alpha.md",
                "/vault/notes/projects/alpha.md",
            ),
        )
        .expect("insert f1");
        FilesRepository::insert(
            &connection,
            &file_record(
                "f2",
                "notes/projects/beta.md",
                "notes/projects/beta.md",
                "/vault/notes/projects/beta.md",
            ),
        )
        .expect("insert f2");
        FilesRepository::insert(
            &connection,
            &file_record(
                "f3",
                "notes/archive/gamma.md",
                "notes/archive/gamma.md",
                "/vault/notes/archive/gamma.md",
            ),
        )
        .expect("insert f3");

        for (property_id, file_id, key, value_json) in [
            ("p1", "f1", "status", "\"active\""),
            ("p2", "f1", "due", "2"),
            ("p3", "f1", "assignee", "\"han\""),
            ("p4", "f2", "status", "\"active\""),
            ("p5", "f2", "due", "1"),
            ("p6", "f2", "assignee", "\"sam\""),
            ("p7", "f3", "status", "\"active\""),
            ("p8", "f3", "due", "3"),
            ("p9", "f3", "assignee", "\"han\""),
        ] {
            PropertiesRepository::upsert(
                &connection,
                &PropertyRecordInput {
                    property_id: property_id.to_string(),
                    file_id: file_id.to_string(),
                    key: key.to_string(),
                    value_type: "string".to_string(),
                    value_json: value_json.to_string(),
                },
            )
            .expect("upsert property");
        }

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: vec![
                "status".to_string(),
                "due".to_string(),
                "assignee".to_string(),
            ],
            filters: vec![
                BaseFilterClause {
                    key: "status".to_string(),
                    op: BaseFilterOp::Eq,
                    value: serde_json::json!("active"),
                },
                BaseFilterClause {
                    key: "assignee".to_string(),
                    op: BaseFilterOp::Contains,
                    value: serde_json::json!("ha"),
                },
            ],
            sorts: vec![BaseSortClause {
                key: "due".to_string(),
                direction: BaseSortDirection::Desc,
            }],
            columns: vec![
                BaseColumnConfig {
                    key: "title".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
                BaseColumnConfig {
                    key: "status".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
                BaseColumnConfig {
                    key: "due".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
            ],
            limit: 25,
            offset: 0,
            property_queries: Vec::new(),
        };

        let page = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect("execute table plan");
        assert_eq!(page.total, 1);
        assert_eq!(page.summaries.len(), 3);
        assert_eq!(page.summaries[2].key, "due");
        assert_eq!(page.summaries[2].count, 1);
        assert_eq!(page.summaries[2].min, Some(serde_json::json!(2)));
        assert_eq!(page.summaries[2].max, Some(serde_json::json!(2)));
        assert_eq!(page.summaries[2].avg, Some(serde_json::json!(2.0)));
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].file_path, "notes/projects/alpha.md");
        assert_eq!(
            page.rows[0].values.get("title"),
            Some(&serde_json::json!("alpha"))
        );
        assert_eq!(
            page.rows[0].values.get("status"),
            Some(&serde_json::json!("active"))
        );
        assert_eq!(page.rows[0].values.get("due"), Some(&serde_json::json!(2)));
    }

    #[test]
    fn base_table_executor_applies_sort_and_pagination_offset() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "f1",
                "notes/projects/alpha.md",
                "notes/projects/alpha.md",
                "/vault/notes/projects/alpha.md",
            ),
        )
        .expect("insert f1");
        FilesRepository::insert(
            &connection,
            &file_record(
                "f2",
                "notes/projects/beta.md",
                "notes/projects/beta.md",
                "/vault/notes/projects/beta.md",
            ),
        )
        .expect("insert f2");

        for (property_id, file_id, key, value_json) in
            [("p1", "f1", "due", "2"), ("p2", "f2", "due", "1")]
        {
            PropertiesRepository::upsert(
                &connection,
                &PropertyRecordInput {
                    property_id: property_id.to_string(),
                    file_id: file_id.to_string(),
                    key: key.to_string(),
                    value_type: "number".to_string(),
                    value_json: value_json.to_string(),
                },
            )
            .expect("upsert property");
        }

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: vec!["due".to_string()],
            filters: Vec::new(),
            sorts: vec![BaseSortClause {
                key: "due".to_string(),
                direction: BaseSortDirection::Asc,
            }],
            columns: vec![BaseColumnConfig {
                key: "path".to_string(),
                label: None,
                width: None,
                hidden: false,
            }],
            limit: 1,
            offset: 1,
            property_queries: Vec::new(),
        };

        let page = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect("execute paged table");
        assert_eq!(page.total, 2);
        assert_eq!(page.summaries.len(), 1);
        assert_eq!(page.summaries[0].key, "path");
        assert_eq!(page.summaries[0].count, 2);
        assert_eq!(
            page.summaries[0].min,
            Some(serde_json::json!("notes/projects/alpha.md"))
        );
        assert_eq!(
            page.summaries[0].max,
            Some(serde_json::json!("notes/projects/beta.md"))
        );
        assert_eq!(page.summaries[0].avg, None);
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].file_id, "f1");
        assert_eq!(
            page.rows[0].values.get("path"),
            Some(&serde_json::json!("notes/projects/alpha.md"))
        );
    }

    #[test]
    fn base_table_executor_reports_invalid_property_json_payloads() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "f1",
                "notes/projects/alpha.md",
                "notes/projects/alpha.md",
                "/vault/notes/projects/alpha.md",
            ),
        )
        .expect("insert f1");
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: "p1".to_string(),
                file_id: "f1".to_string(),
                key: "status".to_string(),
                value_type: "string".to_string(),
                value_json: "{bad-json".to_string(),
            },
        )
        .expect("upsert malformed property");

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: vec!["status".to_string()],
            filters: Vec::new(),
            sorts: Vec::new(),
            columns: vec![BaseColumnConfig {
                key: "status".to_string(),
                label: None,
                width: None,
                hidden: false,
            }],
            limit: 10,
            offset: 0,
            property_queries: Vec::new(),
        };

        let error = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect_err("malformed json should fail");
        assert!(matches!(
            error,
            BaseTableExecutorError::ParsePropertyValue { file_id, key, .. }
            if file_id == "f1" && key == "status"
        ));
    }

    #[test]
    fn base_column_persistence_updates_column_order_and_visibility() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "f-base",
                "views/projects.base",
                "views/projects.base",
                "/vault/views/projects.base",
            ),
        )
        .expect("insert base file");

        let document = BaseDocument {
            views: vec![BaseViewDefinition {
                name: "Projects".to_string(),
                kind: BaseViewKind::Table,
                source: Some("notes/projects".to_string()),
                filters: Vec::new(),
                sorts: Vec::new(),
                columns: vec![
                    BaseColumnConfig {
                        key: "status".to_string(),
                        label: None,
                        width: None,
                        hidden: false,
                    },
                    BaseColumnConfig {
                        key: "due".to_string(),
                        label: None,
                        width: None,
                        hidden: false,
                    },
                ],
                extras: serde_json::Map::new(),
            }],
        };
        let config_json = serde_json::to_string(&document).expect("serialize base config");
        BasesRepository::upsert(
            &connection,
            &BaseRecordInput {
                base_id: "b1".to_string(),
                file_id: "f-base".to_string(),
                config_json,
            },
        )
        .expect("insert base row");

        let result = BaseColumnConfigPersistenceService
            .persist_view_columns(
                &connection,
                "b1",
                "projects",
                vec![
                    BaseColumnConfig {
                        key: "due".to_string(),
                        label: None,
                        width: None,
                        hidden: false,
                    },
                    BaseColumnConfig {
                        key: "status".to_string(),
                        label: Some("Status".to_string()),
                        width: Some(120),
                        hidden: true,
                    },
                ],
            )
            .expect("persist column layout");
        assert_eq!(result.base_id, "b1");
        assert_eq!(result.view_name, "Projects");
        assert_eq!(result.columns_total, 2);

        let persisted = BasesRepository::get_by_id(&connection, "b1")
            .expect("load persisted base")
            .expect("base exists");
        let persisted_document =
            serde_json::from_str::<BaseDocument>(&persisted.config_json).expect("parse persisted");
        let columns = &persisted_document.views[0].columns;
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].key, "due");
        assert_eq!(columns[1].key, "status");
        assert!(columns[1].hidden);
        assert_eq!(columns[1].label.as_deref(), Some("Status"));
    }

    #[test]
    fn base_column_persistence_reports_missing_view() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "f-base",
                "views/projects.base",
                "views/projects.base",
                "/vault/views/projects.base",
            ),
        )
        .expect("insert base file");
        let config_json = serde_json::to_string(&BaseDocument {
            views: vec![BaseViewDefinition {
                name: "Projects".to_string(),
                kind: BaseViewKind::Table,
                source: None,
                filters: Vec::new(),
                sorts: Vec::new(),
                columns: Vec::new(),
                extras: serde_json::Map::new(),
            }],
        })
        .expect("serialize base config");
        BasesRepository::upsert(
            &connection,
            &BaseRecordInput {
                base_id: "b1".to_string(),
                file_id: "f-base".to_string(),
                config_json,
            },
        )
        .expect("insert base row");

        let error = BaseColumnConfigPersistenceService
            .persist_view_columns(&connection, "b1", "missing", Vec::new())
            .expect_err("missing view should fail");
        assert!(matches!(
            error,
            BaseColumnConfigPersistError::ViewNotFound {
                base_id,
                view_name
            } if base_id == "b1" && view_name == "missing"
        ));
    }

    #[test]
    fn base_column_persistence_reports_invalid_stored_config_payload() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "f-base",
                "views/projects.base",
                "views/projects.base",
                "/vault/views/projects.base",
            ),
        )
        .expect("insert base file");
        BasesRepository::upsert(
            &connection,
            &BaseRecordInput {
                base_id: "b1".to_string(),
                file_id: "f-base".to_string(),
                config_json: "{\"raw\":\"legacy\"}".to_string(),
            },
        )
        .expect("insert legacy base row");

        let error = BaseColumnConfigPersistenceService
            .persist_view_columns(&connection, "b1", "projects", Vec::new())
            .expect_err("invalid config should fail");
        assert!(matches!(
            error,
            BaseColumnConfigPersistError::DeserializeConfig { base_id, .. } if base_id == "b1"
        ));
    }

    #[test]
    fn base_validation_service_validates_by_id_and_path() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "f-base",
                "views/projects.base",
                "views/projects.base",
                "/vault/views/projects.base",
            ),
        )
        .expect("insert base file");
        let config_json = serde_json::to_string(&BaseDocument {
            views: vec![BaseViewDefinition {
                name: "Projects".to_string(),
                kind: BaseViewKind::Table,
                source: None,
                filters: Vec::new(),
                sorts: Vec::new(),
                columns: vec![
                    BaseColumnConfig {
                        key: "status".to_string(),
                        label: None,
                        width: None,
                        hidden: false,
                    },
                    BaseColumnConfig {
                        key: "status".to_string(),
                        label: None,
                        width: None,
                        hidden: false,
                    },
                ],
                extras: serde_json::Map::new(),
            }],
        })
        .expect("serialize base config");
        BasesRepository::upsert(
            &connection,
            &BaseRecordInput {
                base_id: "b1".to_string(),
                file_id: "f-base".to_string(),
                config_json,
            },
        )
        .expect("insert base row");

        let by_id = BaseValidationService
            .validate(&connection, "b1")
            .expect("validate by id");
        assert_eq!(by_id.base_id, "b1");
        assert_eq!(by_id.file_path, "views/projects.base");
        assert!(by_id.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "bases.column.duplicate_key"
                && diagnostic.severity == BaseDiagnosticSeverity::Warning
        }));

        let by_path = BaseValidationService
            .validate(&connection, "views/projects.base")
            .expect("validate by path");
        assert_eq!(by_path.base_id, "b1");
        assert_eq!(by_path.file_id, "f-base");
        assert_eq!(by_path.diagnostics, by_id.diagnostics);
    }

    #[test]
    fn base_validation_service_reports_invalid_input_and_missing_base() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let invalid_input = BaseValidationService
            .validate(&connection, "   ")
            .expect_err("empty lookup should fail");
        assert!(matches!(invalid_input, BaseValidationError::InvalidInput));

        let missing = BaseValidationService
            .validate(&connection, "missing")
            .expect_err("missing base should fail");
        assert!(matches!(
            missing,
            BaseValidationError::BaseNotFound { path_or_id } if path_or_id == "missing"
        ));
    }

    #[test]
    fn base_table_cached_query_service_invalidates_on_metadata_change() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "f1",
                "notes/projects/alpha.md",
                "notes/projects/alpha.md",
                "/vault/notes/projects/alpha.md",
            ),
        )
        .expect("insert file");
        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: "p1".to_string(),
                file_id: "f1".to_string(),
                key: "status".to_string(),
                value_type: "string".to_string(),
                value_json: "\"draft\"".to_string(),
            },
        )
        .expect("insert property");

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: vec!["status".to_string()],
            filters: Vec::new(),
            sorts: Vec::new(),
            columns: vec![BaseColumnConfig {
                key: "status".to_string(),
                label: None,
                width: None,
                hidden: false,
            }],
            limit: 10,
            offset: 0,
            property_queries: Vec::new(),
        };

        let cache_service = BaseTableCachedQueryService::default();
        let first = cache_service
            .execute(&connection, &plan)
            .expect("first cached execute");
        assert_eq!(
            first.rows[0].values.get("status"),
            Some(&serde_json::json!("draft"))
        );

        let second = cache_service
            .execute(&connection, &plan)
            .expect("second cached execute");
        assert_eq!(
            second.rows[0].values.get("status"),
            Some(&serde_json::json!("draft"))
        );

        PropertiesRepository::upsert(
            &connection,
            &PropertyRecordInput {
                property_id: "p1".to_string(),
                file_id: "f1".to_string(),
                key: "status".to_string(),
                value_type: "string".to_string(),
                value_json: "\"published\"".to_string(),
            },
        )
        .expect("update property");

        let third = cache_service
            .execute(&connection, &plan)
            .expect("third cached execute");
        assert_eq!(
            third.rows[0].values.get("status"),
            Some(&serde_json::json!("published"))
        );
    }
}
