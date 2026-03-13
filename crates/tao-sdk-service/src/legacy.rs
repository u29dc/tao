//! Service-layer orchestration entrypoints over SDK subsystem crates.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[path = "config.rs"]
mod config;
#[path = "feature_flags.rs"]
mod feature_flags;
#[path = "import_export.rs"]
mod import_export;
#[path = "indexing/mod.rs"]
mod indexing;
#[path = "tracing_hooks.rs"]
mod tracing_hooks;

pub use config::{
    SdkBootstrapError, SdkBootstrapService, SdkBootstrapSnapshot, SdkConfig, SdkConfigError,
    SdkConfigLoader, SdkConfigOverrides, ensure_runtime_paths,
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
use rayon::prelude::*;
pub use tracing_hooks::ServiceTraceContext;

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, params_from_iter};
use serde_json::Value as JsonValue;
use tao_sdk_bases::{
    BaseAggregateOp, BaseAggregateSpec, BaseCoercionMode, BaseColumnConfig, BaseDiagnostic,
    BaseDocument, BaseFieldType, BaseFilterClause, BaseRelationSpec, BaseRollupOp, BaseRollupSpec,
    BaseSortClause, BaseSortDirection, TableQueryPlan, coerce_json_value, compare_json_values,
    compare_optional_json_values, evaluate_filter, validate_base_config_json,
};
use tao_sdk_core::{
    DomainEvent, DomainEventBus, NoteChangeKind, note_extension_from_path, note_folder_from_path,
    note_title_from_path,
};
use tao_sdk_links::resolve_target;
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
    case_policy: CasePolicy,
}

impl Default for NoteCrudService {
    fn default() -> Self {
        Self {
            coordinator: SdkTransactionCoordinator,
            events: DomainEventBus::new(),
            case_policy: CasePolicy::Sensitive,
        }
    }
}

impl NoteCrudService {
    /// Create one service with explicit path case policy.
    #[must_use]
    pub fn with_case_policy(case_policy: CasePolicy) -> Self {
        Self {
            case_policy,
            ..Self::default()
        }
    }

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

        let record =
            fingerprint_to_file_record(file_id, vault_root, relative_path, self.case_policy)?;
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

        let record =
            fingerprint_to_file_record(file_id, vault_root, relative_path, self.case_policy)?;
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

        let record =
            fingerprint_to_file_record(file_id, vault_root, new_relative_path, self.case_policy)?;
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
    case_policy: CasePolicy,
) -> Result<FileRecordInput, NoteCrudError> {
    let fingerprint_service = FileFingerprintService::from_root(vault_root, case_policy)
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

/// Grouped output metadata for one base page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BaseGroupingMetadata {
    /// Grouping keys used to materialize grouped rows.
    pub group_by: Vec<String>,
    /// Aggregate aliases included in grouped rows.
    pub aggregate_aliases: Vec<String>,
}

/// Relation resolution diagnostic scoped to base execution.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BaseRelationDiagnostic {
    /// Source row file id.
    pub file_id: String,
    /// Source row file path.
    pub file_path: String,
    /// Relation field key.
    pub key: String,
    /// Target relation token.
    pub target: String,
    /// Stable reason code.
    pub reason: String,
}

/// Execution metadata for one base page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BaseExecutionMetadata {
    /// Planner adapter label.
    pub adapter: String,
    /// Physical path label.
    pub path: String,
}

/// Paged table result from executing one base query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseTablePage {
    /// Total rows that matched filters before pagination.
    pub total: u64,
    /// Summary rows for configured columns over the filtered result set.
    pub summaries: Vec<BaseTableSummary>,
    /// Optional grouping metadata when grouped mode is enabled.
    pub grouping: Option<BaseGroupingMetadata>,
    /// Relation diagnostics scoped to this base execution.
    pub relation_diagnostics: Vec<BaseRelationDiagnostic>,
    /// Execution metadata for planner-backed dispatch.
    pub execution: BaseExecutionMetadata,
    /// Rows in this page.
    pub rows: Vec<BaseTableRow>,
}

/// Executor service that runs compiled base table plans against SQLite metadata.
#[derive(Debug, Default, Clone, Copy)]
pub struct BaseTableExecutorService;

/// Execution options for base table query plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaseTableExecutionOptions {
    /// Compute summary rows across the filtered result set.
    pub include_summaries: bool,
    /// Coercion mode for typed field normalization.
    pub coercion_mode: BaseCoercionMode,
}

impl Default for BaseTableExecutionOptions {
    fn default() -> Self {
        Self {
            include_summaries: true,
            coercion_mode: BaseCoercionMode::Permissive,
        }
    }
}

impl BaseTableExecutorService {
    /// Execute one compiled table query plan and return a paged result.
    pub fn execute(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
    ) -> Result<BaseTablePage, BaseTableExecutorError> {
        self.execute_with_options(connection, plan, BaseTableExecutionOptions::default())
    }

    /// Execute one compiled table query plan with explicit execution options.
    pub fn execute_with_options(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
        options: BaseTableExecutionOptions,
    ) -> Result<BaseTablePage, BaseTableExecutorError> {
        const PARALLEL_CANDIDATE_THRESHOLD: usize = 1_024;

        if plan.limit == 0 {
            return Err(BaseTableExecutorError::InvalidPlan {
                reason: "limit must be greater than zero".to_string(),
            });
        }

        let mut candidates = load_table_candidates(connection, plan.source_prefix.as_deref())?;
        let candidate_indices = candidates
            .iter()
            .enumerate()
            .map(|(index, row)| (row.file_id.clone(), index))
            .collect::<HashMap<_, _>>();

        if !plan.required_property_keys.is_empty() {
            let key_placeholders = (1..=plan.required_property_keys.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let source_param = plan.required_property_keys.len() + 1;
            let like_param = source_param + 1;
            let query = format!(
                r#"
SELECT
  p.file_id,
  p.key,
  p.value_type,
  p.value_json
FROM properties p
INNER JOIN files f ON f.file_id = p.file_id
WHERE f.is_markdown = 1
  AND p.key IN ({key_placeholders})
  AND (
    ?{source_param} IS NULL
    OR f.normalized_path = ?{source_param}
    OR f.normalized_path LIKE ?{like_param}
  )
ORDER BY p.file_id ASC, p.key ASC
"#
            );
            let mut parameters = plan
                .required_property_keys
                .iter()
                .map(|key| SqlValue::Text(key.clone()))
                .collect::<Vec<_>>();
            if let Some(source_prefix) = plan.source_prefix.as_ref() {
                parameters.push(SqlValue::Text(source_prefix.clone()));
                parameters.push(SqlValue::Text(format!("{source_prefix}/%")));
            } else {
                parameters.push(SqlValue::Null);
                parameters.push(SqlValue::Null);
            }

            let mut statement =
                connection
                    .prepare(&query)
                    .map_err(|source| BaseTableExecutorError::Sql {
                        operation: "prepare_property_projection",
                        source,
                    })?;
            let rows = statement
                .query_map(params_from_iter(parameters), |row| {
                    Ok((
                        row.get::<_, String>("file_id")?,
                        row.get::<_, String>("key")?,
                        row.get::<_, String>("value_type")?,
                        row.get::<_, String>("value_json")?,
                    ))
                })
                .map_err(|source| BaseTableExecutorError::Sql {
                    operation: "query_property_projection",
                    source,
                })?;
            for row in rows {
                let (file_id, key, value_type, value_json) =
                    row.map_err(|source| BaseTableExecutorError::Sql {
                        operation: "map_property_projection_row",
                        source,
                    })?;
                let Some(candidate_index) = candidate_indices.get(&file_id).copied() else {
                    continue;
                };
                let value = serde_json::from_str::<JsonValue>(&value_json).map_err(|source| {
                    BaseTableExecutorError::ParsePropertyValue {
                        file_id: file_id.clone(),
                        key: key.clone(),
                        source,
                    }
                })?;
                let value =
                    coerce_json_value(&value, map_field_type(&value_type), options.coercion_mode)
                        .map_err(|source| BaseTableExecutorError::Coercion {
                        file_id: file_id.clone(),
                        key: key.clone(),
                        source: Box::new(source),
                    })?;
                candidates[candidate_index].properties.insert(key, value);
            }
        }

        let mut relation_diagnostics = Vec::new();
        if !plan.relations.is_empty() {
            let targets = load_relation_target_lookup(connection)?;
            resolve_relation_fields(
                &mut candidates,
                &plan.relations,
                &targets,
                &mut relation_diagnostics,
            );
        }
        if !plan.rollups.is_empty() {
            apply_rollups(connection, &mut candidates, &plan.rollups)?;
        }

        let mut candidates = if candidates.len() >= PARALLEL_CANDIDATE_THRESHOLD {
            candidates
                .into_par_iter()
                .filter(|row| row_matches_filters(row, &plan.filters))
                .collect::<Vec<_>>()
        } else {
            candidates
                .into_iter()
                .filter(|row| row_matches_filters(row, &plan.filters))
                .collect::<Vec<_>>()
        };

        if candidates.len() >= PARALLEL_CANDIDATE_THRESHOLD {
            candidates
                .par_sort_unstable_by(|left, right| compare_table_rows(left, right, &plan.sorts));
        } else {
            candidates.sort_by(|left, right| compare_table_rows(left, right, &plan.sorts));
        }

        let execution = BaseExecutionMetadata {
            adapter: "base_table".to_string(),
            path: "query-planner".to_string(),
        };
        let grouped_mode = !plan.group_by.is_empty() || !plan.aggregates.is_empty();
        let (total, summaries, grouping, rows) = if grouped_mode {
            let grouped_rows =
                materialize_grouped_rows(&candidates, &plan.group_by, &plan.aggregates);
            let total = grouped_rows.len() as u64;
            let rows = grouped_rows
                .into_iter()
                .skip(plan.offset)
                .take(plan.limit)
                .collect::<Vec<_>>();
            let grouping = Some(BaseGroupingMetadata {
                group_by: plan.group_by.clone(),
                aggregate_aliases: plan
                    .aggregates
                    .iter()
                    .map(|aggregate| aggregate.alias.clone())
                    .collect(),
            });
            (total, Vec::new(), grouping, rows)
        } else {
            let total = candidates.len() as u64;
            let summaries = if options.include_summaries {
                compute_table_summaries(&candidates, &plan.columns)
            } else {
                Vec::new()
            };
            let rows = candidates
                .into_iter()
                .skip(plan.offset)
                .take(plan.limit)
                .map(|row| project_table_row(row, &plan.columns))
                .collect::<Vec<_>>();
            (total, summaries, None, rows)
        };

        Ok(BaseTablePage {
            total,
            summaries,
            grouping,
            relation_diagnostics,
            execution,
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
        if key.eq_ignore_ascii_case("folder") || key.eq_ignore_ascii_case("file_folder") {
            return Some(JsonValue::String(note_folder_from_path(&self.file_path)));
        }
        if key.eq_ignore_ascii_case("ext") || key.eq_ignore_ascii_case("file_ext") {
            return Some(JsonValue::String(note_extension_from_path(&self.file_path)));
        }
        if key.eq_ignore_ascii_case("title") {
            return Some(JsonValue::String(note_title_from_path(&self.file_path)));
        }

        self.properties.get(key).cloned()
    }
}

fn map_field_type(value_type: &str) -> BaseFieldType {
    match value_type.trim().to_ascii_lowercase().as_str() {
        "number" | "int" | "integer" | "float" | "double" => BaseFieldType::Number,
        "bool" | "boolean" | "checkbox" => BaseFieldType::Bool,
        "date" | "datetime" => BaseFieldType::Date,
        "json" | "object" | "array" => BaseFieldType::Json,
        _ => BaseFieldType::String,
    }
}

#[derive(Debug, Clone)]
struct RelationTarget {
    file_id: String,
    file_path: String,
}

#[derive(Debug, Clone)]
struct RelationTargetLookup {
    candidates: Vec<String>,
    by_path: HashMap<String, RelationTarget>,
}

fn load_relation_target_lookup(
    connection: &Connection,
) -> Result<RelationTargetLookup, BaseTableExecutorError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT file_id, normalized_path
FROM files
WHERE is_markdown = 1
ORDER BY normalized_path ASC
"#,
        )
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "prepare_relation_lookup",
            source,
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>("file_id")?,
                row.get::<_, String>("normalized_path")?,
            ))
        })
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "query_relation_lookup",
            source,
        })?;

    let mut lookup = HashMap::new();
    let mut candidates = Vec::new();
    for row in rows {
        let (file_id, file_path) = row.map_err(|source| BaseTableExecutorError::Sql {
            operation: "map_relation_lookup_row",
            source,
        })?;
        let target = RelationTarget {
            file_id: file_id.clone(),
            file_path: file_path.clone(),
        };
        candidates.push(file_path.clone());
        lookup.insert(file_path.clone(), target.clone());
        lookup.insert(file_path.to_ascii_lowercase(), target);
    }

    candidates.sort();
    candidates.dedup();

    Ok(RelationTargetLookup {
        candidates,
        by_path: lookup,
    })
}

fn resolve_relation_fields(
    candidates: &mut [TableRowCandidate],
    relations: &[BaseRelationSpec],
    relation_targets: &RelationTargetLookup,
    diagnostics: &mut Vec<BaseRelationDiagnostic>,
) {
    for row in candidates {
        for relation in relations {
            let Some(raw_value) = row.properties.get(&relation.key).cloned() else {
                continue;
            };
            let tokens = extract_relation_tokens(&raw_value);
            if tokens.is_empty() {
                continue;
            }

            let mut resolved_values = Vec::new();
            for token in tokens {
                let Some(normalized_target) = normalize_relation_token(&token) else {
                    diagnostics.push(BaseRelationDiagnostic {
                        file_id: row.file_id.clone(),
                        file_path: row.file_path.clone(),
                        key: relation.key.clone(),
                        target: token.clone(),
                        reason: "invalid_relation_token".to_string(),
                    });
                    resolved_values.push(serde_json::json!({
                        "target": token,
                        "resolved": false,
                        "reason": "invalid_relation_token",
                    }));
                    continue;
                };

                let resolution = resolve_target(
                    &normalized_target,
                    Some(&row.file_path),
                    &relation_targets.candidates,
                );
                if let Some(resolved_path) = resolution.resolved_path {
                    let lookup_key = resolved_path.to_ascii_lowercase();
                    if let Some(target) = relation_targets
                        .by_path
                        .get(&resolved_path)
                        .or_else(|| relation_targets.by_path.get(&lookup_key))
                    {
                        resolved_values.push(serde_json::json!({
                            "file_id": target.file_id,
                            "path": target.file_path,
                            "resolved": true,
                        }));
                    } else {
                        diagnostics.push(BaseRelationDiagnostic {
                            file_id: row.file_id.clone(),
                            file_path: row.file_path.clone(),
                            key: relation.key.clone(),
                            target: normalized_target.clone(),
                            reason: "relation_target_not_found".to_string(),
                        });
                        resolved_values.push(serde_json::json!({
                            "target": normalized_target,
                            "resolved": false,
                            "reason": "relation_target_not_found",
                        }));
                    }
                } else {
                    diagnostics.push(BaseRelationDiagnostic {
                        file_id: row.file_id.clone(),
                        file_path: row.file_path.clone(),
                        key: relation.key.clone(),
                        target: normalized_target.clone(),
                        reason: "relation_target_not_found".to_string(),
                    });
                    resolved_values.push(serde_json::json!({
                        "target": normalized_target,
                        "resolved": false,
                        "reason": "relation_target_not_found",
                    }));
                }
            }

            row.properties
                .insert(relation.key.clone(), JsonValue::Array(resolved_values));
        }
    }
}

fn extract_relation_tokens(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::String(value) => vec![value.clone()],
        JsonValue::Array(values) => values
            .iter()
            .flat_map(extract_relation_tokens)
            .collect::<Vec<_>>(),
        JsonValue::Object(map) => map
            .get("path")
            .and_then(JsonValue::as_str)
            .map(|value| vec![value.to_string()])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn normalize_relation_token(raw: &str) -> Option<String> {
    let mut normalized = raw.trim();
    if normalized.is_empty() {
        return None;
    }
    if let Some(inner) = normalized
        .strip_prefix("[[")
        .and_then(|value| value.strip_suffix("]]"))
    {
        normalized = inner.trim();
    }
    if let Some((before_pipe, _)) = normalized.split_once('|') {
        normalized = before_pipe.trim();
    }
    if let Some((before_fragment, _)) = normalized.split_once('#') {
        normalized = before_fragment.trim();
    }
    normalized = normalized.trim_start_matches('/');
    if normalized.is_empty() {
        return None;
    }
    let normalized = normalized.replace('\\', "/");
    if normalized.to_ascii_lowercase().ends_with(".md") {
        Some(normalized)
    } else {
        Some(format!("{normalized}.md"))
    }
}

fn apply_rollups(
    connection: &Connection,
    candidates: &mut [TableRowCandidate],
    rollups: &[BaseRollupSpec],
) -> Result<(), BaseTableExecutorError> {
    let mut target_file_ids = HashSet::new();
    let mut target_keys = HashSet::new();
    for row in candidates.iter() {
        for rollup in rollups {
            target_keys.insert(rollup.target_key.clone());
            for target_file_id in relation_target_file_ids(row, &rollup.relation_key) {
                target_file_ids.insert(target_file_id);
            }
        }
    }

    let rollup_values = load_rollup_property_values(connection, &target_file_ids, &target_keys)?;

    for row in candidates.iter_mut() {
        for rollup in rollups {
            let target_file_ids = relation_target_file_ids(row, &rollup.relation_key);
            let value =
                match rollup.op {
                    BaseRollupOp::Count => {
                        JsonValue::Number(serde_json::Number::from(target_file_ids.len() as i64))
                    }
                    BaseRollupOp::Sum => {
                        let total = target_file_ids
                            .iter()
                            .filter_map(|file_id| {
                                rollup_values
                                    .get(&(file_id.clone(), rollup.target_key.clone()))
                                    .and_then(JsonValue::as_f64)
                            })
                            .sum::<f64>();
                        serde_json::Number::from_f64(total)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null)
                    }
                    BaseRollupOp::Min => {
                        let mut min: Option<JsonValue> = None;
                        for file_id in &target_file_ids {
                            let Some(candidate) = rollup_values
                                .get(&(file_id.clone(), rollup.target_key.clone()))
                                .cloned()
                            else {
                                continue;
                            };
                            if min.as_ref().is_none_or(|current| {
                                compare_json_values(&candidate, current).is_lt()
                            }) {
                                min = Some(candidate);
                            }
                        }
                        min.unwrap_or(JsonValue::Null)
                    }
                    BaseRollupOp::Max => {
                        let mut max: Option<JsonValue> = None;
                        for file_id in &target_file_ids {
                            let Some(candidate) = rollup_values
                                .get(&(file_id.clone(), rollup.target_key.clone()))
                                .cloned()
                            else {
                                continue;
                            };
                            if max.as_ref().is_none_or(|current| {
                                compare_json_values(&candidate, current).is_gt()
                            }) {
                                max = Some(candidate);
                            }
                        }
                        max.unwrap_or(JsonValue::Null)
                    }
                };
            row.properties.insert(rollup.alias.clone(), value);
        }
    }

    Ok(())
}

fn relation_target_file_ids(row: &TableRowCandidate, relation_key: &str) -> Vec<String> {
    row.properties
        .get(relation_key)
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|entry| {
                    if !entry
                        .get("resolved")
                        .and_then(JsonValue::as_bool)
                        .unwrap_or(false)
                    {
                        return None;
                    }
                    entry
                        .get("file_id")
                        .and_then(JsonValue::as_str)
                        .map(|value| value.to_string())
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn load_rollup_property_values(
    connection: &Connection,
    file_ids: &HashSet<String>,
    keys: &HashSet<String>,
) -> Result<HashMap<(String, String), JsonValue>, BaseTableExecutorError> {
    if file_ids.is_empty() || keys.is_empty() {
        return Ok(HashMap::new());
    }

    let file_ids = file_ids.iter().cloned().collect::<Vec<_>>();
    let keys = keys.iter().cloned().collect::<Vec<_>>();

    let file_placeholders = (1..=file_ids.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let key_placeholders = ((file_ids.len() + 1)..=(file_ids.len() + keys.len()))
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        r#"
SELECT file_id, key, value_type, value_json
FROM properties
WHERE file_id IN ({file_placeholders})
  AND key IN ({key_placeholders})
ORDER BY file_id ASC, key ASC
"#
    );

    let mut parameters = Vec::with_capacity(file_ids.len() + keys.len());
    parameters.extend(
        file_ids
            .iter()
            .map(|file_id| SqlValue::Text(file_id.clone())),
    );
    parameters.extend(keys.iter().map(|key| SqlValue::Text(key.clone())));

    let mut statement =
        connection
            .prepare(&query)
            .map_err(|source| BaseTableExecutorError::Sql {
                operation: "prepare_rollup_projection",
                source,
            })?;
    let rows = statement
        .query_map(params_from_iter(parameters), |row| {
            Ok((
                row.get::<_, String>("file_id")?,
                row.get::<_, String>("key")?,
                row.get::<_, String>("value_type")?,
                row.get::<_, String>("value_json")?,
            ))
        })
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "query_rollup_projection",
            source,
        })?;

    let mut values = HashMap::new();
    for row in rows {
        let (file_id, key, value_type, value_json) =
            row.map_err(|source| BaseTableExecutorError::Sql {
                operation: "map_rollup_projection_row",
                source,
            })?;
        let value = serde_json::from_str::<JsonValue>(&value_json).map_err(|source| {
            BaseTableExecutorError::ParsePropertyValue {
                file_id: file_id.clone(),
                key: key.clone(),
                source,
            }
        })?;
        let value = coerce_json_value(
            &value,
            map_field_type(&value_type),
            BaseCoercionMode::Permissive,
        )
        .map_err(|source| BaseTableExecutorError::Coercion {
            file_id: file_id.clone(),
            key: key.clone(),
            source: Box::new(source),
        })?;
        values.insert((file_id, key), value);
    }

    Ok(values)
}

fn materialize_grouped_rows(
    rows: &[TableRowCandidate],
    group_by: &[String],
    aggregates: &[BaseAggregateSpec],
) -> Vec<BaseTableRow> {
    let mut groups = std::collections::BTreeMap::<String, Vec<&TableRowCandidate>>::new();

    for row in rows {
        let mut group_values = serde_json::Map::new();
        for key in group_by {
            group_values.insert(
                key.clone(),
                row.lookup_value(key).unwrap_or(JsonValue::Null),
            );
        }
        let group_key = serde_json::to_string(&group_values).unwrap_or_default();
        groups.entry(group_key).or_default().push(row);
    }

    groups
        .into_values()
        .map(|members| {
            let anchor = members[0];
            let mut values = serde_json::Map::new();
            for key in group_by {
                values.insert(
                    key.clone(),
                    anchor.lookup_value(key).unwrap_or(JsonValue::Null),
                );
            }
            for aggregate in aggregates {
                values.insert(
                    aggregate.alias.clone(),
                    compute_aggregate_value(&members, aggregate),
                );
            }

            BaseTableRow {
                file_id: anchor.file_id.clone(),
                file_path: anchor.file_path.clone(),
                values,
            }
        })
        .collect()
}

fn compute_aggregate_value(
    rows: &[&TableRowCandidate],
    aggregate: &BaseAggregateSpec,
) -> JsonValue {
    match aggregate.op {
        BaseAggregateOp::Count => JsonValue::Number(serde_json::Number::from(rows.len() as i64)),
        BaseAggregateOp::Sum => {
            let total = aggregate
                .key
                .as_ref()
                .map(|key| {
                    rows.iter()
                        .filter_map(|row| row.lookup_value(key).and_then(|value| value.as_f64()))
                        .sum::<f64>()
                })
                .unwrap_or(0.0);
            serde_json::Number::from_f64(total)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        BaseAggregateOp::Min => aggregate
            .key
            .as_ref()
            .and_then(|key| {
                rows.iter()
                    .filter_map(|row| row.lookup_value(key))
                    .reduce(|left, right| {
                        if compare_json_values(&left, &right).is_le() {
                            left
                        } else {
                            right
                        }
                    })
            })
            .unwrap_or(JsonValue::Null),
        BaseAggregateOp::Max => aggregate
            .key
            .as_ref()
            .and_then(|key| {
                rows.iter()
                    .filter_map(|row| row.lookup_value(key))
                    .reduce(|left, right| {
                        if compare_json_values(&left, &right).is_ge() {
                            left
                        } else {
                            right
                        }
                    })
            })
            .unwrap_or(JsonValue::Null),
    }
}

fn load_table_candidates(
    connection: &Connection,
    source_prefix: Option<&str>,
) -> Result<Vec<TableRowCandidate>, BaseTableExecutorError> {
    let (query, params): (&str, Vec<SqlValue>) = if let Some(prefix) = source_prefix {
        (
            r#"
SELECT
  file_id,
  normalized_path
FROM files
WHERE is_markdown = 1
  AND (normalized_path = ?1 OR normalized_path LIKE ?2)
ORDER BY normalized_path ASC
"#,
            vec![
                SqlValue::Text(prefix.to_string()),
                SqlValue::Text(format!("{prefix}/%")),
            ],
        )
    } else {
        (
            r#"
SELECT
  file_id,
  normalized_path
FROM files
WHERE is_markdown = 1
ORDER BY normalized_path ASC
"#,
            Vec::new(),
        )
    };

    let mut statement =
        connection
            .prepare(query)
            .map_err(|source| BaseTableExecutorError::Sql {
                operation: "prepare_table_candidate_files",
                source,
            })?;
    let rows = statement
        .query_map(params_from_iter(params), |row| {
            Ok(TableRowCandidate {
                file_id: row.get("file_id")?,
                file_path: row.get("normalized_path")?,
                properties: HashMap::new(),
            })
        })
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "query_table_candidate_files",
            source,
        })?;

    rows.map(|row| {
        row.map_err(|source| BaseTableExecutorError::Sql {
            operation: "map_table_candidate_files_row",
            source,
        })
    })
    .collect()
}

fn row_matches_filters(row: &TableRowCandidate, filters: &[BaseFilterClause]) -> bool {
    filters.iter().all(|filter| row_matches_filter(row, filter))
}

fn row_matches_filter(row: &TableRowCandidate, filter: &BaseFilterClause) -> bool {
    evaluate_filter(
        row.lookup_value(&filter.key).as_ref(),
        filter.op,
        &filter.value,
    )
    .unwrap_or(false)
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
            sort.null_order,
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
    const PARALLEL_SUMMARY_ROW_THRESHOLD: usize = 1_024;
    const PARALLEL_SUMMARY_COLUMN_THRESHOLD: usize = 3;

    if rows.len() >= PARALLEL_SUMMARY_ROW_THRESHOLD
        && columns.len() >= PARALLEL_SUMMARY_COLUMN_THRESHOLD
    {
        columns
            .par_iter()
            .map(|column| compute_column_summary(rows, column))
            .collect()
    } else {
        columns
            .iter()
            .map(|column| compute_column_summary(rows, column))
            .collect()
    }
}

fn compute_column_summary(
    rows: &[TableRowCandidate],
    column: &BaseColumnConfig,
) -> BaseTableSummary {
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
        serde_json::Number::from_f64(numeric_sum / (numeric_count as f64)).map(JsonValue::Number)
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
    /// SQL execution failed during property projection.
    #[error("base table property projection sql operation '{operation}' failed: {source}")]
    Sql {
        /// SQL operation label.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
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
    /// Typed coercion failed for one property value.
    #[error("failed to coerce property value for file '{file_id}' key '{key}': {source}")]
    Coercion {
        /// File id.
        file_id: String,
        /// Property key.
        key: String,
        /// Coercion error payload.
        #[source]
        source: Box<tao_sdk_bases::BaseCoercionError>,
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

        let computed = self.executor.execute(connection, plan).map_err(|source| {
            BaseTableCacheError::Execute {
                source: Box::new(source),
            }
        })?;

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
        source: Box<BaseTableExecutorError>,
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
        let (files_total, markdown_files): (u64, u64) = connection
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(CASE WHEN is_markdown = 1 THEN 1 ELSE 0 END), 0) FROM files",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|source| HealthSnapshotError::DatabaseStatus { source })?;

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
            vault_root: vault_root.to_string_lossy().to_string(),
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
    /// Stable unresolved reason code.
    pub unresolved_reason: Option<String>,
    /// Link provenance source field.
    pub source_field: String,
}

/// One graph node row with resolved in/out degree counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphNodeDegreeRow {
    /// Stable file id.
    pub file_id: String,
    /// Normalized path.
    pub path: String,
    /// Resolved incoming count.
    pub incoming_resolved: u64,
    /// Resolved outgoing count.
    pub outgoing_resolved: u64,
}

/// One scoped inbound-link row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphScopedInboundRow {
    /// Stable file id.
    pub file_id: String,
    /// Normalized file path.
    pub path: String,
    /// Whether row path is markdown.
    pub is_markdown: bool,
    /// Resolved inbound edge count.
    pub inbound_resolved: u64,
}

/// Scoped inbound-link summary counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphScopedInboundSummary {
    /// Total matched files.
    pub total_files: u64,
    /// Files with at least one inbound edge.
    pub linked_files: u64,
    /// Files with zero inbound edges.
    pub unlinked_files: u64,
}

/// One strict floating-file row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphFloatingRow {
    /// Stable file id.
    pub file_id: String,
    /// Normalized file path.
    pub path: String,
    /// Whether row path is markdown.
    pub is_markdown: bool,
    /// Resolved inbound edge count.
    pub incoming_resolved: u64,
    /// Resolved outgoing edge count.
    pub outgoing_resolved: u64,
}

/// Strict floating-file summary counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphFloatingSummary {
    /// Total strict floating files.
    pub total_files: u64,
    /// Total strict floating markdown files.
    pub markdown_files: u64,
    /// Total strict floating non-markdown files.
    pub non_markdown_files: u64,
}

/// Input payload for scoped inbound-link audits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphScopedInboundRequest {
    /// Vault-relative scope prefix.
    pub scope_prefix: String,
    /// Include markdown files in result set.
    pub include_markdown: bool,
    /// Include non-markdown files in result set.
    pub include_non_markdown: bool,
    /// Optional excluded scope prefixes.
    pub exclude_prefixes: Vec<String>,
    /// Page size.
    pub limit: u32,
    /// Page offset.
    pub offset: u32,
}

/// One connected component summary row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphComponentRow {
    /// Number of markdown nodes in the component.
    pub size: u64,
    /// Member paths (full list or bounded sample, depending on request).
    pub paths: Vec<String>,
    /// Whether `paths` is truncated compared to full membership.
    pub truncated: bool,
}

/// Connected component traversal mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphComponentMode {
    /// Weakly connected components over undirected projection.
    Weak,
    /// Strongly connected components over directed graph.
    Strong,
}

/// Graph walk traversal direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphWalkDirection {
    /// Edge traversed from source to target.
    Outgoing,
    /// Edge traversed from target to source.
    Incoming,
}

/// One graph walk step row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphWalkStep {
    /// Traversal depth (1-based from root).
    pub depth: u32,
    /// Traversal direction.
    pub direction: GraphWalkDirection,
    /// Stable link identifier.
    pub link_id: String,
    /// Source path.
    pub source_path: String,
    /// Target path when resolved.
    pub target_path: Option<String>,
    /// Raw target token.
    pub raw_target: String,
    /// Whether the edge is resolved.
    pub resolved: bool,
    /// Traversed edge type.
    pub edge_type: GraphWalkEdgeType,
}

/// Graph walk edge classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphWalkEdgeType {
    /// Wikilink edge from indexed markdown links.
    Wikilink,
    /// Folder parent overlay edge.
    FolderParent,
    /// Folder sibling overlay edge.
    FolderSibling,
}

/// Input request for graph walk traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphWalkRequest {
    /// Root note path for traversal.
    pub path: String,
    /// Maximum traversal depth.
    pub depth: u32,
    /// Maximum number of step rows returned.
    pub limit: u32,
    /// Include unresolved outgoing edges.
    pub include_unresolved: bool,
    /// Include folder relationship overlay edges.
    pub include_folders: bool,
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

    /// List one unresolved edges window across vault.
    pub fn unresolved_links_page(
        &self,
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<(u64, Vec<LinkGraphEdge>), LinkGraphServiceError> {
        let total = LinksRepository::count_unresolved(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_unresolved_with_paths_window(connection, limit, offset)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok((total, map_link_edges(rows)))
    }

    /// List one deadends diagnostics window in deterministic path order.
    pub fn deadends_page(
        &self,
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<(u64, Vec<GraphNodeDegreeRow>), LinkGraphServiceError> {
        let total = LinksRepository::count_deadends(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_deadends_window(connection, limit, offset)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok((total, map_graph_node_degrees(rows)))
    }

    /// List one orphans diagnostics window in deterministic path order.
    pub fn orphans_page(
        &self,
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<(u64, Vec<GraphNodeDegreeRow>), LinkGraphServiceError> {
        let total = LinksRepository::count_orphans(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_orphans_window(connection, limit, offset)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        Ok((total, map_graph_node_degrees(rows)))
    }

    /// Return one scoped inbound-link audit window plus summary counters.
    pub fn scoped_inbound_page(
        &self,
        connection: &Connection,
        request: &GraphScopedInboundRequest,
    ) -> Result<(GraphScopedInboundSummary, Vec<GraphScopedInboundRow>), LinkGraphServiceError>
    {
        let summary = LinksRepository::summarize_scoped_inbound(
            connection,
            &request.scope_prefix,
            request.include_markdown,
            request.include_non_markdown,
            &request.exclude_prefixes,
        )
        .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_scoped_inbound_window(
            connection,
            &request.scope_prefix,
            request.include_markdown,
            request.include_non_markdown,
            &request.exclude_prefixes,
            request.limit,
            request.offset,
        )
        .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;

        let items = rows
            .into_iter()
            .map(|row| GraphScopedInboundRow {
                file_id: row.file_id,
                path: row.path,
                is_markdown: row.is_markdown,
                inbound_resolved: row.inbound_resolved,
            })
            .collect::<Vec<_>>();
        Ok((
            GraphScopedInboundSummary {
                total_files: summary.total_files,
                linked_files: summary.linked_files,
                unlinked_files: summary.unlinked_files,
            },
            items,
        ))
    }

    /// Return one strict floating-file window plus summary counters.
    pub fn floating_page(
        &self,
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<(GraphFloatingSummary, Vec<GraphFloatingRow>), LinkGraphServiceError> {
        let summary = LinksRepository::summarize_floating_default(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let rows = LinksRepository::list_floating_default_window(connection, limit, offset)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let items = rows
            .into_iter()
            .map(|row| GraphFloatingRow {
                file_id: row.file_id,
                path: row.path,
                is_markdown: row.is_markdown,
                incoming_resolved: row.incoming_resolved,
                outgoing_resolved: row.outgoing_resolved,
            })
            .collect::<Vec<_>>();
        Ok((
            GraphFloatingSummary {
                total_files: summary.total_files,
                markdown_files: summary.markdown_files,
                non_markdown_files: summary.non_markdown_files,
            },
            items,
        ))
    }

    /// Build connected components over resolved graph edges and return one deterministic page.
    pub fn components_page(
        &self,
        connection: &Connection,
        mode: GraphComponentMode,
        limit: u32,
        offset: u32,
        include_members: bool,
        sample_size: usize,
    ) -> Result<(u64, Vec<GraphComponentRow>), LinkGraphServiceError> {
        let markdown_files = FilesRepository::list_all(connection)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
            .into_iter()
            .filter(|file| file.is_markdown)
            .map(|file| (file.file_id, file.normalized_path))
            .collect::<Vec<_>>();
        let mut paths_by_id = HashMap::with_capacity(markdown_files.len());
        let mut ids = Vec::with_capacity(markdown_files.len());
        for (file_id, path) in markdown_files {
            ids.push(file_id.clone());
            paths_by_id.insert(file_id, path);
        }
        ids.sort();

        let pairs = LinksRepository::list_resolved_pairs(connection)
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
        let components_by_ids = match mode {
            GraphComponentMode::Weak => weak_components(&ids, &pairs),
            GraphComponentMode::Strong => strong_components(&ids, &pairs),
        };
        let mut components = build_component_rows(
            components_by_ids,
            &paths_by_id,
            include_members,
            sample_size,
        );

        components.sort_by(|left, right| {
            right
                .size
                .cmp(&left.size)
                .then_with(|| left.paths.first().cmp(&right.paths.first()))
        });
        let total = u64::try_from(components.len()).unwrap_or(u64::MAX);
        let items = components
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect::<Vec<_>>();
        Ok((total, items))
    }

    /// Walk graph neighbors from one root path using frontier SQL lookups.
    pub fn walk(
        &self,
        connection: &Connection,
        request: &GraphWalkRequest,
    ) -> Result<Vec<GraphWalkStep>, LinkGraphServiceError> {
        if request.depth == 0 || request.limit == 0 {
            return Ok(Vec::new());
        }

        let Some(start_file) =
            FilesRepository::get_by_normalized_path(connection, &request.path)
                .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
        else {
            return Ok(Vec::new());
        };
        let path_by_id = FilesRepository::list_all(connection)
            .map_err(|source| LinkGraphServiceError::FilesRepository { source })?
            .into_iter()
            .filter(|row| row.is_markdown)
            .map(|row| (row.file_id, row.normalized_path))
            .collect::<HashMap<_, _>>();
        let mut folder_members = HashMap::<String, Vec<String>>::new();
        if request.include_folders {
            for (file_id, path) in &path_by_id {
                folder_members
                    .entry(note_folder(path).to_string())
                    .or_default()
                    .push(file_id.clone());
            }
            for members in folder_members.values_mut() {
                members.sort();
                members.dedup();
            }
        }

        let mut steps = Vec::<GraphWalkStep>::new();
        let mut frontier = vec![start_file.file_id];
        let mut visited_depth = HashMap::<String, u32>::new();
        visited_depth.insert(frontier[0].clone(), 0);
        let hard_limit = request.limit as usize;

        for depth in 0..request.depth {
            if frontier.is_empty() || steps.len() >= hard_limit {
                break;
            }

            let outgoing = LinksRepository::list_outgoing_for_sources_with_paths(
                connection,
                &frontier,
                request.include_unresolved,
            )
            .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;
            let incoming =
                LinksRepository::list_incoming_for_targets_with_paths(connection, &frontier)
                    .map_err(|source| LinkGraphServiceError::LinksRepository { source })?;

            let mut next_frontier = Vec::<String>::new();
            let next_depth = depth + 1;

            for edge in outgoing {
                if steps.len() >= hard_limit {
                    break;
                }
                let resolved = !edge.is_unresolved && edge.resolved_file_id.is_some();
                steps.push(GraphWalkStep {
                    depth: next_depth,
                    direction: GraphWalkDirection::Outgoing,
                    link_id: edge.link_id,
                    source_path: edge.source_path,
                    target_path: edge.resolved_path,
                    raw_target: edge.raw_target,
                    resolved,
                    edge_type: GraphWalkEdgeType::Wikilink,
                });

                if let Some(target_id) = edge.resolved_file_id {
                    let should_visit = visited_depth
                        .get(&target_id)
                        .map(|seen_depth| next_depth < *seen_depth)
                        .unwrap_or(true);
                    if should_visit {
                        visited_depth.insert(target_id.clone(), next_depth);
                        next_frontier.push(target_id);
                    }
                }
            }

            for edge in incoming {
                if steps.len() >= hard_limit {
                    break;
                }
                steps.push(GraphWalkStep {
                    depth: next_depth,
                    direction: GraphWalkDirection::Incoming,
                    link_id: edge.link_id,
                    source_path: edge.source_path,
                    target_path: edge.resolved_path,
                    raw_target: edge.raw_target,
                    resolved: true,
                    edge_type: GraphWalkEdgeType::Wikilink,
                });
                let source_id = edge.source_file_id;
                let should_visit = visited_depth
                    .get(&source_id)
                    .map(|seen_depth| next_depth < *seen_depth)
                    .unwrap_or(true);
                if should_visit {
                    visited_depth.insert(source_id.clone(), next_depth);
                    next_frontier.push(source_id);
                }
            }
            if request.include_folders {
                for source_id in &frontier {
                    if steps.len() >= hard_limit {
                        break;
                    }
                    let Some(source_path) = path_by_id.get(source_id) else {
                        continue;
                    };
                    let source_folder = note_folder(source_path).to_string();
                    let mut folder_targets = Vec::<(String, GraphWalkEdgeType)>::new();

                    if let Some(parent_folder) = parent_folder(&source_folder)
                        && let Some(parent_members) = folder_members.get(parent_folder)
                    {
                        for target_id in parent_members {
                            if target_id != source_id {
                                folder_targets
                                    .push((target_id.clone(), GraphWalkEdgeType::FolderParent));
                            }
                        }
                    }
                    if let Some(sibling_members) = folder_members.get(&source_folder) {
                        for target_id in sibling_members {
                            if target_id != source_id {
                                folder_targets
                                    .push((target_id.clone(), GraphWalkEdgeType::FolderSibling));
                            }
                        }
                    }

                    folder_targets.sort_by(|left, right| left.0.cmp(&right.0));
                    folder_targets.dedup();

                    for (target_id, edge_type) in folder_targets {
                        if steps.len() >= hard_limit {
                            break;
                        }
                        let Some(target_path) = path_by_id.get(&target_id).cloned() else {
                            continue;
                        };
                        let link_id = format!(
                            "folder:{source_id}:{target_id}:{}",
                            graph_walk_edge_type_label(&edge_type)
                        );
                        steps.push(GraphWalkStep {
                            depth: next_depth,
                            direction: GraphWalkDirection::Outgoing,
                            link_id,
                            source_path: source_path.clone(),
                            target_path: Some(target_path.clone()),
                            raw_target: target_path,
                            resolved: true,
                            edge_type,
                        });
                        let should_visit = visited_depth
                            .get(&target_id)
                            .map(|seen_depth| next_depth < *seen_depth)
                            .unwrap_or(true);
                        if should_visit {
                            visited_depth.insert(target_id.clone(), next_depth);
                            next_frontier.push(target_id);
                        }
                    }
                }
            }

            next_frontier.sort();
            next_frontier.dedup();
            frontier = next_frontier;
        }

        Ok(steps)
    }
}

fn build_component_rows(
    components_by_ids: Vec<Vec<String>>,
    paths_by_id: &HashMap<String, String>,
    include_members: bool,
    sample_size: usize,
) -> Vec<GraphComponentRow> {
    let mut components = Vec::<GraphComponentRow>::with_capacity(components_by_ids.len());
    for members in components_by_ids {
        let mut paths = members
            .iter()
            .filter_map(|file_id| paths_by_id.get(file_id).cloned())
            .collect::<Vec<_>>();
        paths.sort();
        let full_len = paths.len();
        if !include_members && paths.len() > sample_size {
            paths.truncate(sample_size);
        }
        components.push(GraphComponentRow {
            size: u64::try_from(members.len()).unwrap_or(u64::MAX),
            truncated: !include_members && full_len > paths.len(),
            paths,
        });
    }
    components
}

fn weak_components(
    ids: &[String],
    pairs: &[tao_sdk_storage::ResolvedLinkPair],
) -> Vec<Vec<String>> {
    let mut adjacency = HashMap::<String, Vec<String>>::new();
    for pair in pairs {
        adjacency
            .entry(pair.source_file_id.clone())
            .or_default()
            .push(pair.target_file_id.clone());
        adjacency
            .entry(pair.target_file_id.clone())
            .or_default()
            .push(pair.source_file_id.clone());
    }
    for neighbors in adjacency.values_mut() {
        neighbors.sort();
        neighbors.dedup();
    }

    let mut visited = HashSet::<String>::new();
    let mut components = Vec::<Vec<String>>::new();
    for root in ids {
        if !visited.insert(root.clone()) {
            continue;
        }
        let mut queue = VecDeque::from([root.clone()]);
        let mut members = Vec::<String>::new();
        while let Some(current) = queue.pop_front() {
            members.push(current.clone());
            if let Some(neighbors) = adjacency.get(&current) {
                for next in neighbors {
                    if visited.insert(next.clone()) {
                        queue.push_back(next.clone());
                    }
                }
            }
        }
        members.sort();
        components.push(members);
    }
    components
}

fn strong_components(
    ids: &[String],
    pairs: &[tao_sdk_storage::ResolvedLinkPair],
) -> Vec<Vec<String>> {
    let mut forward = HashMap::<String, Vec<String>>::new();
    let mut reverse = HashMap::<String, Vec<String>>::new();
    for pair in pairs {
        forward
            .entry(pair.source_file_id.clone())
            .or_default()
            .push(pair.target_file_id.clone());
        reverse
            .entry(pair.target_file_id.clone())
            .or_default()
            .push(pair.source_file_id.clone());
    }
    for neighbors in forward.values_mut() {
        neighbors.sort();
        neighbors.dedup();
    }
    for neighbors in reverse.values_mut() {
        neighbors.sort();
        neighbors.dedup();
    }

    let mut visited = HashSet::<String>::new();
    let mut finish_order = Vec::<String>::new();
    for root in ids {
        if visited.contains(root) {
            continue;
        }
        let mut stack = Vec::<(String, bool)>::from([(root.clone(), false)]);
        while let Some((node, expanded)) = stack.pop() {
            if expanded {
                finish_order.push(node);
                continue;
            }
            if !visited.insert(node.clone()) {
                continue;
            }
            stack.push((node.clone(), true));
            if let Some(neighbors) = forward.get(&node) {
                for next in neighbors.iter().rev() {
                    if !visited.contains(next) {
                        stack.push((next.clone(), false));
                    }
                }
            }
        }
    }

    let mut assigned = HashSet::<String>::new();
    let mut components = Vec::<Vec<String>>::new();
    while let Some(root) = finish_order.pop() {
        if !assigned.insert(root.clone()) {
            continue;
        }
        let mut stack = Vec::<String>::from([root]);
        let mut members = Vec::<String>::new();
        while let Some(node) = stack.pop() {
            members.push(node.clone());
            if let Some(neighbors) = reverse.get(&node) {
                for next in neighbors {
                    if assigned.insert(next.clone()) {
                        stack.push(next.clone());
                    }
                }
            }
        }
        members.sort();
        components.push(members);
    }
    components
}

fn note_folder(path: &str) -> &str {
    Path::new(path)
        .parent()
        .and_then(Path::to_str)
        .unwrap_or_default()
}

fn parent_folder(folder: &str) -> Option<&str> {
    if folder.is_empty() {
        return None;
    }
    Path::new(folder)
        .parent()
        .and_then(Path::to_str)
        .or(Some(""))
}

fn graph_walk_edge_type_label(edge_type: &GraphWalkEdgeType) -> &'static str {
    match edge_type {
        GraphWalkEdgeType::Wikilink => "wikilink",
        GraphWalkEdgeType::FolderParent => "folder-parent",
        GraphWalkEdgeType::FolderSibling => "folder-sibling",
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
            unresolved_reason: row.unresolved_reason,
            source_field: row.source_field,
        })
        .collect()
}

fn map_graph_node_degrees(rows: Vec<tao_sdk_storage::GraphNodeDegree>) -> Vec<GraphNodeDegreeRow> {
    rows.into_iter()
        .map(|row| GraphNodeDegreeRow {
            file_id: row.file_id,
            path: row.path,
            incoming_resolved: row.incoming_resolved,
            outgoing_resolved: row.outgoing_resolved,
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
        BaseAggregateOp, BaseAggregateSpec, BaseColumnConfig, BaseDiagnosticSeverity, BaseDocument,
        BaseFilterClause, BaseFilterOp, BaseNullOrder, BaseRelationSpec, BaseRollupOp,
        BaseRollupSpec, BaseSortClause, BaseSortDirection, BaseViewDefinition, BaseViewKind,
        TableQueryPlan,
    };
    use tao_sdk_properties::TypedPropertyValue;
    use tao_sdk_storage::{
        BaseRecordInput, BasesRepository, FileRecordInput, FilesRepository, LinkRecordInput,
        LinksRepository, PropertiesRepository, PropertyRecordInput, known_migrations,
        run_migrations,
    };
    use tempfile::tempdir;

    use super::{
        BacklinkGraphService, BaseColumnConfigPersistError, BaseColumnConfigPersistenceService,
        BaseTableCachedQueryService, BaseTableExecutorError, BaseTableExecutorService,
        BaseValidationError, BaseValidationService, CasePolicy, GraphComponentMode,
        GraphScopedInboundRequest, HealthSnapshotService, MarkdownIngestPipeline, NoteCrudError,
        NoteCrudService, PropertyQueryRequest, PropertyQueryService, PropertyQuerySort,
        PropertyUpdateService, ReconcileService, SdkTransactionCoordinator, ServiceTraceContext,
        StorageWriteService, WatcherStatus,
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
                unresolved_reason: None,
                source_field: "body".to_string(),
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
                unresolved_reason: None,
                source_field: "body".to_string(),
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
                unresolved_reason: None,
                source_field: "body".to_string(),
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
                unresolved_reason: None,
                source_field: "body".to_string(),
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
                unresolved_reason: Some("missing-note".to_string()),
                source_field: "body".to_string(),
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
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert resolved marker link");

        let unresolved = BacklinkGraphService
            .unresolved_links(&connection)
            .expect("query unresolved");
        let (unresolved_total, unresolved_page) = BacklinkGraphService
            .unresolved_links_page(&connection, 1, 0)
            .expect("query unresolved page");
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved_total, 1);
        assert_eq!(unresolved_page.len(), 1);
        assert_eq!(unresolved_page[0].link_id, "l-unresolved");
        assert_eq!(unresolved[0].link_id, "l-unresolved");
        assert!(unresolved[0].is_unresolved);
        assert_eq!(
            unresolved[0].unresolved_reason.as_deref(),
            Some("missing-note")
        );
        assert_eq!(unresolved[0].source_field, "body");
    }

    #[test]
    fn backlink_graph_service_scoped_inbound_audits_non_markdown_targets() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "source",
                "notes/source.md",
                "notes/source.md",
                "/vault/notes/source.md",
            ),
        )
        .expect("insert source");
        FilesRepository::insert(
            &connection,
            &FileRecordInput {
                file_id: "linked".to_string(),
                normalized_path: "notes/assets/linked.pdf".to_string(),
                match_key: "notes/assets/linked.pdf".to_string(),
                absolute_path: "/vault/notes/assets/linked.pdf".to_string(),
                size_bytes: 10,
                modified_unix_ms: 1_700_000_000_000,
                hash_blake3: "hash-linked".to_string(),
                is_markdown: false,
            },
        )
        .expect("insert linked asset");
        FilesRepository::insert(
            &connection,
            &FileRecordInput {
                file_id: "orphan".to_string(),
                normalized_path: "notes/assets/orphan.pdf".to_string(),
                match_key: "notes/assets/orphan.pdf".to_string(),
                absolute_path: "/vault/notes/assets/orphan.pdf".to_string(),
                size_bytes: 10,
                modified_unix_ms: 1_700_000_000_000,
                hash_blake3: "hash-orphan".to_string(),
                is_markdown: false,
            },
        )
        .expect("insert orphan asset");

        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-attachment".to_string(),
                source_file_id: "source".to_string(),
                raw_target: "assets/linked.pdf".to_string(),
                resolved_file_id: Some("linked".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body:markdown".to_string(),
            },
        )
        .expect("insert attachment edge");

        let (summary, rows) = BacklinkGraphService
            .scoped_inbound_page(
                &connection,
                &GraphScopedInboundRequest {
                    scope_prefix: "notes".to_string(),
                    include_markdown: false,
                    include_non_markdown: true,
                    exclude_prefixes: Vec::new(),
                    limit: 100,
                    offset: 0,
                },
            )
            .expect("scoped inbound");
        assert_eq!(summary.total_files, 2);
        assert_eq!(summary.linked_files, 1);
        assert_eq!(summary.unlinked_files, 1);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].path, "notes/assets/linked.pdf");
        assert_eq!(rows[0].inbound_resolved, 1);
        assert_eq!(rows[1].path, "notes/assets/orphan.pdf");
        assert_eq!(rows[1].inbound_resolved, 0);
    }

    #[test]
    fn backlink_graph_service_floating_returns_strict_disconnected_files() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record(
                "note-source",
                "notes/source.md",
                "notes/source.md",
                "/vault/notes/source.md",
            ),
        )
        .expect("insert source note");
        FilesRepository::insert(
            &connection,
            &file_record(
                "note-linked",
                "notes/linked.md",
                "notes/linked.md",
                "/vault/notes/linked.md",
            ),
        )
        .expect("insert linked note");
        FilesRepository::insert(
            &connection,
            &file_record(
                "note-floating",
                "notes/floating.md",
                "notes/floating.md",
                "/vault/notes/floating.md",
            ),
        )
        .expect("insert floating note");
        FilesRepository::insert(
            &connection,
            &FileRecordInput {
                file_id: "asset-floating".to_string(),
                normalized_path: "notes/assets/floating.pdf".to_string(),
                match_key: "notes/assets/floating.pdf".to_string(),
                absolute_path: "/vault/notes/assets/floating.pdf".to_string(),
                size_bytes: 10,
                modified_unix_ms: 1_700_000_000_000,
                hash_blake3: "hash-asset-floating".to_string(),
                is_markdown: false,
            },
        )
        .expect("insert floating asset");
        FilesRepository::insert(
            &connection,
            &FileRecordInput {
                file_id: "noise".to_string(),
                normalized_path: ".DS_Store".to_string(),
                match_key: ".ds_store".to_string(),
                absolute_path: "/vault/.DS_Store".to_string(),
                size_bytes: 10,
                modified_unix_ms: 1_700_000_000_000,
                hash_blake3: "hash-noise".to_string(),
                is_markdown: false,
            },
        )
        .expect("insert noise file");

        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-note".to_string(),
                source_file_id: "note-source".to_string(),
                raw_target: "linked".to_string(),
                resolved_file_id: Some("note-linked".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert note edge");

        let (summary, rows) = BacklinkGraphService
            .floating_page(&connection, 100, 0)
            .expect("floating page");
        assert_eq!(summary.total_files, 2);
        assert_eq!(summary.markdown_files, 1);
        assert_eq!(summary.non_markdown_files, 1);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].path, "notes/assets/floating.pdf");
        assert_eq!(rows[0].incoming_resolved, 0);
        assert_eq!(rows[0].outgoing_resolved, 0);
        assert_eq!(rows[1].path, "notes/floating.md");
        assert_eq!(rows[1].incoming_resolved, 0);
        assert_eq!(rows[1].outgoing_resolved, 0);
    }

    #[test]
    fn backlink_graph_components_supports_weak_and_strong_modes() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(
            &connection,
            &file_record("a", "notes/a.md", "notes/a.md", "/vault/notes/a.md"),
        )
        .expect("insert a");
        FilesRepository::insert(
            &connection,
            &file_record("b", "notes/b.md", "notes/b.md", "/vault/notes/b.md"),
        )
        .expect("insert b");
        FilesRepository::insert(
            &connection,
            &file_record("c", "notes/c.md", "notes/c.md", "/vault/notes/c.md"),
        )
        .expect("insert c");

        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-a-b".to_string(),
                source_file_id: "a".to_string(),
                raw_target: "b".to_string(),
                resolved_file_id: Some("b".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert a->b");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-b-a".to_string(),
                source_file_id: "b".to_string(),
                raw_target: "a".to_string(),
                resolved_file_id: Some("a".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert b->a");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-b-c".to_string(),
                source_file_id: "b".to_string(),
                raw_target: "c".to_string(),
                resolved_file_id: Some("c".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert b->c");

        let (weak_total, weak_rows) = BacklinkGraphService
            .components_page(&connection, GraphComponentMode::Weak, 50, 0, true, 64)
            .expect("weak components");
        assert_eq!(weak_total, 1);
        assert_eq!(weak_rows.len(), 1);
        assert_eq!(weak_rows[0].size, 3);

        let (strong_total, strong_rows) = BacklinkGraphService
            .components_page(&connection, GraphComponentMode::Strong, 50, 0, true, 64)
            .expect("strong components");
        assert_eq!(strong_total, 2);
        let sizes = strong_rows.iter().map(|row| row.size).collect::<Vec<_>>();
        assert_eq!(sizes, vec![2, 1]);
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
        FilesRepository::insert(
            &connection,
            &file_record("f1", "notes/a.md", "notes/a.md", "/vault/notes/a.md"),
        )
        .expect("insert a");
        FilesRepository::insert(
            &connection,
            &file_record("f2", "notes/b.md", "notes/b.md", "/vault/notes/b.md"),
        )
        .expect("insert b");
        let mut non_markdown =
            file_record("f3", "notes/c.png", "notes/c.png", "/vault/notes/c.png");
        non_markdown.is_markdown = false;
        FilesRepository::insert(&connection, &non_markdown).expect("insert c");
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
        assert_eq!(snapshot.db_migrations, known_migrations().len() as u64);
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
                null_order: BaseNullOrder::First,
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
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
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
    fn base_table_executor_supports_grouped_aggregate_output() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        for (file_id, path) in [
            ("f1", "notes/projects/alpha.md"),
            ("f2", "notes/projects/beta.md"),
            ("f3", "notes/projects/gamma.md"),
        ] {
            FilesRepository::insert(
                &connection,
                &file_record(file_id, path, path, &format!("/vault/{path}")),
            )
            .expect("insert file");
        }

        for (property_id, file_id, key, value_type, value_json) in [
            ("p1", "f1", "status", "string", "\"active\""),
            ("p2", "f1", "priority", "number", "2"),
            ("p3", "f2", "status", "string", "\"active\""),
            ("p4", "f2", "priority", "number", "3"),
            ("p5", "f3", "status", "string", "\"paused\""),
            ("p6", "f3", "priority", "number", "5"),
        ] {
            PropertiesRepository::upsert(
                &connection,
                &PropertyRecordInput {
                    property_id: property_id.to_string(),
                    file_id: file_id.to_string(),
                    key: key.to_string(),
                    value_type: value_type.to_string(),
                    value_json: value_json.to_string(),
                },
            )
            .expect("upsert property");
        }

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: vec!["status".to_string(), "priority".to_string()],
            filters: Vec::new(),
            sorts: Vec::new(),
            columns: Vec::new(),
            group_by: vec!["status".to_string()],
            aggregates: vec![
                BaseAggregateSpec {
                    alias: "count_all".to_string(),
                    op: BaseAggregateOp::Count,
                    key: None,
                },
                BaseAggregateSpec {
                    alias: "priority_sum".to_string(),
                    op: BaseAggregateOp::Sum,
                    key: Some("priority".to_string()),
                },
            ],
            relations: Vec::new(),
            rollups: Vec::new(),
            limit: 50,
            offset: 0,
            property_queries: Vec::new(),
        };

        let page = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect("execute grouped table plan");
        assert_eq!(page.total, 2);
        assert!(page.summaries.is_empty());
        assert!(page.grouping.is_some());
        assert_eq!(
            page.grouping.as_ref().map(|value| value.group_by.clone()),
            Some(vec!["status".to_string()])
        );

        let active = page
            .rows
            .iter()
            .find(|row| row.values.get("status") == Some(&serde_json::json!("active")))
            .expect("active group");
        assert_eq!(active.values.get("count_all"), Some(&serde_json::json!(2)));
        assert_eq!(
            active.values.get("priority_sum"),
            Some(&serde_json::json!(5.0))
        );
    }

    #[test]
    fn base_table_executor_resolves_relations_and_rollups() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        for (file_id, path) in [
            ("f-project", "notes/projects/alpha.md"),
            ("f-meeting-1", "notes/meetings/m1.md"),
            ("f-meeting-2", "notes/meetings/m2.md"),
        ] {
            FilesRepository::insert(
                &connection,
                &file_record(file_id, path, path, &format!("/vault/{path}")),
            )
            .expect("insert file");
        }

        for (property_id, file_id, key, value_type, value_json) in [
            (
                "p1",
                "f-project",
                "meetings",
                "json",
                r#"["notes/meetings/m1.md", "[[notes/meetings/m2]]", "[[notes/meetings/missing]]"]"#,
            ),
            ("p2", "f-meeting-1", "duration", "number", "30"),
            ("p3", "f-meeting-2", "duration", "number", "45"),
        ] {
            PropertiesRepository::upsert(
                &connection,
                &PropertyRecordInput {
                    property_id: property_id.to_string(),
                    file_id: file_id.to_string(),
                    key: key.to_string(),
                    value_type: value_type.to_string(),
                    value_json: value_json.to_string(),
                },
            )
            .expect("upsert property");
        }

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: vec!["meetings".to_string(), "duration".to_string()],
            filters: Vec::new(),
            sorts: Vec::new(),
            columns: vec![
                BaseColumnConfig {
                    key: "meetings".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
                BaseColumnConfig {
                    key: "meeting_total".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
            ],
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: vec![BaseRelationSpec {
                key: "meetings".to_string(),
            }],
            rollups: vec![BaseRollupSpec {
                alias: "meeting_total".to_string(),
                relation_key: "meetings".to_string(),
                target_key: "duration".to_string(),
                op: BaseRollupOp::Sum,
            }],
            limit: 10,
            offset: 0,
            property_queries: Vec::new(),
        };

        let page = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect("execute relation/rollup plan");
        assert_eq!(page.total, 1);
        assert_eq!(page.relation_diagnostics.len(), 1);
        assert_eq!(
            page.relation_diagnostics[0].reason,
            "relation_target_not_found"
        );
        assert_eq!(
            page.rows[0].values.get("meeting_total"),
            Some(&serde_json::json!(75.0))
        );
        let meetings = page.rows[0]
            .values
            .get("meetings")
            .and_then(serde_json::Value::as_array)
            .expect("meetings relation array");
        assert_eq!(meetings.len(), 3);
        assert!(meetings.iter().any(|entry| {
            entry.get("resolved").and_then(serde_json::Value::as_bool) == Some(false)
        }));
    }

    #[test]
    fn base_table_executor_resolves_short_wikilink_relation_tokens() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        for (file_id, path) in [
            ("f-project", "notes/projects/alpha.md"),
            ("f-meeting-1", "notes/meetings/m1.md"),
            ("f-meeting-2", "notes/meetings/m2.md"),
        ] {
            FilesRepository::insert(
                &connection,
                &file_record(file_id, path, path, &format!("/vault/{path}")),
            )
            .expect("insert file");
        }

        for (property_id, file_id, key, value_type, value_json) in [
            ("p1", "f-project", "meetings", "json", r#"["[[m1]]", "m2"]"#),
            ("p2", "f-meeting-1", "duration", "number", "30"),
            ("p3", "f-meeting-2", "duration", "number", "45"),
        ] {
            PropertiesRepository::upsert(
                &connection,
                &PropertyRecordInput {
                    property_id: property_id.to_string(),
                    file_id: file_id.to_string(),
                    key: key.to_string(),
                    value_type: value_type.to_string(),
                    value_json: value_json.to_string(),
                },
            )
            .expect("upsert property");
        }

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: vec!["meetings".to_string(), "duration".to_string()],
            filters: Vec::new(),
            sorts: Vec::new(),
            columns: vec![
                BaseColumnConfig {
                    key: "meetings".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
                BaseColumnConfig {
                    key: "meeting_total".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
            ],
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: vec![BaseRelationSpec {
                key: "meetings".to_string(),
            }],
            rollups: vec![BaseRollupSpec {
                alias: "meeting_total".to_string(),
                relation_key: "meetings".to_string(),
                target_key: "duration".to_string(),
                op: BaseRollupOp::Sum,
            }],
            limit: 10,
            offset: 0,
            property_queries: Vec::new(),
        };

        let page = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect("execute relation/rollup plan");
        assert_eq!(page.total, 1);
        assert_eq!(page.relation_diagnostics.len(), 0);
        assert_eq!(
            page.rows[0].values.get("meeting_total"),
            Some(&serde_json::json!(75.0))
        );
        let meetings = page.rows[0]
            .values
            .get("meetings")
            .and_then(serde_json::Value::as_array)
            .expect("meetings relation array");
        assert_eq!(meetings.len(), 2);
        assert!(meetings.iter().all(|entry| {
            entry.get("resolved").and_then(serde_json::Value::as_bool) == Some(true)
        }));
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
                null_order: BaseNullOrder::First,
            }],
            columns: vec![BaseColumnConfig {
                key: "path".to_string(),
                label: None,
                width: None,
                hidden: false,
            }],
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
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
    fn base_table_executor_parallel_fast_path_is_deterministic() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        for index in 0..1_300 {
            let file_id = format!("f{index}");
            let path = format!("notes/projects/{index:04}.md");
            FilesRepository::insert(
                &connection,
                &file_record(&file_id, &path, &path, &format!("/vault/{path}")),
            )
            .expect("insert file");
            PropertiesRepository::upsert(
                &connection,
                &PropertyRecordInput {
                    property_id: format!("p{index}"),
                    file_id: file_id.clone(),
                    key: "due".to_string(),
                    value_type: "number".to_string(),
                    value_json: ((index % 17) as i64).to_string(),
                },
            )
            .expect("upsert property");
        }

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: vec!["due".to_string()],
            filters: vec![BaseFilterClause {
                key: "due".to_string(),
                op: BaseFilterOp::Gte,
                value: serde_json::json!(3),
            }],
            sorts: vec![BaseSortClause {
                key: "due".to_string(),
                direction: BaseSortDirection::Desc,
                null_order: BaseNullOrder::First,
            }],
            columns: vec![
                BaseColumnConfig {
                    key: "path".to_string(),
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
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
            limit: 200,
            offset: 20,
            property_queries: Vec::new(),
        };

        let first = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect("execute first");
        let second = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect("execute second");

        assert_eq!(first.total, second.total);
        assert_eq!(first.summaries, second.summaries);
        assert_eq!(first.rows, second.rows);
    }

    #[test]
    fn base_table_executor_excludes_non_markdown_files_from_candidates() {
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
        .expect("insert markdown");
        let mut non_markdown = file_record(
            "f2",
            "notes/projects/readme.txt",
            "notes/projects/readme.txt",
            "/vault/notes/projects/readme.txt",
        );
        non_markdown.is_markdown = false;
        FilesRepository::insert(&connection, &non_markdown).expect("insert text");

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: Vec::new(),
            filters: Vec::new(),
            sorts: Vec::new(),
            columns: vec![BaseColumnConfig {
                key: "path".to_string(),
                label: None,
                width: None,
                hidden: false,
            }],
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
            limit: 10,
            offset: 0,
            property_queries: Vec::new(),
        };

        let page = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect("execute table plan");
        assert_eq!(page.total, 1);
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].file_path, "notes/projects/alpha.md");
    }

    #[test]
    fn base_table_executor_exposes_file_extension_builtin() {
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
        .expect("insert markdown");

        let plan = TableQueryPlan {
            view_name: "Projects".to_string(),
            source_prefix: Some("notes/projects".to_string()),
            required_property_keys: vec!["file_ext".to_string()],
            filters: vec![BaseFilterClause {
                key: "file_ext".to_string(),
                op: BaseFilterOp::Eq,
                value: serde_json::json!("md"),
            }],
            sorts: Vec::new(),
            columns: vec![BaseColumnConfig {
                key: "file_ext".to_string(),
                label: None,
                width: None,
                hidden: false,
            }],
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
            limit: 10,
            offset: 0,
            property_queries: Vec::new(),
        };

        let page = BaseTableExecutorService
            .execute(&connection, &plan)
            .expect("execute table plan");
        assert_eq!(page.total, 1);
        assert_eq!(
            page.rows[0].values.get("file_ext"),
            Some(&serde_json::json!("md"))
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
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
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
                group_by: Vec::new(),
                aggregates: Vec::new(),
                relations: Vec::new(),
                rollups: Vec::new(),
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
                group_by: Vec::new(),
                aggregates: Vec::new(),
                relations: Vec::new(),
                rollups: Vec::new(),
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
                group_by: Vec::new(),
                aggregates: Vec::new(),
                relations: Vec::new(),
                rollups: Vec::new(),
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
            group_by: Vec::new(),
            aggregates: Vec::new(),
            relations: Vec::new(),
            rollups: Vec::new(),
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
