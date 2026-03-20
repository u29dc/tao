//! Note create/update/delete service.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use tao_sdk_core::{DomainEvent, DomainEventBus, NoteChangeKind};
use tao_sdk_storage::{FileRecordInput, FilesRepository};
use tao_sdk_vault::{
    CasePolicy, FileFingerprintError, FileFingerprintService, PathCanonicalizationError,
};
use thiserror::Error;

use super::{SdkTransactionCoordinator, SdkTransactionError, ServiceTraceContext};

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
