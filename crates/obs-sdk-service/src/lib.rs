//! Service-layer orchestration entrypoints over SDK subsystem crates.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

mod import_export;

pub use import_export::{
    FilesystemImportExportService, ImportExportBoundaryError, ImportExportServiceBoundary,
    TransferExecutionRequest, TransferExecutionResult, TransferFailure, TransferItem,
    TransferItemKind, TransferJobKind, TransferMode, TransferPlan, TransferSummary,
};

use obs_sdk_core::{DomainEvent, DomainEventBus, NoteChangeKind};
use obs_sdk_markdown::{
    MarkdownParseError, MarkdownParseRequest, MarkdownParseResult, MarkdownParser,
};
use obs_sdk_properties::{FrontMatterStatus, TypedPropertyValue, extract_front_matter};
use obs_sdk_storage::{
    FileRecordInput, FilesRepository, PropertiesRepository, PropertyRecordInput,
    StorageTransactionError, with_transaction,
};
use obs_sdk_vault::{
    CasePolicy, FileFingerprintError, FileFingerprintService, PathCanonicalizationError,
    VaultManifestEntry, VaultScanError, VaultScanService,
};
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value as JsonValue;
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
        let absolute = vault_root.join(relative_path);
        if let Some(parent) = absolute.parent() {
            fs::create_dir_all(parent).map_err(|source| NoteCrudError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }

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
        let absolute = vault_root.join(relative_path);
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

        let absolute = vault_root.join(&existing.normalized_path);
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

        let old_absolute = vault_root.join(&existing.normalized_path);
        let new_absolute = vault_root.join(new_relative_path);
        if let Some(parent) = new_absolute.parent() {
            fs::create_dir_all(parent).map_err(|source| NoteCrudError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }

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
        source: obs_sdk_storage::FilesRepositoryError,
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
        source: obs_sdk_storage::FilesRepositoryError,
    },
    /// Properties repository update failed.
    #[error("property repository operation failed: {source}")]
    PropertyRepository {
        /// Properties repository error.
        #[source]
        source: obs_sdk_storage::PropertiesRepositoryError,
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

    use obs_sdk_properties::TypedPropertyValue;
    use obs_sdk_storage::{
        FileRecordInput, FilesRepository, LinkRecordInput, LinksRepository, PropertiesRepository,
        run_migrations,
    };
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::{
        CasePolicy, HealthSnapshotService, MarkdownIngestPipeline, NoteCrudError, NoteCrudService,
        PropertyUpdateService, SdkTransactionCoordinator, StorageWriteService, WatcherStatus,
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
}
