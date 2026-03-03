//! Service-layer orchestration entrypoints over SDK subsystem crates.

use std::fs;
use std::path::{Path, PathBuf};

use obs_sdk_markdown::{
    MarkdownParseError, MarkdownParseRequest, MarkdownParseResult, MarkdownParser,
};
use obs_sdk_storage::{FileRecordInput, StorageTransactionError, with_transaction};
use obs_sdk_vault::{
    CasePolicy, PathCanonicalizationError, VaultManifestEntry, VaultScanError, VaultScanService,
};
use rusqlite::Connection;
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
        file_id: &str,
        replacement: &FileRecordInput,
    ) -> Result<(), SdkTransactionError> {
        with_transaction(connection, |transaction| {
            let _ = transaction.files_delete_by_id(file_id)?;
            transaction.files_insert(replacement)?;
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

    use obs_sdk_storage::{FileRecordInput, FilesRepository, run_migrations};
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::{
        CasePolicy, MarkdownIngestPipeline, SdkTransactionCoordinator, StorageWriteService,
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
}
