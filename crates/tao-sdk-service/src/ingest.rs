//! Markdown ingest pipeline shell for vault scanning and parsing.

use std::fs;
use std::path::{Path, PathBuf};

use tao_sdk_markdown::{
    MarkdownParseError, MarkdownParseRequest, MarkdownParseResult, MarkdownParser,
};
use tao_sdk_vault::{
    CasePolicy, PathCanonicalizationError, VaultManifestEntry, VaultScanError, VaultScanService,
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
