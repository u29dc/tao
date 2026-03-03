use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use thiserror::Error;

use crate::{CasePolicy, PathCanonicalizationError, PathCanonicalizationService};

const HASH_BUFFER_BYTES: usize = 64 * 1024;

/// File fingerprint metadata used by incremental indexing workflows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileFingerprint {
    /// Canonical absolute path.
    pub absolute: PathBuf,
    /// Canonical path relative to vault root.
    pub relative: PathBuf,
    /// NFC normalized relative path with `/` separators.
    pub normalized: String,
    /// Case policy aware comparison key.
    pub match_key: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Last modification timestamp in unix epoch milliseconds.
    pub modified_unix_ms: u128,
    /// BLAKE3 content hash in lowercase hex.
    pub hash_blake3: String,
}

/// Computes file fingerprints from canonicalized vault paths.
#[derive(Debug, Clone)]
pub struct FileFingerprintService {
    canonicalizer: PathCanonicalizationService,
}

impl FileFingerprintService {
    /// Create a fingerprint service from an existing canonicalization service.
    #[must_use]
    pub fn new(canonicalizer: PathCanonicalizationService) -> Self {
        Self { canonicalizer }
    }

    /// Create a fingerprint service from a vault root and case policy.
    pub fn from_root(
        root: impl AsRef<Path>,
        case_policy: CasePolicy,
    ) -> Result<Self, PathCanonicalizationError> {
        let canonicalizer = PathCanonicalizationService::new(root, case_policy)?;
        Ok(Self::new(canonicalizer))
    }

    /// Return the canonical vault root used for fingerprint operations.
    #[must_use]
    pub fn root(&self) -> &Path {
        self.canonicalizer.root()
    }

    /// Compute `size + modified + hash` for a vault file path.
    pub fn fingerprint(
        &self,
        input: impl AsRef<Path>,
    ) -> Result<FileFingerprint, FileFingerprintError> {
        let canonical = self
            .canonicalizer
            .canonicalize(input)
            .map_err(|source| FileFingerprintError::Canonicalize { source })?;

        let metadata =
            fs::metadata(&canonical.absolute).map_err(|source| FileFingerprintError::Metadata {
                path: canonical.absolute.clone(),
                source,
            })?;

        let modified =
            metadata
                .modified()
                .map_err(|source| FileFingerprintError::ModifiedTime {
                    path: canonical.absolute.clone(),
                    source,
                })?;
        let modified_unix_ms = modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|source| FileFingerprintError::InvalidModifiedTime {
                path: canonical.absolute.clone(),
                source,
            })?
            .as_millis();

        let hash_blake3 = hash_file(&canonical.absolute)?;

        Ok(FileFingerprint {
            absolute: canonical.absolute,
            relative: canonical.relative,
            normalized: canonical.normalized,
            match_key: canonical.match_key,
            size_bytes: metadata.len(),
            modified_unix_ms,
            hash_blake3,
        })
    }
}

fn hash_file(path: &Path) -> Result<String, FileFingerprintError> {
    let file = File::open(path).map_err(|source| FileFingerprintError::Open {
        path: path.to_path_buf(),
        source,
    })?;

    let mut reader = BufReader::with_capacity(HASH_BUFFER_BYTES, file);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];

    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| FileFingerprintError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

/// Errors returned by fingerprint operations.
#[derive(Debug, Error)]
pub enum FileFingerprintError {
    /// Input path canonicalization failed.
    #[error("failed to canonicalize file path: {source}")]
    Canonicalize {
        /// Canonicalization error context.
        #[source]
        source: PathCanonicalizationError,
    },
    /// File metadata retrieval failed.
    #[error("failed to read metadata for '{path}': {source}")]
    Metadata {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Reading modified timestamp failed.
    #[error("failed to read modified time for '{path}': {source}")]
    ModifiedTime {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Modified time is before unix epoch.
    #[error("modified time for '{path}' is before unix epoch: {source}")]
    InvalidModifiedTime {
        /// File path.
        path: PathBuf,
        /// Time conversion error.
        #[source]
        source: std::time::SystemTimeError,
    },
    /// Opening file for hashing failed.
    #[error("failed to open '{path}' for hashing: {source}")]
    Open {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Reading file bytes for hashing failed.
    #[error("failed to read '{path}' for hashing: {source}")]
    Read {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::thread;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::{CasePolicy, FileFingerprintService};

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    #[test]
    fn fingerprint_includes_size_modified_and_hash() {
        let temp = tempdir().expect("tempdir");
        let note_path = temp.path().join("note.md");
        fs::write(&note_path, "hello").expect("write note");

        let service = FileFingerprintService::from_root(temp.path(), CasePolicy::Sensitive)
            .expect("create fingerprint service");
        let fingerprint = service
            .fingerprint(PathBuf::from("note.md"))
            .expect("fingerprint");

        assert_eq!(fingerprint.relative, PathBuf::from("note.md"));
        assert_eq!(fingerprint.size_bytes, 5);
        assert!(fingerprint.modified_unix_ms > 0);
        assert_eq!(
            fingerprint.hash_blake3,
            blake3::hash(b"hello").to_hex().to_string()
        );
    }

    #[test]
    fn fingerprint_changes_after_content_update() {
        let temp = tempdir().expect("tempdir");
        let note_path = temp.path().join("note.md");
        fs::write(&note_path, "hello").expect("write note");

        let service = FileFingerprintService::from_root(temp.path(), CasePolicy::Sensitive)
            .expect("create fingerprint service");
        let before = service
            .fingerprint(PathBuf::from("note.md"))
            .expect("fingerprint before");

        thread::sleep(Duration::from_millis(2));
        fs::write(&note_path, "hello world").expect("update note");

        let after = service
            .fingerprint(PathBuf::from("note.md"))
            .expect("fingerprint after");

        assert_ne!(before.hash_blake3, after.hash_blake3);
        assert_ne!(before.size_bytes, after.size_bytes);
        assert!(after.modified_unix_ms >= before.modified_unix_ms);
    }

    #[cfg(unix)]
    #[test]
    fn fingerprint_resolves_symlink_input_to_canonical_target() {
        let temp = tempdir().expect("tempdir");
        let notes_dir = temp.path().join("notes");
        fs::create_dir_all(&notes_dir).expect("create notes dir");

        let note_path = notes_dir.join("note.md");
        fs::write(&note_path, "hello").expect("write note");

        let alias = temp.path().join("alias.md");
        symlink(&note_path, &alias).expect("create symlink");

        let service = FileFingerprintService::from_root(temp.path(), CasePolicy::Sensitive)
            .expect("create fingerprint service");
        let fingerprint = service
            .fingerprint(PathBuf::from("alias.md"))
            .expect("fingerprint");

        assert_eq!(fingerprint.relative, PathBuf::from("notes/note.md"));
        assert_eq!(fingerprint.normalized, "notes/note.md");
    }
}
