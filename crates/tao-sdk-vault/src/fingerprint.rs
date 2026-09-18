use std::fs::{self, Metadata, OpenOptions};
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use thiserror::Error;

use crate::{
    CanonicalPath, CasePolicy, PathCanonicalizationError, PathCanonicalizationService, file_kind,
};

const HASH_BUFFER_BYTES: usize = 64 * 1024;
const MAX_CAPTURE_ATTEMPTS: usize = 3;
/// Maximum in-memory source capture used by [`FileFingerprintService::capture`].
pub const DEFAULT_CAPTURE_LIMIT_BYTES: u64 = 32 * 1024 * 1024;
/// Revision of the policy that leaves inventory-only assets unhashed.
pub const FINGERPRINT_POLICY_VERSION: u32 = 2;

/// Observed metadata. This is a scheduling hint, not a verified content revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    /// Physical absolute path, never reconstructed from a normalized key.
    pub absolute: PathBuf,
    /// Physical path relative to the vault root.
    pub relative: PathBuf,
    /// NFC normalized display path.
    pub normalized: String,
    /// Case-policy-aware comparison key.
    pub match_key: String,
    /// Observed file length in bytes.
    pub size_bytes: u64,
    /// Observed last-modification time in Unix milliseconds.
    pub modified_unix_ms: u128,
}

/// File fingerprint metadata used by incremental indexing workflows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileFingerprint {
    /// Physical absolute path.
    pub absolute: PathBuf,
    /// Physical path relative to vault root.
    pub relative: PathBuf,
    /// NFC normalized relative path with `/` separators.
    pub normalized: String,
    /// Case policy aware comparison key.
    pub match_key: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Last modification timestamp in unix epoch milliseconds.
    pub modified_unix_ms: u128,
    /// BLAKE3 content hash, or the empty string when explicitly uncomputed.
    /// Inventory-only assets (including PDFs awaiting extraction) are not read.
    pub hash_blake3: String,
}

impl FileMetadata {
    fn with_hash(self, hash_blake3: String) -> FileFingerprint {
        FileFingerprint {
            absolute: self.absolute,
            relative: self.relative,
            normalized: self.normalized,
            match_key: self.match_key,
            size_bytes: self.size_bytes,
            modified_unix_ms: self.modified_unix_ms,
            hash_blake3,
        }
    }
}

/// One stable, bounded source capture. All projections should consume these bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedFile {
    /// Identity, observed metadata and digest of exactly `bytes`.
    pub fingerprint: FileFingerprint,
    /// Source bytes captured from the verified open file.
    pub bytes: Vec<u8>,
}

/// Computes file fingerprints from included regular vault paths.
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
        Ok(Self::new(PathCanonicalizationService::new(
            root,
            case_policy,
        )?))
    }

    /// Return the canonical vault root used for fingerprint operations.
    #[must_use]
    pub fn root(&self) -> &Path {
        self.canonicalizer.root()
    }

    /// Observe regular-file identity and metadata without opening the content.
    pub fn metadata(&self, input: impl AsRef<Path>) -> Result<FileMetadata, FileFingerprintError> {
        let canonical = self.regular_file(input.as_ref())?;
        let metadata = path_metadata(&canonical.absolute)?;
        metadata_value(canonical, &metadata)
    }

    /// Verify Markdown, Base and plain-text content with a stable streaming read.
    /// Other formats are metadata-only; their hash is explicitly uncomputed.
    pub fn fingerprint(
        &self,
        input: impl AsRef<Path>,
    ) -> Result<FileFingerprint, FileFingerprintError> {
        if !file_kind(input.as_ref()).is_text() {
            return Ok(self.metadata(input)?.with_hash(String::new()));
        }
        Ok(self
            .read_verified(input.as_ref(), None, &mut || {})?
            .fingerprint)
    }

    /// Capture one stable source revision, bounded to 32 MiB by default.
    /// Explicit extraction callers may use [`Self::capture_with_limit`].
    pub fn capture(&self, input: impl AsRef<Path>) -> Result<CapturedFile, FileFingerprintError> {
        self.capture_with_limit(input, DEFAULT_CAPTURE_LIMIT_BYTES)
    }

    /// Capture exactly one stable revision, rejecting data beyond `max_bytes`.
    pub fn capture_with_limit(
        &self,
        input: impl AsRef<Path>,
        max_bytes: u64,
    ) -> Result<CapturedFile, FileFingerprintError> {
        self.read_verified(input.as_ref(), Some(max_bytes), &mut || {})
    }

    fn regular_file(&self, input: &Path) -> Result<CanonicalPath, FileFingerprintError> {
        self.canonicalizer
            .regular_file(input)
            .map_err(|source| FileFingerprintError::Canonicalize { source })
    }

    fn read_verified(
        &self,
        input: &Path,
        capture_limit: Option<u64>,
        after_read: &mut impl FnMut(),
    ) -> Result<CapturedFile, FileFingerprintError> {
        for _ in 0..MAX_CAPTURE_ATTEMPTS {
            crate::check_index_cancellation()?;
            let canonical = self.regular_file(input)?;
            let path = &canonical.absolute;
            let before = path_metadata(path)?;
            if let Some(limit) = capture_limit {
                check_limit(path, before.len(), limit)?;
            }
            let mut options = OpenOptions::new();
            options.read(true);
            #[cfg(unix)]
            options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
            let mut file = options
                .open(path)
                .map_err(|source| FileFingerprintError::Open {
                    path: path.clone(),
                    source,
                })?;
            let opened = file
                .metadata()
                .map_err(|source| FileFingerprintError::Metadata {
                    path: path.clone(),
                    source,
                })?;
            // Validate the path again after opening, before consuming any bytes.
            // On Unix, device/inode checks also detect path replacement.
            self.regular_file(path)?;
            if !same_revision(&before, &opened) || !same_revision(&opened, &path_metadata(path)?) {
                continue;
            }
            let mut hasher = blake3::Hasher::new();
            let mut bytes = Vec::new();
            let mut bytes_read = 0_u64;
            let mut buffer = [0_u8; HASH_BUFFER_BYTES];
            loop {
                crate::check_index_cancellation()?;
                let read = file
                    .read(&mut buffer)
                    .map_err(|source| FileFingerprintError::Read {
                        path: path.clone(),
                        source,
                    })?;
                if read == 0 {
                    break;
                }
                bytes_read = bytes_read.saturating_add(read as u64);
                if let Some(limit) = capture_limit {
                    check_limit(path, bytes_read, limit)?;
                    bytes.extend_from_slice(&buffer[..read]);
                }
                hasher.update(&buffer[..read]);
            }
            after_read();
            crate::check_index_cancellation()?;
            let after = file
                .metadata()
                .map_err(|source| FileFingerprintError::Metadata {
                    path: path.clone(),
                    source,
                })?;
            let latest = self.regular_file(path)?;
            let at_path = path_metadata(path)?;
            if latest.absolute != canonical.absolute
                || bytes_read != after.len()
                || !same_revision(&opened, &after)
                || !same_revision(&after, &at_path)
            {
                continue;
            }
            let fingerprint = metadata_value(canonical, &after)?
                .with_hash(hasher.finalize().to_hex().to_string());
            return Ok(CapturedFile { fingerprint, bytes });
        }
        Err(FileFingerprintError::UnstableSource {
            path: input.to_path_buf(),
            attempts: MAX_CAPTURE_ATTEMPTS,
        })
    }
}

fn path_metadata(path: &Path) -> Result<Metadata, FileFingerprintError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| FileFingerprintError::Metadata {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(FileFingerprintError::Canonicalize {
            source: PathCanonicalizationError::NotRegularFile {
                path: path.to_path_buf(),
            },
        });
    }
    Ok(metadata)
}

fn metadata_value(
    canonical: CanonicalPath,
    metadata: &Metadata,
) -> Result<FileMetadata, FileFingerprintError> {
    let modified = metadata
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
    Ok(FileMetadata {
        absolute: canonical.absolute,
        relative: canonical.relative,
        normalized: canonical.normalized,
        match_key: canonical.match_key,
        size_bytes: metadata.len(),
        modified_unix_ms,
    })
}

fn same_revision(left: &Metadata, right: &Metadata) -> bool {
    let common = left.is_file()
        && right.is_file()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        common
            && left.dev() == right.dev()
            && left.ino() == right.ino()
            && left.ctime() == right.ctime()
            && left.ctime_nsec() == right.ctime_nsec()
    }
    #[cfg(not(unix))]
    {
        common && left.created().ok() == right.created().ok()
    }
}

fn check_limit(path: &Path, size_bytes: u64, limit_bytes: u64) -> Result<(), FileFingerprintError> {
    if size_bytes > limit_bytes {
        return Err(FileFingerprintError::CaptureTooLarge {
            path: path.to_path_buf(),
            size_bytes,
            limit_bytes,
        });
    }
    Ok(())
}

/// Errors returned by fingerprint and stable capture operations.
#[derive(Debug, Error)]
pub enum FileFingerprintError {
    /// Request cancellation stopped source capture.
    #[error(transparent)]
    Cancelled(#[from] crate::OperationCancelled),
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
    #[error("failed to open '{path}' for content capture: {source}")]
    Open {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Reading file bytes failed.
    #[error("failed to read '{path}' for content capture: {source}")]
    Read {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Concurrent changes prevented a coherent source capture.
    #[error("source '{path}' remained unstable after {attempts} capture attempts")]
    UnstableSource {
        /// Source path.
        path: PathBuf,
        /// Number of bounded attempts.
        attempts: usize,
    },
    /// The content exceeds the explicit in-memory capture bound.
    #[error("source '{path}' exceeds capture limit {limit_bytes} bytes (observed {size_bytes})")]
    CaptureTooLarge {
        /// Source path.
        path: PathBuf,
        /// Observed source size.
        size_bytes: u64,
        /// Configured upper bound.
        limit_bytes: u64,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::thread;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::{CasePolicy, FileFingerprintError, FileFingerprintService};

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
    fn fingerprint_excludes_symlinks_like_inventory_scanning() {
        let temp = tempdir().expect("tempdir");
        let notes_dir = temp.path().join("notes");
        fs::create_dir_all(&notes_dir).expect("create notes dir");

        let note_path = notes_dir.join("note.md");
        fs::write(&note_path, "hello").expect("write note");

        let alias = temp.path().join("alias.md");
        symlink(&note_path, &alias).expect("create symlink");

        let service = FileFingerprintService::from_root(temp.path(), CasePolicy::Sensitive)
            .expect("create fingerprint service");
        let error = service
            .fingerprint(PathBuf::from("alias.md"))
            .expect_err("symlink aliases are not inventory files");
        assert!(matches!(
            error,
            FileFingerprintError::Canonicalize {
                source: crate::PathCanonicalizationError::SymlinkNotIncluded { .. }
            }
        ));
        symlink(&notes_dir, temp.path().join("alias-dir")).expect("directory symlink");
        assert!(service.capture("alias-dir/note.md").is_err());
    }

    #[test]
    fn inventory_assets_do_not_read_or_hash_large_sparse_bodies() {
        let temp = tempdir().expect("tempdir");
        let file = fs::File::create(temp.path().join("large.bin")).expect("sparse file");
        file.set_len(1_u64 << 40).expect("logical length");
        let service =
            FileFingerprintService::from_root(temp.path(), CasePolicy::Sensitive).expect("service");
        let fingerprint = service
            .fingerprint("large.bin")
            .expect("metadata fingerprint");
        assert_eq!(fingerprint.size_bytes, 1_u64 << 40);
        assert!(fingerprint.hash_blake3.is_empty());
        assert_eq!(
            service.metadata("large.bin").expect("metadata").size_bytes,
            1_u64 << 40
        );
    }

    #[test]
    fn capture_hash_and_bytes_identify_the_same_revision() {
        let temp = tempdir().expect("tempdir");
        let source = b"# Header\r\nUTF-8: \xc3\xa4\r\n";
        fs::write(temp.path().join("note.MD"), source).expect("write");
        let service = FileFingerprintService::from_root(temp.path(), CasePolicy::Insensitive)
            .expect("service");
        let captured = service.capture("note.MD").expect("capture");
        assert_eq!(captured.bytes, source);
        assert_eq!(
            captured.fingerprint.hash_blake3,
            blake3::hash(&captured.bytes).to_hex().to_string()
        );
        assert_eq!(captured.fingerprint.size_bytes, source.len() as u64);
        assert_eq!(captured.fingerprint.match_key, "note.md");
        assert!(matches!(
            service.capture_with_limit("note.MD", 2),
            Err(FileFingerprintError::CaptureTooLarge { limit_bytes: 2, .. })
        ));
        assert!(service.capture(".").is_err());
    }

    #[test]
    fn capture_retries_replaced_source_and_rejects_continual_changes() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("note.md");
        fs::write(&path, b"original").expect("write");
        let service =
            FileFingerprintService::from_root(temp.path(), CasePolicy::Sensitive).expect("service");
        let mut changes = 0;
        let captured = service
            .read_verified(Path::new("note.md"), Some(1024), &mut || {
                if changes == 0 {
                    let replacement = temp.path().join("replacement.md");
                    fs::write(&replacement, b"replacement").expect("replacement");
                    fs::rename(replacement, &path).expect("replace source");
                }
                changes += 1;
            })
            .expect("stabilized capture");
        assert_eq!(changes, 2);
        assert_eq!(captured.bytes, b"replacement");
        let error = service
            .read_verified(Path::new("note.md"), Some(1024), &mut || {
                changes += 1;
                fs::write(&path, vec![b'x'; changes]).expect("concurrent change");
            })
            .expect_err("unstable capture");
        assert!(matches!(
            error,
            FileFingerprintError::UnstableSource { attempts: 3, .. }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn capture_revalidates_boundary_after_source_replacement() {
        let temp = tempdir().expect("tempdir");
        let outside = tempdir().expect("outside");
        let path = temp.path().join("note.md");
        let external = outside.path().join("external.md");
        fs::write(&path, b"permitted").expect("source");
        fs::write(&external, b"outside").expect("outside source");
        let service =
            FileFingerprintService::from_root(temp.path(), CasePolicy::Sensitive).expect("service");
        let error = service
            .read_verified(Path::new("note.md"), Some(1024), &mut || {
                fs::remove_file(&path).expect("replace file");
                symlink(&external, &path).expect("outside symlink");
            })
            .expect_err("replaced path cannot be published");
        assert!(matches!(
            error,
            FileFingerprintError::Canonicalize {
                source: crate::PathCanonicalizationError::SymlinkNotIncluded { .. }
            }
        ));
    }
}
