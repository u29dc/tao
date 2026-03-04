use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use thiserror::Error;
use unicode_normalization::UnicodeNormalization;
use walkdir::WalkDir;

use crate::{CasePolicy, PathCanonicalizationError, PathCanonicalizationService};

/// One file record from a vault scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultManifestEntry {
    /// Canonical absolute path after symlink resolution.
    pub absolute: PathBuf,
    /// Canonical path relative to the vault root.
    pub relative: PathBuf,
    /// UTF-8 NFC normalized relative path with `/` separators.
    pub normalized: String,
    /// Case-policy-aware comparison key.
    pub match_key: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Last modified unix timestamp milliseconds.
    pub modified_unix_ms: i64,
}

/// Deterministic snapshot of files currently present in a vault.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultManifest {
    /// Canonical vault root.
    pub root: PathBuf,
    /// Sorted file entries.
    pub entries: Vec<VaultManifestEntry>,
}

/// Performs full vault filesystem scans and returns normalized manifests.
#[derive(Debug, Clone)]
pub struct VaultScanService {
    canonicalizer: PathCanonicalizationService,
}

impl VaultScanService {
    /// Create a scanner from an already-initialized canonicalization service.
    #[must_use]
    pub fn new(canonicalizer: PathCanonicalizationService) -> Self {
        Self { canonicalizer }
    }

    /// Create a scanner from a vault root and case policy.
    pub fn from_root(
        root: impl AsRef<Path>,
        case_policy: CasePolicy,
    ) -> Result<Self, PathCanonicalizationError> {
        let canonicalizer = PathCanonicalizationService::new(root, case_policy)?;
        Ok(Self::new(canonicalizer))
    }

    /// Return the canonical vault root used for scan operations.
    #[must_use]
    pub fn root(&self) -> &Path {
        self.canonicalizer.root()
    }

    /// Perform a full vault scan and return a deterministic manifest.
    pub fn scan(&self) -> Result<VaultManifest, VaultScanError> {
        let mut entries = Vec::new();
        let root = self.canonicalizer.root().to_path_buf();
        let root_for_filter = root.clone();

        for entry in WalkDir::new(&root)
            .follow_links(false)
            .sort_by_file_name()
            .into_iter()
            .filter_entry(|entry| should_descend(entry.path(), &root_for_filter))
        {
            let entry = entry.map_err(|source| VaultScanError::Walk {
                root: root.clone(),
                source,
            })?;

            if !entry.file_type().is_file() {
                continue;
            }

            let absolute = entry.path().to_path_buf();
            let relative = absolute
                .strip_prefix(&root)
                .map_err(|_| VaultScanError::OutsideRoot {
                    root: root.clone(),
                    path: absolute.clone(),
                })?
                .to_path_buf();
            let normalized = normalize_relative_path(&relative)?;
            let match_key = match self.canonicalizer.case_policy() {
                CasePolicy::Sensitive => normalized.clone(),
                CasePolicy::Insensitive => normalized.to_ascii_lowercase(),
            };
            let metadata = entry
                .metadata()
                .map_err(|source| VaultScanError::Metadata {
                    path: absolute.clone(),
                    source,
                })?;
            let modified_unix_ms = metadata
                .modified()
                .map_err(|source| VaultScanError::ModifiedTime {
                    path: absolute.clone(),
                    source,
                })?
                .duration_since(UNIX_EPOCH)
                .map_err(|source| VaultScanError::InvalidModifiedTime {
                    path: absolute.clone(),
                    source,
                })?
                .as_millis();
            let modified_unix_ms = i64::try_from(modified_unix_ms).map_err(|_| {
                VaultScanError::ModifiedTimeOverflow {
                    path: absolute.clone(),
                    value: modified_unix_ms,
                }
            })?;

            entries.push(VaultManifestEntry {
                absolute,
                relative,
                normalized,
                match_key,
                size_bytes: metadata.len(),
                modified_unix_ms,
            });
        }

        entries.sort_unstable_by(|left, right| {
            left.match_key
                .cmp(&right.match_key)
                .then(left.normalized.cmp(&right.normalized))
        });

        Ok(VaultManifest { root, entries })
    }
}

fn normalize_relative_path(path: &Path) -> Result<String, VaultScanError> {
    let mut segments = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                let value = value
                    .to_str()
                    .ok_or_else(|| VaultScanError::NonUtf8Component {
                        path: path.to_path_buf(),
                    })?;
                segments.push(value.nfc().collect::<String>());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(VaultScanError::InvalidPathComponent {
                    path: path.to_path_buf(),
                });
            }
        }
    }

    Ok(segments.join("/"))
}

fn should_descend(path: &Path, root: &Path) -> bool {
    if path == root {
        return true;
    }

    let Ok(relative) = path.strip_prefix(root) else {
        return true;
    };
    let Some(Component::Normal(first_component)) = relative.components().next() else {
        return true;
    };

    !matches!(
        first_component.to_str(),
        Some(".git" | ".obsidian" | ".tao")
    )
}

/// Errors returned by vault scan operations.
#[derive(Debug, Error)]
pub enum VaultScanError {
    /// Filesystem walk failed.
    #[error("failed to walk vault root '{root}': {source}")]
    Walk {
        /// Canonical vault root.
        root: PathBuf,
        /// Walk error with filesystem context.
        #[source]
        source: walkdir::Error,
    },
    /// Reading file metadata failed while scanning.
    #[error("failed to read metadata for scanned path '{path}': {source}")]
    Metadata {
        /// Path seen during scan.
        path: PathBuf,
        /// Filesystem walk metadata error.
        #[source]
        source: walkdir::Error,
    },
    /// Reading modified time from metadata failed.
    #[error("failed to read modified time for scanned path '{path}': {source}")]
    ModifiedTime {
        /// Path seen during scan.
        path: PathBuf,
        /// IO error from modified time read.
        #[source]
        source: std::io::Error,
    },
    /// Modified time preceded unix epoch.
    #[error("modified time for scanned path '{path}' is before unix epoch: {source}")]
    InvalidModifiedTime {
        /// Path seen during scan.
        path: PathBuf,
        /// System time conversion error.
        #[source]
        source: std::time::SystemTimeError,
    },
    /// Modified time milliseconds exceeded `i64`.
    #[error("modified unix timestamp overflow for scanned path '{path}': {value}")]
    ModifiedTimeOverflow {
        /// Path seen during scan.
        path: PathBuf,
        /// Overflow source value.
        value: u128,
    },
    /// File canonicalization failed.
    #[error("failed to canonicalize scanned path '{path}': {source}")]
    Canonicalize {
        /// Path seen during scan.
        path: PathBuf,
        /// Canonicalization error.
        #[source]
        source: PathCanonicalizationError,
    },
    /// Walk entry path resolved outside canonical vault root.
    #[error("scanned path '{path}' resolved outside vault root '{root}'")]
    OutsideRoot {
        /// Canonical root path.
        root: PathBuf,
        /// Walk entry path.
        path: PathBuf,
    },
    /// Path contains non-utf8 component.
    #[error("scanned path '{path}' contains a non-utf8 path component")]
    NonUtf8Component {
        /// Relative path that failed normalization.
        path: PathBuf,
    },
    /// Path contains unsupported component after root-stripping.
    #[error("scanned path '{path}' contains unsupported path components")]
    InvalidPathComponent {
        /// Relative path that failed normalization.
        path: PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::tempdir;

    use crate::{CasePolicy, VaultScanService};

    #[test]
    fn scan_returns_stable_sorted_manifest_entries() {
        let temp = tempdir().expect("tempdir");

        let assets = temp.path().join("assets");
        let notes_upper = temp.path().join("Notes");
        let journal = temp.path().join("journal");

        fs::create_dir_all(&assets).expect("create assets");
        fs::create_dir_all(&notes_upper).expect("create notes upper");
        fs::create_dir_all(&journal).expect("create journal");

        fs::write(assets.join("image.png"), "img").expect("write image");
        fs::write(notes_upper.join("Daily.md"), "daily").expect("write daily");
        fs::write(journal.join("readme.md"), "readme").expect("write readme");

        let service = VaultScanService::from_root(temp.path(), CasePolicy::Insensitive)
            .expect("create scan service");

        let manifest = service.scan().expect("scan vault");

        let normalized: Vec<String> = manifest
            .entries
            .iter()
            .map(|entry| entry.normalized.clone())
            .collect();

        assert_eq!(
            normalized,
            vec!["assets/image.png", "journal/readme.md", "Notes/Daily.md"]
        );
        assert_eq!(manifest.entries[0].match_key, "assets/image.png");
        assert_eq!(manifest.entries[1].match_key, "journal/readme.md");
        assert_eq!(manifest.entries[2].match_key, "notes/daily.md");
    }

    #[test]
    fn scan_returns_absolute_and_relative_paths() {
        let temp = tempdir().expect("tempdir");
        let note = temp.path().join("note.md");
        fs::write(&note, "hello").expect("write note");

        let service = VaultScanService::from_root(temp.path(), CasePolicy::Sensitive)
            .expect("create scan service");
        let manifest = service.scan().expect("scan vault");

        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(manifest.entries[0].relative, PathBuf::from("note.md"));
        assert_eq!(manifest.entries[0].normalized, "note.md");
        assert_eq!(manifest.entries[0].size_bytes, 5);
        assert!(manifest.entries[0].modified_unix_ms > 0);
        assert_eq!(
            manifest.entries[0].absolute,
            fs::canonicalize(note).expect("canonical note")
        );
    }

    #[test]
    fn scan_excludes_internal_directories() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join(".git")).expect("create .git");
        fs::create_dir_all(temp.path().join(".obsidian")).expect("create .obsidian");
        fs::create_dir_all(temp.path().join(".tao")).expect("create .tao");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes");

        fs::write(temp.path().join(".git/HEAD"), "ref").expect("write .git head");
        fs::write(temp.path().join(".obsidian/app.json"), "{}").expect("write app json");
        fs::write(temp.path().join(".tao/index.sqlite"), "sqlite").expect("write tao sqlite");
        fs::write(temp.path().join("notes/live.md"), "# live").expect("write markdown");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");
        let normalized = manifest
            .entries
            .iter()
            .map(|entry| entry.normalized.as_str())
            .collect::<Vec<_>>();

        assert_eq!(normalized, vec!["notes/live.md"]);
    }
}
