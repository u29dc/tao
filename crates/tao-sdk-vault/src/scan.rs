use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use rayon::prelude::*;
use thiserror::Error;
use walkdir::WalkDir;

use crate::{CasePolicy, PathCanonicalizationError, PathCanonicalizationService};

/// One file record from a vault scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultManifestEntry {
    /// Physical absolute path. Symlinks are excluded and this spelling is retained.
    pub absolute: PathBuf,
    /// Physical path relative to the canonical vault root.
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
        self.scan_from(self.root(), true)
    }

    /// Scan one physical vault-relative directory using root inclusion rules.
    /// A non-recursive scan includes only the immediate regular files.
    pub fn scan_subtree(
        &self,
        folder: &str,
        recursive: bool,
    ) -> Result<VaultManifest, VaultScanError> {
        let physical = self.canonicalizer.physical_path(folder).map_err(|source| {
            VaultScanError::Canonicalize {
                path: self.root().join(folder),
                source,
            }
        })?;
        if !physical.absolute.is_dir() {
            return Err(VaultScanError::Canonicalize {
                path: physical.absolute.clone(),
                source: PathCanonicalizationError::RootNotDirectory {
                    path: physical.absolute,
                },
            });
        }
        self.scan_from(&physical.absolute, recursive)
    }

    fn scan_from(
        &self,
        scan_root: &Path,
        recursive: bool,
    ) -> Result<VaultManifest, VaultScanError> {
        crate::check_index_cancellation()?;
        let cancellation = crate::cancellation::current_cancellation();
        let root = self.canonicalizer.root().to_path_buf();
        let case_policy = self.canonicalizer.case_policy();
        let inclusion = VaultInclusionPolicy::load(&root, case_policy)?;
        let mut discovered_files = Vec::new();

        for entry in WalkDir::new(scan_root)
            .follow_links(false)
            .max_depth(if recursive { usize::MAX } else { 1 })
            .sort_by_file_name()
            .into_iter()
            .filter_entry(|entry| inclusion.includes(entry.path(), entry.file_type().is_dir()))
        {
            crate::check_index_cancellation()?;
            let entry = entry.map_err(|source| VaultScanError::Walk {
                root: root.clone(),
                source,
            })?;

            if !entry.file_type().is_file() {
                continue;
            }

            discovered_files.push(entry.path().to_path_buf());
        }

        let mut entries = discovered_files
            .into_par_iter()
            .map(|absolute| {
                if let Some(context) = &cancellation {
                    context.check()?;
                }
                let canonical = self
                    .canonicalizer
                    .regular_file(&absolute)
                    .map_err(|source| VaultScanError::Canonicalize {
                        path: absolute.clone(),
                        source,
                    })?;
                let metadata =
                    fs::symlink_metadata(&absolute).map_err(|source| VaultScanError::Metadata {
                        path: absolute.clone(),
                        source,
                    })?;
                if !metadata.is_file() {
                    return Err(VaultScanError::Canonicalize {
                        path: absolute.clone(),
                        source: PathCanonicalizationError::NotRegularFile { path: absolute },
                    });
                }
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

                Ok(VaultManifestEntry {
                    absolute,
                    relative: canonical.relative,
                    normalized: canonical.normalized,
                    match_key: canonical.match_key,
                    size_bytes: metadata.len(),
                    modified_unix_ms,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        entries.sort_unstable_by(|left, right| {
            left.match_key
                .cmp(&right.match_key)
                .then(left.normalized.cmp(&right.normalized))
        });

        validate_sorted_identities(&entries)?;

        Ok(VaultManifest { root, entries })
    }
}

fn validate_sorted_identities(entries: &[VaultManifestEntry]) -> Result<(), VaultScanError> {
    for pair in entries.windows(2) {
        if pair[0].match_key == pair[1].match_key {
            return Err(VaultScanError::IdentityCollision {
                match_key: pair[0].match_key.clone(),
                first: pair[0].relative.clone(),
                second: pair[1].relative.clone(),
            });
        }
    }
    Ok(())
}

fn load_taoignore(root: &Path, case_policy: CasePolicy) -> Result<Gitignore, VaultScanError> {
    let taoignore_path = root.join(".taoignore");
    let mut builder = GitignoreBuilder::new(root);
    builder
        .case_insensitive(matches!(case_policy, CasePolicy::Insensitive))
        .map_err(|source| VaultScanError::TaoIgnoreCasePolicy {
            path: taoignore_path.clone(),
            source,
        })?;

    if taoignore_path
        .try_exists()
        .map_err(|source| VaultScanError::TaoIgnoreProbe {
            path: taoignore_path.clone(),
            source,
        })?
        && let Some(source) = builder.add(&taoignore_path)
    {
        return Err(VaultScanError::TaoIgnoreParse {
            path: taoignore_path,
            source,
        });
    }

    builder
        .build()
        .map_err(|source| VaultScanError::TaoIgnoreBuild {
            path: taoignore_path,
            source,
        })
}

/// Shared lexical inclusion policy for inventory scans and filesystem events.
/// Symlink and regular-file checks happen at the point of filesystem access.
#[derive(Debug, Clone)]
pub struct VaultInclusionPolicy {
    root: PathBuf,
    taoignore: Gitignore,
    case_policy: CasePolicy,
}

impl VaultInclusionPolicy {
    /// Load only the vault root `.taoignore`; Git ignore files are not consulted.
    pub fn load(root: &Path, case_policy: CasePolicy) -> Result<Self, VaultScanError> {
        Ok(Self {
            root: root.to_path_buf(),
            taoignore: load_taoignore(root, case_policy)?,
            case_policy,
        })
    }

    /// Whether a path is the root ignore-control file, which watchers must observe.
    #[must_use]
    pub fn is_control_path(&self, path: &Path) -> bool {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        };
        path.strip_prefix(&self.root)
            .is_ok_and(|relative| relative == Path::new(".taoignore"))
    }

    /// Whether a physical or vault-relative path belongs in the inventory.
    /// This does not probe the filesystem, so it also works for deletion events.
    #[must_use]
    pub fn includes(&self, path: &Path, is_dir: bool) -> bool {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        };
        should_include_scan_entry(&path, is_dir, &self.root, &self.taoignore, self.case_policy)
    }
}

fn should_include_scan_entry(
    path: &Path,
    is_dir: bool,
    root: &Path,
    taoignore: &Gitignore,
    case_policy: CasePolicy,
) -> bool {
    if path == root {
        return true;
    }

    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    if relative
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return false;
    }
    let Some(Component::Normal(first_component)) = relative.components().next() else {
        return false;
    };
    let is_reserved = first_component.to_str().is_some_and(|name| {
        [".git", ".obsidian", ".tao"].iter().any(|reserved| {
            if case_policy == CasePolicy::Insensitive {
                name.eq_ignore_ascii_case(reserved)
            } else {
                name == *reserved
            }
        })
    });
    if is_reserved {
        return false;
    }

    if relative.components().count() == 1 && first_component.to_str() == Some(".taoignore") {
        return false;
    }

    !taoignore
        .matched_path_or_any_parents(path, is_dir)
        .is_ignore()
}

/// Errors returned by vault scan operations.
#[derive(Debug, Error)]
pub enum VaultScanError {
    /// Request cancellation stopped the scan before publication.
    #[error(transparent)]
    Cancelled(#[from] crate::OperationCancelled),
    /// Two physical paths cannot be represented by one normalized comparison key.
    #[error("vault path identity collision for '{match_key}': '{first}' and '{second}'")]
    IdentityCollision {
        /// Shared NFC/case-policy comparison key.
        match_key: String,
        /// First physical path.
        first: PathBuf,
        /// Second physical path.
        second: PathBuf,
    },
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
        /// Filesystem metadata read error.
        #[source]
        source: std::io::Error,
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
    /// Applying case policy to `.taoignore` failed.
    #[error("failed to configure .taoignore matching for '{path}': {source}")]
    TaoIgnoreCasePolicy {
        /// Vault-local `.taoignore` path.
        path: PathBuf,
        /// Matcher configuration error.
        #[source]
        source: ignore::Error,
    },
    /// Checking for vault-local `.taoignore` failed.
    #[error("failed to inspect .taoignore at '{path}': {source}")]
    TaoIgnoreProbe {
        /// Vault-local `.taoignore` path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Parsing vault-local `.taoignore` failed.
    #[error("failed to parse .taoignore at '{path}': {source}")]
    TaoIgnoreParse {
        /// Vault-local `.taoignore` path.
        path: PathBuf,
        /// Matcher parse error.
        #[source]
        source: ignore::Error,
    },
    /// Building vault-local `.taoignore` matcher failed.
    #[error("failed to build .taoignore matcher for '{path}': {source}")]
    TaoIgnoreBuild {
        /// Vault-local `.taoignore` path.
        path: PathBuf,
        /// Matcher build error.
        #[source]
        source: ignore::Error,
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

    #[test]
    fn scan_without_taoignore_preserves_regular_files() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("_TMP")).expect("create tmp");
        fs::write(temp.path().join("_TMP/scratch.md"), "# scratch").expect("write scratch");
        fs::write(temp.path().join("note.md"), "# note").expect("write note");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(
            normalized_paths(&manifest),
            vec!["_TMP/scratch.md", "note.md"]
        );
    }

    #[test]
    fn scan_taoignore_allows_comments_blank_lines_and_excludes_control_file() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("_TMP")).expect("create tmp");
        fs::write(temp.path().join(".taoignore"), "\n# scratch\n_TMP/\n").expect("write taoignore");
        fs::write(temp.path().join("_TMP/scratch.md"), "# scratch").expect("write scratch");
        fs::write(temp.path().join("note.md"), "# note").expect("write note");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(normalized_paths(&manifest), vec!["note.md"]);
    }

    #[test]
    fn scan_taoignore_excludes_nested_files_in_ignored_directory() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("_TMP/ctvc/nested")).expect("create scratch");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes");
        fs::write(temp.path().join(".taoignore"), "_TMP/\n").expect("write taoignore");
        fs::write(
            temp.path().join("_TMP/ctvc/nested/floating.md"),
            "# scratch",
        )
        .expect("write floating");
        fs::write(temp.path().join("notes/live.md"), "# live").expect("write live");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(normalized_paths(&manifest), vec!["notes/live.md"]);
    }

    #[test]
    fn scan_taoignore_root_relative_directory_only_matches_root() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join(".tmp")).expect("create root tmp");
        fs::create_dir_all(temp.path().join("nested/.tmp")).expect("create nested tmp");
        fs::write(temp.path().join(".taoignore"), "/.tmp/\n").expect("write taoignore");
        fs::write(temp.path().join(".tmp/root.md"), "# root").expect("write root tmp");
        fs::write(temp.path().join("nested/.tmp/keep.md"), "# keep").expect("write nested tmp");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(normalized_paths(&manifest), vec!["nested/.tmp/keep.md"]);
    }

    #[test]
    fn scan_taoignore_supports_directory_globs() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("work/scratch")).expect("create scratch");
        fs::create_dir_all(temp.path().join("notes/scratch")).expect("create note scratch");
        fs::write(temp.path().join(".taoignore"), "scratch/\n").expect("write taoignore");
        fs::write(temp.path().join("work/scratch/a.md"), "# a").expect("write a");
        fs::write(temp.path().join("notes/scratch/b.md"), "# b").expect("write b");
        fs::write(temp.path().join("notes/live.md"), "# live").expect("write live");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(normalized_paths(&manifest), vec!["notes/live.md"]);
    }

    #[test]
    fn scan_taoignore_supports_negation_for_walked_parents() {
        let temp = tempdir().expect("tempdir");
        fs::write(temp.path().join(".taoignore"), "*.md\n!keep.md\n").expect("write taoignore");
        fs::write(temp.path().join("drop.md"), "# drop").expect("write drop");
        fs::write(temp.path().join("keep.md"), "# keep").expect("write keep");
        fs::write(temp.path().join("asset.pdf"), "pdf").expect("write asset");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(normalized_paths(&manifest), vec!["asset.pdf", "keep.md"]);
    }

    #[test]
    fn scan_taoignore_matching_respects_case_policy() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("_TMP")).expect("create tmp");
        fs::write(temp.path().join(".taoignore"), "_tmp/\n").expect("write taoignore");
        fs::write(temp.path().join("_TMP/scratch.md"), "# scratch").expect("write scratch");

        let sensitive =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        assert_eq!(
            normalized_paths(&sensitive.scan().expect("scan sensitive")),
            vec!["_TMP/scratch.md"]
        );

        let insensitive =
            VaultScanService::from_root(temp.path(), CasePolicy::Insensitive).expect("scanner");
        assert!(normalized_paths(&insensitive.scan().expect("scan insensitive")).is_empty());
    }

    #[test]
    fn scan_builtin_exclusions_are_not_overridden_by_taoignore_negation() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join(".git")).expect("create git");
        fs::create_dir_all(temp.path().join(".obsidian")).expect("create obsidian");
        fs::create_dir_all(temp.path().join(".tao")).expect("create tao");
        fs::write(
            temp.path().join(".taoignore"),
            "!.git/HEAD\n!.obsidian/app.json\n!.tao/index.sqlite\n",
        )
        .expect("write taoignore");
        fs::write(temp.path().join(".git/HEAD"), "ref").expect("write git");
        fs::write(temp.path().join(".obsidian/app.json"), "{}").expect("write obsidian");
        fs::write(temp.path().join(".tao/index.sqlite"), "sqlite").expect("write tao");
        fs::write(temp.path().join("note.md"), "# note").expect("write note");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(normalized_paths(&manifest), vec!["note.md"]);
    }

    #[test]
    fn scan_does_not_respect_gitignore_by_default() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("_TMP")).expect("create tmp");
        fs::write(temp.path().join(".gitignore"), "_TMP/\n").expect("write gitignore");
        fs::write(temp.path().join("_TMP/scratch.md"), "# scratch").expect("write scratch");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(
            normalized_paths(&manifest),
            vec![".gitignore", "_TMP/scratch.md"]
        );
    }

    #[test]
    fn scan_does_not_respect_git_info_exclude_by_default() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join(".git/info")).expect("create git info");
        fs::create_dir_all(temp.path().join("_TMP")).expect("create tmp");
        fs::write(temp.path().join(".git/info/exclude"), "_TMP/\n").expect("write exclude");
        fs::write(temp.path().join("_TMP/scratch.md"), "# scratch").expect("write scratch");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(normalized_paths(&manifest), vec!["_TMP/scratch.md"]);
    }

    #[test]
    fn scan_does_not_respect_global_gitignore_by_default() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        let home = temp.path().join("home");
        let xdg = temp.path().join("xdg");
        fs::create_dir_all(vault.join("_GLOBAL")).expect("create global dir");
        fs::create_dir_all(xdg.join("git")).expect("create xdg git");
        fs::create_dir_all(home.join(".config/git")).expect("create home git config");
        fs::write(vault.join("_GLOBAL/scratch.md"), "# scratch").expect("write scratch");
        fs::write(xdg.join("git/ignore"), "_GLOBAL/\n").expect("write xdg global ignore");
        fs::write(home.join(".config/git/ignore"), "_GLOBAL/\n").expect("write home global ignore");

        let status = std::process::Command::new(std::env::current_exe().expect("current exe"))
            .arg("scan_global_gitignore_helper")
            .arg("--nocapture")
            .env("TAO_SCAN_GLOBAL_IGNORE_HELPER", "1")
            .env("TAO_SCAN_GLOBAL_IGNORE_VAULT", &vault)
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &xdg)
            .status()
            .expect("run helper test");

        assert!(status.success(), "global gitignore helper failed");
    }

    #[test]
    fn scan_global_gitignore_helper() {
        if std::env::var_os("TAO_SCAN_GLOBAL_IGNORE_HELPER").is_none() {
            return;
        }
        let vault = std::env::var_os("TAO_SCAN_GLOBAL_IGNORE_VAULT")
            .map(PathBuf::from)
            .expect("helper vault env");

        let service = VaultScanService::from_root(vault, CasePolicy::Sensitive).expect("scanner");
        let manifest = service.scan().expect("scan");

        assert_eq!(normalized_paths(&manifest), vec!["_GLOBAL/scratch.md"]);
    }

    #[test]
    fn scan_surfaces_malformed_taoignore_patterns() {
        let temp = tempdir().expect("tempdir");
        fs::write(temp.path().join(".taoignore"), "{foo,bar\n").expect("write taoignore");

        let service =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let error = service
            .scan()
            .expect_err("scan should reject malformed pattern");

        assert!(error.to_string().contains("failed to parse .taoignore"));
        assert!(error.to_string().contains("{foo,bar"));
    }

    #[test]
    fn scan_fingerprint_and_capture_share_unicode_keys_and_physical_paths() {
        let temp = tempdir().expect("tempdir");
        let physical_name = "A\u{308}pfel.MD";
        fs::write(temp.path().join(physical_name), "# Apple").expect("write");
        let scanner =
            VaultScanService::from_root(temp.path(), CasePolicy::Insensitive).expect("scanner");
        let manifest = scanner.scan().expect("scan");
        let entry = &manifest.entries[0];
        assert_eq!(entry.normalized, "Äpfel.MD");
        assert_eq!(entry.match_key, "äpfel.md");
        assert_eq!(
            fs::read_to_string(&entry.absolute).expect("physical read"),
            "# Apple"
        );
        let service =
            crate::FileFingerprintService::from_root(temp.path(), CasePolicy::Insensitive)
                .expect("service");
        let captured = service
            .capture(&entry.absolute)
            .expect("capture physical path");
        assert_eq!(entry.relative, captured.fingerprint.relative);
        assert_eq!(entry.match_key, captured.fingerprint.match_key);
    }

    #[test]
    fn scan_reports_comparison_collisions_when_physical_names_can_coexist() {
        let temp = tempdir().expect("tempdir");
        fs::write(temp.path().join("Äpfel.md"), "# Upper").expect("upper");
        fs::write(temp.path().join("äpfel.md"), "# Lower").expect("lower");
        if fs::read_dir(temp.path()).expect("entries").count() != 2 {
            return; // Case-insensitive filesystems cannot construct this collision.
        }
        let scanner =
            VaultScanService::from_root(temp.path(), CasePolicy::Insensitive).expect("scanner");
        assert!(matches!(
            scanner.scan(),
            Err(super::VaultScanError::IdentityCollision { .. })
        ));
        assert_eq!(
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive)
                .expect("scanner")
                .scan()
                .expect("sensitive")
                .entries
                .len(),
            2
        );
    }

    #[test]
    fn collision_diagnostic_retains_distinct_physical_spellings() {
        let temp = tempdir().expect("tempdir");
        fs::write(temp.path().join("Café.md"), "# Note").expect("note");
        let scanner =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        let mut entries = scanner.scan().expect("scan").entries;
        // Exercise NFC collisions even on filesystems which cannot create both
        // physical spellings. Production input is the sorted scanner inventory.
        entries[0].relative = PathBuf::from("Café.md");
        let mut decomposed = entries[0].clone();
        decomposed.relative = PathBuf::from("Cafe\u{301}.md");
        entries.push(decomposed);
        let error = super::validate_sorted_identities(&entries).expect_err("NFC collision");
        match error {
            super::VaultScanError::IdentityCollision {
                first,
                second,
                match_key,
            } => {
                assert_ne!(first, second);
                assert_eq!(match_key, "Café.md");
            }
            other => panic!("unexpected diagnostic: {other}"),
        }
    }

    #[test]
    fn scoped_scan_preserves_root_exclusions_and_recursion_boundary() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes/nested")).expect("mkdir");
        fs::write(temp.path().join("notes/a.MD"), "# A").expect("a");
        fs::write(temp.path().join("notes/nested/b.md"), "# B").expect("b");
        fs::write(temp.path().join("notes/excluded.md"), "# Excluded").expect("excluded");
        fs::write(temp.path().join(".taoignore"), "notes/excluded.md\n").expect("ignore");
        let scanner =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        assert_eq!(
            normalized_paths(&scanner.scan_subtree("notes", false).expect("shallow")),
            ["notes/a.MD"]
        );
        assert_eq!(
            normalized_paths(&scanner.scan_subtree("notes", true).expect("recursive")),
            ["notes/a.MD", "notes/nested/b.md"]
        );
        assert!(scanner.scan_subtree("../", true).is_err());
        assert!(scanner.scan_subtree("notes/a.MD", true).is_err());
        let policy = super::VaultInclusionPolicy::load(scanner.root(), CasePolicy::Sensitive)
            .expect("policy");
        assert!(!policy.includes(std::path::Path::new("../outside.md"), false));
        assert!(!policy.includes(std::path::Path::new("/outside.md"), false));
        assert!(policy.is_control_path(std::path::Path::new(".taoignore")));
    }

    #[cfg(unix)]
    #[test]
    fn scanner_and_scoped_scan_exclude_symlinks() {
        use std::os::unix::fs::symlink;
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("mkdir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write");
        symlink(temp.path().join("notes/a.md"), temp.path().join("alias.md")).expect("file alias");
        symlink(temp.path().join("notes"), temp.path().join("alias")).expect("directory alias");
        let scanner =
            VaultScanService::from_root(temp.path(), CasePolicy::Sensitive).expect("scanner");
        assert_eq!(
            normalized_paths(&scanner.scan().expect("scan")),
            ["notes/a.md"]
        );
        assert!(scanner.scan_subtree("alias", true).is_err());
    }

    fn normalized_paths(manifest: &crate::VaultManifest) -> Vec<&str> {
        manifest
            .entries
            .iter()
            .map(|entry| entry.normalized.as_str())
            .collect::<Vec<_>>()
    }
}
