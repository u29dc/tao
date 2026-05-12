use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use rayon::prelude::*;
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
        let root = self.canonicalizer.root().to_path_buf();
        let root_for_filter = root.clone();
        let case_policy = self.canonicalizer.case_policy();
        let taoignore = load_taoignore(&root, case_policy)?;
        let mut discovered_files = Vec::new();

        for entry in WalkDir::new(&root)
            .follow_links(false)
            .sort_by_file_name()
            .into_iter()
            .filter_entry(|entry| {
                should_include_scan_entry(
                    entry.path(),
                    entry.file_type().is_dir(),
                    &root_for_filter,
                    &taoignore,
                )
            })
        {
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
                let relative = absolute
                    .strip_prefix(&root)
                    .map_err(|_| VaultScanError::OutsideRoot {
                        root: root.clone(),
                        path: absolute.clone(),
                    })?
                    .to_path_buf();
                let normalized = normalize_relative_path(&relative)?;
                let match_key = match case_policy {
                    CasePolicy::Sensitive => normalized.clone(),
                    CasePolicy::Insensitive => normalized.to_ascii_lowercase(),
                };
                let metadata =
                    fs::metadata(&absolute).map_err(|source| VaultScanError::Metadata {
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

                Ok(VaultManifestEntry {
                    absolute,
                    relative,
                    normalized,
                    match_key,
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

fn should_include_scan_entry(
    path: &Path,
    is_dir: bool,
    root: &Path,
    taoignore: &Gitignore,
) -> bool {
    if path == root {
        return true;
    }

    let Ok(relative) = path.strip_prefix(root) else {
        return true;
    };
    let Some(Component::Normal(first_component)) = relative.components().next() else {
        return true;
    };

    if matches!(
        first_component.to_str(),
        Some(".git" | ".obsidian" | ".tao")
    ) {
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

    fn normalized_paths(manifest: &crate::VaultManifest) -> Vec<&str> {
        manifest
            .entries
            .iter()
            .map(|entry| entry.normalized.as_str())
            .collect::<Vec<_>>()
    }
}
