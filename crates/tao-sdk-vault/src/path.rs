use std::fs;
use std::path::{Component, Path, PathBuf};

use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

/// Controls how canonicalized paths are compared for matching and indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasePolicy {
    /// Preserve path casing for comparisons.
    Sensitive,
    /// Lowercase canonical paths for comparisons.
    Insensitive,
}

/// Canonicalized path metadata used by index and resolver services.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalPath {
    /// Canonical absolute path after symlink resolution.
    pub absolute: PathBuf,
    /// Canonical path relative to the vault root.
    pub relative: PathBuf,
    /// UTF-8 NFC normalized relative path with `/` separators.
    pub normalized: String,
    /// Deterministic key used for case policy aware comparisons.
    pub match_key: String,
}

/// Validate one vault-relative path string before filesystem access.
pub fn validate_relative_vault_path(input: &str) -> Result<(), RelativeVaultPathError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(RelativeVaultPathError::Empty);
    }

    let path = Path::new(trimmed);
    if path.is_absolute() {
        return Err(RelativeVaultPathError::AbsolutePath {
            path: trimmed.to_string(),
        });
    }

    let mut saw_component = false;
    for component in path.components() {
        match component {
            Component::Normal(_) => saw_component = true,
            Component::CurDir => {
                return Err(RelativeVaultPathError::CurrentDirectoryComponent {
                    path: trimmed.to_string(),
                });
            }
            Component::ParentDir => {
                return Err(RelativeVaultPathError::ParentTraversal {
                    path: trimmed.to_string(),
                });
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(RelativeVaultPathError::AbsolutePath {
                    path: trimmed.to_string(),
                });
            }
        }
    }

    if !saw_component {
        return Err(RelativeVaultPathError::Empty);
    }

    Ok(())
}

/// Canonicalizes vault paths and enforces vault boundary rules.
#[derive(Debug, Clone)]
pub struct PathCanonicalizationService {
    root: PathBuf,
    case_policy: CasePolicy,
}

impl PathCanonicalizationService {
    /// Build a canonicalization service rooted at the provided vault directory.
    pub fn new(
        root: impl AsRef<Path>,
        case_policy: CasePolicy,
    ) -> Result<Self, PathCanonicalizationError> {
        let root_path = root.as_ref().to_path_buf();
        let canonical_root = fs::canonicalize(&root_path).map_err(|source| {
            PathCanonicalizationError::RootCanonicalize {
                path: root_path.clone(),
                source,
            }
        })?;

        if !canonical_root.is_dir() {
            return Err(PathCanonicalizationError::RootNotDirectory {
                path: canonical_root,
            });
        }

        Ok(Self {
            root: canonical_root,
            case_policy,
        })
    }

    /// Return the canonical vault root used for boundary checks.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the active path case policy.
    #[must_use]
    pub fn case_policy(&self) -> CasePolicy {
        self.case_policy
    }

    /// Canonicalize a relative or absolute path and enforce vault boundary rules.
    pub fn canonicalize(
        &self,
        input: impl AsRef<Path>,
    ) -> Result<CanonicalPath, PathCanonicalizationError> {
        let input_path = input.as_ref();
        let candidate = if input_path.is_absolute() {
            input_path.to_path_buf()
        } else {
            self.root.join(input_path)
        };

        let absolute = fs::canonicalize(&candidate).map_err(|source| {
            PathCanonicalizationError::InputCanonicalize {
                path: candidate.clone(),
                source,
            }
        })?;

        let relative = absolute
            .strip_prefix(&self.root)
            .map_err(|_| PathCanonicalizationError::OutsideVault {
                root: self.root.clone(),
                path: absolute.clone(),
            })?
            .to_path_buf();

        let normalized = normalize_relative_path(&relative)?;
        let match_key = apply_case_policy(&normalized, self.case_policy);

        Ok(CanonicalPath {
            absolute,
            relative,
            normalized,
            match_key,
        })
    }
}

fn normalize_relative_path(path: &Path) -> Result<String, PathCanonicalizationError> {
    let mut segments = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                let value =
                    value
                        .to_str()
                        .ok_or_else(|| PathCanonicalizationError::NonUtf8Component {
                            path: path.to_path_buf(),
                        })?;
                segments.push(normalize_component(value));
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(PathCanonicalizationError::InvalidComponent {
                    path: path.to_path_buf(),
                });
            }
        }
    }

    Ok(segments.join("/"))
}

fn normalize_component(component: &str) -> String {
    component.nfc().collect()
}

fn apply_case_policy(value: &str, case_policy: CasePolicy) -> String {
    match case_policy {
        CasePolicy::Sensitive => value.to_string(),
        CasePolicy::Insensitive => value.to_lowercase(),
    }
}

/// Errors returned by path canonicalization operations.
#[derive(Debug, Error)]
pub enum PathCanonicalizationError {
    /// The vault root could not be canonicalized.
    #[error("failed to canonicalize vault root '{path}': {source}")]
    RootCanonicalize {
        /// Vault root path input.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Canonicalized vault root was not a directory.
    #[error("vault root '{path}' is not a directory")]
    RootNotDirectory {
        /// Canonicalized path.
        path: PathBuf,
    },
    /// Input path could not be canonicalized.
    #[error("failed to canonicalize input path '{path}': {source}")]
    InputCanonicalize {
        /// Input path after root join.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Canonicalized path escapes the active vault root.
    #[error("path '{path}' resolves outside vault root '{root}'")]
    OutsideVault {
        /// Canonicalized vault root.
        root: PathBuf,
        /// Canonicalized candidate path.
        path: PathBuf,
    },
    /// Path contains non UTF-8 components.
    #[error("path component in '{path}' is not valid utf-8")]
    NonUtf8Component {
        /// Path that failed UTF-8 conversion.
        path: PathBuf,
    },
    /// Path contains unsupported components after canonicalization.
    #[error("path '{path}' contains unsupported components after canonicalization")]
    InvalidComponent {
        /// Relative path with invalid component.
        path: PathBuf,
    },
}

/// Relative vault path validation failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RelativeVaultPathError {
    /// Path was blank after trimming.
    #[error("path must not be empty")]
    Empty,
    /// Path was absolute.
    #[error("path '{path}' must be vault-relative")]
    AbsolutePath {
        /// Raw path value.
        path: String,
    },
    /// Path contained `..`.
    #[error("path '{path}' must not traverse to parent directories")]
    ParentTraversal {
        /// Raw path value.
        path: String,
    },
    /// Path contained `.` components.
    #[error("path '{path}' must not contain '.' path components")]
    CurrentDirectoryComponent {
        /// Raw path value.
        path: String,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use tempfile::tempdir;

    use super::{
        CasePolicy, PathCanonicalizationError, PathCanonicalizationService, RelativeVaultPathError,
        normalize_component, validate_relative_vault_path,
    };

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    #[test]
    fn normalizes_unicode_components_to_nfc() {
        let decomposed = "Cafe\u{301}.md";
        let composed = "Caf\u{00E9}.md";
        assert_eq!(
            normalize_component(decomposed),
            normalize_component(composed)
        );
    }

    #[test]
    fn canonicalize_applies_case_policy_to_match_key() {
        let temp = tempdir().expect("tempdir");
        let notes_dir = temp.path().join("Notes");
        fs::create_dir_all(&notes_dir).expect("create notes dir");
        let note = notes_dir.join("Daily.md");
        fs::write(&note, "hello").expect("write note");

        let sensitive = PathCanonicalizationService::new(temp.path(), CasePolicy::Sensitive)
            .expect("create sensitive service");
        let insensitive = PathCanonicalizationService::new(temp.path(), CasePolicy::Insensitive)
            .expect("create insensitive service");

        let sensitive_path = sensitive
            .canonicalize(Path::new("Notes/Daily.md"))
            .expect("canonicalize sensitive");
        let insensitive_path = insensitive
            .canonicalize(Path::new("Notes/Daily.md"))
            .expect("canonicalize insensitive");

        assert_eq!(sensitive_path.normalized, "Notes/Daily.md");
        assert_eq!(sensitive_path.match_key, "Notes/Daily.md");
        assert_eq!(insensitive_path.match_key, "notes/daily.md");
    }

    #[test]
    fn validate_relative_vault_path_accepts_normalized_relative_paths() {
        assert!(validate_relative_vault_path("notes/daily.md").is_ok());
        assert!(validate_relative_vault_path("views/projects.base").is_ok());
    }

    #[test]
    fn validate_relative_vault_path_rejects_absolute_and_traversal_inputs() {
        assert!(matches!(
            validate_relative_vault_path("/etc/hosts"),
            Err(RelativeVaultPathError::AbsolutePath { .. })
        ));
        assert!(matches!(
            validate_relative_vault_path("../notes/a.md"),
            Err(RelativeVaultPathError::ParentTraversal { .. })
        ));
        assert!(matches!(
            validate_relative_vault_path("./notes/a.md"),
            Err(RelativeVaultPathError::CurrentDirectoryComponent { .. })
        ));
        assert!(matches!(
            validate_relative_vault_path("   "),
            Err(RelativeVaultPathError::Empty)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn canonicalize_resolves_symlinks_before_relative_mapping() {
        let temp = tempdir().expect("tempdir");
        let notes_dir = temp.path().join("notes");
        fs::create_dir_all(&notes_dir).expect("create notes dir");
        let note = notes_dir.join("today.md");
        fs::write(&note, "hello").expect("write note");

        let alias_dir = temp.path().join("alias");
        symlink(&notes_dir, &alias_dir).expect("create symlink");

        let service = PathCanonicalizationService::new(temp.path(), CasePolicy::Sensitive)
            .expect("create service");
        let canonical = service
            .canonicalize(Path::new("alias/today.md"))
            .expect("canonicalize symlink path");

        assert_eq!(canonical.relative, Path::new("notes/today.md"));
        assert_eq!(canonical.normalized, "notes/today.md");
    }

    #[test]
    fn canonicalize_rejects_paths_outside_vault_root() {
        let vault = tempdir().expect("vault tempdir");
        let outside = tempdir().expect("outside tempdir");
        let outside_note = outside.path().join("external.md");
        fs::write(&outside_note, "hello").expect("write outside note");

        let service = PathCanonicalizationService::new(vault.path(), CasePolicy::Sensitive)
            .expect("create service");
        let error = service
            .canonicalize(&outside_note)
            .expect_err("outside path should fail");

        assert!(matches!(
            error,
            PathCanonicalizationError::OutsideVault { .. }
        ));
    }
}
