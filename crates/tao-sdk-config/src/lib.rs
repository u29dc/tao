//! Typed configuration schema and merge helpers for Tao runtime settings.

use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical config file name used at repository and vault roots.
pub const CONFIG_FILE_NAME: &str = "config.toml";

/// Typed Tao configuration payload.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TaoConfig {
    /// Runtime toggles and policy options.
    pub runtime: RuntimeConfig,
    /// Path-level overrides for runtime storage artifacts.
    pub storage: StorageConfig,
}

/// Runtime configuration settings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Case policy for path matching and canonicalization.
    pub case_policy: Option<PathCasePolicy>,
    /// Toggle structured tracing hooks.
    pub tracing_enabled: Option<bool>,
    /// Optional feature flag allowlist.
    pub feature_flags: Option<Vec<String>>,
}

/// Storage path overrides.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Optional override for runtime data directory.
    pub data_dir: Option<PathBuf>,
    /// Optional override for sqlite database path.
    pub db_path: Option<PathBuf>,
}

/// Case-sensitivity policy for path handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PathCasePolicy {
    /// Respect case-sensitive path matching.
    Sensitive,
    /// Use case-insensitive path matching.
    Insensitive,
}

/// Merge strategy for config precedence.
///
/// Higher-precedence values replace lower-precedence values when non-`None`.
pub trait Merge {
    /// Merge `self` with a higher-precedence overlay.
    fn merge(&self, overlay: &Self) -> Self;
}

impl Merge for TaoConfig {
    fn merge(&self, overlay: &Self) -> Self {
        Self {
            runtime: self.runtime.merge(&overlay.runtime),
            storage: self.storage.merge(&overlay.storage),
        }
    }
}

impl Merge for RuntimeConfig {
    fn merge(&self, overlay: &Self) -> Self {
        Self {
            case_policy: overlay.case_policy.or(self.case_policy),
            tracing_enabled: overlay.tracing_enabled.or(self.tracing_enabled),
            feature_flags: overlay
                .feature_flags
                .clone()
                .or_else(|| self.feature_flags.clone()),
        }
    }
}

impl Merge for StorageConfig {
    fn merge(&self, overlay: &Self) -> Self {
        Self {
            data_dir: overlay.data_dir.clone().or_else(|| self.data_dir.clone()),
            db_path: overlay.db_path.clone().or_else(|| self.db_path.clone()),
        }
    }
}

impl TaoConfig {
    /// Return canonical config defaults used for precedence resolution.
    pub fn defaults() -> Self {
        Self {
            runtime: RuntimeConfig {
                case_policy: Some(PathCasePolicy::Sensitive),
                tracing_enabled: Some(true),
                feature_flags: Some(Vec::new()),
            },
            storage: StorageConfig::default(),
        }
    }

    /// Normalize and validate config values.
    pub fn normalized(mut self) -> Result<Self, TaoConfigError> {
        if let Some(flags) = self.runtime.feature_flags.take() {
            let mut normalized = flags
                .into_iter()
                .map(|flag| flag.trim().to_ascii_lowercase())
                .filter(|flag| !flag.is_empty())
                .collect::<Vec<_>>();
            normalized.sort();
            normalized.dedup();
            self.runtime.feature_flags = Some(normalized);
        }

        if let Some(path) = &self.storage.db_path
            && !matches!(path.components().next_back(), Some(Component::Normal(_)))
        {
            return Err(TaoConfigError::InvalidValue {
                field: "storage.db_path",
                reason: "db_path must include a filename".to_string(),
            });
        }

        Ok(self)
    }
}

/// Parse config payload from TOML input.
pub fn parse_toml(input: &str) -> Result<TaoConfig, TaoConfigError> {
    let parsed: TaoConfig = toml::from_str(input).map_err(TaoConfigError::Decode)?;
    parsed.normalized()
}

/// Load config from a TOML file path.
pub fn load_from_path(path: &Path) -> Result<TaoConfig, TaoConfigError> {
    let body = fs::read_to_string(path).map_err(|source| TaoConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    parse_toml(&body)
}

/// Persist default config template at `path` if file does not already exist.
pub fn bootstrap_default_file(path: &Path) -> Result<(), TaoConfigError> {
    if path.exists() {
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| TaoConfigError::CreateParent {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    fs::write(path, default_template()).map_err(|source| TaoConfigError::Write {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

/// Render a deterministic default config template.
pub fn default_template() -> &'static str {
    r#"[runtime]
# case_policy = "sensitive"
# tracing_enabled = true
# feature_flags = []

[storage]
# data_dir = ".tao"
# db_path = ".tao.sqlite"
"#
}

/// Configuration parse/load/bootstrap failures.
#[derive(Debug, Error)]
pub enum TaoConfigError {
    /// TOML payload failed to decode.
    #[error("failed to decode config.toml: {0}")]
    Decode(#[source] toml::de::Error),
    /// Config file read failure.
    #[error("failed to read config file '{path}': {source}")]
    Read {
        /// Config file path.
        path: PathBuf,
        /// Underlying IO error.
        #[source]
        source: std::io::Error,
    },
    /// Parent directory creation failure.
    #[error("failed to create config parent directory '{path}': {source}")]
    CreateParent {
        /// Parent directory path.
        path: PathBuf,
        /// Underlying IO error.
        #[source]
        source: std::io::Error,
    },
    /// Config file write failure.
    #[error("failed to write config file '{path}': {source}")]
    Write {
        /// Config file path.
        path: PathBuf,
        /// Underlying IO error.
        #[source]
        source: std::io::Error,
    },
    /// Semantic validation failure.
    #[error("invalid config value for '{field}': {reason}")]
    InvalidValue {
        /// Dot-path field name.
        field: &'static str,
        /// Validation reason.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        CONFIG_FILE_NAME, Merge, PathCasePolicy, RuntimeConfig, StorageConfig, TaoConfig,
        TaoConfigError, bootstrap_default_file, default_template, load_from_path, parse_toml,
    };

    #[test]
    fn parse_toml_decodes_typed_schema() {
        let config = parse_toml(
            r#"
            [runtime]
            case_policy = "insensitive"
            tracing_enabled = false
            feature_flags = ["reconcile-auto-heal", "bridge-batching"]

            [storage]
            data_dir = ".tao"
            db_path = ".tao.sqlite"
            "#,
        )
        .expect("parse config");

        assert_eq!(
            config.runtime,
            RuntimeConfig {
                case_policy: Some(PathCasePolicy::Insensitive),
                tracing_enabled: Some(false),
                feature_flags: Some(vec![
                    "bridge-batching".to_string(),
                    "reconcile-auto-heal".to_string()
                ]),
            }
        );
        assert_eq!(
            config.storage,
            StorageConfig {
                data_dir: Some(".tao".into()),
                db_path: Some(".tao.sqlite".into()),
            }
        );
    }

    #[test]
    fn merge_applies_overlay_precedence() {
        let low = TaoConfig {
            runtime: RuntimeConfig {
                case_policy: Some(PathCasePolicy::Sensitive),
                tracing_enabled: Some(true),
                feature_flags: Some(vec!["bridge-batching".to_string()]),
            },
            storage: StorageConfig {
                data_dir: Some(".tao".into()),
                db_path: Some(".tao.sqlite".into()),
            },
        };

        let high = TaoConfig {
            runtime: RuntimeConfig {
                case_policy: Some(PathCasePolicy::Insensitive),
                tracing_enabled: None,
                feature_flags: Some(vec!["reconcile-auto-heal".to_string()]),
            },
            storage: StorageConfig {
                data_dir: None,
                db_path: Some(".tao/custom.sqlite".into()),
            },
        };

        let merged = low.merge(&high);
        assert_eq!(
            merged.runtime.case_policy,
            Some(PathCasePolicy::Insensitive)
        );
        assert_eq!(merged.runtime.tracing_enabled, Some(true));
        assert_eq!(
            merged.runtime.feature_flags,
            Some(vec!["reconcile-auto-heal".to_string()])
        );
        assert_eq!(merged.storage.data_dir, Some(".tao".into()));
        assert_eq!(merged.storage.db_path, Some(".tao/custom.sqlite".into()));
    }

    #[test]
    fn normalized_rejects_db_path_without_filename() {
        let error = parse_toml(
            r#"
            [storage]
            db_path = "."
            "#,
        )
        .expect_err("invalid db path should fail");

        assert!(matches!(error, TaoConfigError::InvalidValue { .. }));
    }

    #[test]
    fn bootstrap_default_file_creates_template_once() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join(CONFIG_FILE_NAME);

        bootstrap_default_file(&path).expect("create defaults");
        let body = std::fs::read_to_string(&path).expect("read config");
        assert_eq!(body, default_template());

        std::fs::write(&path, "[runtime]\ntracing_enabled = false\n").expect("overwrite config");
        bootstrap_default_file(&path).expect("bootstrap should not overwrite existing file");

        let loaded = load_from_path(&path).expect("load existing config");
        assert_eq!(loaded.runtime.tracing_enabled, Some(false));
    }

    #[test]
    fn defaults_expose_expected_baseline() {
        let defaults = TaoConfig::defaults();
        assert_eq!(
            defaults.runtime.case_policy,
            Some(PathCasePolicy::Sensitive)
        );
        assert_eq!(defaults.runtime.tracing_enabled, Some(true));
        assert_eq!(defaults.runtime.feature_flags, Some(Vec::new()));
        assert_eq!(defaults.storage, StorageConfig::default());
    }
}
