use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use tao_sdk_config::load_or_bootstrap;
use tao_sdk_vault::CasePolicy;
use thiserror::Error;

const ENV_VAULT_ROOT: &str = "TAO_VAULT_ROOT";
const ENV_DATA_DIR: &str = "TAO_DATA_DIR";
const ENV_DB_PATH: &str = "TAO_DB_PATH";
const ENV_CASE_POLICY: &str = "TAO_CASE_POLICY";
const ENV_TRACING_ENABLED: &str = "TAO_TRACING_ENABLED";
const ENV_FEATURE_FLAGS: &str = "TAO_FEATURE_FLAGS";

/// Runtime SDK configuration loaded from defaults, environment, and explicit overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdkConfig {
    /// Canonical vault root.
    pub vault_root: PathBuf,
    /// Data directory for SDK runtime artifacts.
    pub data_dir: PathBuf,
    /// SQLite database file path.
    pub db_path: PathBuf,
    /// Path case policy for vault operations.
    pub case_policy: CasePolicy,
    /// Toggle for service-level tracing hooks.
    pub tracing_enabled: bool,
    /// Enabled feature flags by canonical key.
    pub feature_flags: Vec<String>,
}

/// Explicit override values with highest precedence.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SdkConfigOverrides {
    /// Override vault root.
    pub vault_root: Option<PathBuf>,
    /// Override data directory.
    pub data_dir: Option<PathBuf>,
    /// Override sqlite database path.
    pub db_path: Option<PathBuf>,
    /// Override path case policy.
    pub case_policy: Option<CasePolicy>,
    /// Override tracing toggle.
    pub tracing_enabled: Option<bool>,
    /// Override enabled feature flags.
    pub feature_flags: Option<Vec<String>>,
}

/// Loader for SDK configuration with deterministic precedence.
#[derive(Debug, Default, Clone, Copy)]
pub struct SdkConfigLoader;

impl SdkConfigLoader {
    /// Load SDK configuration from process environment and explicit overrides.
    pub fn load(overrides: SdkConfigOverrides) -> Result<SdkConfig, SdkConfigError> {
        let env: HashMap<String, String> = std::env::vars().collect();
        let cwd = std::env::current_dir()
            .map_err(|source| SdkConfigError::CurrentDirectory { source })?;
        Self::load_from_map(overrides, &env, &cwd)
    }

    /// Load SDK configuration from supplied environment map (primarily for tests).
    pub fn load_from_map(
        overrides: SdkConfigOverrides,
        env: &HashMap<String, String>,
        cwd: &Path,
    ) -> Result<SdkConfig, SdkConfigError> {
        load_or_bootstrap(cwd).map_err(|source| SdkConfigError::RootConfig {
            path: cwd.join("config.toml"),
            source,
        })?;

        let vault_root_input = choose_path(
            overrides.vault_root,
            env.get(ENV_VAULT_ROOT).map(PathBuf::from),
            cwd.to_path_buf(),
        );
        let vault_root = canonicalize_existing_directory(vault_root_input)?;

        let data_dir_input = choose_path(
            overrides.data_dir,
            env.get(ENV_DATA_DIR).map(PathBuf::from),
            vault_root.join(".tao"),
        );
        let data_dir = absolutize_from(&vault_root, data_dir_input);
        fs::create_dir_all(&data_dir).map_err(|source| SdkConfigError::CreateDataDir {
            path: data_dir.clone(),
            source,
        })?;

        let db_path_input = choose_path(
            overrides.db_path,
            env.get(ENV_DB_PATH).map(PathBuf::from),
            data_dir.join("index.sqlite"),
        );
        let db_path = absolutize_from(&data_dir, db_path_input);
        if db_path.file_name().is_none() {
            return Err(SdkConfigError::InvalidDbPath {
                path: db_path,
                reason: "database path must include a filename".to_string(),
            });
        }

        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SdkConfigError::CreateDbParent {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let case_policy = if let Some(value) = overrides.case_policy {
            value
        } else if let Some(value) = env.get(ENV_CASE_POLICY) {
            parse_case_policy(value)?
        } else {
            CasePolicy::Sensitive
        };

        let tracing_enabled = if let Some(value) = overrides.tracing_enabled {
            value
        } else if let Some(value) = env.get(ENV_TRACING_ENABLED) {
            parse_bool(value)?
        } else {
            true
        };

        let feature_flags = if let Some(value) = overrides.feature_flags {
            normalize_feature_flags(value)
        } else if let Some(value) = env.get(ENV_FEATURE_FLAGS) {
            parse_feature_flags(value)
        } else {
            Vec::new()
        };

        Ok(SdkConfig {
            vault_root,
            data_dir,
            db_path,
            case_policy,
            tracing_enabled,
            feature_flags,
        })
    }
}

fn choose_path(
    override_value: Option<PathBuf>,
    env_value: Option<PathBuf>,
    default: PathBuf,
) -> PathBuf {
    override_value.or(env_value).unwrap_or(default)
}

fn absolutize_from(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn canonicalize_existing_directory(path: PathBuf) -> Result<PathBuf, SdkConfigError> {
    if !path.exists() {
        return Err(SdkConfigError::VaultRootMissing { path });
    }

    if !path.is_dir() {
        return Err(SdkConfigError::VaultRootNotDirectory { path });
    }

    fs::canonicalize(&path).map_err(|source| SdkConfigError::CanonicalizeVaultRoot { path, source })
}

fn parse_case_policy(value: &str) -> Result<CasePolicy, SdkConfigError> {
    if value.eq_ignore_ascii_case("sensitive") {
        Ok(CasePolicy::Sensitive)
    } else if value.eq_ignore_ascii_case("insensitive") {
        Ok(CasePolicy::Insensitive)
    } else {
        Err(SdkConfigError::InvalidCasePolicy {
            value: value.to_string(),
        })
    }
}

fn parse_bool(value: &str) -> Result<bool, SdkConfigError> {
    if value.eq_ignore_ascii_case("true") || value == "1" {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") || value == "0" {
        Ok(false)
    } else {
        Err(SdkConfigError::InvalidBool {
            key: ENV_TRACING_ENABLED,
            value: value.to_string(),
        })
    }
}

fn parse_feature_flags(value: &str) -> Vec<String> {
    let parsed: Vec<String> = value
        .split(',')
        .map(|segment| segment.trim().to_ascii_lowercase())
        .filter(|segment| !segment.is_empty())
        .collect();
    normalize_feature_flags(parsed)
}

fn normalize_feature_flags(mut flags: Vec<String>) -> Vec<String> {
    flags.retain(|flag| !flag.trim().is_empty());
    flags.sort();
    flags.dedup();
    flags
}

/// SDK config loading failures.
#[derive(Debug, Error)]
pub enum SdkConfigError {
    /// Current working directory could not be resolved.
    #[error("failed to read current directory for config defaults: {source}")]
    CurrentDirectory {
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Loading root config.toml failed.
    #[error("failed to load root config '{path}': {source}")]
    RootConfig {
        /// Root config path.
        path: PathBuf,
        /// Config decode/bootstrap error.
        #[source]
        source: tao_sdk_config::TaoConfigError,
    },
    /// Vault root path does not exist.
    #[error("vault root path does not exist: '{path}'")]
    VaultRootMissing {
        /// Missing vault path.
        path: PathBuf,
    },
    /// Vault root path is not a directory.
    #[error("vault root path is not a directory: '{path}'")]
    VaultRootNotDirectory {
        /// Invalid vault path.
        path: PathBuf,
    },
    /// Vault root canonicalization failed.
    #[error("failed to canonicalize vault root '{path}': {source}")]
    CanonicalizeVaultRoot {
        /// Vault path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Creating data directory failed.
    #[error("failed to create sdk data directory '{path}': {source}")]
    CreateDataDir {
        /// Data directory path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Database parent directory creation failed.
    #[error("failed to create database parent directory '{path}': {source}")]
    CreateDbParent {
        /// Parent directory path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Invalid database path.
    #[error("invalid database path '{path}': {reason}")]
    InvalidDbPath {
        /// Database path.
        path: PathBuf,
        /// Validation failure reason.
        reason: String,
    },
    /// Invalid `TAO_CASE_POLICY` value.
    #[error("invalid case policy value '{value}'; expected 'sensitive' or 'insensitive'")]
    InvalidCasePolicy {
        /// Raw env value.
        value: String,
    },
    /// Invalid boolean-like environment value.
    #[error("invalid boolean value for '{key}': '{value}'")]
    InvalidBool {
        /// Env key.
        key: &'static str,
        /// Raw env value.
        value: String,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use tempfile::tempdir;

    use super::{CasePolicy, SdkConfigError, SdkConfigLoader, SdkConfigOverrides};

    #[test]
    fn load_from_map_applies_precedence_override_then_env_then_default() {
        let temp = tempdir().expect("tempdir");
        let env_vault = temp.path().join("env-vault");
        let override_vault = temp.path().join("override-vault");
        fs::create_dir_all(&env_vault).expect("create env vault");
        fs::create_dir_all(&override_vault).expect("create override vault");

        let mut env = HashMap::new();
        env.insert(
            "TAO_VAULT_ROOT".to_string(),
            env_vault.to_string_lossy().to_string(),
        );
        env.insert("TAO_CASE_POLICY".to_string(), "insensitive".to_string());
        env.insert("TAO_TRACING_ENABLED".to_string(), "0".to_string());
        env.insert(
            "TAO_FEATURE_FLAGS".to_string(),
            "bridge-batching,reconcile-auto-heal".to_string(),
        );

        let config = SdkConfigLoader::load_from_map(
            SdkConfigOverrides {
                vault_root: Some(override_vault.clone()),
                case_policy: Some(CasePolicy::Sensitive),
                tracing_enabled: Some(true),
                feature_flags: Some(vec!["tui-preview".to_string()]),
                ..SdkConfigOverrides::default()
            },
            &env,
            temp.path(),
        )
        .expect("load config");

        assert_eq!(
            config.vault_root,
            fs::canonicalize(override_vault).expect("canonical vault")
        );
        assert_eq!(config.case_policy, CasePolicy::Sensitive);
        assert!(config.tracing_enabled);
        assert_eq!(config.feature_flags, vec!["tui-preview".to_string()]);
    }

    #[test]
    fn load_from_map_uses_env_when_override_missing() {
        let temp = tempdir().expect("tempdir");
        let env_vault = temp.path().join("vault");
        fs::create_dir_all(&env_vault).expect("create vault");

        let mut env = HashMap::new();
        env.insert(
            "TAO_VAULT_ROOT".to_string(),
            env_vault.to_string_lossy().to_string(),
        );
        env.insert("TAO_CASE_POLICY".to_string(), "insensitive".to_string());
        env.insert(
            "TAO_FEATURE_FLAGS".to_string(),
            "bridge-batching,reconcile-auto-heal".to_string(),
        );

        let config =
            SdkConfigLoader::load_from_map(SdkConfigOverrides::default(), &env, temp.path())
                .expect("load config");

        assert_eq!(config.case_policy, CasePolicy::Insensitive);
        assert!(config.db_path.ends_with("index.sqlite"));
        assert_eq!(
            config.feature_flags,
            vec![
                "bridge-batching".to_string(),
                "reconcile-auto-heal".to_string()
            ]
        );
    }

    #[test]
    fn load_from_map_rejects_missing_vault_root() {
        let temp = tempdir().expect("tempdir");
        let mut env = HashMap::new();
        env.insert(
            "TAO_VAULT_ROOT".to_string(),
            temp.path()
                .join("missing-vault")
                .to_string_lossy()
                .to_string(),
        );

        let error =
            SdkConfigLoader::load_from_map(SdkConfigOverrides::default(), &env, temp.path())
                .expect_err("missing root should fail");

        assert!(matches!(error, SdkConfigError::VaultRootMissing { .. }));
    }

    #[test]
    fn load_from_map_rejects_invalid_case_policy() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(&vault).expect("create vault");

        let mut env = HashMap::new();
        env.insert(
            "TAO_VAULT_ROOT".to_string(),
            vault.to_string_lossy().to_string(),
        );
        env.insert("TAO_CASE_POLICY".to_string(), "mixed".to_string());

        let error =
            SdkConfigLoader::load_from_map(SdkConfigOverrides::default(), &env, temp.path())
                .expect_err("invalid case policy should fail");

        assert!(matches!(error, SdkConfigError::InvalidCasePolicy { .. }));
    }

    #[test]
    fn load_from_map_bootstraps_root_config_when_missing() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(&vault).expect("create vault");

        let mut env = HashMap::new();
        env.insert(
            "TAO_VAULT_ROOT".to_string(),
            vault.to_string_lossy().to_string(),
        );

        let root_config = temp.path().join("config.toml");
        assert!(!root_config.exists(), "test precondition");

        let loaded =
            SdkConfigLoader::load_from_map(SdkConfigOverrides::default(), &env, temp.path())
                .expect("load config with bootstrap");
        assert_eq!(
            loaded.vault_root,
            fs::canonicalize(vault).expect("canonical vault")
        );
        assert!(root_config.exists(), "root config should be bootstrapped");
    }
}
