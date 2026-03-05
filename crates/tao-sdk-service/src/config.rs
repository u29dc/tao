use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use tao_sdk_config::{Merge, PathCasePolicy, TaoConfig, load_from_path, load_or_bootstrap};
use tao_sdk_storage::{MigrationRunnerError, preflight_migrations, run_migrations};
use tao_sdk_vault::CasePolicy;
use thiserror::Error;

const ENV_VAULT_ROOT: &str = "TAO_VAULT_ROOT";
const ENV_CONFIG_PATH: &str = "TAO_CONFIG_PATH";
const ENV_DATA_DIR: &str = "TAO_DATA_DIR";
const ENV_DB_PATH: &str = "TAO_DB_PATH";
const ENV_CASE_POLICY: &str = "TAO_CASE_POLICY";
const ENV_TRACING_ENABLED: &str = "TAO_TRACING_ENABLED";
const ENV_FEATURE_FLAGS: &str = "TAO_FEATURE_FLAGS";
const ENV_READ_ONLY: &str = "TAO_READ_ONLY";

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
    /// Effective write policy gate.
    pub read_only: bool,
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
    /// Override read-only gate.
    pub read_only: Option<bool>,
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
        let root_dir = resolve_root_config_dir(cwd);
        let root_config = load_root_config_or_defaults(root_dir.as_deref())?;
        let global_config_path = resolve_global_config_path(env);
        let global_config = load_global_config_or_defaults(global_config_path.as_deref())?;
        let global_config_dir = global_config_path
            .as_deref()
            .and_then(Path::parent)
            .map(Path::to_path_buf);

        let configured_vault_root =
            root_config
                .vault
                .root
                .clone()
                .map(|path| absolutize_with_optional_base(root_dir.as_deref(), path))
                .or_else(|| {
                    global_config.vault.root.clone().map(|path| {
                        absolutize_with_optional_base(global_config_dir.as_deref(), path)
                    })
                });

        let vault_root_input = choose_optional_path(
            overrides.vault_root,
            env.get(ENV_VAULT_ROOT).map(PathBuf::from),
            configured_vault_root,
        )
        .ok_or(SdkConfigError::VaultRootUnconfigured)?;
        let vault_root = canonicalize_existing_directory(vault_root_input)?;
        let vault_config =
            load_or_bootstrap(&vault_root).map_err(|source| SdkConfigError::VaultConfig {
                path: vault_root.join("config.toml"),
                source,
            })?;

        let effective_config = TaoConfig::defaults()
            .merge(&global_config)
            .merge(&root_config)
            .merge(&vault_config);

        let configured_data_dir =
            vault_config
                .storage
                .data_dir
                .as_ref()
                .map(|path| absolutize_from(&vault_root, path.clone()))
                .or_else(|| {
                    root_config.storage.data_dir.as_ref().map(|path| {
                        absolutize_with_optional_base(root_dir.as_deref(), path.clone())
                    })
                })
                .or_else(|| {
                    global_config.storage.data_dir.as_ref().map(|path| {
                        absolutize_with_optional_base(global_config_dir.as_deref(), path.clone())
                    })
                })
                .unwrap_or_else(|| vault_root.join(".tao"));

        let data_dir_input = choose_path(
            overrides.data_dir,
            env.get(ENV_DATA_DIR).map(PathBuf::from),
            configured_data_dir,
        );
        let data_dir = absolutize_from(&vault_root, data_dir_input);
        fs::create_dir_all(&data_dir).map_err(|source| SdkConfigError::CreateDataDir {
            path: data_dir.clone(),
            source,
        })?;

        let configured_db_path =
            vault_config
                .storage
                .db_path
                .as_ref()
                .map(|path| absolutize_from(&vault_root, path.clone()))
                .or_else(|| {
                    root_config.storage.db_path.as_ref().map(|path| {
                        absolutize_with_optional_base(root_dir.as_deref(), path.clone())
                    })
                })
                .or_else(|| {
                    global_config.storage.db_path.as_ref().map(|path| {
                        absolutize_with_optional_base(global_config_dir.as_deref(), path.clone())
                    })
                })
                .unwrap_or_else(|| data_dir.join("index.sqlite"));

        let db_path_input = choose_path(
            overrides.db_path,
            env.get(ENV_DB_PATH).map(PathBuf::from),
            configured_db_path,
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
            effective_config
                .runtime
                .case_policy
                .map(case_policy_from_config)
                .unwrap_or(CasePolicy::Sensitive)
        };

        let tracing_enabled = if let Some(value) = overrides.tracing_enabled {
            value
        } else if let Some(value) = env.get(ENV_TRACING_ENABLED) {
            parse_bool(ENV_TRACING_ENABLED, value)?
        } else {
            effective_config.runtime.tracing_enabled.unwrap_or(true)
        };

        let feature_flags = if let Some(value) = overrides.feature_flags {
            normalize_feature_flags(value)
        } else if let Some(value) = env.get(ENV_FEATURE_FLAGS) {
            parse_feature_flags(value)
        } else {
            normalize_feature_flags(effective_config.runtime.feature_flags.unwrap_or_default())
        };

        let read_only = if let Some(value) = overrides.read_only {
            value
        } else if let Some(value) = env.get(ENV_READ_ONLY) {
            parse_bool(ENV_READ_ONLY, value)?
        } else {
            effective_config.security.read_only.unwrap_or(true)
        };

        Ok(SdkConfig {
            vault_root,
            data_dir,
            db_path,
            case_policy,
            tracing_enabled,
            feature_flags,
            read_only,
        })
    }
}

fn load_root_config_or_defaults(root_dir: Option<&Path>) -> Result<TaoConfig, SdkConfigError> {
    let Some(root_dir) = root_dir else {
        return Ok(TaoConfig::defaults());
    };
    let path = root_dir.join("config.toml");
    if !path.exists() {
        return Ok(TaoConfig::defaults());
    }
    load_from_path(&path).map_err(|source| SdkConfigError::RootConfig { path, source })
}

fn load_global_config_or_defaults(
    global_config_path: Option<&Path>,
) -> Result<TaoConfig, SdkConfigError> {
    let Some(path) = global_config_path else {
        return Ok(TaoConfig::defaults());
    };
    if !path.exists() {
        return Ok(TaoConfig::defaults());
    }
    load_from_path(path).map_err(|source| SdkConfigError::GlobalConfig {
        path: path.to_path_buf(),
        source,
    })
}

fn resolve_global_config_path(env: &HashMap<String, String>) -> Option<PathBuf> {
    env.get(ENV_CONFIG_PATH)
        .map(PathBuf::from)
        .or_else(|| resolve_home_dir(env).map(|home| home.join(".tools/tao/config.toml")))
}

fn resolve_home_dir(env: &HashMap<String, String>) -> Option<PathBuf> {
    env.get("HOME")
        .map(PathBuf::from)
        .or_else(|| env.get("USERPROFILE").map(PathBuf::from))
}

/// Bootstrap snapshot returned after config resolution and migration readiness checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdkBootstrapSnapshot {
    /// Final resolved SDK configuration.
    pub config: SdkConfig,
    /// Resolved root config probe path.
    pub root_config_path: PathBuf,
    /// Vault config path (`<vault>/config.toml`).
    pub vault_config_path: PathBuf,
    /// Database path used by this bootstrap run.
    pub db_path: PathBuf,
    /// True when database was opened and migrations validated/applied.
    pub db_ready: bool,
    /// Count of known migrations compiled into the binary.
    pub known_migrations: u64,
    /// Count of applied migrations after bootstrap.
    pub applied_migrations: u64,
    /// Count of pending migrations after bootstrap.
    pub pending_migrations: u64,
}

/// Bootstrap service that resolves config and initializes sqlite state.
#[derive(Debug, Default, Clone, Copy)]
pub struct SdkBootstrapService;

impl SdkBootstrapService {
    /// Bootstrap from process environment and explicit overrides.
    pub fn bootstrap(
        overrides: SdkConfigOverrides,
    ) -> Result<SdkBootstrapSnapshot, SdkBootstrapError> {
        let env: HashMap<String, String> = std::env::vars().collect();
        let cwd = std::env::current_dir()
            .map_err(|source| SdkBootstrapError::CurrentDirectory { source })?;
        Self::bootstrap_from_map(overrides, &env, &cwd)
    }

    /// Bootstrap from injected environment map and cwd (primarily for tests).
    pub fn bootstrap_from_map(
        overrides: SdkConfigOverrides,
        env: &HashMap<String, String>,
        cwd: &Path,
    ) -> Result<SdkBootstrapSnapshot, SdkBootstrapError> {
        let root_dir = resolve_root_config_dir(cwd);
        let config = SdkConfigLoader::load_from_map(overrides, env, cwd)
            .map_err(|source| SdkBootstrapError::LoadConfig { source })?;

        let mut connection = Connection::open(&config.db_path).map_err(|source| {
            SdkBootstrapError::OpenDatabase {
                path: config.db_path.clone(),
                source,
            }
        })?;

        run_migrations(&mut connection).map_err(|source| SdkBootstrapError::RunMigrations {
            path: config.db_path.clone(),
            source,
        })?;

        let preflight =
            preflight_migrations(&connection).map_err(|source| SdkBootstrapError::Preflight {
                path: config.db_path.clone(),
                source,
            })?;

        Ok(SdkBootstrapSnapshot {
            root_config_path: root_dir
                .unwrap_or_else(|| cwd.to_path_buf())
                .join("config.toml"),
            vault_config_path: config.vault_root.join("config.toml"),
            db_path: config.db_path.clone(),
            db_ready: true,
            known_migrations: preflight.known_migrations,
            applied_migrations: preflight.applied_migrations,
            pending_migrations: preflight.pending_migrations,
            config,
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

fn choose_optional_path(
    override_value: Option<PathBuf>,
    env_value: Option<PathBuf>,
    default: Option<PathBuf>,
) -> Option<PathBuf> {
    override_value.or(env_value).or(default)
}

fn absolutize_from(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn absolutize_with_optional_base(base: Option<&Path>, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    if let Some(base) = base {
        return base.join(path);
    }
    path
}

fn resolve_root_config_dir(cwd: &Path) -> Option<PathBuf> {
    let mut cursor = Some(cwd);
    while let Some(path) = cursor {
        if path.join(".git").exists() {
            return Some(path.to_path_buf());
        }
        if path.join("config.toml").exists() {
            return Some(path.to_path_buf());
        }
        cursor = path.parent();
    }
    None
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

fn case_policy_from_config(value: PathCasePolicy) -> CasePolicy {
    match value {
        PathCasePolicy::Sensitive => CasePolicy::Sensitive,
        PathCasePolicy::Insensitive => CasePolicy::Insensitive,
    }
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

fn parse_bool(key: &'static str, value: &str) -> Result<bool, SdkConfigError> {
    if value.eq_ignore_ascii_case("true") || value == "1" {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") || value == "0" {
        Ok(false)
    } else {
        Err(SdkConfigError::InvalidBool {
            key,
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
    /// Loading global config failed.
    #[error("failed to load global config '{path}': {source}")]
    GlobalConfig {
        /// Global config path.
        path: PathBuf,
        /// Config decode/bootstrap error.
        #[source]
        source: tao_sdk_config::TaoConfigError,
    },
    /// Loading vault config.toml failed.
    #[error("failed to load vault config '{path}': {source}")]
    VaultConfig {
        /// Vault config path.
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
    /// Vault root was not configured through any supported source.
    #[error(
        "vault root is not configured; pass --vault-root, set TAO_VAULT_ROOT, or set [vault].root in ~/.tools/tao/config.toml"
    )]
    VaultRootUnconfigured,
}

/// SDK bootstrap service failures.
#[derive(Debug, Error)]
pub enum SdkBootstrapError {
    /// Current working directory could not be resolved.
    #[error("failed to read current directory for sdk bootstrap: {source}")]
    CurrentDirectory {
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Config resolution failed.
    #[error("failed to resolve sdk config during bootstrap: {source}")]
    LoadConfig {
        /// Config loader error.
        #[source]
        source: SdkConfigError,
    },
    /// Opening sqlite database failed.
    #[error("failed to open sqlite database '{path}' during bootstrap: {source}")]
    OpenDatabase {
        /// Database path.
        path: PathBuf,
        /// SQLite open error.
        #[source]
        source: rusqlite::Error,
    },
    /// Running migrations failed.
    #[error("failed to run sqlite migrations for '{path}' during bootstrap: {source}")]
    RunMigrations {
        /// Database path.
        path: PathBuf,
        /// Migration runner error.
        #[source]
        source: MigrationRunnerError,
    },
    /// Post-bootstrap migration preflight failed.
    #[error("failed to preflight migrations for '{path}' during bootstrap: {source}")]
    Preflight {
        /// Database path.
        path: PathBuf,
        /// Migration preflight error.
        #[source]
        source: MigrationRunnerError,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use tao_sdk_storage::known_migrations;
    use tempfile::tempdir;

    use super::{
        CasePolicy, SdkBootstrapService, SdkConfigError, SdkConfigLoader, SdkConfigOverrides,
    };

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
        assert!(
            config.read_only,
            "default security policy should be read-only"
        );
        assert_eq!(
            config.feature_flags,
            vec![
                "bridge-batching".to_string(),
                "reconcile-auto-heal".to_string()
            ]
        );
    }

    #[test]
    fn load_from_map_rejects_unconfigured_vault_root() {
        let temp = tempdir().expect("tempdir");
        let env = HashMap::new();

        let error =
            SdkConfigLoader::load_from_map(SdkConfigOverrides::default(), &env, temp.path())
                .expect_err("unconfigured root should fail");

        assert!(matches!(error, SdkConfigError::VaultRootUnconfigured));
    }

    #[test]
    fn load_from_map_rejects_missing_vault_root_path() {
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
    fn load_from_map_does_not_bootstrap_root_config_when_missing() {
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
                .expect("load config without root bootstrap");
        assert_eq!(
            loaded.vault_root,
            fs::canonicalize(vault).expect("canonical vault")
        );
        assert!(
            !root_config.exists(),
            "root config should not be bootstrapped outside repo roots"
        );
    }

    #[test]
    fn load_from_map_bootstraps_vault_config_when_missing() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(&vault).expect("create vault");

        let mut env = HashMap::new();
        env.insert(
            "TAO_VAULT_ROOT".to_string(),
            vault.to_string_lossy().to_string(),
        );

        let vault_config = vault.join("config.toml");
        assert!(!vault_config.exists(), "test precondition");

        let loaded =
            SdkConfigLoader::load_from_map(SdkConfigOverrides::default(), &env, temp.path())
                .expect("load config with vault bootstrap");
        assert_eq!(
            loaded.vault_root,
            fs::canonicalize(vault).expect("canonical vault")
        );
        assert!(vault_config.exists(), "vault config should be bootstrapped");
    }

    #[test]
    fn load_from_map_applies_config_precedence_defaults_root_then_vault() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(&vault).expect("create vault");

        fs::write(
            temp.path().join("config.toml"),
            r#"[runtime]
case_policy = "insensitive"
tracing_enabled = false
feature_flags = ["root-flag"]

[storage]
data_dir = "root-data"
db_path = "root.sqlite"
"#,
        )
        .expect("write root config");

        fs::write(
            vault.join("config.toml"),
            r#"[runtime]
tracing_enabled = true
feature_flags = ["vault-flag"]

[storage]
data_dir = ".vault-data"
db_path = ".vault.sqlite"
"#,
        )
        .expect("write vault config");

        let mut env = HashMap::new();
        env.insert(
            "TAO_VAULT_ROOT".to_string(),
            vault.to_string_lossy().to_string(),
        );

        let loaded =
            SdkConfigLoader::load_from_map(SdkConfigOverrides::default(), &env, temp.path())
                .expect("load config");

        let canonical_vault = fs::canonicalize(&vault).expect("canonical vault");
        assert_eq!(loaded.case_policy, CasePolicy::Insensitive);
        assert!(loaded.tracing_enabled);
        assert_eq!(loaded.feature_flags, vec!["vault-flag".to_string()]);
        assert_eq!(loaded.data_dir, canonical_vault.join(".vault-data"));
        assert_eq!(loaded.db_path, canonical_vault.join(".vault.sqlite"));
    }

    #[test]
    fn load_from_map_uses_global_config_defaults_when_env_and_override_missing() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(&vault).expect("create vault");
        let home = temp.path().join("home");
        fs::create_dir_all(home.join(".tools/tao")).expect("create global config dir");
        fs::write(
            home.join(".tools/tao/config.toml"),
            format!(
                r#"[vault]
root = "{}"

[security]
read_only = true
"#,
                vault.display()
            ),
        )
        .expect("write global config");

        let mut env = HashMap::new();
        env.insert("HOME".to_string(), home.to_string_lossy().to_string());
        env.insert("TAO_READ_ONLY".to_string(), "0".to_string());

        let loaded =
            SdkConfigLoader::load_from_map(SdkConfigOverrides::default(), &env, temp.path())
                .expect("load config");

        assert_eq!(
            loaded.vault_root,
            fs::canonicalize(vault).expect("canonical vault")
        );
        assert!(
            !loaded.read_only,
            "TAO_READ_ONLY=0 should override global read_only=true"
        );
        assert!(loaded.db_path.ends_with("index.sqlite"));
    }

    #[test]
    fn sdk_bootstrap_service_returns_config_paths_and_db_ready_metadata() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(&vault).expect("create vault");

        let mut env = HashMap::new();
        env.insert(
            "TAO_VAULT_ROOT".to_string(),
            vault.to_string_lossy().to_string(),
        );

        let snapshot = SdkBootstrapService::bootstrap_from_map(
            SdkConfigOverrides::default(),
            &env,
            temp.path(),
        )
        .expect("sdk bootstrap");

        assert!(snapshot.db_ready);
        assert_eq!(snapshot.known_migrations, known_migrations().len() as u64);
        assert_eq!(snapshot.pending_migrations, 0);
        assert_eq!(snapshot.applied_migrations, known_migrations().len() as u64);
        assert_eq!(snapshot.db_path, snapshot.config.db_path);
        assert!(
            !snapshot.root_config_path.exists(),
            "root config is probe-only and should not be created implicitly"
        );
        assert!(snapshot.vault_config_path.exists());
    }
}
