use std::collections::HashSet;

use thiserror::Error;

use crate::SdkConfig;

/// Supported SDK feature flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SdkFeature {
    /// Enables experimental reconcile self-heal routines.
    ReconcileAutoHeal,
    /// Enables experimental Bases planner path.
    BasesPlannerV2,
    /// Enables experimental bridge event stream batching.
    BridgeBatching,
    /// Enables experimental TUI package integration hooks.
    TuiPreview,
}

impl SdkFeature {
    /// Return canonical flag key used in config and environment strings.
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            SdkFeature::ReconcileAutoHeal => "reconcile-auto-heal",
            SdkFeature::BasesPlannerV2 => "bases-planner-v2",
            SdkFeature::BridgeBatching => "bridge-batching",
            SdkFeature::TuiPreview => "tui-preview",
        }
    }
}

/// Parse errors for typed feature keys.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FeatureFlagParseError {
    /// Unrecognized feature key.
    #[error("unknown feature flag key '{key}'")]
    Unknown {
        /// Raw key.
        key: String,
    },
}

impl TryFrom<&str> for SdkFeature {
    type Error = FeatureFlagParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "reconcile-auto-heal" => Ok(SdkFeature::ReconcileAutoHeal),
            "bases-planner-v2" => Ok(SdkFeature::BasesPlannerV2),
            "bridge-batching" => Ok(SdkFeature::BridgeBatching),
            "tui-preview" => Ok(SdkFeature::TuiPreview),
            _ => Err(FeatureFlagParseError::Unknown {
                key: value.to_string(),
            }),
        }
    }
}

/// Typed registry for SDK feature flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureFlagRegistry {
    enabled: HashSet<SdkFeature>,
    unknown_keys: Vec<String>,
}

impl FeatureFlagRegistry {
    /// Build registry from configured feature key list.
    #[must_use]
    pub fn from_config(config: &SdkConfig) -> Self {
        Self::from_keys(&config.feature_flags)
    }

    /// Build registry from raw feature keys.
    #[must_use]
    pub fn from_keys(keys: &[String]) -> Self {
        let mut enabled = HashSet::new();
        let mut unknown_keys = Vec::new();

        for key in keys {
            match SdkFeature::try_from(key.as_str()) {
                Ok(flag) => {
                    enabled.insert(flag);
                }
                Err(_) => unknown_keys.push(key.clone()),
            }
        }

        unknown_keys.sort();
        unknown_keys.dedup();

        Self {
            enabled,
            unknown_keys,
        }
    }

    /// Check whether a feature is enabled.
    #[must_use]
    pub fn is_enabled(&self, feature: SdkFeature) -> bool {
        self.enabled.contains(&feature)
    }

    /// Enable one feature in-memory.
    pub fn enable(&mut self, feature: SdkFeature) {
        self.enabled.insert(feature);
    }

    /// Disable one feature in-memory.
    pub fn disable(&mut self, feature: SdkFeature) {
        self.enabled.remove(&feature);
    }

    /// Return sorted keys of currently enabled features.
    #[must_use]
    pub fn enabled_keys(&self) -> Vec<&'static str> {
        let mut keys: Vec<&'static str> =
            self.enabled.iter().map(|feature| feature.key()).collect();
        keys.sort_unstable();
        keys
    }

    /// Return unknown feature keys seen during registry construction.
    #[must_use]
    pub fn unknown_keys(&self) -> &[String] {
        &self.unknown_keys
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tao_sdk_vault::CasePolicy;

    use super::{FeatureFlagRegistry, SdkFeature};
    use crate::SdkConfig;

    fn sample_config(feature_flags: Vec<String>) -> SdkConfig {
        SdkConfig {
            vault_root: PathBuf::from("/tmp/vault"),
            data_dir: PathBuf::from("/tmp/vault/.tao"),
            db_path: PathBuf::from("/tmp/vault/.tao/index.sqlite"),
            case_policy: CasePolicy::Sensitive,
            tracing_enabled: true,
            feature_flags,
        }
    }

    #[test]
    fn registry_enables_known_flags_and_tracks_unknown_keys() {
        let config = sample_config(vec![
            "reconcile-auto-heal".to_string(),
            "bridge-batching".to_string(),
            "unknown-flag".to_string(),
        ]);

        let registry = FeatureFlagRegistry::from_config(&config);
        assert!(registry.is_enabled(SdkFeature::ReconcileAutoHeal));
        assert!(registry.is_enabled(SdkFeature::BridgeBatching));
        assert!(!registry.is_enabled(SdkFeature::TuiPreview));
        assert_eq!(registry.unknown_keys(), &["unknown-flag".to_string()]);
    }

    #[test]
    fn registry_supports_runtime_enable_disable() {
        let config = sample_config(Vec::new());
        let mut registry = FeatureFlagRegistry::from_config(&config);

        assert!(!registry.is_enabled(SdkFeature::TuiPreview));
        registry.enable(SdkFeature::TuiPreview);
        assert!(registry.is_enabled(SdkFeature::TuiPreview));

        registry.disable(SdkFeature::TuiPreview);
        assert!(!registry.is_enabled(SdkFeature::TuiPreview));
    }
}
