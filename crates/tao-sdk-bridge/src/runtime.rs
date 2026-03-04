use std::path::PathBuf;
use std::sync::Mutex;

use serde::Serialize;
use tao_sdk_service::{SdkConfigLoader, SdkConfigOverrides};
use thiserror::Error;

use crate::{BridgeEnvelope, BridgeKernel, BridgeNoteListPage, BridgeVaultStats};

const DEFAULT_WINDOW_LIMIT: u64 = 128;
const MAX_WINDOW_LIMIT: u64 = 1_000;

#[derive(Debug, Error, uniffi::Error)]
pub enum TaoBridgeRuntimeError {
    #[error("{0}")]
    Runtime(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BridgeStartupBundle {
    pub stats: BridgeVaultStats,
    pub notes: BridgeNoteListPage,
}

#[derive(uniffi::Object)]
pub struct TaoBridgeRuntime {
    kernel: Mutex<BridgeKernel>,
    vault_root: String,
    db_path: String,
}

#[uniffi::export]
impl TaoBridgeRuntime {
    #[uniffi::constructor]
    pub fn new(vault_root: String, db_path: Option<String>) -> Result<Self, TaoBridgeRuntimeError> {
        let config = SdkConfigLoader::load(SdkConfigOverrides {
            vault_root: Some(PathBuf::from(vault_root)),
            db_path: db_path.map(PathBuf::from),
            ..SdkConfigOverrides::default()
        })
        .map_err(|source| {
            TaoBridgeRuntimeError::Runtime(format!("resolve config failed: {source}"))
        })?;

        let kernel = BridgeKernel::open(&config.vault_root, &config.db_path).map_err(|source| {
            TaoBridgeRuntimeError::Runtime(format!("open bridge kernel failed: {source}"))
        })?;

        Ok(Self {
            kernel: Mutex::new(kernel),
            vault_root: config.vault_root.to_string_lossy().to_string(),
            db_path: config.db_path.to_string_lossy().to_string(),
        })
    }

    pub fn vault_root(&self) -> String {
        self.vault_root.clone()
    }

    pub fn db_path(&self) -> String {
        self.db_path.clone()
    }

    pub fn schema_version(&self) -> String {
        with_kernel(&self.kernel, |kernel| {
            Ok(kernel.schema_version().to_string())
        })
        .unwrap_or_else(|_| "v1.0".to_string())
    }

    pub fn vault_stats_json(&self) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| envelope_json(kernel.vault_stats()))
    }

    pub fn note_get_json(&self, normalized_path: String) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.note_get(&normalized_path))
        })
    }

    pub fn note_context_json(
        &self,
        normalized_path: String,
    ) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.note_context(&normalized_path))
        })
    }

    pub fn notes_window_json(
        &self,
        after_path: Option<String>,
        limit: Option<u64>,
    ) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.notes_list(after_path.as_deref(), normalize_window_limit(limit)))
        })
    }

    pub fn note_put_json(
        &self,
        normalized_path: String,
        content: String,
    ) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.note_put(&normalized_path, &content))
        })
    }

    pub fn note_links_json(
        &self,
        normalized_path: String,
    ) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.note_links(&normalized_path))
        })
    }

    pub fn bases_list_json(&self) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| envelope_json(kernel.bases_list()))
    }

    pub fn bases_view_json(
        &self,
        path_or_id: String,
        view_name: String,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<String, TaoBridgeRuntimeError> {
        let page = page.unwrap_or(1);
        let page_size = page_size.unwrap_or(50);
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.bases_view(&path_or_id, &view_name, page, page_size))
        })
    }

    pub fn events_poll_json(
        &self,
        after_id: Option<u64>,
        limit: Option<u64>,
    ) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.events_poll(after_id.unwrap_or(0), normalize_window_limit(limit)))
        })
    }

    pub fn startup_bundle_json(&self, limit: Option<u64>) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| {
            let stats = expect_envelope_value(kernel.vault_stats(), "vault_stats")?;
            let notes = expect_envelope_value(
                kernel.notes_list(None, normalize_window_limit(limit)),
                "notes_list",
            )?;
            let payload = BridgeStartupBundle { stats, notes };
            serde_json::to_string(&payload).map_err(|source| {
                TaoBridgeRuntimeError::Runtime(format!("serialize startup bundle failed: {source}"))
            })
        })
    }
}

fn normalize_window_limit(limit: Option<u64>) -> u64 {
    limit
        .unwrap_or(DEFAULT_WINDOW_LIMIT)
        .clamp(1, MAX_WINDOW_LIMIT)
}

fn envelope_json<T>(envelope: BridgeEnvelope<T>) -> Result<String, TaoBridgeRuntimeError>
where
    T: Serialize,
{
    serde_json::to_string(&envelope).map_err(|source| {
        TaoBridgeRuntimeError::Runtime(format!("serialize bridge envelope failed: {source}"))
    })
}

fn expect_envelope_value<T>(
    envelope: BridgeEnvelope<T>,
    operation: &str,
) -> Result<T, TaoBridgeRuntimeError> {
    if envelope.ok {
        return envelope.value.ok_or_else(|| {
            TaoBridgeRuntimeError::Runtime(format!("{operation} returned success without payload"))
        });
    }

    let message = envelope
        .error
        .map(|error| format!("{operation} failed [{}]: {}", error.code, error.message))
        .unwrap_or_else(|| format!("{operation} failed without error payload"));
    Err(TaoBridgeRuntimeError::Runtime(message))
}

fn with_kernel<T>(
    kernel: &Mutex<BridgeKernel>,
    operation: impl FnOnce(&mut BridgeKernel) -> Result<T, TaoBridgeRuntimeError>,
) -> Result<T, TaoBridgeRuntimeError> {
    let mut guard = kernel
        .lock()
        .map_err(|_| TaoBridgeRuntimeError::Runtime("bridge runtime lock poisoned".to_string()))?;
    operation(&mut guard)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::TaoBridgeRuntime;

    #[test]
    fn runtime_bootstraps_and_returns_startup_bundle_json() {
        let temp = tempfile::tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");

        let runtime = TaoBridgeRuntime::new(vault_root.to_string_lossy().to_string(), None)
            .expect("create runtime");
        let put_envelope = runtime
            .note_put_json("notes/alpha.md".to_string(), "# Alpha\n".to_string())
            .expect("put note");
        assert!(put_envelope.contains("\"ok\":true"));

        let startup = runtime
            .startup_bundle_json(Some(32))
            .expect("startup bundle json");

        assert!(startup.contains("\"stats\""));
        assert!(startup.contains("\"notes\""));
        assert!(startup.contains("notes/alpha.md"));
        assert!(runtime.db_path().ends_with("index.sqlite"));
    }
}
