use std::path::PathBuf;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use serde::Serialize;
use tao_sdk_service::{SdkConfigLoader, SdkConfigOverrides};
use tao_sdk_watch::VaultChangeMonitor;
use thiserror::Error;

use crate::{
    BRIDGE_ERROR_STARTUP_BUNDLE_FAILED, BridgeEnvelope, BridgeError, BridgeKernel,
    BridgeNoteListPage, BridgeVaultStats,
};

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
    monitor: VaultChangeMonitor,
    last_synced_generation: AtomicU64,
    read_only: bool,
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

        let kernel = BridgeKernel::open_with_case_policy(
            &config.vault_root,
            &config.db_path,
            config.case_policy,
        )
        .map_err(|source| {
            TaoBridgeRuntimeError::Runtime(format!("open bridge kernel failed: {source}"))
        })?;
        let monitor = VaultChangeMonitor::start(&config.vault_root).map_err(|source| {
            TaoBridgeRuntimeError::Runtime(format!(
                "start bridge filesystem monitor failed: {source}"
            ))
        })?;

        Ok(Self {
            kernel: Mutex::new(kernel),
            monitor,
            last_synced_generation: AtomicU64::new(u64::MAX),
            read_only: config.read_only,
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
        self.sync_index_if_needed()?;
        with_kernel(&self.kernel, |kernel| envelope_json(kernel.vault_stats()))
    }

    pub fn note_get_json(&self, normalized_path: String) -> Result<String, TaoBridgeRuntimeError> {
        self.sync_index_if_needed()?;
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.note_get(&normalized_path))
        })
    }

    pub fn note_context_json(
        &self,
        normalized_path: String,
    ) -> Result<String, TaoBridgeRuntimeError> {
        self.sync_index_if_needed()?;
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.note_context(&normalized_path))
        })
    }

    pub fn notes_window_json(
        &self,
        after_path: Option<String>,
        limit: Option<u64>,
    ) -> Result<String, TaoBridgeRuntimeError> {
        self.sync_index_if_needed()?;
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.notes_list(after_path.as_deref(), normalize_window_limit(limit)))
        })
    }

    pub fn note_put_json(
        &self,
        normalized_path: String,
        content: String,
        allow_writes: Option<bool>,
    ) -> Result<String, TaoBridgeRuntimeError> {
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.note_put_with_policy(
                &normalized_path,
                &content,
                allow_writes.unwrap_or(false) || !self.read_only,
            ))
        })
    }

    pub fn note_links_json(
        &self,
        normalized_path: String,
    ) -> Result<String, TaoBridgeRuntimeError> {
        self.sync_index_if_needed()?;
        with_kernel(&self.kernel, |kernel| {
            envelope_json(kernel.note_links(&normalized_path))
        })
    }

    pub fn bases_list_json(&self) -> Result<String, TaoBridgeRuntimeError> {
        self.sync_index_if_needed()?;
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
        self.sync_index_if_needed()?;
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
        self.sync_index_if_needed()?;
        with_kernel(&self.kernel, |kernel| {
            let result = (|| {
                let stats = extract_envelope_value(kernel.vault_stats(), "vault_stats")?;
                let notes = extract_envelope_value(
                    kernel.notes_list(None, normalize_window_limit(limit)),
                    "notes_list",
                )?;
                Ok::<BridgeStartupBundle, String>(BridgeStartupBundle { stats, notes })
            })();

            let envelope = match result {
                Ok(payload) => BridgeEnvelope::success(payload),
                Err(message) => BridgeEnvelope::failure(
                    BridgeError::with_code(BRIDGE_ERROR_STARTUP_BUNDLE_FAILED, message).with_hint(
                        "ensure vault root and bridge database are readable, then retry startup",
                    ),
                ),
            };
            envelope_json(envelope)
        })
    }

    fn sync_index_if_needed(&self) -> Result<(), TaoBridgeRuntimeError> {
        let generation = self.monitor.generation();
        if self.last_synced_generation.load(Ordering::Relaxed) == generation {
            return Ok(());
        }

        with_kernel(&self.kernel, |kernel| {
            kernel.ensure_indexed().map_err(|source| {
                TaoBridgeRuntimeError::Runtime(format!("sync bridge index failed: {source}"))
            })?;
            Ok(())
        })?;
        self.last_synced_generation
            .store(generation, Ordering::Relaxed);
        Ok(())
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

fn extract_envelope_value<T>(envelope: BridgeEnvelope<T>, operation: &str) -> Result<T, String> {
    if envelope.ok {
        return envelope
            .value
            .ok_or_else(|| format!("{operation} returned success without payload"));
    }

    let message = envelope
        .error
        .map(|error| format!("{operation} failed [{}]: {}", error.code, error.message))
        .unwrap_or_else(|| format!("{operation} failed without error payload"));
    Err(message)
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
    use std::thread;
    use std::time::Duration;

    use serde_json::Value as JsonValue;

    use super::TaoBridgeRuntime;

    #[test]
    fn runtime_startup_bundle_bootstraps_index_from_vault_filesystem() {
        let temp = tempfile::tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(
            vault_root.join("notes/alpha.md"),
            "---\nstatus: active\n---\n\n# Alpha\n\nBody",
        )
        .expect("seed note");

        let runtime = TaoBridgeRuntime::new(vault_root.to_string_lossy().to_string(), None)
            .expect("create runtime");

        let startup = runtime
            .startup_bundle_json(Some(32))
            .expect("startup bundle json");

        let envelope: JsonValue = serde_json::from_str(&startup).expect("parse startup envelope");
        assert_eq!(envelope.get("ok").and_then(JsonValue::as_bool), Some(true));
        assert!(
            envelope
                .get("value")
                .and_then(|value| value.get("stats"))
                .is_some(),
            "startup bundle should use standard envelope value payload"
        );
        assert!(startup.contains("notes/alpha.md"));
        assert!(runtime.db_path().ends_with("index.sqlite"));
    }

    #[test]
    fn runtime_refreshes_index_after_external_vault_change() {
        let temp = tempfile::tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/alpha.md"), "# Alpha").expect("seed alpha");

        let runtime = TaoBridgeRuntime::new(vault_root.to_string_lossy().to_string(), None)
            .expect("create runtime");
        runtime
            .startup_bundle_json(Some(32))
            .expect("seed startup bundle");

        fs::write(vault_root.join("notes/beta.md"), "# Beta").expect("seed beta");
        thread::sleep(Duration::from_millis(300));
        let payload = runtime
            .notes_window_json(None, Some(64))
            .expect("notes window json");
        assert!(
            payload.contains("notes/beta.md"),
            "runtime should surface externally added note after periodic refresh"
        );
    }

    #[test]
    fn runtime_note_put_respects_configured_read_only_policy_override() {
        let temp = tempfile::tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(
            vault_root.join("config.toml"),
            "[security]\nread_only = false\n",
        )
        .expect("write vault config");

        let runtime = TaoBridgeRuntime::new(vault_root.to_string_lossy().to_string(), None)
            .expect("create runtime");
        let response = runtime
            .note_put_json(
                "notes/runtime.md".to_string(),
                "# Runtime".to_string(),
                None,
            )
            .expect("note_put json");
        let envelope: JsonValue = serde_json::from_str(&response).expect("parse note_put");
        assert_eq!(envelope.get("ok").and_then(JsonValue::as_bool), Some(true));
    }
}
