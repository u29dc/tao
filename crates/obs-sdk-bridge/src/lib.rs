//! Swift bridge adapter shell over SDK services.

use std::fs;
use std::path::{Path, PathBuf};

use obs_sdk_markdown::{MarkdownParseRequest, MarkdownParser};
use obs_sdk_service::{HealthSnapshotService, NoteCrudService, WatcherStatus};
use obs_sdk_storage::{FilesRepository, run_migrations};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use thiserror::Error;

/// Current bridge DTO schema version.
pub const BRIDGE_SCHEMA_VERSION: &str = "v1";

/// Standard bridge envelope used for all boundary responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeEnvelope<T> {
    /// Bridge schema version string.
    pub schema_version: String,
    /// Success flag.
    pub ok: bool,
    /// Value payload when successful.
    pub value: Option<T>,
    /// Error payload when failed.
    pub error: Option<BridgeError>,
}

impl<T> BridgeEnvelope<T> {
    /// Build one successful envelope.
    #[must_use]
    pub fn success(value: T) -> Self {
        Self {
            schema_version: BRIDGE_SCHEMA_VERSION.to_string(),
            ok: true,
            value: Some(value),
            error: None,
        }
    }

    /// Build one failed envelope.
    #[must_use]
    pub fn failure(error: BridgeError) -> Self {
        Self {
            schema_version: BRIDGE_SCHEMA_VERSION.to_string(),
            ok: false,
            value: None,
            error: Some(error),
        }
    }
}

/// Typed bridge error payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeError {
    /// Stable error code.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Optional remediation hint.
    pub hint: Option<String>,
    /// Machine-readable context values.
    pub context: JsonMap<String, JsonValue>,
}

impl BridgeError {
    #[must_use]
    pub fn with_code(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            hint: None,
            context: JsonMap::new(),
        }
    }

    #[must_use]
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    #[must_use]
    pub fn with_context(mut self, key: impl Into<String>, value: JsonValue) -> Self {
        self.context.insert(key.into(), value);
        self
    }
}

/// Minimal bridge health DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgePing {
    /// Ping message.
    pub message: String,
}

/// Bridge vault stats DTO exposed to UI adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeVaultStats {
    /// Canonical vault root path.
    pub vault_root: String,
    /// Total files discovered by health snapshot.
    pub files_total: u64,
    /// Total markdown files.
    pub markdown_files: u64,
    /// Database health state.
    pub db_healthy: bool,
    /// Last index timestamp when present.
    pub last_index_updated_at: Option<String>,
}

/// Bridge note DTO for read flows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeNoteView {
    /// Requested normalized path.
    pub path: String,
    /// Derived title.
    pub title: String,
    /// Optional front matter payload.
    pub front_matter: Option<String>,
    /// Markdown body without front matter fences.
    pub body: String,
    /// Heading count parsed from note body.
    pub headings_total: u64,
}

/// Bridge write acknowledgement DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeWriteAck {
    /// Path that was written.
    pub path: String,
    /// Stable file id.
    pub file_id: String,
    /// Write action label.
    pub action: String,
}

/// Bridge runtime kernel with vault root and opened SQLite connection.
#[derive(Debug)]
pub struct BridgeKernel {
    vault_root: PathBuf,
    connection: Connection,
    parser: MarkdownParser,
}

impl BridgeKernel {
    /// Open bridge runtime with vault root and sqlite database path.
    pub fn open(
        vault_root: impl AsRef<Path>,
        db_path: impl AsRef<Path>,
    ) -> Result<Self, BridgeInitError> {
        let vault_root = vault_root.as_ref().to_path_buf();
        if !vault_root.exists() {
            return Err(BridgeInitError::VaultRootMissing { vault_root });
        }

        let mut connection =
            Connection::open(db_path).map_err(|source| BridgeInitError::OpenDb { source })?;
        run_migrations(&mut connection)
            .map_err(|source| BridgeInitError::RunMigrations { source })?;

        Ok(Self {
            vault_root,
            connection,
            parser: MarkdownParser,
        })
    }

    /// Return bridge schema version.
    #[must_use]
    pub fn schema_version(&self) -> &'static str {
        BRIDGE_SCHEMA_VERSION
    }

    /// Return bridge ping envelope.
    #[must_use]
    pub fn ping(&self) -> BridgeEnvelope<BridgePing> {
        BridgeEnvelope::success(BridgePing {
            message: "ok".to_string(),
        })
    }

    /// Return vault stats envelope from SDK health snapshot service.
    #[must_use]
    pub fn vault_stats(&self) -> BridgeEnvelope<BridgeVaultStats> {
        match HealthSnapshotService.snapshot(
            &self.vault_root,
            &self.connection,
            0,
            WatcherStatus::Stopped,
        ) {
            Ok(snapshot) => BridgeEnvelope::success(BridgeVaultStats {
                vault_root: snapshot.vault_root,
                files_total: snapshot.files_total,
                markdown_files: snapshot.markdown_files,
                db_healthy: snapshot.db_healthy,
                last_index_updated_at: snapshot.last_index_updated_at,
            }),
            Err(source) => BridgeEnvelope::failure(
                BridgeError::with_code("bridge.vault_stats.failed", source.to_string())
                    .with_hint("ensure vault path and sqlite database are readable"),
            ),
        }
    }

    /// Return parsed note payload for one normalized path.
    #[must_use]
    pub fn note_get(&self, normalized_path: &str) -> BridgeEnvelope<BridgeNoteView> {
        let normalized_path = normalized_path.trim();
        if normalized_path.is_empty() {
            return BridgeEnvelope::failure(
                BridgeError::with_code(
                    "bridge.note_get.invalid_path",
                    "normalized path must not be empty",
                )
                .with_hint("provide a vault-relative markdown path"),
            );
        }

        let absolute_path = self.vault_root.join(normalized_path);
        let raw = match fs::read_to_string(&absolute_path) {
            Ok(raw) => raw,
            Err(source) => {
                return BridgeEnvelope::failure(
                    BridgeError::with_code("bridge.note_get.read_failed", source.to_string())
                        .with_hint("ensure the note exists and is readable")
                        .with_context(
                            "path",
                            JsonValue::String(absolute_path.to_string_lossy().to_string()),
                        ),
                );
            }
        };

        match self.parser.parse(MarkdownParseRequest {
            normalized_path: normalized_path.to_string(),
            raw,
        }) {
            Ok(parsed) => BridgeEnvelope::success(BridgeNoteView {
                path: normalized_path.to_string(),
                title: parsed.title,
                front_matter: parsed.front_matter,
                body: parsed.body,
                headings_total: parsed.headings.len() as u64,
            }),
            Err(source) => BridgeEnvelope::failure(
                BridgeError::with_code("bridge.note_get.parse_failed", source.to_string())
                    .with_hint("fix note markdown syntax issues and retry"),
            ),
        }
    }

    /// Create or update one note safely through SDK write services.
    #[must_use]
    pub fn note_put(
        &mut self,
        normalized_path: &str,
        content: &str,
    ) -> BridgeEnvelope<BridgeWriteAck> {
        let normalized_path = normalized_path.trim();
        if normalized_path.is_empty() {
            return BridgeEnvelope::failure(
                BridgeError::with_code(
                    "bridge.note_put.invalid_path",
                    "normalized path must not be empty",
                )
                .with_hint("provide a vault-relative markdown path"),
            );
        }

        let note_service = NoteCrudService::default();
        let relative = Path::new(normalized_path);
        let existing =
            match FilesRepository::get_by_normalized_path(&self.connection, normalized_path) {
                Ok(existing) => existing,
                Err(source) => {
                    return BridgeEnvelope::failure(
                        BridgeError::with_code("bridge.note_put.lookup_failed", source.to_string())
                            .with_hint("ensure bridge database is available"),
                    );
                }
            };

        if let Some(existing) = existing {
            match note_service.update_note(
                &self.vault_root,
                &mut self.connection,
                &existing.file_id,
                relative,
                content,
            ) {
                Ok(result) => BridgeEnvelope::success(BridgeWriteAck {
                    path: result.normalized_path,
                    file_id: result.file_id,
                    action: "updated".to_string(),
                }),
                Err(source) => BridgeEnvelope::failure(
                    BridgeError::with_code("bridge.note_put.update_failed", source.to_string())
                        .with_hint("fix note payload or path and retry"),
                ),
            }
        } else {
            let file_id = deterministic_file_id(normalized_path);
            match note_service.create_note(
                &self.vault_root,
                &mut self.connection,
                &file_id,
                relative,
                content,
            ) {
                Ok(result) => BridgeEnvelope::success(BridgeWriteAck {
                    path: result.normalized_path,
                    file_id: result.file_id,
                    action: "created".to_string(),
                }),
                Err(source) => BridgeEnvelope::failure(
                    BridgeError::with_code("bridge.note_put.create_failed", source.to_string())
                        .with_hint("ensure vault path exists and target note path is valid"),
                ),
            }
        }
    }
}

fn deterministic_file_id(normalized_path: &str) -> String {
    let hash = blake3::hash(normalized_path.as_bytes()).to_hex();
    format!("f_{}", &hash[..16])
}

/// Bridge initialization failures.
#[derive(Debug, Error)]
pub enum BridgeInitError {
    /// Vault root path does not exist.
    #[error("bridge vault root does not exist: {vault_root}")]
    VaultRootMissing {
        /// Vault root path.
        vault_root: PathBuf,
    },
    /// Opening sqlite db failed.
    #[error("failed to open bridge sqlite database: {source}")]
    OpenDb {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Running schema migrations failed.
    #[error("failed to run bridge sqlite migrations: {source}")]
    RunMigrations {
        /// Migration error.
        #[source]
        source: obs_sdk_storage::MigrationRunnerError,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{BRIDGE_SCHEMA_VERSION, BridgeKernel};

    #[test]
    fn bridge_kernel_exposes_schema_version_and_ping() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault");
        let db_path = temp.path().join("obs.db");

        let kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        assert_eq!(kernel.schema_version(), BRIDGE_SCHEMA_VERSION);

        let ping = kernel.ping();
        assert!(ping.ok);
        assert_eq!(ping.value.expect("ping value").message, "ok");
    }

    #[test]
    fn bridge_kernel_returns_vault_stats_envelope() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A").expect("write markdown");
        let db_path = temp.path().join("obs.db");

        let kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let stats = kernel.vault_stats();

        assert!(stats.ok);
        let value = stats.value.expect("stats value");
        assert_eq!(value.files_total, 1);
        assert_eq!(value.markdown_files, 1);
        assert!(value.db_healthy);
    }

    #[test]
    fn bridge_kernel_note_get_returns_title_and_body() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(
            vault_root.join("notes/a.md"),
            "---\nstatus: draft\n---\n# Alpha\ncontent",
        )
        .expect("write markdown");
        let db_path = temp.path().join("obs.db");

        let kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let note = kernel.note_get("notes/a.md");

        assert!(note.ok);
        let value = note.value.expect("note value");
        assert_eq!(value.title, "Alpha");
        assert_eq!(value.front_matter.as_deref(), Some("status: draft"));
        assert_eq!(value.body, "# Alpha\ncontent");
        assert_eq!(value.headings_total, 1);
    }

    #[test]
    fn bridge_kernel_note_put_creates_and_updates_notes() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        let db_path = temp.path().join("obs.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let created = kernel.note_put("notes/a.md", "# A\nfirst");
        assert!(created.ok);
        assert_eq!(created.value.expect("created value").action, "created");

        let created_note = kernel.note_get("notes/a.md");
        assert!(created_note.ok);
        assert!(
            created_note
                .value
                .expect("created note")
                .body
                .contains("first")
        );

        let updated = kernel.note_put("notes/a.md", "# A\nsecond");
        assert!(updated.ok);
        assert_eq!(updated.value.expect("updated value").action, "updated");

        let updated_note = kernel.note_get("notes/a.md");
        assert!(updated_note.ok);
        assert!(
            updated_note
                .value
                .expect("updated note")
                .body
                .contains("second")
        );
    }
}
