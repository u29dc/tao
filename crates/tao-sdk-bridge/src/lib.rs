//! Swift bridge adapter shell over SDK services.

mod runtime;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tao_sdk_bases::{
    BaseDocument, BaseTableQueryPlanner, BaseViewRegistry, TableQueryPlanRequest,
    parse_base_document,
};
use tao_sdk_markdown::{MarkdownParseRequest, MarkdownParser};
use tao_sdk_service::{
    BaseTableExecutorService, FullIndexService, HealthSnapshotService, IncrementalIndexService,
    NoteCrudService, ReconciliationScannerService, WatcherStatus,
};
use tao_sdk_storage::{
    BasesRepository, FilesRepository, LinkWithPaths, LinksRepository, run_migrations,
};
use tao_sdk_vault::{CasePolicy, validate_relative_vault_path};
use thiserror::Error;

pub use runtime::{BridgeStartupBundle, TaoBridgeRuntime, TaoBridgeRuntimeError};

uniffi::setup_scaffolding!();

/// Current bridge DTO schema version.
pub const BRIDGE_SCHEMA_VERSION: &str = "v1.0";
/// Supported bridge DTO major version for compatibility checks.
pub const BRIDGE_SCHEMA_MAJOR: u16 = 1;
/// Bridge error code when kernel initialization fails.
pub const BRIDGE_ERROR_INIT_FAILED: &str = "bridge.init.failed";
/// Bridge error code when vault stats lookup fails.
pub const BRIDGE_ERROR_VAULT_STATS_FAILED: &str = "bridge.vault_stats.failed";
/// Bridge error code when note-get path is invalid.
pub const BRIDGE_ERROR_NOTE_GET_INVALID_PATH: &str = "bridge.note_get.invalid_path";
/// Bridge error code when note-get read fails.
pub const BRIDGE_ERROR_NOTE_GET_READ_FAILED: &str = "bridge.note_get.read_failed";
/// Bridge error code when note-get parse fails.
pub const BRIDGE_ERROR_NOTE_GET_PARSE_FAILED: &str = "bridge.note_get.parse_failed";
/// Bridge error code when notes-list limit is invalid.
pub const BRIDGE_ERROR_NOTES_LIST_INVALID_LIMIT: &str = "bridge.notes_list.invalid_limit";
/// Bridge error code when notes-list query fails.
pub const BRIDGE_ERROR_NOTES_LIST_QUERY_FAILED: &str = "bridge.notes_list.query_failed";
/// Bridge error code when note-links path is invalid.
pub const BRIDGE_ERROR_NOTE_LINKS_INVALID_PATH: &str = "bridge.note_links.invalid_path";
/// Bridge error code when note-links source lookup fails.
pub const BRIDGE_ERROR_NOTE_LINKS_LOOKUP_FAILED: &str = "bridge.note_links.lookup_failed";
/// Bridge error code when note-links source note is missing.
pub const BRIDGE_ERROR_NOTE_LINKS_NOT_FOUND: &str = "bridge.note_links.not_found";
/// Bridge error code when note-links query fails.
pub const BRIDGE_ERROR_NOTE_LINKS_QUERY_FAILED: &str = "bridge.note_links.query_failed";
/// Bridge error code when note-context envelope assembly fails unexpectedly.
pub const BRIDGE_ERROR_NOTE_CONTEXT_MISSING_VALUE: &str = "bridge.note_context.missing_value";
/// Bridge error code when bases-list lookup fails.
pub const BRIDGE_ERROR_BASES_LIST_QUERY_FAILED: &str = "bridge.bases_list.query_failed";
/// Bridge error code when bases-list config decode fails.
pub const BRIDGE_ERROR_BASES_LIST_CONFIG_FAILED: &str = "bridge.bases_list.config_failed";
/// Bridge error code when bases-view input is invalid.
pub const BRIDGE_ERROR_BASES_VIEW_INVALID_INPUT: &str = "bridge.bases_view.invalid_input";
/// Bridge error code when bases-view lookup fails.
pub const BRIDGE_ERROR_BASES_VIEW_LOOKUP_FAILED: &str = "bridge.bases_view.lookup_failed";
/// Bridge error code when bases-view target is missing.
pub const BRIDGE_ERROR_BASES_VIEW_NOT_FOUND: &str = "bridge.bases_view.not_found";
/// Bridge error code when bases-view config decode fails.
pub const BRIDGE_ERROR_BASES_VIEW_CONFIG_FAILED: &str = "bridge.bases_view.config_failed";
/// Bridge error code when bases-view plan compilation fails.
pub const BRIDGE_ERROR_BASES_VIEW_PLAN_FAILED: &str = "bridge.bases_view.plan_failed";
/// Bridge error code when bases-view execution fails.
pub const BRIDGE_ERROR_BASES_VIEW_EXECUTE_FAILED: &str = "bridge.bases_view.execute_failed";
/// Bridge error code when note-put path is invalid.
pub const BRIDGE_ERROR_NOTE_PUT_INVALID_PATH: &str = "bridge.note_put.invalid_path";
/// Bridge error code when note-put is attempted while writes are disabled.
pub const BRIDGE_ERROR_NOTE_PUT_WRITE_DISABLED: &str = "bridge.note_put.write_disabled";
/// Bridge error code when note-put lookup fails.
pub const BRIDGE_ERROR_NOTE_PUT_LOOKUP_FAILED: &str = "bridge.note_put.lookup_failed";
/// Bridge error code when note-put create fails.
pub const BRIDGE_ERROR_NOTE_PUT_CREATE_FAILED: &str = "bridge.note_put.create_failed";
/// Bridge error code when note-put update fails.
pub const BRIDGE_ERROR_NOTE_PUT_UPDATE_FAILED: &str = "bridge.note_put.update_failed";
/// Bridge error code when note-put index refresh fails.
pub const BRIDGE_ERROR_NOTE_PUT_INDEX_FAILED: &str = "bridge.note_put.index_failed";
/// Bridge error code when note-put event persistence fails.
pub const BRIDGE_ERROR_NOTE_PUT_EVENT_LOG_FAILED: &str = "bridge.note_put.event_log_failed";
/// Bridge error code when events-poll limit is invalid.
pub const BRIDGE_ERROR_EVENTS_POLL_INVALID_LIMIT: &str = "bridge.events_poll.invalid_limit";
/// Bridge error code when events-poll database query fails.
pub const BRIDGE_ERROR_EVENTS_POLL_FAILED: &str = "bridge.events_poll.failed";
/// Bridge error code when startup bundle sync or query fails.
pub const BRIDGE_ERROR_STARTUP_BUNDLE_FAILED: &str = "bridge.startup_bundle.failed";
/// Bridge error code when serialization fails.
pub const BRIDGE_ERROR_SERIALIZE_FAILED: &str = "bridge.serialize.failed";

/// Parsed bridge schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BridgeSchemaVersion {
    /// Major schema version.
    pub major: u16,
    /// Minor schema version.
    pub minor: u16,
}

/// Parse bridge schema version from `v<major>[.<minor>]`.
#[must_use]
pub fn parse_bridge_schema_version(raw: &str) -> Option<BridgeSchemaVersion> {
    let trimmed = raw.trim();
    let without_prefix = trimmed.strip_prefix('v')?;
    let (major_raw, minor_raw) = match without_prefix.split_once('.') {
        Some((major, minor)) => (major, minor),
        None => (without_prefix, "0"),
    };

    if major_raw.is_empty() || minor_raw.is_empty() {
        return None;
    }

    let major = major_raw.parse::<u16>().ok()?;
    let minor = minor_raw.parse::<u16>().ok()?;
    Some(BridgeSchemaVersion { major, minor })
}

/// Return whether the provided schema version is compatible with the current bridge.
#[must_use]
pub fn is_bridge_schema_compatible(raw: &str) -> bool {
    parse_bridge_schema_version(raw).is_some_and(|version| version.major == BRIDGE_SCHEMA_MAJOR)
}

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

    /// Return whether the envelope schema version is compatible with this bridge client.
    #[must_use]
    pub fn schema_compatible(&self) -> bool {
        is_bridge_schema_compatible(&self.schema_version)
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

/// Bridge note summary DTO for paged list endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeNoteSummary {
    /// Stable file id.
    pub file_id: String,
    /// Canonical normalized path.
    pub path: String,
    /// Derived display title.
    pub title: String,
    /// Last updated timestamp when available.
    pub updated_at: Option<String>,
}

/// Bridge paged notes list DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeNoteListPage {
    /// Windowed note summaries.
    pub items: Vec<BridgeNoteSummary>,
    /// Cursor for the next page when available.
    pub next_cursor: Option<String>,
}

/// Bridge link reference row for outgoing/backlink panels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeLinkRef {
    /// Source normalized path.
    pub source_path: String,
    /// Resolved target path when available.
    pub target_path: Option<String>,
    /// Optional heading slug target.
    pub heading: Option<String>,
    /// Optional block id target.
    pub block_id: Option<String>,
    /// Optional display text.
    pub display_text: Option<String>,
    /// Link kind label.
    pub kind: String,
    /// Whether target is resolved.
    pub resolved: bool,
}

/// Bridge links payload containing outgoing and backlink panels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeLinkPanels {
    /// Outgoing links from selected note.
    pub outgoing: Vec<BridgeLinkRef>,
    /// Backlinks into selected note.
    pub backlinks: Vec<BridgeLinkRef>,
}

/// Bridge note context payload combining note content and link panels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeNoteContext {
    /// Note read payload.
    pub note: BridgeNoteView,
    /// Outgoing/backlink graph payload.
    pub links: BridgeLinkPanels,
}

/// Base metadata reference row exposed by `bases-list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeBaseRef {
    /// Stable base identifier.
    pub base_id: String,
    /// Normalized base file path.
    pub file_path: String,
    /// Available view names defined in config.
    pub views: Vec<String>,
    /// Last base metadata update timestamp.
    pub updated_at: String,
}

/// Base table column metadata for one view page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeBaseColumn {
    /// Stable column key.
    pub key: String,
    /// Optional display label.
    pub label: Option<String>,
    /// Column hidden state.
    pub hidden: bool,
    /// Optional column width hint.
    pub width: Option<u16>,
}

/// Base table row payload for UI rendering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeBaseTableRow {
    /// Stable file id.
    pub file_id: String,
    /// Normalized source file path.
    pub file_path: String,
    /// Display-friendly cell values keyed by column key.
    pub values: BTreeMap<String, String>,
}

/// Paged base table result payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeBaseTablePage {
    /// Base identifier used by query.
    pub base_id: String,
    /// Base file path used by query.
    pub file_path: String,
    /// View name compiled by planner.
    pub view_name: String,
    /// One-based page number.
    pub page: u32,
    /// Requested page size.
    pub page_size: u32,
    /// Total matching rows before pagination.
    pub total: u64,
    /// Whether additional pages remain after this page.
    pub has_more: bool,
    /// Column metadata for table rendering.
    pub columns: Vec<BridgeBaseColumn>,
    /// Paged row payloads.
    pub rows: Vec<BridgeBaseTableRow>,
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
    /// Whether index refresh completed successfully.
    pub index_synced: bool,
    /// Whether event logging completed successfully.
    pub event_logged: bool,
    /// Follow-up warnings emitted after the file write succeeded.
    pub warnings: Vec<String>,
}

/// Bridge event item exposed to Swift subscribers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeEvent {
    /// Monotonic event identifier.
    pub id: u64,
    /// Stable event kind.
    pub kind: String,
    /// Event source file id when applicable.
    pub file_id: Option<String>,
    /// Event source normalized path when applicable.
    pub path: Option<String>,
    /// Event action label when applicable.
    pub action: Option<String>,
    /// Event timestamp in UTC.
    pub created_at: String,
}

/// Bridge event batch and cursor for polling subscriptions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeEventBatch {
    /// Ordered list of events after the requested cursor.
    pub events: Vec<BridgeEvent>,
    /// Next cursor value to continue polling.
    pub next_cursor: u64,
}

/// Bridge runtime kernel with vault root and opened SQLite connection.
#[derive(Debug)]
pub struct BridgeKernel {
    vault_root: PathBuf,
    case_policy: CasePolicy,
    connection: Connection,
    parser: MarkdownParser,
}

impl BridgeKernel {
    /// Open bridge runtime with vault root and sqlite database path.
    pub fn open(
        vault_root: impl AsRef<Path>,
        db_path: impl AsRef<Path>,
    ) -> Result<Self, BridgeInitError> {
        Self::open_with_case_policy(vault_root, db_path, CasePolicy::Sensitive)
    }

    /// Open bridge runtime with explicit path case policy.
    pub fn open_with_case_policy(
        vault_root: impl AsRef<Path>,
        db_path: impl AsRef<Path>,
        case_policy: CasePolicy,
    ) -> Result<Self, BridgeInitError> {
        let vault_root = vault_root.as_ref().to_path_buf();
        if !vault_root.exists() {
            return Err(BridgeInitError::VaultRootMissing { vault_root });
        }

        if let Some(parent) = db_path.as_ref().parent() {
            fs::create_dir_all(parent).map_err(|source| BridgeInitError::CreateDbParent {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let mut connection =
            Connection::open(db_path).map_err(|source| BridgeInitError::OpenDb { source })?;
        run_migrations(&mut connection)
            .map_err(|source| BridgeInitError::RunMigrations { source })?;
        ensure_bridge_event_log(&connection)
            .map_err(|source| BridgeInitError::InitEventLog { source })?;

        Ok(Self {
            vault_root,
            case_policy,
            connection,
            parser: MarkdownParser,
        })
    }

    /// Return bridge schema version.
    #[must_use]
    pub fn schema_version(&self) -> &'static str {
        BRIDGE_SCHEMA_VERSION
    }

    /// Ensure bridge index is synchronized with the current vault filesystem.
    pub fn ensure_indexed(&mut self) -> Result<bool, BridgeEnsureIndexError> {
        let existing_markdown_rows = self
            .connection
            .query_row(
                "SELECT COUNT(1) FROM files WHERE is_markdown = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|source| BridgeEnsureIndexError::CountIndexedFiles { source })?;

        if existing_markdown_rows > 0 {
            let reconcile = ReconciliationScannerService::default()
                .scan_and_repair(
                    &self.vault_root,
                    &mut self.connection,
                    self.case_policy,
                    128,
                )
                .map_err(|source| BridgeEnsureIndexError::ReconcileIndex { source })?;
            return Ok(reconcile.drift_paths > 0);
        }

        FullIndexService::default()
            .rebuild(&self.vault_root, &mut self.connection, self.case_policy)
            .map_err(|source| BridgeEnsureIndexError::RebuildIndex { source })?;

        Ok(true)
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
                BridgeError::with_code(BRIDGE_ERROR_VAULT_STATS_FAILED, source.to_string())
                    .with_hint("ensure vault path and sqlite database are readable"),
            ),
        }
    }

    /// Return parsed note payload for one normalized path.
    #[must_use]
    pub fn note_get(&self, normalized_path: &str) -> BridgeEnvelope<BridgeNoteView> {
        let normalized_path = normalized_path.trim();
        let absolute_path = match resolve_bridge_note_read_path(&self.vault_root, normalized_path) {
            Ok(path) => path,
            Err(error) => return BridgeEnvelope::failure(error),
        };
        let raw = match fs::read_to_string(&absolute_path) {
            Ok(raw) => raw,
            Err(source) => {
                return BridgeEnvelope::failure(
                    BridgeError::with_code(BRIDGE_ERROR_NOTE_GET_READ_FAILED, source.to_string())
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
                BridgeError::with_code(BRIDGE_ERROR_NOTE_GET_PARSE_FAILED, source.to_string())
                    .with_hint("fix note markdown syntax issues and retry"),
            ),
        }
    }

    /// Return one paged window of markdown note summaries.
    #[must_use]
    pub fn notes_list(
        &self,
        after_path: Option<&str>,
        limit: u64,
    ) -> BridgeEnvelope<BridgeNoteListPage> {
        if limit == 0 || limit > 1_000 {
            return BridgeEnvelope::failure(
                BridgeError::with_code(
                    BRIDGE_ERROR_NOTES_LIST_INVALID_LIMIT,
                    "limit must be between 1 and 1000",
                )
                .with_hint("set `limit` to one value in range [1, 1000]")
                .with_context("limit", JsonValue::String(limit.to_string())),
            );
        }

        match query_note_summaries_page(&self.connection, after_path, limit) {
            Ok(page) => BridgeEnvelope::success(page),
            Err(source) => {
                let mut error = BridgeError::with_code(
                    BRIDGE_ERROR_NOTES_LIST_QUERY_FAILED,
                    source.to_string(),
                )
                .with_hint("ensure bridge database is available");
                if let Some(after_path) = after_path {
                    error =
                        error.with_context("after_path", JsonValue::String(after_path.to_string()));
                }
                BridgeEnvelope::failure(
                    error.with_context("limit", JsonValue::String(limit.to_string())),
                )
            }
        }
    }

    /// Return outgoing/backlink panels for one note by normalized path.
    #[must_use]
    pub fn note_links(&self, normalized_path: &str) -> BridgeEnvelope<BridgeLinkPanels> {
        let normalized_path = normalized_path.trim();
        if normalized_path.is_empty() {
            return BridgeEnvelope::failure(
                BridgeError::with_code(
                    BRIDGE_ERROR_NOTE_LINKS_INVALID_PATH,
                    "normalized path must not be empty",
                )
                .with_hint("provide a vault-relative markdown path"),
            );
        }

        let source =
            match FilesRepository::get_by_normalized_path(&self.connection, normalized_path) {
                Ok(Some(file)) => file,
                Ok(None) => {
                    return BridgeEnvelope::failure(
                        BridgeError::with_code(
                            BRIDGE_ERROR_NOTE_LINKS_NOT_FOUND,
                            "note is not indexed for links lookup",
                        )
                        .with_hint("reindex the vault and retry")
                        .with_context("path", JsonValue::String(normalized_path.to_string())),
                    );
                }
                Err(source) => {
                    return BridgeEnvelope::failure(
                        BridgeError::with_code(
                            BRIDGE_ERROR_NOTE_LINKS_LOOKUP_FAILED,
                            source.to_string(),
                        )
                        .with_hint("ensure bridge database is available")
                        .with_context("path", JsonValue::String(normalized_path.to_string())),
                    );
                }
            };

        let outgoing =
            match LinksRepository::list_outgoing_with_paths(&self.connection, &source.file_id) {
                Ok(rows) => rows.into_iter().map(map_link_with_paths).collect(),
                Err(source) => {
                    return BridgeEnvelope::failure(
                        BridgeError::with_code(
                            BRIDGE_ERROR_NOTE_LINKS_QUERY_FAILED,
                            source.to_string(),
                        )
                        .with_hint("ensure links index tables are readable")
                        .with_context("path", JsonValue::String(normalized_path.to_string())),
                    );
                }
            };

        let backlinks =
            match LinksRepository::list_backlinks_with_paths(&self.connection, &source.file_id) {
                Ok(rows) => rows.into_iter().map(map_link_with_paths).collect(),
                Err(source) => {
                    return BridgeEnvelope::failure(
                        BridgeError::with_code(
                            BRIDGE_ERROR_NOTE_LINKS_QUERY_FAILED,
                            source.to_string(),
                        )
                        .with_hint("ensure links index tables are readable")
                        .with_context("path", JsonValue::String(normalized_path.to_string())),
                    );
                }
            };

        BridgeEnvelope::success(BridgeLinkPanels {
            outgoing,
            backlinks,
        })
    }

    /// Return note payload and link panels in one bridge response.
    #[must_use]
    pub fn note_context(&self, normalized_path: &str) -> BridgeEnvelope<BridgeNoteContext> {
        let note = match self.note_get(normalized_path) {
            BridgeEnvelope {
                ok: true,
                value: Some(value),
                ..
            } => value,
            BridgeEnvelope {
                ok: false,
                error: Some(error),
                ..
            } => return BridgeEnvelope::failure(error),
            _ => {
                return BridgeEnvelope::failure(BridgeError::with_code(
                    BRIDGE_ERROR_NOTE_CONTEXT_MISSING_VALUE,
                    "note context note payload missing",
                ));
            }
        };

        let links = match self.note_links(normalized_path) {
            BridgeEnvelope {
                ok: true,
                value: Some(value),
                ..
            } => value,
            BridgeEnvelope {
                ok: false,
                error: Some(error),
                ..
            } => return BridgeEnvelope::failure(error),
            _ => {
                return BridgeEnvelope::failure(BridgeError::with_code(
                    BRIDGE_ERROR_NOTE_CONTEXT_MISSING_VALUE,
                    "note context links payload missing",
                ));
            }
        };

        BridgeEnvelope::success(BridgeNoteContext { note, links })
    }

    /// List indexed bases with available view names.
    #[must_use]
    pub fn bases_list(&self) -> BridgeEnvelope<Vec<BridgeBaseRef>> {
        let bases = match BasesRepository::list_with_paths(&self.connection) {
            Ok(bases) => bases,
            Err(source) => {
                return BridgeEnvelope::failure(
                    BridgeError::with_code(
                        BRIDGE_ERROR_BASES_LIST_QUERY_FAILED,
                        source.to_string(),
                    )
                    .with_hint("ensure base metadata is indexed and sqlite is readable"),
                );
            }
        };

        let mut refs = Vec::with_capacity(bases.len());
        for base in bases {
            let document = match decode_base_document(&base.config_json) {
                Ok(document) => document,
                Err(source) => {
                    return BridgeEnvelope::failure(
                        BridgeError::with_code(
                            BRIDGE_ERROR_BASES_LIST_CONFIG_FAILED,
                            source.to_string(),
                        )
                        .with_hint("reindex bases to repair malformed config payloads")
                        .with_context("base_id", JsonValue::String(base.base_id))
                        .with_context("file_path", JsonValue::String(base.file_path)),
                    );
                }
            };

            let views = document
                .views
                .into_iter()
                .map(|view| view.name)
                .collect::<Vec<_>>();
            refs.push(BridgeBaseRef {
                base_id: base.base_id,
                file_path: base.file_path,
                views,
                updated_at: base.updated_at,
            });
        }

        BridgeEnvelope::success(refs)
    }

    /// Execute one base table view page by base id/path and view name.
    #[must_use]
    pub fn bases_view(
        &self,
        path_or_id: &str,
        view_name: &str,
        page: u32,
        page_size: u32,
    ) -> BridgeEnvelope<BridgeBaseTablePage> {
        let path_or_id = path_or_id.trim();
        let view_name = view_name.trim();
        if path_or_id.is_empty() || view_name.is_empty() || page == 0 || page_size == 0 {
            return BridgeEnvelope::failure(
                BridgeError::with_code(
                    BRIDGE_ERROR_BASES_VIEW_INVALID_INPUT,
                    "path_or_id, view_name, page, and page_size must be valid",
                )
                .with_hint("provide non-empty path_or_id/view_name and pagination >= 1")
                .with_context("path_or_id", JsonValue::String(path_or_id.to_string()))
                .with_context("view_name", JsonValue::String(view_name.to_string()))
                .with_context("page", JsonValue::String(page.to_string()))
                .with_context("page_size", JsonValue::String(page_size.to_string())),
            );
        }
        if page_size > 1_000 {
            return BridgeEnvelope::failure(
                BridgeError::with_code(
                    BRIDGE_ERROR_BASES_VIEW_INVALID_INPUT,
                    "page_size must be between 1 and 1000",
                )
                .with_hint("set page_size in range [1, 1000]")
                .with_context("page_size", JsonValue::String(page_size.to_string())),
            );
        }

        let Some(base) = (match resolve_base_by_id_or_path(&self.connection, path_or_id) {
            Ok(base) => base,
            Err(source) => {
                return BridgeEnvelope::failure(
                    BridgeError::with_code(
                        BRIDGE_ERROR_BASES_VIEW_LOOKUP_FAILED,
                        source.to_string(),
                    )
                    .with_hint("ensure bases metadata is indexed and readable")
                    .with_context("path_or_id", JsonValue::String(path_or_id.to_string())),
                );
            }
        }) else {
            return BridgeEnvelope::failure(
                BridgeError::with_code(BRIDGE_ERROR_BASES_VIEW_NOT_FOUND, "base id/path not found")
                    .with_hint("call bases-list to discover valid base ids and paths")
                    .with_context("path_or_id", JsonValue::String(path_or_id.to_string())),
            );
        };

        let document = match decode_base_document(&base.config_json) {
            Ok(document) => document,
            Err(source) => {
                return BridgeEnvelope::failure(
                    BridgeError::with_code(
                        BRIDGE_ERROR_BASES_VIEW_CONFIG_FAILED,
                        source.to_string(),
                    )
                    .with_hint("reindex bases to repair malformed config payloads")
                    .with_context("base_id", JsonValue::String(base.base_id.clone()))
                    .with_context("file_path", JsonValue::String(base.file_path.clone())),
                );
            }
        };
        let registry = match BaseViewRegistry::from_document(&document) {
            Ok(registry) => registry,
            Err(source) => {
                return BridgeEnvelope::failure(
                    BridgeError::with_code(
                        BRIDGE_ERROR_BASES_VIEW_CONFIG_FAILED,
                        source.to_string(),
                    )
                    .with_hint("fix duplicate/invalid base view definitions and reindex")
                    .with_context("base_id", JsonValue::String(base.base_id.clone()))
                    .with_context("file_path", JsonValue::String(base.file_path.clone())),
                );
            }
        };

        let planner = BaseTableQueryPlanner;
        let plan = match planner.compile(
            &registry,
            &TableQueryPlanRequest {
                view_name: view_name.to_string(),
                page,
                page_size,
            },
        ) {
            Ok(plan) => plan,
            Err(source) => {
                return BridgeEnvelope::failure(
                    BridgeError::with_code(BRIDGE_ERROR_BASES_VIEW_PLAN_FAILED, source.to_string())
                        .with_hint("verify the requested view exists and pagination is valid")
                        .with_context("base_id", JsonValue::String(base.base_id.clone()))
                        .with_context("file_path", JsonValue::String(base.file_path.clone()))
                        .with_context("view_name", JsonValue::String(view_name.to_string()))
                        .with_context("page", JsonValue::String(page.to_string()))
                        .with_context("page_size", JsonValue::String(page_size.to_string())),
                );
            }
        };

        let page_result = match BaseTableExecutorService.execute(&self.connection, &plan) {
            Ok(page_result) => page_result,
            Err(source) => {
                return BridgeEnvelope::failure(
                    BridgeError::with_code(
                        BRIDGE_ERROR_BASES_VIEW_EXECUTE_FAILED,
                        source.to_string(),
                    )
                    .with_hint("ensure property/files metadata tables are healthy")
                    .with_context("base_id", JsonValue::String(base.base_id.clone()))
                    .with_context("file_path", JsonValue::String(base.file_path.clone()))
                    .with_context("view_name", JsonValue::String(plan.view_name.clone())),
                );
            }
        };

        let offset = u64::try_from(plan.offset).unwrap_or(u64::MAX);
        let returned = page_result.rows.len() as u64;
        let has_more = page_result.total > offset.saturating_add(returned);
        let rows = page_result
            .rows
            .into_iter()
            .map(|row| BridgeBaseTableRow {
                file_id: row.file_id,
                file_path: row.file_path,
                values: row
                    .values
                    .into_iter()
                    .map(|(key, value)| (key, json_value_to_display(value)))
                    .collect(),
            })
            .collect::<Vec<_>>();
        let columns = plan
            .columns
            .iter()
            .map(|column| BridgeBaseColumn {
                key: column.key.clone(),
                label: column.label.clone(),
                hidden: column.hidden,
                width: column.width,
            })
            .collect::<Vec<_>>();

        BridgeEnvelope::success(BridgeBaseTablePage {
            base_id: base.base_id,
            file_path: base.file_path,
            view_name: plan.view_name,
            page,
            page_size,
            total: page_result.total,
            has_more,
            columns,
            rows,
        })
    }

    /// Create or update one note.
    ///
    /// Write operations are disabled by default. Use `note_put_with_policy(..., true)` to enable.
    #[must_use]
    pub fn note_put(
        &mut self,
        normalized_path: &str,
        content: &str,
    ) -> BridgeEnvelope<BridgeWriteAck> {
        self.note_put_with_policy(normalized_path, content, false)
    }

    /// Create or update one note with explicit write policy.
    #[must_use]
    pub fn note_put_with_policy(
        &mut self,
        normalized_path: &str,
        content: &str,
        allow_writes: bool,
    ) -> BridgeEnvelope<BridgeWriteAck> {
        if !allow_writes {
            return BridgeEnvelope::failure(
                BridgeError::with_code(
                    BRIDGE_ERROR_NOTE_PUT_WRITE_DISABLED,
                    "bridge note_put is disabled by default",
                )
                .with_hint("set allow_writes=true to enable vault content mutations"),
            );
        }

        let normalized_path = normalized_path.trim();
        if normalized_path.is_empty() {
            return BridgeEnvelope::failure(
                BridgeError::with_code(
                    BRIDGE_ERROR_NOTE_PUT_INVALID_PATH,
                    "normalized path must not be empty",
                )
                .with_hint("provide a vault-relative markdown path"),
            );
        }
        if let Err(error) = validate_bridge_relative_note_path(
            normalized_path,
            BRIDGE_ERROR_NOTE_PUT_INVALID_PATH,
            "provide a vault-relative markdown path",
        ) {
            return BridgeEnvelope::failure(error);
        }

        let note_service = NoteCrudService::with_case_policy(self.case_policy);
        let relative = Path::new(normalized_path);
        let existing =
            match FilesRepository::get_by_normalized_path(&self.connection, normalized_path) {
                Ok(existing) => existing,
                Err(source) => {
                    return BridgeEnvelope::failure(
                        BridgeError::with_code(
                            BRIDGE_ERROR_NOTE_PUT_LOOKUP_FAILED,
                            source.to_string(),
                        )
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
                Ok(result) => complete_note_put(self, result, "updated"),
                Err(source) => BridgeEnvelope::failure(
                    BridgeError::with_code(BRIDGE_ERROR_NOTE_PUT_UPDATE_FAILED, source.to_string())
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
                Ok(result) => complete_note_put(self, result, "created"),
                Err(source) => BridgeEnvelope::failure(
                    BridgeError::with_code(BRIDGE_ERROR_NOTE_PUT_CREATE_FAILED, source.to_string())
                        .with_hint("ensure vault path exists and target note path is valid"),
                ),
            }
        }
    }

    /// Poll bridge events after one cursor value.
    #[must_use]
    pub fn events_poll(&self, after_id: u64, limit: u64) -> BridgeEnvelope<BridgeEventBatch> {
        if limit == 0 || limit > 1_000 {
            return BridgeEnvelope::failure(
                BridgeError::with_code(
                    BRIDGE_ERROR_EVENTS_POLL_INVALID_LIMIT,
                    "limit must be between 1 and 1000",
                )
                .with_hint("set `limit` to one value in range [1, 1000]")
                .with_context("limit", JsonValue::String(limit.to_string())),
            );
        }

        match poll_bridge_events(&self.connection, after_id, limit) {
            Ok(batch) => BridgeEnvelope::success(batch),
            Err(source) => BridgeEnvelope::failure(
                BridgeError::with_code(BRIDGE_ERROR_EVENTS_POLL_FAILED, source.to_string())
                    .with_hint("ensure bridge database is readable")
                    .with_context("after_id", JsonValue::String(after_id.to_string()))
                    .with_context("limit", JsonValue::String(limit.to_string())),
            ),
        }
    }
}

fn query_note_summaries_page(
    connection: &Connection,
    after_path: Option<&str>,
    limit: u64,
) -> Result<BridgeNoteListPage, rusqlite::Error> {
    let limit_plus_one = limit.saturating_add(1);
    let limit_plus_one_i64 = i64::try_from(limit_plus_one).unwrap_or(i64::MAX);

    let mut statement = connection.prepare(
        "SELECT file_id, normalized_path, indexed_at
         FROM files
         WHERE is_markdown = 1
           AND (?1 IS NULL OR normalized_path > ?1)
         ORDER BY normalized_path ASC
         LIMIT ?2",
    )?;

    let rows = statement.query_map(params![after_path, limit_plus_one_i64], |row| {
        let file_id: String = row.get(0)?;
        let normalized_path: String = row.get(1)?;
        let indexed_at: String = row.get(2)?;
        let title = Path::new(&normalized_path)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(ToString::to_string)
            .unwrap_or_else(|| normalized_path.clone());
        Ok(BridgeNoteSummary {
            file_id,
            path: normalized_path,
            title,
            updated_at: Some(indexed_at),
        })
    })?;

    let mut items: Vec<BridgeNoteSummary> = rows.collect::<Result<Vec<_>, _>>()?;
    let has_more = items.len() > usize::try_from(limit).unwrap_or(usize::MAX);
    if has_more {
        items.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    }
    let next_cursor = if has_more {
        items.last().map(|item| item.path.clone())
    } else {
        None
    };

    Ok(BridgeNoteListPage { items, next_cursor })
}

fn complete_note_put(
    kernel: &mut BridgeKernel,
    result: tao_sdk_service::NoteCrudResult,
    action: &str,
) -> BridgeEnvelope<BridgeWriteAck> {
    let mut warnings = Vec::new();
    let mut index_synced = true;
    if let Err(source) = IncrementalIndexService::default().apply_changes_force(
        &kernel.vault_root,
        &mut kernel.connection,
        &[PathBuf::from(result.normalized_path.clone())],
        kernel.case_policy,
    ) {
        index_synced = false;
        warnings.push(format!(
            "incremental index refresh failed after write: {source}"
        ));
        if let Ok(reconcile) = ReconciliationScannerService::default().scan_and_repair(
            &kernel.vault_root,
            &mut kernel.connection,
            kernel.case_policy,
            128,
        ) {
            index_synced = true;
            warnings.push(format!(
                "recovered index state via scan_and_repair (drift_paths={})",
                reconcile.drift_paths
            ));
        }
    }

    let event_logged = match append_bridge_note_changed_event(
        &kernel.connection,
        &result.file_id,
        &result.normalized_path,
        action,
    ) {
        Ok(()) => true,
        Err(source) => {
            warnings.push(format!("event logging failed after write: {source}"));
            false
        }
    };

    BridgeEnvelope::success(BridgeWriteAck {
        path: result.normalized_path,
        file_id: result.file_id,
        action: action.to_string(),
        index_synced,
        event_logged,
        warnings,
    })
}

fn resolve_bridge_note_read_path(
    vault_root: &Path,
    normalized_path: &str,
) -> Result<PathBuf, BridgeError> {
    validate_bridge_relative_note_path(
        normalized_path,
        BRIDGE_ERROR_NOTE_GET_INVALID_PATH,
        "provide a vault-relative markdown path",
    )?;

    let joined = vault_root.join(normalized_path);
    if !joined.exists() {
        return Ok(joined);
    }

    let canonical_vault = fs::canonicalize(vault_root).map_err(|source| {
        BridgeError::with_code(
            BRIDGE_ERROR_NOTE_GET_READ_FAILED,
            format!("failed to canonicalize vault root: {source}"),
        )
        .with_hint("ensure vault root is readable")
        .with_context(
            "vault_root",
            JsonValue::String(vault_root.to_string_lossy().to_string()),
        )
    })?;
    let canonical_note = fs::canonicalize(&joined).map_err(|source| {
        BridgeError::with_code(
            BRIDGE_ERROR_NOTE_GET_READ_FAILED,
            format!("failed to canonicalize note path: {source}"),
        )
        .with_hint("ensure note path is readable")
        .with_context(
            "path",
            JsonValue::String(joined.to_string_lossy().to_string()),
        )
    })?;

    if !canonical_note.starts_with(&canonical_vault) {
        return Err(BridgeError::with_code(
            BRIDGE_ERROR_NOTE_GET_INVALID_PATH,
            "normalized path resolves outside vault root",
        )
        .with_hint("provide a vault-relative markdown path inside the selected vault")
        .with_context("path", JsonValue::String(normalized_path.to_string())));
    }

    Ok(joined)
}

fn validate_bridge_relative_note_path(
    normalized_path: &str,
    code: &'static str,
    hint: &'static str,
) -> Result<(), BridgeError> {
    if let Err(source) = validate_relative_vault_path(normalized_path) {
        return Err(BridgeError::with_code(code, source.to_string())
            .with_hint(hint)
            .with_context(
                "path",
                JsonValue::String(normalized_path.trim().to_string()),
            ));
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct ResolvedBridgeBase {
    base_id: String,
    file_path: String,
    config_json: String,
}

fn decode_base_document(config_json: &str) -> Result<BaseDocument, String> {
    if let Ok(document) = serde_json::from_str::<BaseDocument>(config_json) {
        return Ok(document);
    }

    let raw_value =
        serde_json::from_str::<JsonValue>(config_json).map_err(|source| source.to_string())?;
    let Some(raw_yaml) = raw_value.get("raw").and_then(JsonValue::as_str) else {
        return Err("base config json is not a supported document payload".to_string());
    };
    parse_base_document(raw_yaml).map_err(|source| source.to_string())
}

fn resolve_base_by_id_or_path(
    connection: &Connection,
    path_or_id: &str,
) -> Result<Option<ResolvedBridgeBase>, tao_sdk_storage::BasesRepositoryError> {
    let target = path_or_id.trim();
    let bases = BasesRepository::list_with_paths(connection)?;
    Ok(bases.into_iter().find_map(|base| {
        if base.base_id == target || base.file_path == target {
            return Some(ResolvedBridgeBase {
                base_id: base.base_id,
                file_path: base.file_path,
                config_json: base.config_json,
            });
        }
        None
    }))
}

fn json_value_to_display(value: JsonValue) -> String {
    match value {
        JsonValue::Null => String::new(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => value,
        JsonValue::Array(value) => {
            serde_json::to_string(&value).unwrap_or_else(|_| "[]".to_string())
        }
        JsonValue::Object(value) => {
            serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string())
        }
    }
}

fn map_link_with_paths(row: LinkWithPaths) -> BridgeLinkRef {
    let kind = if row.block_id.is_some() {
        "block"
    } else if row.heading_slug.is_some() {
        "heading"
    } else if row.source_field == "body:embed" {
        "embed"
    } else if row.source_field == "body:markdown" {
        "markdown"
    } else {
        "wikilink"
    };

    BridgeLinkRef {
        source_path: row.source_path,
        target_path: row.resolved_path,
        heading: row.heading_slug,
        block_id: row.block_id,
        display_text: None,
        kind: kind.to_string(),
        resolved: !row.is_unresolved && row.resolved_file_id.is_some(),
    }
}

fn ensure_bridge_event_log(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS bridge_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );
        CREATE INDEX IF NOT EXISTS idx_bridge_events_id ON bridge_events(id);",
    )
}

fn append_bridge_note_changed_event(
    connection: &Connection,
    file_id: &str,
    path: &str,
    action: &str,
) -> Result<(), rusqlite::Error> {
    let payload = serde_json::json!({
        "file_id": file_id,
        "path": path,
        "action": action
    });
    connection.execute(
        "INSERT INTO bridge_events (event_type, payload_json) VALUES (?1, ?2)",
        params!["note_changed", payload.to_string()],
    )?;
    Ok(())
}

fn poll_bridge_events(
    connection: &Connection,
    after_id: u64,
    limit: u64,
) -> Result<BridgeEventBatch, rusqlite::Error> {
    let after_id_i64 = i64::try_from(after_id).unwrap_or(i64::MAX);
    let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);

    let mut statement = connection.prepare(
        "SELECT id, event_type, payload_json, created_at
         FROM bridge_events
         WHERE id > ?1
         ORDER BY id ASC
         LIMIT ?2",
    )?;

    let rows = statement.query_map(params![after_id_i64, limit_i64], |row| {
        let id: u64 = row.get(0)?;
        let kind: String = row.get(1)?;
        let payload_raw: String = row.get(2)?;
        let created_at: String = row.get(3)?;

        let payload = serde_json::from_str::<JsonValue>(&payload_raw).unwrap_or(JsonValue::Null);
        let file_id = payload
            .get("file_id")
            .and_then(JsonValue::as_str)
            .map(ToString::to_string);
        let path = payload
            .get("path")
            .and_then(JsonValue::as_str)
            .map(ToString::to_string);
        let action = payload
            .get("action")
            .and_then(JsonValue::as_str)
            .map(ToString::to_string);

        Ok(BridgeEvent {
            id,
            kind,
            file_id,
            path,
            action,
            created_at,
        })
    })?;

    let mut events = Vec::new();
    let mut next_cursor = after_id;
    for event in rows {
        let event = event?;
        if event.id > next_cursor {
            next_cursor = event.id;
        }
        events.push(event);
    }

    Ok(BridgeEventBatch {
        events,
        next_cursor,
    })
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
    /// Creating sqlite parent directory failed.
    #[error("failed to create bridge sqlite parent directory '{path}': {source}")]
    CreateDbParent {
        /// Parent directory path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
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
        source: tao_sdk_storage::MigrationRunnerError,
    },
    /// Initializing bridge event log table failed.
    #[error("failed to initialize bridge event log schema: {source}")]
    InitEventLog {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
}

/// Bridge index bootstrap failures.
#[derive(Debug, Error)]
pub enum BridgeEnsureIndexError {
    /// Counting indexed markdown rows failed.
    #[error("failed to count indexed markdown rows: {source}")]
    CountIndexedFiles {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Full index rebuild failed during bootstrap.
    #[error("failed to rebuild index: {source}")]
    RebuildIndex {
        /// Full index service error.
        #[source]
        source: tao_sdk_service::FullIndexError,
    },
    /// Reconciliation refresh failed during bootstrap.
    #[error("failed to reconcile index state: {source}")]
    ReconcileIndex {
        /// Reconciliation scanner error.
        #[source]
        source: tao_sdk_service::ReconciliationScanError,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tao_sdk_bases::parse_base_document;
    use tao_sdk_storage::{
        BaseRecordInput, BasesRepository, FileRecordInput, FilesRepository, LinkRecordInput,
        LinksRepository,
    };
    use tempfile::tempdir;

    use super::{
        BRIDGE_ERROR_NOTE_GET_INVALID_PATH, BRIDGE_ERROR_NOTE_PUT_WRITE_DISABLED,
        BRIDGE_SCHEMA_VERSION, BridgeEnvelope, BridgeKernel, BridgePing, BridgeSchemaVersion,
        BridgeWriteAck, is_bridge_schema_compatible, parse_bridge_schema_version,
    };

    fn note_put_allowed(
        kernel: &mut BridgeKernel,
        normalized_path: &str,
        content: &str,
    ) -> BridgeEnvelope<BridgeWriteAck> {
        kernel.note_put_with_policy(normalized_path, content, true)
    }

    #[test]
    fn schema_version_parser_and_compatibility_checks_are_stable() {
        assert_eq!(
            parse_bridge_schema_version("v1"),
            Some(BridgeSchemaVersion { major: 1, minor: 0 })
        );
        assert_eq!(
            parse_bridge_schema_version("v1.7"),
            Some(BridgeSchemaVersion { major: 1, minor: 7 })
        );
        assert_eq!(
            parse_bridge_schema_version(" v3.14 "),
            Some(BridgeSchemaVersion {
                major: 3,
                minor: 14
            })
        );
        assert_eq!(parse_bridge_schema_version("v1."), None);
        assert_eq!(parse_bridge_schema_version("v1.alpha"), None);
        assert_eq!(parse_bridge_schema_version("1.0"), None);
        assert_eq!(parse_bridge_schema_version(""), None);

        assert!(is_bridge_schema_compatible("v1"));
        assert!(is_bridge_schema_compatible("v1.99"));
        assert!(!is_bridge_schema_compatible("v2"));
        assert!(!is_bridge_schema_compatible("1"));
    }

    #[test]
    fn bridge_envelope_exposes_schema_compatibility_check() {
        let compatible = BridgeEnvelope::success(BridgePing {
            message: "ok".to_string(),
        });
        assert!(compatible.schema_compatible());

        let incompatible = BridgeEnvelope::<BridgePing> {
            schema_version: "v2.0".to_string(),
            ok: true,
            value: Some(BridgePing {
                message: "ok".to_string(),
            }),
            error: None,
        };
        assert!(!incompatible.schema_compatible());
    }

    #[test]
    fn bridge_kernel_exposes_schema_version_and_ping() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(&vault_root).expect("create vault");
        let db_path = temp.path().join("tao.db");

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
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        kernel.ensure_indexed().expect("ensure indexed");
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
        let db_path = temp.path().join("tao.db");

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
    fn bridge_kernel_note_get_rejects_absolute_and_parent_paths() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(vault_root.join("notes/a.md"), "# A").expect("write markdown");
        let db_path = temp.path().join("tao.db");

        let kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let absolute = kernel.note_get("/etc/hosts");
        assert!(!absolute.ok);
        let absolute_error = absolute.error.expect("absolute path error");
        assert_eq!(absolute_error.code, BRIDGE_ERROR_NOTE_GET_INVALID_PATH);

        let parent = kernel.note_get("../notes/a.md");
        assert!(!parent.ok);
        let parent_error = parent.error.expect("parent path error");
        assert_eq!(parent_error.code, BRIDGE_ERROR_NOTE_GET_INVALID_PATH);
    }

    #[test]
    fn bridge_kernel_note_put_is_denied_by_default() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let denied = kernel.note_put("notes/a.md", "# A");
        assert!(!denied.ok);
        let error = denied.error.expect("write gate error");
        assert_eq!(error.code, BRIDGE_ERROR_NOTE_PUT_WRITE_DISABLED);
    }

    #[test]
    fn bridge_kernel_notes_list_pages_markdown_results() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        assert!(note_put_allowed(&mut kernel, "notes/c.md", "# C").ok);
        assert!(note_put_allowed(&mut kernel, "notes/a.md", "# A").ok);
        assert!(note_put_allowed(&mut kernel, "notes/b.md", "# B").ok);

        let first_page = kernel.notes_list(None, 2);
        assert!(first_page.ok);
        let first = first_page.value.expect("first page");
        assert_eq!(first.items.len(), 2);
        assert_eq!(first.items[0].path, "notes/a.md");
        assert_eq!(first.items[1].path, "notes/b.md");
        assert_eq!(first.next_cursor.as_deref(), Some("notes/b.md"));

        let second_page = kernel.notes_list(first.next_cursor.as_deref(), 2);
        assert!(second_page.ok);
        let second = second_page.value.expect("second page");
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.items[0].path, "notes/c.md");
        assert_eq!(second.next_cursor, None);
    }

    #[test]
    fn bridge_kernel_note_put_creates_and_updates_notes() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let created = note_put_allowed(&mut kernel, "notes/a.md", "# A\nfirst");
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

        let updated = note_put_allowed(&mut kernel, "notes/a.md", "# A\nsecond");
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

    #[test]
    fn bridge_kernel_note_put_refreshes_link_index_for_updated_content() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        assert!(note_put_allowed(&mut kernel, "notes/target.md", "# Target").ok);
        assert!(note_put_allowed(&mut kernel, "notes/source.md", "# Source").ok);

        let initial_links = kernel.note_links("notes/source.md");
        assert!(initial_links.ok);
        assert!(
            initial_links
                .value
                .expect("initial links")
                .outgoing
                .is_empty()
        );

        let updated = note_put_allowed(&mut kernel, "notes/source.md", "# Source\n[[target]]");
        assert!(updated.ok);

        let refreshed_links = kernel.note_links("notes/source.md");
        assert!(refreshed_links.ok);
        let outgoing = refreshed_links.value.expect("refreshed links").outgoing;
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].target_path.as_deref(), Some("notes/target.md"));
        assert!(outgoing[0].resolved);
    }

    #[test]
    fn bridge_kernel_note_links_returns_outgoing_and_backlinks() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let source = note_put_allowed(&mut kernel, "notes/source.md", "# Source");
        let source_id = source.value.expect("source").file_id;
        let target = note_put_allowed(&mut kernel, "notes/target.md", "# Target");
        let target_id = target.value.expect("target").file_id;
        let incoming = note_put_allowed(&mut kernel, "notes/incoming.md", "# Incoming");
        let incoming_id = incoming.value.expect("incoming").file_id;

        LinksRepository::insert(
            &kernel.connection,
            &LinkRecordInput {
                link_id: "l-outgoing".to_string(),
                source_file_id: source_id.clone(),
                raw_target: "target".to_string(),
                resolved_file_id: Some(target_id.clone()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert outgoing link");

        LinksRepository::insert(
            &kernel.connection,
            &LinkRecordInput {
                link_id: "l-backlink".to_string(),
                source_file_id: incoming_id,
                raw_target: "source".to_string(),
                resolved_file_id: Some(source_id),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert backlink");

        let links = kernel.note_links("notes/source.md");
        assert!(links.ok);
        let value = links.value.expect("links value");
        assert_eq!(value.outgoing.len(), 1);
        assert_eq!(
            value.outgoing[0].target_path.as_deref(),
            Some("notes/target.md")
        );
        assert_eq!(value.backlinks.len(), 1);
        assert_eq!(value.backlinks[0].source_path.as_str(), "notes/incoming.md");
    }

    #[test]
    fn bridge_kernel_note_links_labels_markdown_and_embed_kinds() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/assets")).expect("create notes/assets");
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let source = note_put_allowed(&mut kernel, "notes/source.md", "# Source");
        let source_id = source.value.expect("source").file_id;
        let target_md = note_put_allowed(&mut kernel, "notes/target.md", "# Target");
        let target_md_id = target_md.value.expect("target md").file_id;
        let target_pdf_id = "file-pdf".to_string();
        FilesRepository::insert(
            &kernel.connection,
            &FileRecordInput {
                file_id: target_pdf_id.clone(),
                normalized_path: "notes/assets/deck.pdf".to_string(),
                match_key: "notes/assets/deck.pdf".to_string(),
                absolute_path: vault_root
                    .join("notes/assets/deck.pdf")
                    .to_string_lossy()
                    .to_string(),
                size_bytes: 3,
                modified_unix_ms: 1_700_000_000_000,
                hash_blake3: "hash-pdf".to_string(),
                is_markdown: false,
            },
        )
        .expect("insert target pdf file record");

        LinksRepository::insert(
            &kernel.connection,
            &LinkRecordInput {
                link_id: "l-wikilink".to_string(),
                source_file_id: source_id.clone(),
                raw_target: "target".to_string(),
                resolved_file_id: Some(target_md_id),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert wikilink");
        LinksRepository::insert(
            &kernel.connection,
            &LinkRecordInput {
                link_id: "l-markdown".to_string(),
                source_file_id: source_id.clone(),
                raw_target: "assets/deck.pdf".to_string(),
                resolved_file_id: Some(target_pdf_id.clone()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body:markdown".to_string(),
            },
        )
        .expect("insert markdown link");
        LinksRepository::insert(
            &kernel.connection,
            &LinkRecordInput {
                link_id: "l-embed".to_string(),
                source_file_id: source_id,
                raw_target: "assets/deck.pdf".to_string(),
                resolved_file_id: Some(target_pdf_id),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body:embed".to_string(),
            },
        )
        .expect("insert embed link");

        let links = kernel.note_links("notes/source.md");
        assert!(links.ok);
        let mut outgoing = links.value.expect("links").outgoing;
        outgoing.sort_by(|left, right| left.kind.cmp(&right.kind));
        assert_eq!(outgoing.len(), 3);
        assert_eq!(outgoing[0].kind, "embed");
        assert_eq!(outgoing[1].kind, "markdown");
        assert_eq!(outgoing[2].kind, "wikilink");
    }

    #[test]
    fn bridge_kernel_note_context_returns_note_and_links() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let source = note_put_allowed(&mut kernel, "notes/source.md", "# Source");
        let source_id = source.value.expect("source").file_id;
        let target = note_put_allowed(&mut kernel, "notes/target.md", "# Target");
        let target_id = target.value.expect("target").file_id;

        LinksRepository::insert(
            &kernel.connection,
            &LinkRecordInput {
                link_id: "l-note-context".to_string(),
                source_file_id: source_id,
                raw_target: "target".to_string(),
                resolved_file_id: Some(target_id),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert link");

        let context = kernel.note_context("notes/source.md");
        assert!(context.ok);
        let value = context.value.expect("note context value");
        assert_eq!(value.note.path, "notes/source.md");
        assert_eq!(value.note.title, "Source");
        assert_eq!(value.links.outgoing.len(), 1);
        assert_eq!(
            value.links.outgoing[0].target_path.as_deref(),
            Some("notes/target.md")
        );
    }

    #[test]
    fn bridge_kernel_bases_list_and_view_paginate_rows() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::create_dir_all(vault_root.join("views")).expect("create views");
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        assert!(note_put_allowed(&mut kernel, "notes/a.md", "# A").ok);
        assert!(note_put_allowed(&mut kernel, "notes/b.md", "# B").ok);
        assert!(note_put_allowed(&mut kernel, "notes/c.md", "# C").ok);

        let base_yaml = r#"
views:
  - name: Projects
    type: table
    source: notes
    columns:
      - key: title
      - key: path
"#;
        let document = parse_base_document(base_yaml).expect("parse base yaml");
        let config_json = serde_json::to_string(&document).expect("serialize base config");
        fs::write(
            vault_root.join("views/projects.base"),
            base_yaml.trim_start(),
        )
        .expect("write base file");

        let base_file_path = "views/projects.base".to_string();
        FilesRepository::insert(
            &kernel.connection,
            &FileRecordInput {
                file_id: "f-base".to_string(),
                normalized_path: base_file_path.clone(),
                match_key: base_file_path.to_lowercase(),
                absolute_path: vault_root
                    .join(&base_file_path)
                    .to_string_lossy()
                    .to_string(),
                size_bytes: config_json.len() as u64,
                modified_unix_ms: 1_700_000_000_000,
                hash_blake3: blake3::hash(config_json.as_bytes()).to_hex().to_string(),
                is_markdown: false,
            },
        )
        .expect("insert base file");
        BasesRepository::upsert(
            &kernel.connection,
            &BaseRecordInput {
                base_id: "b-projects".to_string(),
                file_id: "f-base".to_string(),
                config_json,
            },
        )
        .expect("insert base row");

        let listed = kernel.bases_list();
        assert!(listed.ok);
        let refs = listed.value.expect("bases list value");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].base_id, "b-projects");
        assert_eq!(refs[0].file_path, "views/projects.base");
        assert_eq!(refs[0].views, vec!["Projects".to_string()]);

        let first_page = kernel.bases_view("b-projects", "Projects", 1, 2);
        assert!(first_page.ok);
        let first = first_page.value.expect("first page value");
        assert_eq!(first.total, 3);
        assert_eq!(first.rows.len(), 2);
        assert!(first.has_more);
        assert_eq!(first.columns.len(), 2);
        assert_eq!(first.rows[0].file_path, "notes/a.md");
        assert_eq!(
            first.rows[0].values.get("title").map(String::as_str),
            Some("a")
        );

        let second_page = kernel.bases_view("views/projects.base", "Projects", 2, 2);
        assert!(second_page.ok);
        let second = second_page.value.expect("second page value");
        assert_eq!(second.total, 3);
        assert_eq!(second.rows.len(), 1);
        assert!(!second.has_more);
        assert_eq!(second.rows[0].file_path, "notes/c.md");
    }

    #[test]
    fn bridge_kernel_events_poll_returns_note_write_events() {
        let temp = tempdir().expect("tempdir");
        let vault_root = temp.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        let db_path = temp.path().join("tao.db");

        let mut kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let created = note_put_allowed(&mut kernel, "notes/events.md", "# Event\ncreated");
        assert!(created.ok);
        let updated = note_put_allowed(&mut kernel, "notes/events.md", "# Event\nupdated");
        assert!(updated.ok);

        let first_batch = kernel.events_poll(0, 10);
        assert!(first_batch.ok);
        let first_value = first_batch.value.expect("first batch");
        assert_eq!(first_value.events.len(), 2);
        assert_eq!(first_value.events[0].kind, "note_changed");
        assert_eq!(first_value.events[0].action.as_deref(), Some("created"));
        assert_eq!(first_value.events[1].action.as_deref(), Some("updated"));

        let second_batch = kernel.events_poll(first_value.next_cursor, 10);
        assert!(second_batch.ok);
        let second_value = second_batch.value.expect("second batch");
        assert!(second_value.events.is_empty());
        assert_eq!(second_value.next_cursor, first_value.next_cursor);
    }
}
