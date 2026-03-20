//! Health snapshot service for SDK diagnostics.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension};
use tao_sdk_vault::{PathCanonicalizationError, VaultScanError};
use thiserror::Error;

/// Health snapshot status for watcher subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatcherStatus {
    /// Filesystem watcher is active.
    Running,
    /// Filesystem watcher is disabled.
    Stopped,
    /// Filesystem watcher is active with degraded guarantees.
    Degraded { reason: String },
}

impl WatcherStatus {
    fn as_label(&self) -> &'static str {
        match self {
            WatcherStatus::Running => "running",
            WatcherStatus::Stopped => "stopped",
            WatcherStatus::Degraded { .. } => "degraded",
        }
    }
}

/// Consolidated SDK health snapshot payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    /// Canonical vault root used for snapshot.
    pub vault_root: String,
    /// Database status flag.
    pub db_healthy: bool,
    /// Applied migration row count.
    pub db_migrations: u64,
    /// Current index lag count.
    pub index_lag: u64,
    /// Watcher status label.
    pub watcher_status: String,
    /// Total scanned files.
    pub files_total: u64,
    /// Total markdown files from latest scan.
    pub markdown_files: u64,
    /// Last index update timestamp when present.
    pub last_index_updated_at: Option<String>,
}

/// Service that builds SDK health snapshot payloads.
#[derive(Debug, Default, Clone, Copy)]
pub struct HealthSnapshotService;

impl HealthSnapshotService {
    /// Build one health snapshot from vault scan + sqlite status + watcher state.
    pub fn snapshot(
        &self,
        vault_root: &Path,
        connection: &Connection,
        index_lag: u64,
        watcher_status: WatcherStatus,
    ) -> Result<HealthSnapshot, HealthSnapshotError> {
        let (files_total, markdown_files): (u64, u64) = connection
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(CASE WHEN is_markdown = 1 THEN 1 ELSE 0 END), 0) FROM files",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|source| HealthSnapshotError::DatabaseStatus { source })?;

        let db_migrations = connection
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .map_err(|source| HealthSnapshotError::DatabaseStatus { source })?;

        let last_index_updated_at = connection
            .query_row(
                "SELECT updated_at FROM index_state WHERE key = 'last_index_at'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| HealthSnapshotError::DatabaseStatus { source })?;

        Ok(HealthSnapshot {
            vault_root: vault_root.to_string_lossy().to_string(),
            db_healthy: true,
            db_migrations,
            index_lag,
            watcher_status: watcher_status.as_label().to_string(),
            files_total,
            markdown_files,
            last_index_updated_at,
        })
    }
}

/// Errors returned by health snapshot service operations.
#[derive(Debug, Error)]
pub enum HealthSnapshotError {
    /// Scanner initialization failed.
    #[error("failed to initialize vault scanner for health snapshot: {source}")]
    CreateScanner {
        /// Scanner path initialization error.
        #[source]
        source: PathCanonicalizationError,
    },
    /// Vault scan failed.
    #[error("failed to scan vault for health snapshot: {source}")]
    Scan {
        /// Vault scan error.
        #[source]
        source: VaultScanError,
    },
    /// Database status query failed.
    #[error("failed to query database status for health snapshot: {source}")]
    DatabaseStatus {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
}
