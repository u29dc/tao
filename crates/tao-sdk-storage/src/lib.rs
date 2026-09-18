//! SQLite schema and migration primitives for the core SDK.

use std::collections::HashMap;

use rusqlite::{Connection, params};
use thiserror::Error;

mod bases;
mod content;
mod documents;
mod files;
mod generations;
mod index_state;
mod link_evidence;
mod links;
mod metadata_aggregation;
mod properties;
mod search_aliases;
mod search_segments;
mod tasks;
mod transaction;

pub use content::{
    ContentDocumentRecord, ContentQueueCounts, ContentRepository, ContentSegmentRecord,
    ExtractionJobRecord, MAX_EXTRACTION_WORKERS,
};
pub use documents::{
    DiagnosticsRepository, DocumentRecord, DocumentRecordInput, DocumentStructureRecord,
    DocumentsRepository, FileDiagnosticInput,
};
pub use generations::{IndexGenerationRepository, IndexGenerations};
pub use link_evidence::{LinkEvidenceInput, LinkEvidenceRepository};

/// Conservative shared SQL parameter batch size.
pub const SQL_PARAMETER_CHUNK: usize = 256;

pub use bases::{BaseRecord, BaseRecordInput, BaseWithPath, BasesRepository, BasesRepositoryError};
pub use files::{
    FileReconcileRecord, FileRecord, FileRecordInput, FilesRepository, FilesRepositoryError,
};
pub use index_state::{
    IndexStateRecord, IndexStateRecordInput, IndexStateRepository, IndexStateRepositoryError,
};
pub use links::{
    GraphNodeDegree, LinkRecord, LinkRecordInput, LinkWithPaths, LinksRepository,
    LinksRepositoryError, ResolvedLinkPair, ScopedInboundRow, ScopedInboundSummary,
};
pub use metadata_aggregation::{
    MAX_METADATA_AGGREGATION_LIMIT, MetadataAggregateRecord, MetadataAggregateWindow,
    MetadataAggregationRepository, MetadataAggregationRepositoryError,
};
pub use properties::{
    PropertiesRepository, PropertiesRepositoryError, PropertyRecord, PropertyRecordInput,
    PropertyWithPath,
};
pub use search_aliases::{
    SearchAliasInput, SearchAliasMatch, SearchAliasRepository, SearchAliasRepositoryError,
};
pub use search_segments::{
    SEARCH_RANK_SCALE, SearchSegmentCandidate, SearchSegmentInput, SearchSegmentMatch,
    SearchSegmentQuery, SearchSegmentRepository, SearchSegmentRepositoryError,
};
pub use tasks::{TaskRecord, TaskRecordInput, TaskWithPath, TasksRepository, TasksRepositoryError};
pub use transaction::{StorageTransaction, StorageTransactionError, with_transaction};

/// Single supported index format. Old indexes are rebuildable caches, never upgraded in place.
pub const CURRENT_FORMAT_EPOCH: u32 = 1;
/// Identifier of the current schema bootstrap, retained in diagnostic reports.
pub const CURRENT_SCHEMA_ID: &str = "current_1";
/// Canonical and derived schema for a newly created index.
pub const CURRENT_SCHEMA_SQL: &str = concat!(
    include_str!("../schema/current.sql"),
    "\n",
    include_str!("../schema/search.sql")
);
pub(crate) const SEARCH_SCHEMA_SQL: &str = include_str!("../schema/search.sql");

/// Current schema definition. There is no historical migration chain.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    /// Current bootstrap identifier.
    pub id: &'static str,
    /// Current complete schema.
    pub sql: &'static str,
}
const CURRENT_SCHEMA: [Migration; 1] = [Migration {
    id: CURRENT_SCHEMA_ID,
    sql: CURRENT_SCHEMA_SQL,
}];

/// Return the single current schema definition for bootstrap diagnostics.
#[must_use]
pub fn known_migrations() -> &'static [Migration] {
    &CURRENT_SCHEMA
}

/// Schema initialization outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// Current schema identifier when newly initialized.
    pub applied: Vec<String>,
    /// Current schema identifier when already initialized.
    pub skipped: Vec<String>,
}
/// Read-only schema compatibility status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPreflightReport {
    /// Whether the current checksum metadata table exists.
    pub migrations_table_exists: bool,
    /// Always one current schema definition.
    pub known_migrations: u64,
    /// One for a valid current index; zero for an empty database.
    pub applied_migrations: u64,
    /// One for an empty database awaiting bootstrap, otherwise zero.
    pub pending_migrations: u64,
}

/// Reject older/newer/non-Tao indexes without changing journal mode or schema.
pub fn preflight_migrations(
    connection: &Connection,
) -> Result<MigrationPreflightReport, MigrationRunnerError> {
    let epoch: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|source| MigrationRunnerError::PreflightTableCheck { source })?;
    let populated: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE substr(name,1,7)<>'sqlite_')",
            [],
            |row| row.get(0),
        )
        .map_err(|source| MigrationRunnerError::PreflightTableCheck { source })?;
    if !populated && epoch == 0 {
        return Ok(MigrationPreflightReport {
            migrations_table_exists: false,
            known_migrations: 1,
            applied_migrations: 0,
            pending_migrations: 1,
        });
    }
    if epoch != CURRENT_FORMAT_EPOCH {
        return Err(MigrationRunnerError::UnsupportedFormat {
            found: epoch,
            expected: CURRENT_FORMAT_EPOCH,
        });
    }
    let checksums = load_applied_checksums(connection)?;
    if checksums.len() != 1 || !checksums.contains_key(CURRENT_SCHEMA_ID) {
        return Err(MigrationRunnerError::UnsupportedSchema {
            migration_id: checksums
                .keys()
                .next()
                .cloned()
                .unwrap_or_else(|| "missing current schema metadata".into()),
        });
    }
    let recorded_checksum = &checksums[CURRENT_SCHEMA_ID];
    let expected_checksum = migration_checksum(CURRENT_SCHEMA_SQL);
    if recorded_checksum != &expected_checksum {
        return Err(MigrationRunnerError::ChecksumMismatch {
            migration_id: CURRENT_SCHEMA_ID.into(),
            expected_checksum,
            recorded_checksum: recorded_checksum.clone(),
        });
    }
    Ok(MigrationPreflightReport {
        migrations_table_exists: true,
        known_migrations: 1,
        applied_migrations: 1,
        pending_migrations: 0,
    })
}

/// Initialize an empty database, or verify the existing current-format index.
pub fn run_migrations(
    connection: &mut Connection,
) -> Result<MigrationReport, MigrationRunnerError> {
    initialize_schema(connection)
}
/// Initialize an empty connection using the same epoch/checksum contract.
pub fn apply_initial_schema(connection: &Connection) -> Result<(), StorageSchemaError> {
    initialize_schema(connection)
        .map(|_| ())
        .map_err(|source| StorageSchemaError::Runner {
            source: Box::new(source),
        })
}
fn initialize_schema(connection: &Connection) -> Result<MigrationReport, MigrationRunnerError> {
    let preflight = preflight_migrations(connection)?;
    for pragma in [
        "PRAGMA foreign_keys=ON",
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA temp_store=FILE",
        "PRAGMA cache_size=-20000",
        "PRAGMA wal_autocheckpoint=1000",
        "PRAGMA busy_timeout=5000",
    ] {
        connection
            .execute_batch(pragma)
            .map_err(|source| MigrationRunnerError::SetPragma { pragma, source })?;
    }
    if preflight.pending_migrations == 0 {
        return Ok(MigrationReport {
            applied: Vec::new(),
            skipped: vec![CURRENT_SCHEMA_ID.into()],
        });
    }
    let transaction = connection
        .unchecked_transaction()
        .map_err(|source| MigrationRunnerError::BeginTransaction { source })?;
    transaction
        .execute_batch(CURRENT_SCHEMA_SQL)
        .map_err(|source| MigrationRunnerError::ApplyMigration {
            migration_id: CURRENT_SCHEMA_ID.into(),
            source,
        })?;
    transaction
        .execute(
            "INSERT INTO schema_migrations(id,checksum) VALUES(?1,?2)",
            params![CURRENT_SCHEMA_ID, migration_checksum(CURRENT_SCHEMA_SQL)],
        )
        .map_err(|source| MigrationRunnerError::RecordMigration {
            migration_id: CURRENT_SCHEMA_ID.into(),
            source,
        })?;
    transaction
        .pragma_update(None, "user_version", CURRENT_FORMAT_EPOCH)
        .map_err(|source| MigrationRunnerError::RecordMigration {
            migration_id: CURRENT_SCHEMA_ID.into(),
            source,
        })?;
    transaction
        .commit()
        .map_err(|source| MigrationRunnerError::CommitTransaction { source })?;
    Ok(MigrationReport {
        applied: vec![CURRENT_SCHEMA_ID.into()],
        skipped: Vec::new(),
    })
}
fn load_applied_checksums(
    connection: &Connection,
) -> Result<HashMap<String, String>, MigrationRunnerError> {
    connection
        .prepare("SELECT id,checksum FROM schema_migrations")
        .map_err(|source| MigrationRunnerError::LoadAppliedChecksums { source })?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|source| MigrationRunnerError::LoadAppliedChecksums { source })?
        .map(|row| row.map_err(|source| MigrationRunnerError::LoadAppliedChecksums { source }))
        .collect()
}
fn migration_checksum(sql: &str) -> String {
    blake3::hash(sql.as_bytes()).to_hex().to_string()
}

/// Current schema initialization failure.
#[derive(Debug, Error)]
pub enum StorageSchemaError {
    /// Schema initialization or compatibility failure.
    #[error("index schema initialization failed: {source}")]
    Runner {
        #[source]
        source: Box<MigrationRunnerError>,
    },
}
/// Index format validation and initialization failures.
#[derive(Debug, Error)]
pub enum MigrationRunnerError {
    /// Existing index belongs to another format epoch.
    #[error(
        "unsupported index format epoch {found} (expected {expected}); archive or remove the internal .tao index directory, then run vault reindex to build a fresh index; source vault files are unchanged"
    )]
    UnsupportedFormat {
        /// Observed format epoch.
        found: u32,
        /// Supported format epoch.
        expected: u32,
    },
    /// Metadata does not identify the sole current schema.
    #[error(
        "unsupported index schema '{migration_id}'; archive or remove the internal .tao index directory and run vault reindex"
    )]
    UnsupportedSchema {
        /// Recorded schema identifier.
        migration_id: String,
    },
    /// Connection setting failure.
    #[error("failed to configure SQLite pragma '{pragma}': {source}")]
    SetPragma {
        /// Statement.
        pragma: &'static str,
        #[source]
        source: rusqlite::Error,
    },
    /// Read-only epoch/schema inspection failed.
    #[error("failed to inspect index format: {source}")]
    PreflightTableCheck {
        #[source]
        source: rusqlite::Error,
    },
    /// Opening atomic bootstrap failed.
    #[error("failed to begin schema initialization: {source}")]
    BeginTransaction {
        #[source]
        source: rusqlite::Error,
    },
    /// Current format metadata could not be read.
    #[error(
        "failed to read current schema metadata: {source}; archive or remove the internal .tao index directory and run vault reindex"
    )]
    LoadAppliedChecksums {
        #[source]
        source: rusqlite::Error,
    },
    /// Current schema could not be created.
    #[error("failed to initialize schema '{migration_id}': {source}")]
    ApplyMigration {
        /// Current schema identifier.
        migration_id: String,
        #[source]
        source: rusqlite::Error,
    },
    /// Current format metadata could not be saved.
    #[error("failed to record schema '{migration_id}': {source}")]
    RecordMigration {
        /// Current schema identifier.
        migration_id: String,
        #[source]
        source: rusqlite::Error,
    },
    /// Schema SQL differs from the recorded current format.
    #[error(
        "index schema checksum mismatch for '{migration_id}': expected {expected_checksum}, got {recorded_checksum}; archive or remove the internal .tao index directory and run vault reindex"
    )]
    ChecksumMismatch {
        /// Current schema identifier.
        migration_id: String,
        /// Current schema checksum.
        expected_checksum: String,
        /// Stored schema checksum.
        recorded_checksum: String,
    },
    /// Atomic initialization commit failed.
    #[error("failed to commit schema initialization: {source}")]
    CommitTransaction {
        #[source]
        source: rusqlite::Error,
    },
}

#[cfg(test)]
mod publication_tests;
