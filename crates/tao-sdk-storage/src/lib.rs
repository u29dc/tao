//! SQLite schema and migration primitives for the core SDK.

use std::collections::HashMap;

use rusqlite::{Connection, params};
use thiserror::Error;

mod bases;
mod files;
mod index_state;
mod links;
mod properties;
mod render_cache;
mod search_index;
mod tasks;
mod transaction;

pub use bases::{BaseRecord, BaseRecordInput, BaseWithPath, BasesRepository, BasesRepositoryError};
pub use files::{FileRecord, FileRecordInput, FilesRepository, FilesRepositoryError};
pub use index_state::{
    IndexStateRecord, IndexStateRecordInput, IndexStateRepository, IndexStateRepositoryError,
};
pub use links::{
    LinkRecord, LinkRecordInput, LinkWithPaths, LinksRepository, LinksRepositoryError,
};
pub use properties::{
    PropertiesRepository, PropertiesRepositoryError, PropertyRecord, PropertyRecordInput,
    PropertyWithPath,
};
pub use render_cache::{
    RenderCacheRecord, RenderCacheRecordInput, RenderCacheRepository, RenderCacheRepositoryError,
};
pub use search_index::{
    SearchIndexRecord, SearchIndexRecordInput, SearchIndexRepository, SearchIndexRepositoryError,
};
pub use tasks::{TaskRecord, TaskRecordInput, TaskWithPath, TasksRepository, TasksRepositoryError};
pub use transaction::{StorageTransaction, StorageTransactionError, with_transaction};

/// Initial schema migration identifier.
pub const MIGRATION_0001_ID: &str = "0001_init";
/// Initial schema SQL payload.
pub const MIGRATION_0001_SQL: &str = include_str!("../migrations/0001_init.sql");
/// Search index schema migration identifier.
pub const MIGRATION_0002_ID: &str = "0002_search_index";
/// Search index schema SQL payload.
pub const MIGRATION_0002_SQL: &str = include_str!("../migrations/0002_search_index.sql");
/// Tasks index schema migration identifier.
pub const MIGRATION_0003_ID: &str = "0003_tasks";
/// Tasks index schema SQL payload.
pub const MIGRATION_0003_SQL: &str = include_str!("../migrations/0003_tasks.sql");
/// Link query performance migration identifier.
pub const MIGRATION_0004_ID: &str = "0004_links_perf";
/// Link query performance SQL payload.
pub const MIGRATION_0004_SQL: &str = include_str!("../migrations/0004_links_perf.sql");

const CREATE_SCHEMA_MIGRATIONS_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
  id TEXT PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"#;

/// Static migration definition.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    /// Ordered migration id.
    pub id: &'static str,
    /// SQL payload.
    pub sql: &'static str,
}

const MIGRATIONS: [Migration; 4] = [
    Migration {
        id: MIGRATION_0001_ID,
        sql: MIGRATION_0001_SQL,
    },
    Migration {
        id: MIGRATION_0002_ID,
        sql: MIGRATION_0002_SQL,
    },
    Migration {
        id: MIGRATION_0003_ID,
        sql: MIGRATION_0003_SQL,
    },
    Migration {
        id: MIGRATION_0004_ID,
        sql: MIGRATION_0004_SQL,
    },
];

const SQLITE_PRAGMA_PROFILE: [&str; 7] = [
    "PRAGMA foreign_keys = ON;",
    "PRAGMA journal_mode = WAL;",
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA temp_store = MEMORY;",
    "PRAGMA cache_size = -20000;",
    "PRAGMA wal_autocheckpoint = 1000;",
    "PRAGMA busy_timeout = 5000;",
];

/// Apply known schema migration SQL directly to an active connection.
pub fn apply_initial_schema(connection: &Connection) -> Result<(), StorageSchemaError> {
    for migration in known_migrations() {
        connection.execute_batch(migration.sql).map_err(|source| {
            StorageSchemaError::ApplyMigration {
                migration_id: migration.id,
                source,
            }
        })?;
    }

    Ok(())
}

/// Return ordered migration definitions.
#[must_use]
pub fn known_migrations() -> &'static [Migration] {
    &MIGRATIONS
}

/// Report for one migration runner execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// Migration ids newly applied in this run.
    pub applied: Vec<String>,
    /// Migration ids already present and checksum-verified.
    pub skipped: Vec<String>,
}

/// Report for migration preflight validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPreflightReport {
    /// Whether `schema_migrations` table exists.
    pub migrations_table_exists: bool,
    /// Number of known migrations in binary.
    pub known_migrations: u64,
    /// Number of applied migrations recorded in database.
    pub applied_migrations: u64,
    /// Number of pending migrations.
    pub pending_migrations: u64,
}

/// Validate migration metadata/checksums before attempting startup migration apply.
pub fn preflight_migrations(
    connection: &Connection,
) -> Result<MigrationPreflightReport, MigrationRunnerError> {
    let known_total = known_migrations().len() as u64;
    let migrations_table_exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations')",
            [],
            |row| row.get(0),
        )
        .map_err(|source| MigrationRunnerError::PreflightTableCheck { source })?;

    if !migrations_table_exists {
        return Ok(MigrationPreflightReport {
            migrations_table_exists: false,
            known_migrations: known_total,
            applied_migrations: 0,
            pending_migrations: known_total,
        });
    }

    let applied_checksums = load_applied_checksums_connection(connection)?;
    for migration in known_migrations() {
        let expected_checksum = migration_checksum(migration.sql);
        if let Some(recorded_checksum) = applied_checksums.get(migration.id)
            && recorded_checksum != &expected_checksum
        {
            return Err(MigrationRunnerError::ChecksumMismatch {
                migration_id: migration.id.to_string(),
                expected_checksum,
                recorded_checksum: recorded_checksum.clone(),
            });
        }
    }

    let applied_total = applied_checksums.len() as u64;
    let pending_total = known_total.saturating_sub(applied_total.min(known_total));
    Ok(MigrationPreflightReport {
        migrations_table_exists: true,
        known_migrations: known_total,
        applied_migrations: applied_total,
        pending_migrations: pending_total,
    })
}

/// Apply forward-only migrations and enforce checksum guards.
pub fn run_migrations(
    connection: &mut Connection,
) -> Result<MigrationReport, MigrationRunnerError> {
    configure_connection_pragmas(connection)?;
    preflight_migrations(connection)?;

    let transaction = connection
        .transaction()
        .map_err(|source| MigrationRunnerError::BeginTransaction { source })?;

    transaction
        .execute_batch(CREATE_SCHEMA_MIGRATIONS_SQL)
        .map_err(|source| MigrationRunnerError::EnsureMigrationsTable { source })?;

    let applied_checksums = load_applied_checksums(&transaction)?;
    let mut report = MigrationReport {
        applied: Vec::new(),
        skipped: Vec::new(),
    };

    for migration in known_migrations() {
        let expected_checksum = migration_checksum(migration.sql);

        if let Some(recorded_checksum) = applied_checksums.get(migration.id) {
            if recorded_checksum != &expected_checksum {
                return Err(MigrationRunnerError::ChecksumMismatch {
                    migration_id: migration.id.to_string(),
                    expected_checksum,
                    recorded_checksum: recorded_checksum.clone(),
                });
            }

            report.skipped.push(migration.id.to_string());
            continue;
        }

        transaction.execute_batch(migration.sql).map_err(|source| {
            MigrationRunnerError::ApplyMigration {
                migration_id: migration.id.to_string(),
                source,
            }
        })?;

        transaction
            .execute(
                "INSERT INTO schema_migrations (id, checksum) VALUES (?1, ?2)",
                params![migration.id, expected_checksum],
            )
            .map_err(|source| MigrationRunnerError::RecordMigration {
                migration_id: migration.id.to_string(),
                source,
            })?;

        report.applied.push(migration.id.to_string());
    }

    transaction
        .commit()
        .map_err(|source| MigrationRunnerError::CommitTransaction { source })?;

    Ok(report)
}

fn configure_connection_pragmas(connection: &Connection) -> Result<(), MigrationRunnerError> {
    for pragma in SQLITE_PRAGMA_PROFILE {
        connection
            .execute_batch(pragma)
            .map_err(|source| MigrationRunnerError::SetPragma { pragma, source })?;
    }

    Ok(())
}

fn load_applied_checksums(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<HashMap<String, String>, MigrationRunnerError> {
    let mut statement = transaction
        .prepare("SELECT id, checksum FROM schema_migrations")
        .map_err(|source| MigrationRunnerError::LoadAppliedChecksums { source })?;

    let rows = statement
        .query_map([], |row| {
            let id: String = row.get(0)?;
            let checksum: String = row.get(1)?;
            Ok((id, checksum))
        })
        .map_err(|source| MigrationRunnerError::LoadAppliedChecksums { source })?;

    let mut checksums = HashMap::new();
    for row in rows {
        let (id, checksum) =
            row.map_err(|source| MigrationRunnerError::LoadAppliedChecksums { source })?;
        checksums.insert(id, checksum);
    }

    Ok(checksums)
}

fn load_applied_checksums_connection(
    connection: &Connection,
) -> Result<HashMap<String, String>, MigrationRunnerError> {
    let mut statement = connection
        .prepare("SELECT id, checksum FROM schema_migrations")
        .map_err(|source| MigrationRunnerError::LoadAppliedChecksums { source })?;

    let rows = statement
        .query_map([], |row| {
            let id: String = row.get(0)?;
            let checksum: String = row.get(1)?;
            Ok((id, checksum))
        })
        .map_err(|source| MigrationRunnerError::LoadAppliedChecksums { source })?;

    let mut checksums = HashMap::new();
    for row in rows {
        let (id, checksum) =
            row.map_err(|source| MigrationRunnerError::LoadAppliedChecksums { source })?;
        checksums.insert(id, checksum);
    }

    Ok(checksums)
}

fn migration_checksum(sql: &str) -> String {
    blake3::hash(sql.as_bytes()).to_hex().to_string()
}

/// Storage schema initialization errors.
#[derive(Debug, Error)]
pub enum StorageSchemaError {
    /// Running migration SQL failed.
    #[error("failed to apply migration '{migration_id}': {source}")]
    ApplyMigration {
        /// Migration id.
        migration_id: &'static str,
        /// SQLite execution error.
        #[source]
        source: rusqlite::Error,
    },
}

/// Migration runner failures.
#[derive(Debug, Error)]
pub enum MigrationRunnerError {
    /// Setting pragma values failed.
    #[error("failed to configure sqlite pragma '{pragma}': {source}")]
    SetPragma {
        /// SQL pragma statement that failed.
        pragma: &'static str,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Checking migration table metadata during preflight failed.
    #[error("failed to preflight schema_migrations table state: {source}")]
    PreflightTableCheck {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Opening migration transaction failed.
    #[error("failed to begin migration transaction: {source}")]
    BeginTransaction {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Ensuring migration table exists failed.
    #[error("failed to ensure schema_migrations table: {source}")]
    EnsureMigrationsTable {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Reading applied migration checksums failed.
    #[error("failed to load applied migration checksums: {source}")]
    LoadAppliedChecksums {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Applying migration SQL failed.
    #[error("failed to apply migration '{migration_id}': {source}")]
    ApplyMigration {
        /// Migration id.
        migration_id: String,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Persisting migration checksum failed.
    #[error("failed to persist migration '{migration_id}' checksum: {source}")]
    RecordMigration {
        /// Migration id.
        migration_id: String,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Stored checksum differs from current migration SQL checksum.
    #[error(
        "migration checksum mismatch for '{migration_id}': expected {expected_checksum}, got {recorded_checksum}"
    )]
    ChecksumMismatch {
        /// Migration id.
        migration_id: String,
        /// Current migration SQL checksum.
        expected_checksum: String,
        /// Checksum recorded in schema_migrations.
        recorded_checksum: String,
    },
    /// Committing migration transaction failed.
    #[error("failed to commit migration transaction: {source}")]
    CommitTransaction {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rusqlite::{Connection, OptionalExtension, params};
    use tempfile::tempdir;

    use super::{
        MIGRATION_0001_ID, MIGRATION_0002_ID, MIGRATION_0003_ID, MIGRATION_0004_ID,
        MigrationRunnerError, apply_initial_schema, known_migrations, migration_checksum,
        preflight_migrations, run_migrations,
    };

    #[test]
    fn apply_initial_schema_creates_expected_tables() {
        let connection = Connection::open_in_memory().expect("open in-memory database");
        apply_initial_schema(&connection).expect("apply initial schema");

        let mut statement = connection
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .expect("prepare table query");
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query tables");

        let tables: HashSet<String> = rows.map(|row| row.expect("read table name")).collect();

        let expected = [
            "schema_migrations",
            "files",
            "links",
            "properties",
            "bases",
            "render_cache",
            "index_state",
            "search_index",
            "tasks",
        ];

        for table in expected {
            assert!(tables.contains(table), "expected table '{table}' to exist");
        }
    }

    #[test]
    fn apply_initial_schema_is_idempotent() {
        let connection = Connection::open_in_memory().expect("open in-memory database");

        apply_initial_schema(&connection).expect("apply initial schema once");
        apply_initial_schema(&connection).expect("apply initial schema twice");
    }

    #[test]
    fn run_migrations_applies_pending_and_records_checksums() {
        let mut connection = Connection::open_in_memory().expect("open in-memory database");
        let report = run_migrations(&mut connection).expect("run migrations");

        assert_eq!(
            report.applied,
            vec![
                MIGRATION_0001_ID.to_string(),
                MIGRATION_0002_ID.to_string(),
                MIGRATION_0003_ID.to_string(),
                MIGRATION_0004_ID.to_string()
            ]
        );
        assert!(report.skipped.is_empty());

        let recorded_checksum: Option<String> = connection
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE id = ?1",
                params![MIGRATION_0001_ID],
                |row| row.get(0),
            )
            .optional()
            .expect("read checksum");

        assert_eq!(
            recorded_checksum,
            Some(migration_checksum(super::MIGRATION_0001_SQL))
        );
    }

    #[test]
    fn preflight_reports_pending_migrations_before_first_apply() {
        let connection = Connection::open_in_memory().expect("open in-memory database");
        let report = preflight_migrations(&connection).expect("preflight migrations");
        let known_total = known_migrations().len() as u64;

        assert!(!report.migrations_table_exists);
        assert_eq!(report.known_migrations, known_total);
        assert_eq!(report.applied_migrations, 0);
        assert_eq!(report.pending_migrations, known_total);
    }

    #[test]
    fn run_migrations_applies_sqlite_pragma_profile_for_file_database() {
        let temp = tempdir().expect("create temp directory");
        let db_path = temp.path().join("tao.sqlite");
        let mut connection = Connection::open(db_path).expect("open sqlite database");

        run_migrations(&mut connection).expect("run migrations");

        let foreign_keys: i64 = connection
            .query_row("PRAGMA foreign_keys;", [], |row| row.get(0))
            .expect("read foreign_keys pragma");
        assert_eq!(foreign_keys, 1);

        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .expect("read journal_mode pragma");
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");

        let synchronous: i64 = connection
            .query_row("PRAGMA synchronous;", [], |row| row.get(0))
            .expect("read synchronous pragma");
        assert_eq!(synchronous, 1);

        let temp_store: i64 = connection
            .query_row("PRAGMA temp_store;", [], |row| row.get(0))
            .expect("read temp_store pragma");
        assert_eq!(temp_store, 2);

        let cache_size: i64 = connection
            .query_row("PRAGMA cache_size;", [], |row| row.get(0))
            .expect("read cache_size pragma");
        assert_eq!(cache_size, -20_000);

        let wal_autocheckpoint: i64 = connection
            .query_row("PRAGMA wal_autocheckpoint;", [], |row| row.get(0))
            .expect("read wal_autocheckpoint pragma");
        assert_eq!(wal_autocheckpoint, 1_000);

        let busy_timeout: i64 = connection
            .query_row("PRAGMA busy_timeout;", [], |row| row.get(0))
            .expect("read busy_timeout pragma");
        assert_eq!(busy_timeout, 5_000);
    }

    #[test]
    fn run_migrations_is_idempotent() {
        let mut connection = Connection::open_in_memory().expect("open in-memory database");

        let first = run_migrations(&mut connection).expect("run first migration pass");
        let second = run_migrations(&mut connection).expect("run second migration pass");

        assert_eq!(
            first.applied,
            vec![
                MIGRATION_0001_ID.to_string(),
                MIGRATION_0002_ID.to_string(),
                MIGRATION_0003_ID.to_string(),
                MIGRATION_0004_ID.to_string()
            ]
        );
        assert!(first.skipped.is_empty());
        assert!(second.applied.is_empty());
        assert_eq!(
            second.skipped,
            vec![
                MIGRATION_0001_ID.to_string(),
                MIGRATION_0002_ID.to_string(),
                MIGRATION_0003_ID.to_string(),
                MIGRATION_0004_ID.to_string()
            ]
        );
    }

    #[test]
    fn run_migrations_fails_on_checksum_mismatch() {
        let mut connection = Connection::open_in_memory().expect("open in-memory database");
        run_migrations(&mut connection).expect("run initial migration pass");

        connection
            .execute(
                "UPDATE schema_migrations SET checksum = ?1 WHERE id = ?2",
                params!["bad-checksum", MIGRATION_0001_ID],
            )
            .expect("tamper checksum");

        let error = run_migrations(&mut connection).expect_err("checksum mismatch should fail");

        match error {
            MigrationRunnerError::ChecksumMismatch {
                migration_id,
                expected_checksum,
                recorded_checksum,
            } => {
                assert_eq!(migration_id, MIGRATION_0001_ID);
                assert_eq!(recorded_checksum, "bad-checksum");
                assert_eq!(
                    expected_checksum,
                    migration_checksum(super::MIGRATION_0001_SQL)
                );
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
