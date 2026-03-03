//! SQLite schema and migration primitives for the core SDK.

use std::collections::HashMap;

use rusqlite::{Connection, params};
use thiserror::Error;

/// Initial schema migration identifier.
pub const MIGRATION_0001_ID: &str = "0001_init";
/// Initial schema SQL payload.
pub const MIGRATION_0001_SQL: &str = include_str!("../migrations/0001_init.sql");

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

const MIGRATIONS: [Migration; 1] = [Migration {
    id: MIGRATION_0001_ID,
    sql: MIGRATION_0001_SQL,
}];

/// Apply initial schema migration SQL directly to an active connection.
pub fn apply_initial_schema(connection: &Connection) -> Result<(), StorageSchemaError> {
    connection
        .execute_batch(MIGRATION_0001_SQL)
        .map_err(|source| StorageSchemaError::ApplyMigration {
            migration_id: MIGRATION_0001_ID,
            source,
        })
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

/// Apply forward-only migrations and enforce checksum guards.
pub fn run_migrations(
    connection: &mut Connection,
) -> Result<MigrationReport, MigrationRunnerError> {
    connection
        .execute_batch("PRAGMA foreign_keys = ON;")
        .map_err(|source| MigrationRunnerError::SetPragma { source })?;

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
    #[error("failed to configure sqlite pragmas: {source}")]
    SetPragma {
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

    use super::{
        MIGRATION_0001_ID, MigrationRunnerError, apply_initial_schema, migration_checksum,
        run_migrations,
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

        assert_eq!(report.applied, vec![MIGRATION_0001_ID.to_string()]);
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
    fn run_migrations_is_idempotent() {
        let mut connection = Connection::open_in_memory().expect("open in-memory database");

        let first = run_migrations(&mut connection).expect("run first migration pass");
        let second = run_migrations(&mut connection).expect("run second migration pass");

        assert_eq!(first.applied, vec![MIGRATION_0001_ID.to_string()]);
        assert!(first.skipped.is_empty());
        assert!(second.applied.is_empty());
        assert_eq!(second.skipped, vec![MIGRATION_0001_ID.to_string()]);
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
