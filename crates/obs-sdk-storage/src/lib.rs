//! SQLite schema and storage primitives for the core SDK.

use rusqlite::Connection;
use thiserror::Error;

/// Initial schema migration identifier.
pub const MIGRATION_0001_ID: &str = "0001_init";
/// Initial schema SQL payload.
pub const MIGRATION_0001_SQL: &str = include_str!("../migrations/0001_init.sql");

/// Apply initial schema migration SQL to the active database connection.
pub fn apply_initial_schema(connection: &Connection) -> Result<(), StorageSchemaError> {
    connection
        .execute_batch(MIGRATION_0001_SQL)
        .map_err(|source| StorageSchemaError::ApplyMigration {
            migration_id: MIGRATION_0001_ID,
            source,
        })
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rusqlite::Connection;

    use super::apply_initial_schema;

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
}
