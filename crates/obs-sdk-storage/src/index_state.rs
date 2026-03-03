use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

/// Persisted row model for `index_state` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStateRecord {
    /// State key.
    pub key: String,
    /// JSON-encoded value payload.
    pub value_json: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Input payload for index state upserts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStateRecordInput {
    /// State key.
    pub key: String,
    /// JSON-encoded value payload.
    pub value_json: String,
}

/// Repository operations over `index_state` table.
#[derive(Debug, Default, Clone, Copy)]
pub struct IndexStateRepository;

impl IndexStateRepository {
    /// Insert or update one state row keyed by `key`.
    pub fn upsert(
        connection: &Connection,
        state: &IndexStateRecordInput,
    ) -> Result<(), IndexStateRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO index_state (
  key,
  value_json
)
VALUES (?1, ?2)
ON CONFLICT(key)
DO UPDATE SET
  value_json = excluded.value_json,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
                params![state.key, state.value_json],
            )
            .map_err(|source| IndexStateRepositoryError::Sql {
                operation: "upsert",
                source,
            })?;

        Ok(())
    }

    /// Fetch one state row by key.
    pub fn get_by_key(
        connection: &Connection,
        key: &str,
    ) -> Result<Option<IndexStateRecord>, IndexStateRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  key,
  value_json,
  updated_at
FROM index_state
WHERE key = ?1
"#,
            )
            .map_err(|source| IndexStateRepositoryError::Sql {
                operation: "prepare_get_by_key",
                source,
            })?;

        statement
            .query_row(params![key], row_to_record)
            .optional()
            .map_err(|source| IndexStateRepositoryError::Sql {
                operation: "get_by_key",
                source,
            })
    }

    /// List all state rows in deterministic key order.
    pub fn list_all(
        connection: &Connection,
    ) -> Result<Vec<IndexStateRecord>, IndexStateRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  key,
  value_json,
  updated_at
FROM index_state
ORDER BY key ASC
"#,
            )
            .map_err(|source| IndexStateRepositoryError::Sql {
                operation: "prepare_list_all",
                source,
            })?;

        let rows = statement.query_map([], row_to_record).map_err(|source| {
            IndexStateRepositoryError::Sql {
                operation: "list_all",
                source,
            }
        })?;

        rows.map(|row| {
            row.map_err(|source| IndexStateRepositoryError::Sql {
                operation: "list_all_row",
                source,
            })
        })
        .collect()
    }

    /// Delete one state row by key.
    pub fn delete_by_key(
        connection: &Connection,
        key: &str,
    ) -> Result<bool, IndexStateRepositoryError> {
        let deleted = connection
            .execute("DELETE FROM index_state WHERE key = ?1", params![key])
            .map_err(|source| IndexStateRepositoryError::Sql {
                operation: "delete_by_key",
                source,
            })?;

        Ok(deleted > 0)
    }
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<IndexStateRecord> {
    Ok(IndexStateRecord {
        key: row.get("key")?,
        value_json: row.get("value_json")?,
        updated_at: row.get("updated_at")?,
    })
}

/// Index state repository operation failures.
#[derive(Debug, Error)]
pub enum IndexStateRepositoryError {
    /// SQL error with operation context.
    #[error("index state repository operation '{operation}' failed: {source}")]
    Sql {
        /// Repository operation name.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::{IndexStateRecordInput, IndexStateRepository, run_migrations};

    #[test]
    fn upsert_get_list_and_delete_index_state_rows() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        IndexStateRepository::upsert(
            &connection,
            &IndexStateRecordInput {
                key: "checkpoint.cursor".to_string(),
                value_json: "{\"offset\":10}".to_string(),
            },
        )
        .expect("upsert checkpoint");

        IndexStateRepository::upsert(
            &connection,
            &IndexStateRecordInput {
                key: "reconcile.last_run".to_string(),
                value_json: "\"2026-03-03T12:00:00Z\"".to_string(),
            },
        )
        .expect("upsert reconcile");

        IndexStateRepository::upsert(
            &connection,
            &IndexStateRecordInput {
                key: "checkpoint.cursor".to_string(),
                value_json: "{\"offset\":11}".to_string(),
            },
        )
        .expect("update checkpoint");

        let checkpoint = IndexStateRepository::get_by_key(&connection, "checkpoint.cursor")
            .expect("get checkpoint")
            .expect("checkpoint exists");
        assert_eq!(checkpoint.value_json, "{\"offset\":11}");

        let listed = IndexStateRepository::list_all(&connection).expect("list all");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].key, "checkpoint.cursor");
        assert_eq!(listed[1].key, "reconcile.last_run");

        let deleted = IndexStateRepository::delete_by_key(&connection, "reconcile.last_run")
            .expect("delete key");
        assert!(deleted);

        assert!(
            IndexStateRepository::get_by_key(&connection, "reconcile.last_run")
                .expect("get deleted")
                .is_none()
        );
    }
}
