use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

/// Persisted row model for `bases` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseRecord {
    /// Stable base identifier.
    pub base_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Base config payload encoded as JSON.
    pub config_json: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Input payload for base upserts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseRecordInput {
    /// Stable base identifier.
    pub base_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Base config payload encoded as JSON.
    pub config_json: String,
}

/// Base row enriched with file normalized path from join queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseWithPath {
    /// Stable base identifier.
    pub base_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning file normalized path.
    pub file_path: String,
    /// Base config payload encoded as JSON.
    pub config_json: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Repository operations over `bases` table.
#[derive(Debug, Default, Clone, Copy)]
pub struct BasesRepository;

impl BasesRepository {
    /// Insert or update one base row keyed by `base_id`.
    pub fn upsert(
        connection: &Connection,
        base: &BaseRecordInput,
    ) -> Result<(), BasesRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO bases (
  base_id,
  file_id,
  config_json
)
VALUES (?1, ?2, ?3)
ON CONFLICT(base_id)
DO UPDATE SET
  file_id = excluded.file_id,
  config_json = excluded.config_json,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
                params![base.base_id, base.file_id, base.config_json],
            )
            .map_err(|source| BasesRepositoryError::Sql {
                operation: "upsert",
                source,
            })?;

        Ok(())
    }

    /// Fetch one base row by base id.
    pub fn get_by_id(
        connection: &Connection,
        base_id: &str,
    ) -> Result<Option<BaseRecord>, BasesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  base_id,
  file_id,
  config_json,
  updated_at
FROM bases
WHERE base_id = ?1
"#,
            )
            .map_err(|source| BasesRepositoryError::Sql {
                operation: "prepare_get_by_id",
                source,
            })?;

        statement
            .query_row(params![base_id], row_to_base_record)
            .optional()
            .map_err(|source| BasesRepositoryError::Sql {
                operation: "get_by_id",
                source,
            })
    }

    /// Fetch one base row by owning file id.
    pub fn get_by_file_id(
        connection: &Connection,
        file_id: &str,
    ) -> Result<Option<BaseRecord>, BasesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  base_id,
  file_id,
  config_json,
  updated_at
FROM bases
WHERE file_id = ?1
"#,
            )
            .map_err(|source| BasesRepositoryError::Sql {
                operation: "prepare_get_by_file_id",
                source,
            })?;

        statement
            .query_row(params![file_id], row_to_base_record)
            .optional()
            .map_err(|source| BasesRepositoryError::Sql {
                operation: "get_by_file_id",
                source,
            })
    }

    /// Delete one base row by base id.
    pub fn delete_by_id(
        connection: &Connection,
        base_id: &str,
    ) -> Result<bool, BasesRepositoryError> {
        let deleted = connection
            .execute("DELETE FROM bases WHERE base_id = ?1", params![base_id])
            .map_err(|source| BasesRepositoryError::Sql {
                operation: "delete_by_id",
                source,
            })?;

        Ok(deleted > 0)
    }

    /// List all bases joined with owning normalized file paths.
    pub fn list_with_paths(
        connection: &Connection,
    ) -> Result<Vec<BaseWithPath>, BasesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  b.base_id,
  b.file_id,
  f.normalized_path AS file_path,
  b.config_json,
  b.updated_at
FROM bases b
JOIN files f ON f.file_id = b.file_id
ORDER BY f.normalized_path ASC
"#,
            )
            .map_err(|source| BasesRepositoryError::Sql {
                operation: "prepare_list_with_paths",
                source,
            })?;

        let rows = statement
            .query_map([], row_to_base_with_path)
            .map_err(|source| BasesRepositoryError::Sql {
                operation: "list_with_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| BasesRepositoryError::Sql {
                operation: "list_with_paths_row",
                source,
            })
        })
        .collect()
    }
}

fn row_to_base_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<BaseRecord> {
    Ok(BaseRecord {
        base_id: row.get("base_id")?,
        file_id: row.get("file_id")?,
        config_json: row.get("config_json")?,
        updated_at: row.get("updated_at")?,
    })
}

fn row_to_base_with_path(row: &rusqlite::Row<'_>) -> rusqlite::Result<BaseWithPath> {
    Ok(BaseWithPath {
        base_id: row.get("base_id")?,
        file_id: row.get("file_id")?,
        file_path: row.get("file_path")?,
        config_json: row.get("config_json")?,
        updated_at: row.get("updated_at")?,
    })
}

/// Bases repository operation failures.
#[derive(Debug, Error)]
pub enum BasesRepositoryError {
    /// SQL error with operation context.
    #[error("bases repository operation '{operation}' failed: {source}")]
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

    use crate::{
        BaseRecordInput, BasesRepository, FileRecordInput, FilesRepository, run_migrations,
    };

    fn file_record(file_id: &str, path: &str) -> FileRecordInput {
        FileRecordInput {
            file_id: file_id.to_string(),
            normalized_path: path.to_string(),
            match_key: path.to_lowercase(),
            absolute_path: format!("/vault/{path}"),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: format!("hash-{file_id}"),
            is_markdown: true,
        }
    }

    #[test]
    fn upsert_get_delete_base_by_id() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let file = file_record("f1", "views/projects.base");
        FilesRepository::insert(&connection, &file).expect("insert file");

        let first = BaseRecordInput {
            base_id: "b1".to_string(),
            file_id: "f1".to_string(),
            config_json: "{\"views\":[\"table\"]}".to_string(),
        };
        BasesRepository::upsert(&connection, &first).expect("upsert first");

        let updated = BaseRecordInput {
            base_id: "b1".to_string(),
            file_id: "f1".to_string(),
            config_json: "{\"views\":[\"table\",\"calendar\"]}".to_string(),
        };
        BasesRepository::upsert(&connection, &updated).expect("upsert updated");

        let fetched = BasesRepository::get_by_id(&connection, "b1")
            .expect("get by id")
            .expect("base exists");
        assert_eq!(fetched.config_json, updated.config_json);

        let by_file = BasesRepository::get_by_file_id(&connection, "f1")
            .expect("get by file")
            .expect("base by file exists");
        assert_eq!(by_file.base_id, "b1");

        let deleted = BasesRepository::delete_by_id(&connection, "b1").expect("delete by id");
        assert!(deleted);
        assert!(
            BasesRepository::get_by_id(&connection, "b1")
                .expect("get deleted")
                .is_none()
        );
    }

    #[test]
    fn list_with_paths_returns_deterministic_order() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let file_a = file_record("fa", "views/a.base");
        let file_b = file_record("fb", "views/b.base");
        FilesRepository::insert(&connection, &file_b).expect("insert file b");
        FilesRepository::insert(&connection, &file_a).expect("insert file a");

        BasesRepository::upsert(
            &connection,
            &BaseRecordInput {
                base_id: "bb".to_string(),
                file_id: "fb".to_string(),
                config_json: "{}".to_string(),
            },
        )
        .expect("upsert base b");
        BasesRepository::upsert(
            &connection,
            &BaseRecordInput {
                base_id: "ba".to_string(),
                file_id: "fa".to_string(),
                config_json: "{}".to_string(),
            },
        )
        .expect("upsert base a");

        let listed = BasesRepository::list_with_paths(&connection).expect("list with paths");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].base_id, "ba");
        assert_eq!(listed[0].file_path, "views/a.base");
        assert_eq!(listed[1].base_id, "bb");
        assert_eq!(listed[1].file_path, "views/b.base");
    }
}
