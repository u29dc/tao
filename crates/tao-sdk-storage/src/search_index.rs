use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

/// Persisted row model for `search_index` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexRecord {
    /// Stable file identifier.
    pub file_id: String,
    /// Normalized path projection preserving original path casing.
    pub normalized_path: String,
    /// Lower-cased normalized path projection.
    pub normalized_path_lc: String,
    /// Lower-cased title projection.
    pub title_lc: String,
    /// Lower-cased markdown content projection.
    pub content_lc: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Input payload for inserting or updating search index rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexRecordInput {
    /// Stable file identifier.
    pub file_id: String,
    /// Normalized path projection preserving original path casing.
    pub normalized_path: String,
    /// Lower-cased normalized path projection.
    pub normalized_path_lc: String,
    /// Lower-cased title projection.
    pub title_lc: String,
    /// Lower-cased markdown content projection.
    pub content_lc: String,
}

/// Repository operations over `search_index` table.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchIndexRepository;

impl SearchIndexRepository {
    /// Insert or update one search index row keyed by `file_id`.
    pub fn upsert(
        connection: &Connection,
        record: &SearchIndexRecordInput,
    ) -> Result<(), SearchIndexRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO search_index (
  file_id,
  normalized_path,
  normalized_path_lc,
  title_lc,
  content_lc
)
VALUES (?1, ?2, ?3, ?4, ?5)
ON CONFLICT(file_id)
DO UPDATE SET
  normalized_path = excluded.normalized_path,
  normalized_path_lc = excluded.normalized_path_lc,
  title_lc = excluded.title_lc,
  content_lc = excluded.content_lc,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
                params![
                    record.file_id,
                    record.normalized_path,
                    record.normalized_path_lc,
                    record.title_lc,
                    record.content_lc
                ],
            )
            .map_err(|source| SearchIndexRepositoryError::Sql {
                operation: "upsert",
                source,
            })?;

        Ok(())
    }

    /// Read one search index row by file id.
    pub fn get_by_file_id(
        connection: &Connection,
        file_id: &str,
    ) -> Result<Option<SearchIndexRecord>, SearchIndexRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  file_id,
  normalized_path,
  normalized_path_lc,
  title_lc,
  content_lc,
  updated_at
FROM search_index
WHERE file_id = ?1
"#,
            )
            .map_err(|source| SearchIndexRepositoryError::Sql {
                operation: "prepare_get_by_file_id",
                source,
            })?;

        statement
            .query_row(params![file_id], row_to_search_index_record)
            .optional()
            .map_err(|source| SearchIndexRepositoryError::Sql {
                operation: "get_by_file_id",
                source,
            })
    }

    /// Delete one search index row by file id.
    pub fn delete_by_file_id(
        connection: &Connection,
        file_id: &str,
    ) -> Result<bool, SearchIndexRepositoryError> {
        let deleted = connection
            .execute(
                "DELETE FROM search_index WHERE file_id = ?1",
                params![file_id],
            )
            .map_err(|source| SearchIndexRepositoryError::Sql {
                operation: "delete_by_file_id",
                source,
            })?;
        Ok(deleted > 0)
    }

    /// List all search index rows in deterministic file-id order.
    pub fn list_all(
        connection: &Connection,
    ) -> Result<Vec<SearchIndexRecord>, SearchIndexRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  file_id,
  normalized_path,
  normalized_path_lc,
  title_lc,
  content_lc,
  updated_at
FROM search_index
ORDER BY file_id ASC
"#,
            )
            .map_err(|source| SearchIndexRepositoryError::Sql {
                operation: "prepare_list_all",
                source,
            })?;

        let rows = statement
            .query_map([], row_to_search_index_record)
            .map_err(|source| SearchIndexRepositoryError::Sql {
                operation: "list_all",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| SearchIndexRepositoryError::Sql {
                operation: "list_all_row",
                source,
            })
        })
        .collect()
    }
}

fn row_to_search_index_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchIndexRecord> {
    Ok(SearchIndexRecord {
        file_id: row.get("file_id")?,
        normalized_path: row.get("normalized_path")?,
        normalized_path_lc: row.get("normalized_path_lc")?,
        title_lc: row.get("title_lc")?,
        content_lc: row.get("content_lc")?,
        updated_at: row.get("updated_at")?,
    })
}

/// Repository operation failures.
#[derive(Debug, Error)]
pub enum SearchIndexRepositoryError {
    /// SQL error with operation context.
    #[error("search_index repository operation '{operation}' failed: {source}")]
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
        FileRecordInput, FilesRepository, SearchIndexRecordInput, SearchIndexRepository,
        run_migrations,
    };

    fn sample_file(file_id: &str, path: &str) -> FileRecordInput {
        FileRecordInput {
            file_id: file_id.to_string(),
            normalized_path: path.to_string(),
            match_key: path.to_ascii_lowercase(),
            absolute_path: format!("/vault/{path}"),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: "hash".to_string(),
            is_markdown: path.ends_with(".md"),
        }
    }

    #[test]
    fn search_index_repository_supports_upsert_get_list_and_delete() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &sample_file("f1", "notes/alpha.md"))
            .expect("insert file");

        SearchIndexRepository::upsert(
            &connection,
            &SearchIndexRecordInput {
                file_id: "f1".to_string(),
                normalized_path: "notes/alpha.md".to_string(),
                normalized_path_lc: "notes/alpha.md".to_string(),
                title_lc: "alpha".to_string(),
                content_lc: "hello world".to_string(),
            },
        )
        .expect("upsert search row");

        let fetched = SearchIndexRepository::get_by_file_id(&connection, "f1")
            .expect("get by file_id")
            .expect("search row exists");
        assert_eq!(fetched.title_lc, "alpha");
        assert_eq!(fetched.content_lc, "hello world");

        let listed = SearchIndexRepository::list_all(&connection).expect("list all");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].file_id, "f1");

        let deleted =
            SearchIndexRepository::delete_by_file_id(&connection, "f1").expect("delete by file_id");
        assert!(deleted);
        assert!(
            SearchIndexRepository::get_by_file_id(&connection, "f1")
                .expect("read deleted search row")
                .is_none()
        );
    }

    #[test]
    fn search_index_repository_rows_cascade_on_file_delete() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &sample_file("f1", "notes/alpha.md"))
            .expect("insert file");
        SearchIndexRepository::upsert(
            &connection,
            &SearchIndexRecordInput {
                file_id: "f1".to_string(),
                normalized_path: "notes/alpha.md".to_string(),
                normalized_path_lc: "notes/alpha.md".to_string(),
                title_lc: "alpha".to_string(),
                content_lc: "hello world".to_string(),
            },
        )
        .expect("upsert search row");

        FilesRepository::delete_by_id(&connection, "f1").expect("delete file");
        assert!(
            SearchIndexRepository::get_by_file_id(&connection, "f1")
                .expect("get cascaded row")
                .is_none()
        );
    }
}
