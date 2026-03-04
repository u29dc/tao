use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

/// Persisted row model for `files` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecord {
    /// Stable file identifier.
    pub file_id: String,
    /// Canonical normalized path in vault.
    pub normalized_path: String,
    /// Case policy aware lookup key.
    pub match_key: String,
    /// Canonical absolute path.
    pub absolute_path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Last modified unix timestamp milliseconds.
    pub modified_unix_ms: i64,
    /// Content hash fingerprint.
    pub hash_blake3: String,
    /// Whether file is markdown content.
    pub is_markdown: bool,
    /// Indexed timestamp.
    pub indexed_at: String,
}

/// Lightweight metadata row used by drift-reconciliation scans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileReconcileRecord {
    /// Canonical normalized path in vault.
    pub normalized_path: String,
    /// Case policy aware lookup key.
    pub match_key: String,
    /// Canonical absolute path.
    pub absolute_path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Last modified unix timestamp milliseconds.
    pub modified_unix_ms: i64,
}

/// Input payload for inserting or updating file records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecordInput {
    /// Stable file identifier.
    pub file_id: String,
    /// Canonical normalized path in vault.
    pub normalized_path: String,
    /// Case policy aware lookup key.
    pub match_key: String,
    /// Canonical absolute path.
    pub absolute_path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Last modified unix timestamp milliseconds.
    pub modified_unix_ms: i64,
    /// Content hash fingerprint.
    pub hash_blake3: String,
    /// Whether file is markdown content.
    pub is_markdown: bool,
}

/// Repository operations over `files` table.
#[derive(Debug, Default, Clone, Copy)]
pub struct FilesRepository;

impl FilesRepository {
    /// Insert one file row.
    pub fn insert(
        connection: &Connection,
        record: &FileRecordInput,
    ) -> Result<(), FilesRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO files (
  file_id,
  normalized_path,
  match_key,
  absolute_path,
  size_bytes,
  modified_unix_ms,
  hash_blake3,
  is_markdown
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
"#,
                params![
                    record.file_id,
                    record.normalized_path,
                    record.match_key,
                    record.absolute_path,
                    record.size_bytes,
                    record.modified_unix_ms,
                    record.hash_blake3,
                    i64::from(record.is_markdown)
                ],
            )
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "insert",
                source,
            })?;

        Ok(())
    }

    /// Insert or update one file row keyed by `file_id`.
    pub fn upsert(
        connection: &Connection,
        record: &FileRecordInput,
    ) -> Result<(), FilesRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO files (
  file_id,
  normalized_path,
  match_key,
  absolute_path,
  size_bytes,
  modified_unix_ms,
  hash_blake3,
  is_markdown
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
ON CONFLICT(file_id)
DO UPDATE SET
  normalized_path = excluded.normalized_path,
  match_key = excluded.match_key,
  absolute_path = excluded.absolute_path,
  size_bytes = excluded.size_bytes,
  modified_unix_ms = excluded.modified_unix_ms,
  hash_blake3 = excluded.hash_blake3,
  is_markdown = excluded.is_markdown,
  indexed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
                params![
                    record.file_id,
                    record.normalized_path,
                    record.match_key,
                    record.absolute_path,
                    record.size_bytes,
                    record.modified_unix_ms,
                    record.hash_blake3,
                    i64::from(record.is_markdown)
                ],
            )
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "upsert",
                source,
            })?;

        Ok(())
    }

    /// Read one file row by file id.
    pub fn get_by_id(
        connection: &Connection,
        file_id: &str,
    ) -> Result<Option<FileRecord>, FilesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  file_id,
  normalized_path,
  match_key,
  absolute_path,
  size_bytes,
  modified_unix_ms,
  hash_blake3,
  is_markdown,
  indexed_at
FROM files
WHERE file_id = ?1
"#,
            )
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "prepare_get_by_id",
                source,
            })?;

        statement
            .query_row(params![file_id], row_to_file_record)
            .optional()
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "get_by_id",
                source,
            })
    }

    /// Read one file row by normalized path.
    pub fn get_by_normalized_path(
        connection: &Connection,
        normalized_path: &str,
    ) -> Result<Option<FileRecord>, FilesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  file_id,
  normalized_path,
  match_key,
  absolute_path,
  size_bytes,
  modified_unix_ms,
  hash_blake3,
  is_markdown,
  indexed_at
FROM files
WHERE normalized_path = ?1
"#,
            )
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "prepare_get_by_normalized_path",
                source,
            })?;

        statement
            .query_row(params![normalized_path], row_to_file_record)
            .optional()
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "get_by_normalized_path",
                source,
            })
    }

    /// Delete one file row by file id.
    pub fn delete_by_id(
        connection: &Connection,
        file_id: &str,
    ) -> Result<bool, FilesRepositoryError> {
        let deleted = connection
            .execute("DELETE FROM files WHERE file_id = ?1", params![file_id])
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "delete_by_id",
                source,
            })?;
        Ok(deleted > 0)
    }

    /// List all file rows in deterministic normalized-path order.
    pub fn list_all(connection: &Connection) -> Result<Vec<FileRecord>, FilesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  file_id,
  normalized_path,
  match_key,
  absolute_path,
  size_bytes,
  modified_unix_ms,
  hash_blake3,
  is_markdown,
  indexed_at
FROM files
ORDER BY normalized_path ASC
"#,
            )
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "prepare_list_all",
                source,
            })?;

        let rows = statement
            .query_map([], row_to_file_record)
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "list_all",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| FilesRepositoryError::Sql {
                operation: "list_all_row",
                source,
            })
        })
        .collect()
    }

    /// List lightweight file metadata rows in deterministic reconcile order.
    pub fn list_reconcile(
        connection: &Connection,
    ) -> Result<Vec<FileReconcileRecord>, FilesRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  normalized_path,
  match_key,
  absolute_path,
  size_bytes,
  modified_unix_ms
FROM files
ORDER BY match_key ASC, normalized_path ASC
"#,
            )
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "prepare_list_reconcile",
                source,
            })?;

        let rows = statement
            .query_map([], row_to_file_reconcile_record)
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "list_reconcile",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| FilesRepositoryError::Sql {
                operation: "list_reconcile_row",
                source,
            })
        })
        .collect()
    }

    /// Upsert multiple file rows within one transaction.
    pub fn bulk_upsert(
        connection: &mut Connection,
        records: &[FileRecordInput],
    ) -> Result<usize, FilesRepositoryError> {
        if records.is_empty() {
            return Ok(0);
        }

        let transaction = connection
            .transaction()
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "bulk_upsert_begin",
                source,
            })?;

        let mut statement = transaction
            .prepare(
                r#"
INSERT INTO files (
  file_id,
  normalized_path,
  match_key,
  absolute_path,
  size_bytes,
  modified_unix_ms,
  hash_blake3,
  is_markdown
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
ON CONFLICT(file_id)
DO UPDATE SET
  normalized_path = excluded.normalized_path,
  match_key = excluded.match_key,
  absolute_path = excluded.absolute_path,
  size_bytes = excluded.size_bytes,
  modified_unix_ms = excluded.modified_unix_ms,
  hash_blake3 = excluded.hash_blake3,
  is_markdown = excluded.is_markdown,
  indexed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
            )
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "bulk_upsert_prepare",
                source,
            })?;

        let mut affected = 0;
        for record in records {
            affected += statement
                .execute(params![
                    record.file_id,
                    record.normalized_path,
                    record.match_key,
                    record.absolute_path,
                    record.size_bytes,
                    record.modified_unix_ms,
                    record.hash_blake3,
                    i64::from(record.is_markdown)
                ])
                .map_err(|source| FilesRepositoryError::Sql {
                    operation: "bulk_upsert_execute",
                    source,
                })?;
        }

        drop(statement);
        transaction
            .commit()
            .map_err(|source| FilesRepositoryError::Sql {
                operation: "bulk_upsert_commit",
                source,
            })?;

        Ok(affected)
    }
}

fn row_to_file_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    let is_markdown: i64 = row.get("is_markdown")?;

    Ok(FileRecord {
        file_id: row.get("file_id")?,
        normalized_path: row.get("normalized_path")?,
        match_key: row.get("match_key")?,
        absolute_path: row.get("absolute_path")?,
        size_bytes: row.get("size_bytes")?,
        modified_unix_ms: row.get("modified_unix_ms")?,
        hash_blake3: row.get("hash_blake3")?,
        is_markdown: is_markdown != 0,
        indexed_at: row.get("indexed_at")?,
    })
}

fn row_to_file_reconcile_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileReconcileRecord> {
    Ok(FileReconcileRecord {
        normalized_path: row.get("normalized_path")?,
        match_key: row.get("match_key")?,
        absolute_path: row.get("absolute_path")?,
        size_bytes: row.get("size_bytes")?,
        modified_unix_ms: row.get("modified_unix_ms")?,
    })
}

/// Repository operation failures.
#[derive(Debug, Error)]
pub enum FilesRepositoryError {
    /// SQL error with operation context.
    #[error("files repository operation '{operation}' failed: {source}")]
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

    use crate::{FileRecordInput, FilesRepository, run_migrations};

    fn sample_record(file_id: &str, path: &str, size_bytes: u64, hash: &str) -> FileRecordInput {
        FileRecordInput {
            file_id: file_id.to_string(),
            normalized_path: path.to_string(),
            match_key: path.to_lowercase(),
            absolute_path: format!("/vault/{path}"),
            size_bytes,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: hash.to_string(),
            is_markdown: path.ends_with(".md"),
        }
    }

    #[test]
    fn files_repository_supports_insert_get_list_and_delete() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let record = sample_record("f1", "notes/today.md", 123, "abc123");
        FilesRepository::insert(&connection, &record).expect("insert");

        let fetched = FilesRepository::get_by_id(&connection, "f1")
            .expect("get by id")
            .expect("record exists");
        assert_eq!(fetched.normalized_path, "notes/today.md");
        assert_eq!(fetched.size_bytes, 123);
        assert_eq!(fetched.hash_blake3, "abc123");

        let by_path = FilesRepository::get_by_normalized_path(&connection, "notes/today.md")
            .expect("get by path")
            .expect("record exists by path");
        assert_eq!(by_path.file_id, "f1");

        let listed = FilesRepository::list_all(&connection).expect("list all");
        assert_eq!(listed.len(), 1);

        let deleted = FilesRepository::delete_by_id(&connection, "f1").expect("delete by id");
        assert!(deleted);
        assert!(
            FilesRepository::get_by_id(&connection, "f1")
                .expect("get deleted")
                .is_none()
        );
    }

    #[test]
    fn files_repository_supports_bulk_upsert_insert_and_update() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let first = sample_record("f1", "notes/a.md", 10, "h1");
        let second = sample_record("f2", "notes/b.md", 20, "h2");
        let inserted =
            FilesRepository::bulk_upsert(&mut connection, &[first.clone(), second.clone()])
                .expect("bulk insert");

        assert_eq!(inserted, 2);
        assert_eq!(
            FilesRepository::list_all(&connection)
                .expect("list after insert")
                .len(),
            2
        );

        let mut first_updated = first;
        first_updated.size_bytes = 999;
        first_updated.hash_blake3 = "h1-updated".to_string();

        let updated =
            FilesRepository::bulk_upsert(&mut connection, &[first_updated]).expect("bulk update");
        assert_eq!(updated, 1);

        let fetched = FilesRepository::get_by_id(&connection, "f1")
            .expect("get updated")
            .expect("updated row exists");
        assert_eq!(fetched.size_bytes, 999);
        assert_eq!(fetched.hash_blake3, "h1-updated");
    }

    #[test]
    fn files_repository_lists_reconcile_rows_in_match_key_order() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let records = vec![
            sample_record("f1", "Notes/Z.md", 10, "h1"),
            sample_record("f2", "notes/a.md", 20, "h2"),
            sample_record("f3", "notes/B.md", 30, "h3"),
        ];
        FilesRepository::bulk_upsert(&mut connection, &records).expect("bulk insert");

        let listed = FilesRepository::list_reconcile(&connection).expect("list reconcile");
        let ordered: Vec<&str> = listed.iter().map(|row| row.match_key.as_str()).collect();
        assert_eq!(ordered, vec!["notes/a.md", "notes/b.md", "notes/z.md"]);
    }
}
