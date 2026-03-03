use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

/// Persisted row model for `render_cache` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderCacheRecord {
    /// Stable cache key.
    pub cache_key: String,
    /// Optional associated file id.
    pub file_id: Option<String>,
    /// Cached rendered html payload.
    pub html: String,
    /// Source content hash used for cache validity.
    pub content_hash: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Input payload for render cache upserts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderCacheRecordInput {
    /// Stable cache key.
    pub cache_key: String,
    /// Optional associated file id.
    pub file_id: Option<String>,
    /// Cached rendered html payload.
    pub html: String,
    /// Source content hash used for cache validity.
    pub content_hash: String,
}

/// Repository operations over `render_cache` table.
#[derive(Debug, Default, Clone, Copy)]
pub struct RenderCacheRepository;

impl RenderCacheRepository {
    /// Insert or update one render cache row keyed by `cache_key`.
    pub fn upsert(
        connection: &Connection,
        cache: &RenderCacheRecordInput,
    ) -> Result<(), RenderCacheRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO render_cache (
  cache_key,
  file_id,
  html,
  content_hash
)
VALUES (?1, ?2, ?3, ?4)
ON CONFLICT(cache_key)
DO UPDATE SET
  file_id = excluded.file_id,
  html = excluded.html,
  content_hash = excluded.content_hash,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
                params![
                    cache.cache_key,
                    cache.file_id,
                    cache.html,
                    cache.content_hash
                ],
            )
            .map_err(|source| RenderCacheRepositoryError::Sql {
                operation: "upsert",
                source,
            })?;

        Ok(())
    }

    /// Fetch one render cache row by `cache_key`.
    pub fn get_by_key(
        connection: &Connection,
        cache_key: &str,
    ) -> Result<Option<RenderCacheRecord>, RenderCacheRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  cache_key,
  file_id,
  html,
  content_hash,
  updated_at
FROM render_cache
WHERE cache_key = ?1
"#,
            )
            .map_err(|source| RenderCacheRepositoryError::Sql {
                operation: "prepare_get_by_key",
                source,
            })?;

        statement
            .query_row(params![cache_key], row_to_record)
            .optional()
            .map_err(|source| RenderCacheRepositoryError::Sql {
                operation: "get_by_key",
                source,
            })
    }

    /// List all cache rows for one file id.
    pub fn list_for_file(
        connection: &Connection,
        file_id: &str,
    ) -> Result<Vec<RenderCacheRecord>, RenderCacheRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  cache_key,
  file_id,
  html,
  content_hash,
  updated_at
FROM render_cache
WHERE file_id = ?1
ORDER BY cache_key ASC
"#,
            )
            .map_err(|source| RenderCacheRepositoryError::Sql {
                operation: "prepare_list_for_file",
                source,
            })?;

        let rows = statement
            .query_map(params![file_id], row_to_record)
            .map_err(|source| RenderCacheRepositoryError::Sql {
                operation: "list_for_file",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| RenderCacheRepositoryError::Sql {
                operation: "list_for_file_row",
                source,
            })
        })
        .collect()
    }

    /// Delete one cache row by cache key.
    pub fn delete_by_key(
        connection: &Connection,
        cache_key: &str,
    ) -> Result<bool, RenderCacheRepositoryError> {
        let deleted = connection
            .execute(
                "DELETE FROM render_cache WHERE cache_key = ?1",
                params![cache_key],
            )
            .map_err(|source| RenderCacheRepositoryError::Sql {
                operation: "delete_by_key",
                source,
            })?;

        Ok(deleted > 0)
    }

    /// Delete all cache rows for one file id.
    pub fn delete_for_file(
        connection: &Connection,
        file_id: &str,
    ) -> Result<u64, RenderCacheRepositoryError> {
        let deleted = connection
            .execute(
                "DELETE FROM render_cache WHERE file_id = ?1",
                params![file_id],
            )
            .map_err(|source| RenderCacheRepositoryError::Sql {
                operation: "delete_for_file",
                source,
            })?;

        Ok(deleted as u64)
    }
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<RenderCacheRecord> {
    Ok(RenderCacheRecord {
        cache_key: row.get("cache_key")?,
        file_id: row.get("file_id")?,
        html: row.get("html")?,
        content_hash: row.get("content_hash")?,
        updated_at: row.get("updated_at")?,
    })
}

/// Render cache repository operation failures.
#[derive(Debug, Error)]
pub enum RenderCacheRepositoryError {
    /// SQL error with operation context.
    #[error("render cache repository operation '{operation}' failed: {source}")]
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
        FileRecordInput, FilesRepository, RenderCacheRecordInput, RenderCacheRepository,
        run_migrations,
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
    fn upsert_get_list_and_delete_render_cache_rows() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let file = file_record("f1", "notes/a.md");
        FilesRepository::insert(&connection, &file).expect("insert file");

        let first = RenderCacheRecordInput {
            cache_key: "k1".to_string(),
            file_id: Some("f1".to_string()),
            html: "<h1>A</h1>".to_string(),
            content_hash: "h1".to_string(),
        };
        RenderCacheRepository::upsert(&connection, &first).expect("upsert first");

        let updated = RenderCacheRecordInput {
            cache_key: "k1".to_string(),
            file_id: Some("f1".to_string()),
            html: "<h1>A+</h1>".to_string(),
            content_hash: "h2".to_string(),
        };
        RenderCacheRepository::upsert(&connection, &updated).expect("upsert updated");

        let second = RenderCacheRecordInput {
            cache_key: "k2".to_string(),
            file_id: Some("f1".to_string()),
            html: "<p>x</p>".to_string(),
            content_hash: "h3".to_string(),
        };
        RenderCacheRepository::upsert(&connection, &second).expect("upsert second");

        let fetched = RenderCacheRepository::get_by_key(&connection, "k1")
            .expect("get by key")
            .expect("row exists");
        assert_eq!(fetched.html, "<h1>A+</h1>");
        assert_eq!(fetched.content_hash, "h2");

        let listed =
            RenderCacheRepository::list_for_file(&connection, "f1").expect("list for file");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].cache_key, "k1");
        assert_eq!(listed[1].cache_key, "k2");

        let deleted_one =
            RenderCacheRepository::delete_by_key(&connection, "k2").expect("delete by key");
        assert!(deleted_one);

        let deleted_for_file =
            RenderCacheRepository::delete_for_file(&connection, "f1").expect("delete for file");
        assert_eq!(deleted_for_file, 1);
        assert!(
            RenderCacheRepository::get_by_key(&connection, "k1")
                .expect("get deleted")
                .is_none()
        );
    }

    #[test]
    fn upsert_supports_unbound_rows_without_file_id() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let unbound = RenderCacheRecordInput {
            cache_key: "orphankey".to_string(),
            file_id: None,
            html: "<p>orphan</p>".to_string(),
            content_hash: "hash".to_string(),
        };
        RenderCacheRepository::upsert(&connection, &unbound).expect("upsert unbound");

        let fetched = RenderCacheRepository::get_by_key(&connection, "orphankey")
            .expect("get unbound")
            .expect("row exists");
        assert!(fetched.file_id.is_none());
    }
}
