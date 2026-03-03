use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

/// Persisted row model for `links` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRecord {
    /// Stable link identifier.
    pub link_id: String,
    /// Source file id.
    pub source_file_id: String,
    /// Raw target payload from markdown.
    pub raw_target: String,
    /// Resolved file id when link target can be resolved.
    pub resolved_file_id: Option<String>,
    /// Optional heading slug target.
    pub heading_slug: Option<String>,
    /// Optional block id target.
    pub block_id: Option<String>,
    /// Unresolved marker.
    pub is_unresolved: bool,
    /// Creation timestamp.
    pub created_at: String,
}

/// Input payload for inserting link rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRecordInput {
    /// Stable link identifier.
    pub link_id: String,
    /// Source file id.
    pub source_file_id: String,
    /// Raw target payload from markdown.
    pub raw_target: String,
    /// Resolved file id when available.
    pub resolved_file_id: Option<String>,
    /// Optional heading slug target.
    pub heading_slug: Option<String>,
    /// Optional block id target.
    pub block_id: Option<String>,
    /// Unresolved marker.
    pub is_unresolved: bool,
}

/// Link row enriched with source/target normalized paths from join queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkWithPaths {
    /// Stable link identifier.
    pub link_id: String,
    /// Source file id.
    pub source_file_id: String,
    /// Source normalized path.
    pub source_path: String,
    /// Raw target payload from markdown.
    pub raw_target: String,
    /// Resolved file id when available.
    pub resolved_file_id: Option<String>,
    /// Resolved normalized path when available.
    pub resolved_path: Option<String>,
    /// Optional heading slug target.
    pub heading_slug: Option<String>,
    /// Optional block id target.
    pub block_id: Option<String>,
    /// Unresolved marker.
    pub is_unresolved: bool,
}

/// Repository operations over `links` table.
#[derive(Debug, Default, Clone, Copy)]
pub struct LinksRepository;

impl LinksRepository {
    /// Insert one link record.
    pub fn insert(
        connection: &Connection,
        record: &LinkRecordInput,
    ) -> Result<(), LinksRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO links (
  link_id,
  source_file_id,
  raw_target,
  resolved_file_id,
  heading_slug,
  block_id,
  is_unresolved
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
"#,
                params![
                    record.link_id,
                    record.source_file_id,
                    record.raw_target,
                    record.resolved_file_id,
                    record.heading_slug,
                    record.block_id,
                    i64::from(record.is_unresolved)
                ],
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "insert",
                source,
            })?;

        Ok(())
    }

    /// Fetch one link by id.
    pub fn get_by_id(
        connection: &Connection,
        link_id: &str,
    ) -> Result<Option<LinkRecord>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  link_id,
  source_file_id,
  raw_target,
  resolved_file_id,
  heading_slug,
  block_id,
  is_unresolved,
  created_at
FROM links
WHERE link_id = ?1
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_get_by_id",
                source,
            })?;

        statement
            .query_row(params![link_id], row_to_link_record)
            .optional()
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "get_by_id",
                source,
            })
    }

    /// List outgoing links for source file with joined file paths.
    pub fn list_outgoing_with_paths(
        connection: &Connection,
        source_file_id: &str,
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
WHERE l.source_file_id = ?1
ORDER BY l.link_id ASC
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_list_outgoing_with_paths",
                source,
            })?;

        let rows = statement
            .query_map(params![source_file_id], row_to_link_with_paths)
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_outgoing_with_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_outgoing_with_paths_row",
                source,
            })
        })
        .collect()
    }

    /// List backlinks for resolved target file with joined file paths.
    pub fn list_backlinks_with_paths(
        connection: &Connection,
        target_file_id: &str,
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
WHERE l.resolved_file_id = ?1
ORDER BY sf.normalized_path ASC, l.link_id ASC
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_list_backlinks_with_paths",
                source,
            })?;

        let rows = statement
            .query_map(params![target_file_id], row_to_link_with_paths)
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_backlinks_with_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_backlinks_with_paths_row",
                source,
            })
        })
        .collect()
    }
}

fn row_to_link_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<LinkRecord> {
    let is_unresolved: i64 = row.get("is_unresolved")?;

    Ok(LinkRecord {
        link_id: row.get("link_id")?,
        source_file_id: row.get("source_file_id")?,
        raw_target: row.get("raw_target")?,
        resolved_file_id: row.get("resolved_file_id")?,
        heading_slug: row.get("heading_slug")?,
        block_id: row.get("block_id")?,
        is_unresolved: is_unresolved != 0,
        created_at: row.get("created_at")?,
    })
}

fn row_to_link_with_paths(row: &rusqlite::Row<'_>) -> rusqlite::Result<LinkWithPaths> {
    let is_unresolved: i64 = row.get("is_unresolved")?;

    Ok(LinkWithPaths {
        link_id: row.get("link_id")?,
        source_file_id: row.get("source_file_id")?,
        source_path: row.get("source_path")?,
        raw_target: row.get("raw_target")?,
        resolved_file_id: row.get("resolved_file_id")?,
        resolved_path: row.get("resolved_path")?,
        heading_slug: row.get("heading_slug")?,
        block_id: row.get("block_id")?,
        is_unresolved: is_unresolved != 0,
    })
}

/// Links repository operation failures.
#[derive(Debug, Error)]
pub enum LinksRepositoryError {
    /// SQL error with operation context.
    #[error("links repository operation '{operation}' failed: {source}")]
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
        FileRecordInput, FilesRepository, LinkRecordInput, LinksRepository, run_migrations,
    };

    fn file(file_id: &str, path: &str) -> FileRecordInput {
        FileRecordInput {
            file_id: file_id.to_string(),
            normalized_path: path.to_string(),
            match_key: path.to_lowercase(),
            absolute_path: format!("/vault/{path}"),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: format!("hash-{file_id}"),
            is_markdown: path.ends_with(".md"),
        }
    }

    #[test]
    fn link_join_queries_return_source_and_target_paths() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &file("source", "notes/source.md"))
            .expect("insert source file");
        FilesRepository::insert(&connection, &file("target", "notes/target.md"))
            .expect("insert target file");

        let link = LinkRecordInput {
            link_id: "l1".to_string(),
            source_file_id: "source".to_string(),
            raw_target: "target".to_string(),
            resolved_file_id: Some("target".to_string()),
            heading_slug: Some("heading".to_string()),
            block_id: None,
            is_unresolved: false,
        };

        LinksRepository::insert(&connection, &link).expect("insert link");

        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, "source")
            .expect("list outgoing");
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].source_path, "notes/source.md");
        assert_eq!(
            outgoing[0].resolved_path.as_deref(),
            Some("notes/target.md")
        );

        let backlinks = LinksRepository::list_backlinks_with_paths(&connection, "target")
            .expect("list backlinks");
        assert_eq!(backlinks.len(), 1);
        assert_eq!(backlinks[0].source_path, "notes/source.md");
        assert_eq!(
            backlinks[0].resolved_path.as_deref(),
            Some("notes/target.md")
        );
    }
}
