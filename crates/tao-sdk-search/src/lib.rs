//! Deterministic query service for indexed vault notes.

use std::path::Path;

use rusqlite::{Connection, params};
use thiserror::Error;

/// Search query input parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQueryRequest {
    /// Raw query string.
    pub query: String,
    /// One-based page window size.
    pub limit: u64,
    /// Zero-based offset.
    pub offset: u64,
}

/// One matched note row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQueryItem {
    /// Stable file id from index.
    pub file_id: String,
    /// Normalized vault-relative path.
    pub path: String,
    /// File stem title projection.
    pub title: String,
    /// Indexed timestamp string.
    pub indexed_at: String,
    /// Ordered list of matching surfaces: `title`, `path`, `content`.
    pub matched_in: Vec<String>,
}

/// Search page payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQueryPage {
    /// Original normalized query.
    pub query: String,
    /// Requested limit.
    pub limit: u64,
    /// Requested offset.
    pub offset: u64,
    /// Total matched rows before pagination.
    pub total: u64,
    /// Windowed result rows.
    pub items: Vec<SearchQueryItem>,
}

/// Search query service over indexed markdown files.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchQueryService;

impl SearchQueryService {
    /// Execute one deterministic search query over title/path/content surfaces.
    pub fn query(
        &self,
        _vault_root: &Path,
        connection: &Connection,
        request: SearchQueryRequest,
    ) -> Result<SearchQueryPage, SearchQueryError> {
        let query = request.query.trim();
        if query.is_empty() {
            return Err(SearchQueryError::EmptyQuery);
        }
        if request.limit == 0 || request.limit > 1_000 {
            return Err(SearchQueryError::InvalidLimit {
                value: request.limit,
            });
        }
        let limit_i64 =
            i64::try_from(request.limit).map_err(|_| SearchQueryError::InvalidLimit {
                value: request.limit,
            })?;
        let offset_i64 =
            i64::try_from(request.offset).map_err(|_| SearchQueryError::InvalidOffset {
                value: request.offset,
            })?;

        let needle = query.to_ascii_lowercase();
        let fts_query = build_fts_query(query);
        let mut statement = connection
            .prepare_cached(
                r#"
WITH matches AS (
  SELECT
    si.file_id,
    COALESCE(si.normalized_path, si.normalized_path_lc) AS normalized_path,
    si.updated_at AS indexed_at,
    si.title_lc,
    si.normalized_path_lc,
    si.content_lc
  FROM search_index si
  JOIN search_index_fts ON search_index_fts.rowid = si.rowid
  WHERE search_index_fts MATCH ?1
),
scored AS (
  SELECT
    file_id,
    normalized_path,
    indexed_at,
    CASE WHEN instr(title_lc, ?2) > 0 THEN 1 ELSE 0 END AS title_match,
    CASE WHEN instr(normalized_path_lc, ?2) > 0 THEN 1 ELSE 0 END AS path_match,
    CASE WHEN instr(content_lc, ?2) > 0 THEN 1 ELSE 0 END AS content_match
  FROM matches
)
SELECT
  file_id,
  normalized_path,
  indexed_at,
  title_match,
  path_match,
  content_match,
  (
    CASE WHEN title_match > 0 THEN 3 ELSE 0 END
    + CASE WHEN path_match > 0 THEN 2 ELSE 0 END
    + CASE WHEN content_match > 0 THEN 1 ELSE 0 END
  ) AS score,
  COUNT(*) OVER() AS total_count
FROM scored
ORDER BY score DESC, normalized_path ASC
LIMIT ?3
OFFSET ?4
"#,
            )
            .map_err(|source| SearchQueryError::PrepareQuery { source })?;

        let rows = statement
            .query_map(params![fts_query, needle, limit_i64, offset_i64], |row| {
                let path: String = row.get("normalized_path")?;
                let title_match: i64 = row.get("title_match")?;
                let path_match: i64 = row.get("path_match")?;
                let content_match: i64 = row.get("content_match")?;
                let total: u64 = row.get("total_count")?;
                let mut matched_in = Vec::new();
                if title_match != 0 {
                    matched_in.push("title".to_string());
                }
                if path_match != 0 {
                    matched_in.push("path".to_string());
                }
                if matched_in.is_empty() && content_match != 0 {
                    matched_in.push("content".to_string());
                }
                Ok(SearchQueryItem {
                    file_id: row.get("file_id")?,
                    title: title_from_path(&path),
                    path,
                    indexed_at: row.get("indexed_at")?,
                    matched_in,
                })
                .map(|item| (item, total))
            })
            .map_err(|source| SearchQueryError::RunQuery { source })?;
        let mut total = 0_u64;
        let items = rows
            .map(|row| row.map_err(|source| SearchQueryError::MapQueryRow { source }))
            .map(|row| {
                row.map(|(item, row_total)| {
                    total = row_total;
                    item
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SearchQueryPage {
            query: query.to_string(),
            limit: request.limit,
            offset: request.offset,
            total,
            items,
        })
    }
}

fn title_from_path(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(std::string::ToString::to_string)
        .unwrap_or_else(|| path.to_string())
}

fn build_fts_query(query: &str) -> String {
    let tokens = query
        .split_whitespace()
        .filter_map(|token| {
            let sanitized = token
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/'))
                .collect::<String>()
                .to_ascii_lowercase();
            if sanitized.is_empty() {
                None
            } else {
                Some(sanitized)
            }
        })
        .collect::<Vec<_>>();

    if tokens.is_empty() {
        return String::from("\"\"");
    }

    tokens
        .into_iter()
        .map(|token| format!("\"{token}\"*"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Search query failures.
#[derive(Debug, Error)]
pub enum SearchQueryError {
    /// Query text was empty.
    #[error("search query must not be empty")]
    EmptyQuery,
    /// Limit was outside valid range.
    #[error("search query limit must be between 1 and 1000 (got {value})")]
    InvalidLimit { value: u64 },
    /// Offset value overflows supported sqlite integer range.
    #[error("search query offset exceeds sqlite integer range (got {value})")]
    InvalidOffset { value: u64 },
    /// Preparing paged query failed.
    #[error("failed to prepare paged search query: {source}")]
    PrepareQuery {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Running paged query failed.
    #[error("failed to execute paged search query: {source}")]
    RunQuery {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Mapping one query row failed.
    #[error("failed to map one paged search row: {source}")]
    MapQueryRow {
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rusqlite::Connection;
    use tao_sdk_service::FullIndexService;
    use tao_sdk_storage::run_migrations;
    use tao_sdk_vault::CasePolicy;
    use tempfile::tempdir;

    use super::{SearchQueryRequest, SearchQueryService};

    #[test]
    fn query_matches_title_path_and_content_surfaces() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(vault.join("notes/deep")).expect("create notes");
        fs::write(vault.join("notes/alpha.md"), "# Alpha\nhello world").expect("write alpha");
        fs::write(
            vault.join("notes/deep/project-note.md"),
            "# Overview\nno keyword",
        )
        .expect("write project");

        let mut connection = Connection::open(temp.path().join("index.sqlite")).expect("open db");
        run_migrations(&mut connection).expect("migrate");
        FullIndexService::default()
            .rebuild(&vault, &mut connection, CasePolicy::Sensitive)
            .expect("rebuild");

        let result = SearchQueryService
            .query(
                &vault,
                &connection,
                SearchQueryRequest {
                    query: "project".to_string(),
                    limit: 50,
                    offset: 0,
                },
            )
            .expect("query");
        assert_eq!(result.total, 1);
        assert_eq!(result.items[0].path, "notes/deep/project-note.md");
        assert!(result.items[0].matched_in.contains(&"title".to_string()));
        assert!(result.items[0].matched_in.contains(&"path".to_string()));

        let content = SearchQueryService
            .query(
                &vault,
                &connection,
                SearchQueryRequest {
                    query: "hello".to_string(),
                    limit: 50,
                    offset: 0,
                },
            )
            .expect("content query");
        assert_eq!(content.total, 1);
        assert_eq!(content.items[0].path, "notes/alpha.md");
        assert_eq!(content.items[0].matched_in, vec!["content".to_string()]);
    }

    #[test]
    fn query_applies_offset_and_limit() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(vault.join("notes")).expect("create notes");
        fs::write(vault.join("notes/a.md"), "# A").expect("write a");
        fs::write(vault.join("notes/b.md"), "# B").expect("write b");
        fs::write(vault.join("notes/c.md"), "# C").expect("write c");

        let mut connection = Connection::open(temp.path().join("index.sqlite")).expect("open db");
        run_migrations(&mut connection).expect("migrate");
        FullIndexService::default()
            .rebuild(&vault, &mut connection, CasePolicy::Sensitive)
            .expect("rebuild");

        let result = SearchQueryService
            .query(
                &vault,
                &connection,
                SearchQueryRequest {
                    query: "notes".to_string(),
                    limit: 1,
                    offset: 1,
                },
            )
            .expect("query");
        assert_eq!(result.total, 3);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].path, "notes/b.md");
    }
}
