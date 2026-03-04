//! Deterministic query service for indexed vault notes.

use std::fs;
use std::path::Path;

use rusqlite::Connection;
use tao_sdk_storage::FilesRepository;
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
        vault_root: &Path,
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

        let needle = query.to_ascii_lowercase();
        let mut scored = Vec::new();
        let files = FilesRepository::list_all(connection)
            .map_err(|source| SearchQueryError::ListIndexedFiles { source })?;

        for file in files.into_iter().filter(|file| file.is_markdown) {
            let title = title_from_path(&file.normalized_path);
            let mut matched_in = Vec::new();
            let path_lc = file.normalized_path.to_ascii_lowercase();
            let title_lc = title.to_ascii_lowercase();
            let mut score = 0_u8;

            if title_lc.contains(&needle) {
                matched_in.push("title".to_string());
                score = score.saturating_add(3);
            }
            if path_lc.contains(&needle) {
                matched_in.push("path".to_string());
                score = score.saturating_add(2);
            }

            if matched_in.is_empty() {
                let absolute_path = vault_root.join(&file.normalized_path);
                if let Ok(content) = fs::read_to_string(&absolute_path)
                    && content.to_ascii_lowercase().contains(&needle)
                {
                    matched_in.push("content".to_string());
                    score = score.saturating_add(1);
                }
            }

            if matched_in.is_empty() {
                continue;
            }

            scored.push((
                score,
                SearchQueryItem {
                    file_id: file.file_id,
                    path: file.normalized_path,
                    title,
                    indexed_at: file.indexed_at,
                    matched_in,
                },
            ));
        }

        scored.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.path.cmp(&right.1.path))
        });

        let total = scored.len() as u64;
        let offset = usize::try_from(request.offset).unwrap_or(usize::MAX);
        let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);
        let items = scored
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(_, row)| row)
            .collect::<Vec<_>>();

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

/// Search query failures.
#[derive(Debug, Error)]
pub enum SearchQueryError {
    /// Query text was empty.
    #[error("search query must not be empty")]
    EmptyQuery,
    /// Limit was outside valid range.
    #[error("search query limit must be between 1 and 1000 (got {value})")]
    InvalidLimit { value: u64 },
    /// Reading indexed file rows failed.
    #[error("failed to list indexed files for search: {source}")]
    ListIndexedFiles {
        /// Files repository failure.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
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
