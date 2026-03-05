//! Deterministic query service for indexed vault notes.

use std::path::Path;

use rusqlite::Connection;
use thiserror::Error;

pub mod adapters;
pub mod execution;
pub mod logical_plan;
pub mod optimizer;
pub mod parser;
pub mod physical_plan;

pub use execution::{
    QueryEvalError, apply_sort, apply_where_filter, title_from_path as derive_title_from_path,
};
pub use logical_plan::{LogicalPlanBuilder, LogicalQueryPlan, LogicalQueryPlanRequest, QueryScope};
pub use optimizer::PhysicalPlanOptimizer;
pub use parser::{
    CompareOp, LiteralValue, NullOrder, ParseError, SortDirection, SortKey, WhereExpr,
    parse_sort_keys, parse_where_expression, parse_where_expression_opt,
};
pub use physical_plan::{PhysicalPlanBuilder, PhysicalQueryPlan};

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

/// Optional field projection for search query output rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchQueryProjection {
    /// Include stable file id values.
    pub include_file_id: bool,
    /// Include normalized path values.
    pub include_path: bool,
    /// Include derived title values.
    pub include_title: bool,
    /// Include matching-surface classification values.
    pub include_matched_in: bool,
}

impl Default for SearchQueryProjection {
    fn default() -> Self {
        Self {
            include_file_id: true,
            include_path: true,
            include_title: true,
            include_matched_in: true,
        }
    }
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

/// One matched note row for projected queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQueryProjectedItem {
    /// Stable file id from index (when projected).
    pub file_id: Option<String>,
    /// Normalized vault-relative path (when projected).
    pub path: Option<String>,
    /// File stem title projection (when projected).
    pub title: Option<String>,
    /// Indexed timestamp string.
    pub indexed_at: String,
    /// Ordered list of matching surfaces when projected.
    pub matched_in: Option<Vec<String>>,
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

/// Search page payload for projected search queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQueryProjectedPage {
    /// Original normalized query.
    pub query: String,
    /// Requested limit.
    pub limit: u64,
    /// Requested offset.
    pub offset: u64,
    /// Total matched rows before pagination.
    pub total: u64,
    /// Windowed result rows.
    pub items: Vec<SearchQueryProjectedItem>,
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
        let projected = self.query_projected(
            vault_root,
            connection,
            request,
            SearchQueryProjection::default(),
        )?;
        let items = projected
            .items
            .into_iter()
            .map(|item| SearchQueryItem {
                file_id: item.file_id.unwrap_or_default(),
                path: item.path.unwrap_or_default(),
                title: item.title.unwrap_or_default(),
                indexed_at: item.indexed_at,
                matched_in: item.matched_in.unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        Ok(SearchQueryPage {
            query: projected.query,
            limit: projected.limit,
            offset: projected.offset,
            total: projected.total,
            items,
        })
    }

    /// Execute one deterministic projected search query over title/path/content surfaces.
    pub fn query_projected(
        &self,
        _vault_root: &Path,
        connection: &Connection,
        request: SearchQueryRequest,
        projection: SearchQueryProjection,
    ) -> Result<SearchQueryProjectedPage, SearchQueryError> {
        execution::execute_projected_query(connection, request, projection)
    }
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
    fn query_supports_non_ascii_terms_with_unicode_casing() {
        let temp = tempdir().expect("tempdir");
        let vault = temp.path().join("vault");
        fs::create_dir_all(vault.join("notes")).expect("create notes");
        fs::write(vault.join("notes/cafe.md"), "# Cafe\nCafé au lait").expect("write cafe");

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
                    query: "CAFÉ".to_string(),
                    limit: 50,
                    offset: 0,
                },
            )
            .expect("query non ascii");
        assert_eq!(result.total, 1);
        assert_eq!(result.items[0].path, "notes/cafe.md");
        assert!(result.items[0].matched_in.contains(&"content".to_string()));
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
