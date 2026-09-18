//! Property query service for structured property lookup and filtering.

use rusqlite::{Connection, params};
use thiserror::Error;

/// Sorting strategies supported by property query APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyQuerySort {
    /// Sort by file path ascending.
    FilePathAsc,
    /// Sort by file path descending.
    FilePathDesc,
    /// Sort by update timestamp ascending.
    UpdatedAtAsc,
    /// Sort by update timestamp descending.
    UpdatedAtDesc,
    /// Sort by raw JSON value ascending.
    ValueAsc,
    /// Sort by raw JSON value descending.
    ValueDesc,
}

/// Request payload for property query APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyQueryRequest {
    /// Property key to query across files.
    pub key: String,
    /// Optional substring filter applied to JSON value payload.
    pub value_contains: Option<String>,
    /// Optional max rows to return.
    pub limit: Option<usize>,
    /// Row offset for pagination.
    pub offset: usize,
    /// Sort strategy.
    pub sort: PropertyQuerySort,
}

/// Property query row returned by query APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyQueryRow {
    /// Stable property id.
    pub property_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning file normalized path.
    pub file_path: String,
    /// Property key.
    pub key: String,
    /// Property value type.
    pub value_type: String,
    /// Property value payload JSON.
    pub value_json: String,
    /// Updated timestamp.
    pub updated_at: String,
}

/// Property query result page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyQueryResult {
    /// Total rows matching filters before pagination.
    pub total: u64,
    /// Page rows after sort/pagination.
    pub rows: Vec<PropertyQueryRow>,
}

/// Query service for filtering and sorting property rows across files.
#[derive(Debug, Default, Clone, Copy)]
pub struct PropertyQueryService;

impl PropertyQueryService {
    /// Query property rows by key with filter/sort/pagination controls.
    pub fn query(
        &self,
        connection: &Connection,
        request: &PropertyQueryRequest,
    ) -> Result<PropertyQueryResult, PropertyQueryError> {
        let key = request.key.trim();
        if key.is_empty() {
            return Err(PropertyQueryError::InvalidKey);
        }
        if matches!(request.limit, Some(0)) {
            return Err(PropertyQueryError::InvalidLimit { limit: 0 });
        }

        let order = match request.sort {
            PropertyQuerySort::FilePathAsc => "f.normalized_path ASC, p.property_id ASC",
            PropertyQuerySort::FilePathDesc => "f.normalized_path DESC, p.property_id ASC",
            PropertyQuerySort::UpdatedAtAsc => {
                "p.updated_at ASC, f.normalized_path ASC, p.property_id ASC"
            }
            PropertyQuerySort::UpdatedAtDesc => {
                "p.updated_at DESC, f.normalized_path ASC, p.property_id ASC"
            }
            PropertyQuerySort::ValueAsc => {
                "p.value_json ASC, f.normalized_path ASC, p.property_id ASC"
            }
            PropertyQuerySort::ValueDesc => {
                "p.value_json DESC, f.normalized_path ASC, p.property_id ASC"
            }
        };
        let filter = request
            .value_contains
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_lowercase);
        // SQLite's lower() is ASCII-only. Keep Unicode substring semantics in a
        // streaming fallback, rather than silently changing them for pushdown.
        let (limit, offset) = if filter.is_some() {
            (-1, 0)
        } else {
            (
                request
                    .limit
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| PropertyQueryError::InvalidLimit {
                        limit: request.limit.unwrap_or_default(),
                    })?
                    .unwrap_or(-1),
                i64::try_from(request.offset).map_err(|_| PropertyQueryError::InvalidOffset {
                    offset: request.offset,
                })?,
            )
        };
        let query = format!(
            "SELECT p.property_id, p.file_id, f.normalized_path, p.key, p.value_type, p.value_json, p.updated_at FROM properties p JOIN files f ON f.file_id = p.file_id WHERE p.key = ?1 ORDER BY {order} LIMIT ?2 OFFSET ?3"
        );
        let mut statement = connection
            .prepare(&query)
            .map_err(|source| PropertyQueryError::Sql { source })?;
        let mapped = statement
            .query_map(params![key, limit, offset], |row| {
                Ok(PropertyQueryRow {
                    property_id: row.get(0)?,
                    file_id: row.get(1)?,
                    file_path: row.get(2)?,
                    key: row.get(3)?,
                    value_type: row.get(4)?,
                    value_json: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            })
            .map_err(|source| PropertyQueryError::Sql { source })?;
        let mut total = 0_u64;
        let mut rows = Vec::new();
        for row in mapped {
            let row = row.map_err(|source| PropertyQueryError::Sql { source })?;
            if let Some(filter) = &filter {
                if !row.value_json.to_lowercase().contains(filter) {
                    continue;
                }
                let position = total;
                total += 1;
                if position < request.offset as u64
                    || request.limit.is_some_and(|limit| rows.len() >= limit)
                {
                    continue;
                }
            }
            rows.push(row);
        }
        if filter.is_none() {
            total = connection.query_row("SELECT COUNT(*) FROM properties p JOIN files f ON f.file_id=p.file_id WHERE p.key=?1", [key], |row| row.get(0)).map_err(|source| PropertyQueryError::Sql { source })?;
        }

        Ok(PropertyQueryResult { total, rows })
    }
}

/// Property query failures.
#[derive(Debug, Error)]
pub enum PropertyQueryError {
    /// SQL query execution failed.
    #[error("property query failed: {source}")]
    Sql {
        #[source]
        source: rusqlite::Error,
    },
    /// Requested offset does not fit SQLite's integer range.
    #[error("property query offset is out of range: {offset}")]
    InvalidOffset { offset: usize },
    /// Query key was empty.
    #[error("property query key must not be empty")]
    InvalidKey,
    /// Query limit was invalid.
    #[error("property query limit must be greater than zero")]
    InvalidLimit {
        /// Invalid limit value.
        limit: usize,
    },
}
