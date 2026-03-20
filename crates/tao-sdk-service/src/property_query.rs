//! Property query service for structured property lookup and filtering.

use std::cmp::Ordering;

use rusqlite::Connection;
use tao_sdk_storage::PropertiesRepository;
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

        let mut rows = PropertiesRepository::list_by_key_with_paths(connection, key)
            .map_err(|source| PropertyQueryError::Repository { source })?;

        if let Some(filter) = request
            .value_contains
            .as_deref()
            .map(str::trim)
            .filter(|filter| !filter.is_empty())
        {
            let filter = filter.to_lowercase();
            rows.retain(|row| row.value_json.to_lowercase().contains(&filter));
        }

        rows.sort_by(|left, right| compare_property_rows(left, right, request.sort));

        let total = rows.len() as u64;
        let iter = rows.into_iter().skip(request.offset);
        let paged_rows = match request.limit {
            Some(limit) => iter.take(limit).collect::<Vec<_>>(),
            None => iter.collect::<Vec<_>>(),
        };

        let rows = paged_rows
            .into_iter()
            .map(|row| PropertyQueryRow {
                property_id: row.property_id,
                file_id: row.file_id,
                file_path: row.file_path,
                key: row.key,
                value_type: row.value_type,
                value_json: row.value_json,
                updated_at: row.updated_at,
            })
            .collect();

        Ok(PropertyQueryResult { total, rows })
    }
}

fn compare_property_rows(
    left: &tao_sdk_storage::PropertyWithPath,
    right: &tao_sdk_storage::PropertyWithPath,
    sort: PropertyQuerySort,
) -> Ordering {
    match sort {
        PropertyQuerySort::FilePathAsc => left
            .file_path
            .cmp(&right.file_path)
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::FilePathDesc => right
            .file_path
            .cmp(&left.file_path)
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::UpdatedAtAsc => left
            .updated_at
            .cmp(&right.updated_at)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::UpdatedAtDesc => right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::ValueAsc => left
            .value_json
            .cmp(&right.value_json)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.property_id.cmp(&right.property_id)),
        PropertyQuerySort::ValueDesc => right
            .value_json
            .cmp(&left.value_json)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.property_id.cmp(&right.property_id)),
    }
}

/// Property query failures.
#[derive(Debug, Error)]
pub enum PropertyQueryError {
    /// Query key was empty.
    #[error("property query key must not be empty")]
    InvalidKey,
    /// Query limit was invalid.
    #[error("property query limit must be greater than zero")]
    InvalidLimit {
        /// Invalid limit value.
        limit: usize,
    },
    /// Properties repository query failed.
    #[error("property query repository operation failed: {source}")]
    Repository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::PropertiesRepositoryError,
    },
}
