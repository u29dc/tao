//! Bounded metadata aggregation queries over canonical property values.

use rusqlite::{Connection, params};
use thiserror::Error;

/// Maximum supported metadata output window.
pub const MAX_METADATA_AGGREGATION_LIMIT: u32 = 1000;

/// One aggregate, with exact token/key spelling and document count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataAggregateRecord {
    /// Canonical property key or token, without additional splitting/normalization.
    pub value: String,
    /// Number of documents containing this exact key or token.
    pub total: u64,
}

/// Exact aggregate cardinality plus a bounded output window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataAggregateWindow {
    /// Total number of distinct aggregates before pagination.
    pub total: u64,
    /// At most the requested number of rows.
    pub items: Vec<MetadataAggregateRecord>,
}

/// Metadata aggregation stays inside SQLite; source rows are never loaded into a Rust vector.
#[derive(Debug, Default, Clone, Copy)]
pub struct MetadataAggregationRepository;

impl MetadataAggregationRepository {
    /// Count property keys in lexical order using one coherent SQL statement.
    pub fn property_keys(
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<MetadataAggregateWindow, MetadataAggregationRepositoryError> {
        validate_limit(limit)?;
        let sql = r#"
WITH aggregated AS MATERIALIZED (
    SELECT key COLLATE BINARY AS value, COUNT(*) AS occurrences
    FROM properties
    GROUP BY key COLLATE BINARY
), page AS (
    SELECT value, occurrences FROM aggregated
    ORDER BY value COLLATE BINARY ASC LIMIT ?1 OFFSET ?2
)
SELECT (SELECT COUNT(*) FROM aggregated), page.value, page.occurrences
FROM (SELECT 1) LEFT JOIN page ON 1
ORDER BY page.value COLLATE BINARY ASC
"#;
        let mut statement = connection.prepare(sql).map_err(sql_error)?;
        let mut rows = statement.query(params![limit, offset]).map_err(sql_error)?;
        collect_window(&mut rows, limit)
    }

    /// Count complete canonical tag/alias tokens, ordered by count then exact spelling.
    ///
    /// JSON expansion and grouping are performed by SQLite. Only the selected
    /// output rows cross into Rust memory. Existing explicit list boundaries are
    /// authoritative: commas, spaces, leading `#` and Unicode are never rewritten.
    /// Repeated exact tokens in a document count once. Across documents, case and
    /// Unicode spelling remain distinct, matching canonical metadata values.
    pub fn tokens(
        connection: &Connection,
        property_key: &str,
        limit: u32,
        offset: u32,
    ) -> Result<MetadataAggregateWindow, MetadataAggregationRepositoryError> {
        validate_limit(limit)?;
        if !matches!(property_key, "tags" | "aliases") {
            return Err(MetadataAggregationRepositoryError::UnsupportedPropertyKey {
                key: property_key.to_owned(),
            });
        }
        let sql = r#"
WITH aggregated AS MATERIALIZED (
    SELECT CASE WHEN token.type = 'text' THEN token.value
                ELSE json('invalid canonical metadata token') END COLLATE BINARY AS value,
           COUNT(DISTINCT property.file_id) AS occurrences
    FROM properties AS property
    JOIN json_each(CASE WHEN json_type(property.value_json) IN ('array', 'text')
                        THEN property.value_json
                        ELSE json('invalid canonical metadata property') END) AS token
    WHERE property.key = ?1
      AND (token.type <> 'text' OR token.value <> '')
    GROUP BY token.value COLLATE BINARY, token.type
), page AS (
    SELECT value, occurrences FROM aggregated
    ORDER BY occurrences DESC, value COLLATE BINARY ASC LIMIT ?2 OFFSET ?3
)
SELECT (SELECT COUNT(*) FROM aggregated), page.value, page.occurrences
FROM (SELECT 1) LEFT JOIN page ON 1
ORDER BY page.occurrences DESC, page.value COLLATE BINARY ASC
"#;
        let mut statement = connection.prepare(sql).map_err(sql_error)?;
        let mut rows = statement
            .query(params![property_key, limit, offset])
            .map_err(sql_error)?;
        collect_window(&mut rows, limit)
    }
}

fn collect_window(
    rows: &mut rusqlite::Rows<'_>,
    limit: u32,
) -> Result<MetadataAggregateWindow, MetadataAggregationRepositoryError> {
    let mut total = 0;
    let mut items = Vec::with_capacity(limit as usize);
    while let Some(row) = rows.next().map_err(sql_error)? {
        total = row.get(0).map_err(sql_error)?;
        if let Some(value) = row.get::<_, Option<String>>(1).map_err(sql_error)? {
            items.push(MetadataAggregateRecord {
                value,
                total: row.get(2).map_err(sql_error)?,
            });
        }
    }
    Ok(MetadataAggregateWindow { total, items })
}

fn validate_limit(limit: u32) -> Result<(), MetadataAggregationRepositoryError> {
    if !(1..=MAX_METADATA_AGGREGATION_LIMIT).contains(&limit) {
        return Err(MetadataAggregationRepositoryError::InvalidLimit { limit });
    }
    Ok(())
}

fn sql_error(source: rusqlite::Error) -> MetadataAggregationRepositoryError {
    MetadataAggregationRepositoryError::Sql { source }
}

/// Metadata aggregate execution failures.
#[derive(Debug, Error)]
pub enum MetadataAggregationRepositoryError {
    /// A bound was rejected instead of silently clamped.
    #[error("metadata limit must be in 1..=1000 (received {limit})")]
    InvalidLimit {
        /// Rejected output bound.
        limit: u32,
    },
    /// Only canonical tag/alias lists support token aggregation.
    #[error("unsupported metadata token key '{key}'; expected tags or aliases")]
    UnsupportedPropertyKey {
        /// Rejected property key.
        key: String,
    },
    /// SQLite failed, including malformed stored JSON.
    #[error("metadata aggregation query failed: {source}")]
    Sql {
        /// Underlying database error.
        #[source]
        source: rusqlite::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_bounds_and_keys_are_rejected_before_database_access() {
        let connection = Connection::open_in_memory().unwrap();
        for limit in [0, 1001, u32::MAX] {
            assert!(matches!(
                MetadataAggregationRepository::property_keys(&connection, limit, 0),
                Err(MetadataAggregationRepositoryError::InvalidLimit { .. })
            ));
            assert!(matches!(
                MetadataAggregationRepository::tokens(&connection, "aliases", limit, 0),
                Err(MetadataAggregationRepositoryError::InvalidLimit { .. })
            ));
        }
        assert!(matches!(
            MetadataAggregationRepository::tokens(&connection, "untrusted-key", 1, 0),
            Err(MetadataAggregationRepositoryError::UnsupportedPropertyKey { .. })
        ));
    }
}
