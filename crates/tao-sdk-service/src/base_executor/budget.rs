//! Explicit hydration budgets. SQL measures stored sizes before Rust allocates rows or JSON.
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, params_from_iter};

use super::BaseTableExecutorError;

pub(super) const MAX_BASE_ROWS: u64 = 50_000;
pub(super) const MAX_BASE_PROPERTY_ROWS: u64 = 250_000;
pub(super) const MAX_BASE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_BASE_CELLS: u64 = 500_000;

pub(super) fn check_limit(
    resource: &'static str,
    observed: u64,
    limit: u64,
) -> Result<(), BaseTableExecutorError> {
    if observed > limit {
        return Err(BaseTableExecutorError::WorkBudgetExceeded {
            resource,
            observed,
            limit,
        });
    }
    Ok(())
}

pub(super) fn check_candidates(
    connection: &Connection,
    query: &str,
    values: &[SqlValue],
) -> Result<u64, BaseTableExecutorError> {
    let aggregate = format!(
        "SELECT COUNT(*),COALESCE(SUM(length(CAST(file_id AS BLOB))+length(CAST(normalized_path AS BLOB))),0) FROM ({query})"
    );
    let (rows, bytes): (u64, u64) = connection
        .query_row(&aggregate, params_from_iter(values), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "measure_base_candidates",
            source,
        })?;
    check_limit("candidate rows", rows, MAX_BASE_ROWS)?;
    check_limit("candidate path bytes", bytes, MAX_BASE_BYTES)?;
    Ok(rows)
}

pub(super) fn check_cells(rows: usize, columns: usize) -> Result<(), BaseTableExecutorError> {
    check_limit(
        "materialized cells",
        (rows as u64).saturating_mul(columns as u64),
        MAX_BASE_CELLS,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_preflight_accepts_five_thousand_and_rejects_excess_without_hydration() {
        let connection = Connection::open_in_memory().unwrap();
        for (rows, accepted) in [(5000, true), (50001, false)] {
            let query = format!(
                "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<{rows}) SELECT CAST(i AS TEXT) AS file_id, 'note-'||i||'.md' AS normalized_path FROM n"
            );
            let result = check_candidates(&connection, &query, &[]);
            if accepted {
                assert_eq!(result.unwrap(), rows);
            } else {
                assert!(matches!(
                    result,
                    Err(BaseTableExecutorError::WorkBudgetExceeded {
                        resource: "candidate rows",
                        observed: 50001,
                        limit: 50000
                    })
                ));
            }
        }
    }
}
