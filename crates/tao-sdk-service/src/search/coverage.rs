//! Coverage includes pending and failed content even when no search result matches.

use super::{SearchContentCoverage, VaultSearchError, sorted_extensions};
use rusqlite::{Connection, params_from_iter, types::Value};
use std::collections::HashSet;

pub(super) fn search_content_coverage(
    connection: &Connection,
    scope: Option<&str>,
    insensitive: bool,
    extensions: &HashSet<String>,
) -> Result<SearchContentCoverage, VaultSearchError> {
    let mut clauses = vec!["1=1".to_string()];
    let mut parameters = Vec::<Value>::new();
    if let Some(scope) = scope.filter(|scope| !scope.is_empty()) {
        let column = if insensitive {
            "f.match_key"
        } else {
            "f.normalized_path"
        };
        clauses.push(format!("({column}=? OR instr({column},?)=1)"));
        parameters.push(Value::Text(scope.to_string()));
        parameters.push(Value::Text(format!("{scope}/")));
    }
    if !extensions.is_empty() {
        let extensions = sorted_extensions(extensions);
        clauses.push(format!("EXISTS(SELECT 1 FROM search_segments inventory WHERE inventory.file_id=f.file_id AND inventory.surface='files' AND inventory.extension IN ({}))",vec!["?"; extensions.len()].join(",")));
        parameters.extend(extensions.into_iter().map(Value::Text));
    }
    let sql = format!(
        r#"
WITH scoped AS (
 SELECT f.is_markdown,d.file_id IS NOT NULL AS markdown_ready,
 c.file_id AS content_id,c.served_revision,c.desired_revision,c.served_extractor_identity,c.extractor_identity,c.coverage,c.availability,
 f.is_markdown=1 OR substr(lower(f.normalized_path),-4) IN('.txt','.pdf') AS supported,
 EXISTS(SELECT 1 FROM file_diagnostics x WHERE x.path=f.normalized_path) AS diagnostic
 FROM files f LEFT JOIN canonical_documents d ON d.file_id=f.file_id LEFT JOIN content_documents c ON c.file_id=f.file_id
 WHERE {}
)
SELECT COUNT(*),
 COALESCE(SUM(markdown_ready OR served_revision IS NOT NULL),0),
 COALESCE(SUM(is_markdown),0),
 COALESCE(SUM(NOT is_markdown AND served_revision IS NOT NULL),0),
 COALESCE(SUM(supported AND NOT is_markdown AND (content_id IS NULL OR coverage='pending')),0),
 COALESCE(SUM(supported AND ((is_markdown AND NOT markdown_ready) OR diagnostic OR coverage='failed' OR availability='unavailable')),0),
 COALESCE(SUM(supported AND NOT is_markdown AND coverage NOT IN('complete','pending','failed','unsupported')),0),
 COALESCE(SUM(NOT is_markdown AND served_revision IS NOT NULL AND (served_revision<>desired_revision OR served_extractor_identity IS NOT extractor_identity)),0),
 COALESCE(SUM(NOT supported),0),COALESCE(SUM(diagnostic),0)
FROM scoped
"#,
        clauses.join(" AND ")
    );
    let mut coverage = connection
        .query_row(&sql, params_from_iter(parameters.iter()), |row| {
            Ok(SearchContentCoverage {
                total_files: row.get(0)?,
                searchable_files: row.get(1)?,
                markdown_files: row.get(2)?,
                extracted_files: row.get(3)?,
                pending_files: row.get(4)?,
                failed_files: row.get(5)?,
                partial_files: row.get(6)?,
                stale_files: row.get(7)?,
                unsupported_files: row.get(8)?,
                diagnostic_files: row.get(9)?,
                complete: false,
            })
        })
        .map_err(|source| VaultSearchError::Sql {
            operation: "scoped_content_coverage",
            source,
        })?;
    coverage.complete = coverage.pending_files == 0
        && coverage.failed_files == 0
        && coverage.partial_files == 0
        && coverage.stale_files == 0
        && coverage.diagnostic_files == 0;
    Ok(coverage)
}
