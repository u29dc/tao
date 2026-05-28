use rusqlite::{Connection, params, params_from_iter, types::Value};
use thiserror::Error;

/// Input payload for one exact/normalized search alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchAliasInput {
    /// Stable alias identifier.
    pub alias_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning normalized path.
    pub normalized_path: String,
    /// Lower-cased normalized path.
    pub normalized_path_lc: String,
    /// Lowercase extension.
    pub extension: String,
    /// Search surface label.
    pub surface: String,
    /// Lowercase normalized alias text.
    pub alias_norm: String,
    /// Compact normalized alias text.
    pub alias_compact: String,
    /// Alias source label.
    pub source: String,
    /// Static ranking weight.
    pub weight: i64,
}

/// Alias match returned from indexed exact search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchAliasMatch {
    /// Stable alias identifier.
    pub alias_id: String,
    /// Owning file id.
    pub file_id: String,
    /// Owning normalized path.
    pub normalized_path: String,
    /// Lowercase extension.
    pub extension: String,
    /// Search surface label.
    pub surface: String,
    /// Alias source label.
    pub source: String,
    /// Static ranking weight.
    pub weight: i64,
}

/// Repository operations over exact search aliases.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchAliasRepository;

impl SearchAliasRepository {
    /// Insert a batch of search aliases.
    pub fn insert_many(
        connection: &Connection,
        records: &[SearchAliasInput],
    ) -> Result<(), SearchAliasRepositoryError> {
        let mut statement = connection
            .prepare_cached(
                r#"
INSERT OR REPLACE INTO search_aliases (
  alias_id,
  file_id,
  normalized_path,
  normalized_path_lc,
  extension,
  surface,
  alias_norm,
  alias_compact,
  source,
  weight
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
"#,
            )
            .map_err(|source| SearchAliasRepositoryError::Sql {
                operation: "prepare_insert_many",
                source,
            })?;

        for record in records {
            statement
                .execute(params![
                    record.alias_id,
                    record.file_id,
                    record.normalized_path,
                    record.normalized_path_lc,
                    record.extension,
                    record.surface,
                    record.alias_norm,
                    record.alias_compact,
                    record.source,
                    record.weight
                ])
                .map_err(|source| SearchAliasRepositoryError::Sql {
                    operation: "insert_many",
                    source,
                })?;
        }

        Ok(())
    }

    /// Remove all aliases.
    pub fn clear(connection: &Connection) -> Result<(), SearchAliasRepositoryError> {
        connection
            .execute("DELETE FROM search_aliases", [])
            .map_err(|source| SearchAliasRepositoryError::Sql {
                operation: "clear",
                source,
            })?;
        Ok(())
    }

    /// Remove aliases for one file id.
    pub fn delete_by_file_id(
        connection: &Connection,
        file_id: &str,
    ) -> Result<u64, SearchAliasRepositoryError> {
        let removed = connection
            .execute(
                "DELETE FROM search_aliases WHERE file_id = ?1",
                params![file_id],
            )
            .map_err(|source| SearchAliasRepositoryError::Sql {
                operation: "delete_by_file_id",
                source,
            })?;
        Ok(removed as u64)
    }

    /// Count aliases.
    pub fn count(connection: &Connection) -> Result<u64, SearchAliasRepositoryError> {
        connection
            .query_row("SELECT COUNT(*) FROM search_aliases", [], |row| row.get(0))
            .map_err(|source| SearchAliasRepositoryError::Sql {
                operation: "count",
                source,
            })
    }

    /// Query aliases by exact normalized or compact query text.
    pub fn query(
        connection: &Connection,
        alias_norm: &str,
        alias_compact: &str,
        surfaces: &[String],
        scope: Option<&str>,
        extensions: &[String],
        limit: u32,
    ) -> Result<Vec<SearchAliasMatch>, SearchAliasRepositoryError> {
        let mut clauses = vec!["(alias_norm = ? OR alias_compact = ?)".to_string()];
        let mut params = vec![
            Value::Text(alias_norm.to_string()),
            Value::Text(alias_compact.to_string()),
        ];

        if !surfaces.is_empty() {
            clauses.push(format!(
                "surface IN ({})",
                vec!["?"; surfaces.len()].join(", ")
            ));
            params.extend(surfaces.iter().cloned().map(Value::Text));
        }

        if let Some(scope) = scope.filter(|scope| !scope.is_empty()) {
            clauses.push("(normalized_path = ? OR normalized_path LIKE ? ESCAPE '\\')".to_string());
            params.push(Value::Text(scope.to_string()));
            params.push(Value::Text(format!("{}/%", escape_like(scope))));
        }

        if !extensions.is_empty() {
            clauses.push(format!(
                "extension IN ({})",
                vec!["?"; extensions.len()].join(", ")
            ));
            params.extend(extensions.iter().cloned().map(Value::Text));
        }

        let sql = format!(
            r#"
SELECT
  alias_id,
  file_id,
  normalized_path,
  extension,
  surface,
  source,
  weight
FROM search_aliases
WHERE {}
ORDER BY weight DESC, normalized_path ASC, source ASC
LIMIT ?
"#,
            clauses.join(" AND ")
        );
        params.push(Value::Integer(i64::from(limit)));

        let mut statement =
            connection
                .prepare(&sql)
                .map_err(|source| SearchAliasRepositoryError::Sql {
                    operation: "prepare_query",
                    source,
                })?;
        let rows = statement
            .query_map(params_from_iter(params.iter()), |row| {
                Ok(SearchAliasMatch {
                    alias_id: row.get("alias_id")?,
                    file_id: row.get("file_id")?,
                    normalized_path: row.get("normalized_path")?,
                    extension: row.get("extension")?,
                    surface: row.get("surface")?,
                    source: row.get("source")?,
                    weight: row.get("weight")?,
                })
            })
            .map_err(|source| SearchAliasRepositoryError::Sql {
                operation: "query",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| SearchAliasRepositoryError::Sql {
                operation: "query_row",
                source,
            })
        })
        .collect()
    }

    /// Return distinct candidate paths for one exact alias query.
    pub fn distinct_paths(
        connection: &Connection,
        alias_norm: &str,
        alias_compact: &str,
        surfaces: &[String],
        scope: Option<&str>,
        extensions: &[String],
    ) -> Result<Vec<String>, SearchAliasRepositoryError> {
        let mut clauses = vec!["(alias_norm = ? OR alias_compact = ?)".to_string()];
        let mut params = vec![
            Value::Text(alias_norm.to_string()),
            Value::Text(alias_compact.to_string()),
        ];

        if !surfaces.is_empty() {
            clauses.push(format!(
                "surface IN ({})",
                vec!["?"; surfaces.len()].join(", ")
            ));
            params.extend(surfaces.iter().cloned().map(Value::Text));
        }

        if let Some(scope) = scope.filter(|scope| !scope.is_empty()) {
            clauses.push("(normalized_path = ? OR normalized_path LIKE ? ESCAPE '\\')".to_string());
            params.push(Value::Text(scope.to_string()));
            params.push(Value::Text(format!("{}/%", escape_like(scope))));
        }

        if !extensions.is_empty() {
            clauses.push(format!(
                "extension IN ({})",
                vec!["?"; extensions.len()].join(", ")
            ));
            params.extend(extensions.iter().cloned().map(Value::Text));
        }

        let sql = format!(
            r#"
SELECT DISTINCT normalized_path
FROM search_aliases
WHERE {}
ORDER BY normalized_path ASC
"#,
            clauses.join(" AND ")
        );
        let mut statement =
            connection
                .prepare(&sql)
                .map_err(|source| SearchAliasRepositoryError::Sql {
                    operation: "prepare_distinct_paths",
                    source,
                })?;
        let rows = statement
            .query_map(params_from_iter(params.iter()), |row| {
                row.get::<_, String>(0)
            })
            .map_err(|source| SearchAliasRepositoryError::Sql {
                operation: "distinct_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| SearchAliasRepositoryError::Sql {
                operation: "distinct_paths_row",
                source,
            })
        })
        .collect()
    }
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// Search alias repository operation failures.
#[derive(Debug, Error)]
pub enum SearchAliasRepositoryError {
    /// SQL error with operation context.
    #[error("search alias repository operation '{operation}' failed: {source}")]
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
    use rusqlite::{Connection, params};

    use super::{SearchAliasInput, SearchAliasRepository};
    use crate::{FileRecordInput, FilesRepository, apply_initial_schema};

    #[test]
    fn aliases_query_normalized_forms_and_cascade_with_files() {
        let connection = Connection::open_in_memory().expect("open database");
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .expect("enable foreign keys");
        apply_initial_schema(&connection).expect("apply schema");
        FilesRepository::insert(&connection, &fixture_file()).expect("insert file");

        SearchAliasRepository::insert_many(
            &connection,
            &[SearchAliasInput {
                alias_id: "alias-1".to_string(),
                file_id: "file-1".to_string(),
                normalized_path: "people/jordan_hart.md".to_string(),
                normalized_path_lc: "people/jordan_hart.md".to_string(),
                extension: "md".to_string(),
                surface: "docs".to_string(),
                alias_norm: "jordan hart".to_string(),
                alias_compact: "jordanhart".to_string(),
                source: "title".to_string(),
                weight: 100,
            }],
        )
        .expect("insert alias");

        let normalized = SearchAliasRepository::query(
            &connection,
            "jordan hart",
            "jordanhart",
            &[],
            None,
            &[],
            10,
        )
        .expect("query normalized alias");
        assert_eq!(normalized.len(), 1);
        assert_eq!(normalized[0].normalized_path, "people/jordan_hart.md");

        let compact = SearchAliasRepository::query(
            &connection,
            "jordan_hart",
            "jordanhart",
            &[],
            None,
            &[],
            10,
        )
        .expect("query compact alias");
        assert_eq!(compact.len(), 1);

        connection
            .execute("DELETE FROM files WHERE file_id = ?1", params!["file-1"])
            .expect("delete file");
        assert_eq!(
            SearchAliasRepository::count(&connection).expect("count aliases"),
            0
        );
    }

    fn fixture_file() -> FileRecordInput {
        FileRecordInput {
            file_id: "file-1".to_string(),
            normalized_path: "people/jordan_hart.md".to_string(),
            match_key: "people/jordan_hart.md".to_string(),
            absolute_path: "/tmp/vault/people/jordan_hart.md".to_string(),
            size_bytes: 12,
            modified_unix_ms: 1,
            hash_blake3: "hash".to_string(),
            is_markdown: true,
        }
    }
}
