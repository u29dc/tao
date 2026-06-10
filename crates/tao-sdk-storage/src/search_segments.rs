use rusqlite::{Connection, params, params_from_iter, types::Value};
use thiserror::Error;

/// Input payload for one unified search segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSegmentInput {
    /// Stable segment identifier.
    pub segment_id: String,
    /// Search surface label.
    pub surface: String,
    /// Owning file identifier.
    pub file_id: String,
    /// Owning normalized path.
    pub normalized_path: String,
    /// Lower-cased normalized path.
    pub normalized_path_lc: String,
    /// Lowercase file extension.
    pub extension: String,
    /// Field label within the surface.
    pub field: String,
    /// Optional source record identifier.
    pub record_id: Option<String>,
    /// Human label for diagnostics/ranking.
    pub label: String,
    /// Static ranking weight.
    pub weight: i64,
    /// JSON payload needed by result hydration.
    pub payload_json: String,
    /// Path/search text column.
    pub path_text: String,
    /// Title/search text column.
    pub title_text: String,
    /// Alias/search text column.
    pub alias_text: String,
    /// Body/search text column.
    pub body_text: String,
    /// Property/search text column.
    pub property_text: String,
    /// Task/search text column.
    pub task_text: String,
    /// Link/search text column.
    pub link_text: String,
    /// Base/search text column.
    pub base_text: String,
}

/// Indexed segment match returned from the unified corpus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSegmentMatch {
    /// Stable segment identifier.
    pub segment_id: String,
    /// Search surface label.
    pub surface: String,
    /// Owning file identifier.
    pub file_id: String,
    /// Owning normalized path.
    pub normalized_path: String,
    /// File extension.
    pub extension: String,
    /// Field label.
    pub field: String,
    /// Optional source record identifier.
    pub record_id: Option<String>,
    /// Human label.
    pub label: String,
    /// Static ranking weight.
    pub weight: i64,
    /// JSON payload.
    pub payload_json: String,
    /// Updated timestamp.
    pub updated_at: String,
    /// FTS rank score converted to larger-is-better integer.
    pub rank_score: i64,
}

/// Lightweight indexed segment candidate returned before payload hydration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSegmentCandidate {
    /// Stable segment identifier.
    pub segment_id: String,
    /// Search surface label.
    pub surface: String,
    /// Owning file identifier.
    pub file_id: String,
    /// Owning normalized path.
    pub normalized_path: String,
    /// File extension.
    pub extension: String,
    /// Field label.
    pub field: String,
    /// Optional source record identifier.
    pub record_id: Option<String>,
    /// Human label.
    pub label: String,
    /// Static ranking weight.
    pub weight: i64,
    /// Updated timestamp.
    pub updated_at: String,
    /// FTS rank score converted to larger-is-better integer.
    pub rank_score: i64,
}

/// Query payload for unified search segment matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSegmentQuery {
    /// FTS5 MATCH expression.
    pub fts_query: String,
    /// Surface labels to include. Empty means all surfaces.
    pub surfaces: Vec<String>,
    /// Optional path scope.
    pub scope: Option<String>,
    /// Extension filters. Empty means all extensions.
    pub extensions: Vec<String>,
    /// Max rows to return.
    pub limit: u32,
}

/// Repository operations over unified search segment tables.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchSegmentRepository;

impl SearchSegmentRepository {
    /// Insert a batch of unified search segments.
    pub fn insert_many(
        connection: &Connection,
        records: &[SearchSegmentInput],
    ) -> Result<(), SearchSegmentRepositoryError> {
        let mut statement = connection
            .prepare_cached(
                r#"
INSERT INTO search_segments (
  segment_id,
  surface,
  file_id,
  normalized_path,
  normalized_path_lc,
  extension,
  field,
  record_id,
  label,
  weight,
  payload_json,
  path_text,
  title_text,
  alias_text,
  body_text,
  property_text,
  task_text,
  link_text,
  base_text
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
"#,
            )
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "prepare_insert_many",
                source,
            })?;

        for record in records {
            statement
                .execute(params![
                    record.segment_id,
                    record.surface,
                    record.file_id,
                    record.normalized_path,
                    record.normalized_path_lc,
                    record.extension,
                    record.field,
                    record.record_id,
                    record.label,
                    record.weight,
                    record.payload_json,
                    record.path_text,
                    record.title_text,
                    record.alias_text,
                    record.body_text,
                    record.property_text,
                    record.task_text,
                    record.link_text,
                    record.base_text
                ])
                .map_err(|source| SearchSegmentRepositoryError::Sql {
                    operation: "insert_many",
                    source,
                })?;
        }

        Ok(())
    }

    /// Remove all unified search segments.
    pub fn clear(connection: &Connection) -> Result<(), SearchSegmentRepositoryError> {
        connection
            .execute("DELETE FROM search_segments", [])
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "clear",
                source,
            })?;
        Ok(())
    }

    /// Remove all segments for one file id.
    pub fn delete_by_file_id(
        connection: &Connection,
        file_id: &str,
    ) -> Result<u64, SearchSegmentRepositoryError> {
        let removed = connection
            .execute(
                "DELETE FROM search_segments WHERE file_id = ?1",
                params![file_id],
            )
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "delete_by_file_id",
                source,
            })?;
        Ok(removed as u64)
    }

    /// Count unified search segments.
    pub fn count(connection: &Connection) -> Result<u64, SearchSegmentRepositoryError> {
        connection
            .query_row("SELECT COUNT(*) FROM search_segments", [], |row| row.get(0))
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "count",
                source,
            })
    }

    /// Query indexed search segments through FTS5 and indexed filters.
    pub fn query(
        connection: &Connection,
        query: &SearchSegmentQuery,
    ) -> Result<Vec<SearchSegmentMatch>, SearchSegmentRepositoryError> {
        let candidates = Self::query_candidates(connection, query)?;
        let segment_ids = candidates
            .iter()
            .map(|candidate| candidate.segment_id.clone())
            .collect::<Vec<_>>();
        let order_by_id = segment_ids
            .iter()
            .enumerate()
            .map(|(index, segment_id)| (segment_id.clone(), index))
            .collect::<std::collections::HashMap<_, _>>();
        let mut rank_by_id = candidates
            .into_iter()
            .map(|candidate| (candidate.segment_id, candidate.rank_score))
            .collect::<std::collections::HashMap<_, _>>();
        let mut hydrated = Self::hydrate_by_segment_ids(connection, &segment_ids)?;
        for segment in &mut hydrated {
            if let Some(rank_score) = rank_by_id.remove(&segment.segment_id) {
                segment.rank_score = rank_score;
            }
        }
        hydrated.sort_by(|left, right| {
            order_by_id
                .get(&left.segment_id)
                .copied()
                .unwrap_or(usize::MAX)
                .cmp(
                    &order_by_id
                        .get(&right.segment_id)
                        .copied()
                        .unwrap_or(usize::MAX),
                )
        });
        Ok(hydrated)
    }

    /// Query indexed search segments through FTS5 without hydrating payload JSON.
    pub fn query_candidates(
        connection: &Connection,
        query: &SearchSegmentQuery,
    ) -> Result<Vec<SearchSegmentCandidate>, SearchSegmentRepositoryError> {
        let mut clauses = vec!["search_segments_fts MATCH ?".to_string()];
        let mut params = vec![Value::Text(query.fts_query.clone())];

        if !query.surfaces.is_empty() {
            clauses.push(format!(
                "s.surface IN ({})",
                vec!["?"; query.surfaces.len()].join(", ")
            ));
            params.extend(query.surfaces.iter().cloned().map(Value::Text));
        }

        if let Some(scope) = query.scope.as_ref().filter(|scope| !scope.is_empty()) {
            clauses.push(
                "(s.normalized_path = ? OR s.normalized_path LIKE ? ESCAPE '\\')".to_string(),
            );
            params.push(Value::Text(scope.clone()));
            params.push(Value::Text(format!("{}/%", escape_like(scope))));
        }

        if !query.extensions.is_empty() {
            clauses.push(format!(
                "s.extension IN ({})",
                vec!["?"; query.extensions.len()].join(", ")
            ));
            params.extend(query.extensions.iter().cloned().map(Value::Text));
        }

        let sql = format!(
            r#"
SELECT
  s.segment_id,
  s.surface,
  s.file_id,
  s.normalized_path,
  s.extension,
  s.field,
  s.record_id,
  s.label,
  s.weight,
  s.updated_at,
  CAST((0 - bm25(search_segments_fts, 5.0, 6.0, 7.0, 0.8, 3.0, 2.0, 1.5, 2.5)) * 1000000 AS INTEGER) AS rank_score
FROM search_segments_fts
JOIN search_segments s ON s.rowid = search_segments_fts.rowid
WHERE {}
ORDER BY (s.weight + rank_score) DESC, s.normalized_path ASC, s.segment_id ASC
LIMIT ?
"#,
            clauses.join(" AND ")
        );
        params.push(Value::Integer(i64::from(query.limit)));

        let mut statement =
            connection
                .prepare(&sql)
                .map_err(|source| SearchSegmentRepositoryError::Sql {
                    operation: "prepare_query_candidates",
                    source,
                })?;
        let rows = statement
            .query_map(params_from_iter(params.iter()), row_to_segment_candidate)
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "query_candidates",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "query_candidates_row",
                source,
            })
        })
        .collect()
    }

    /// Hydrate full segment rows by segment ids.
    pub fn hydrate_by_segment_ids(
        connection: &Connection,
        segment_ids: &[String],
    ) -> Result<Vec<SearchSegmentMatch>, SearchSegmentRepositoryError> {
        if segment_ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = vec!["?"; segment_ids.len()].join(", ");
        let sql = format!(
            r#"
SELECT
  segment_id,
  surface,
  file_id,
  normalized_path,
  extension,
  field,
  record_id,
  label,
  weight,
  payload_json,
  updated_at,
  0 AS rank_score
FROM search_segments
WHERE segment_id IN ({placeholders})
"#
        );
        let mut statement =
            connection
                .prepare(&sql)
                .map_err(|source| SearchSegmentRepositoryError::Sql {
                    operation: "prepare_hydrate_by_segment_ids",
                    source,
                })?;
        let rows = statement
            .query_map(params_from_iter(segment_ids.iter()), row_to_segment_match)
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "hydrate_by_segment_ids",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "hydrate_by_segment_ids_row",
                source,
            })
        })
        .collect()
    }

    /// List all materialized document search segments in deterministic path order.
    pub fn list_docs(
        connection: &Connection,
    ) -> Result<Vec<SearchSegmentMatch>, SearchSegmentRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  segment_id,
  surface,
  file_id,
  normalized_path,
  extension,
  field,
  record_id,
  label,
  weight,
  payload_json,
  updated_at,
  0 AS rank_score
FROM search_segments
WHERE surface = 'docs'
ORDER BY normalized_path ASC
"#,
            )
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "prepare_list_docs",
                source,
            })?;
        let rows = statement
            .query_map([], row_to_segment_match)
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "list_docs",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "list_docs_row",
                source,
            })
        })
        .collect()
    }

    /// Count distinct candidate paths for one indexed segment query.
    pub fn count_distinct_paths(
        connection: &Connection,
        query: &SearchSegmentQuery,
    ) -> Result<u64, SearchSegmentRepositoryError> {
        let mut clauses = vec!["search_segments_fts MATCH ?".to_string()];
        let mut params = vec![Value::Text(query.fts_query.clone())];

        if !query.surfaces.is_empty() {
            clauses.push(format!(
                "s.surface IN ({})",
                vec!["?"; query.surfaces.len()].join(", ")
            ));
            params.extend(query.surfaces.iter().cloned().map(Value::Text));
        }
        if let Some(scope) = query.scope.as_ref().filter(|scope| !scope.is_empty()) {
            clauses.push(
                "(s.normalized_path = ? OR s.normalized_path LIKE ? ESCAPE '\\')".to_string(),
            );
            params.push(Value::Text(scope.clone()));
            params.push(Value::Text(format!("{}/%", escape_like(scope))));
        }
        if !query.extensions.is_empty() {
            clauses.push(format!(
                "s.extension IN ({})",
                vec!["?"; query.extensions.len()].join(", ")
            ));
            params.extend(query.extensions.iter().cloned().map(Value::Text));
        }

        let sql = format!(
            r#"
SELECT COUNT(DISTINCT s.normalized_path)
FROM search_segments_fts
JOIN search_segments s ON s.rowid = search_segments_fts.rowid
WHERE {}
"#,
            clauses.join(" AND ")
        );
        connection
            .query_row(&sql, params_from_iter(params.iter()), |row| row.get(0))
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "count_distinct_paths",
                source,
            })
    }

    /// Return distinct candidate paths for one indexed segment query.
    pub fn distinct_paths(
        connection: &Connection,
        query: &SearchSegmentQuery,
    ) -> Result<Vec<String>, SearchSegmentRepositoryError> {
        let mut clauses = vec!["search_segments_fts MATCH ?".to_string()];
        let mut params = vec![Value::Text(query.fts_query.clone())];

        if !query.surfaces.is_empty() {
            clauses.push(format!(
                "s.surface IN ({})",
                vec!["?"; query.surfaces.len()].join(", ")
            ));
            params.extend(query.surfaces.iter().cloned().map(Value::Text));
        }
        if let Some(scope) = query.scope.as_ref().filter(|scope| !scope.is_empty()) {
            clauses.push(
                "(s.normalized_path = ? OR s.normalized_path LIKE ? ESCAPE '\\')".to_string(),
            );
            params.push(Value::Text(scope.clone()));
            params.push(Value::Text(format!("{}/%", escape_like(scope))));
        }
        if !query.extensions.is_empty() {
            clauses.push(format!(
                "s.extension IN ({})",
                vec!["?"; query.extensions.len()].join(", ")
            ));
            params.extend(query.extensions.iter().cloned().map(Value::Text));
        }

        let sql = format!(
            r#"
SELECT DISTINCT s.normalized_path
FROM search_segments_fts
JOIN search_segments s ON s.rowid = search_segments_fts.rowid
WHERE {}
ORDER BY s.normalized_path ASC
"#,
            clauses.join(" AND ")
        );
        let mut statement =
            connection
                .prepare(&sql)
                .map_err(|source| SearchSegmentRepositoryError::Sql {
                    operation: "prepare_distinct_paths",
                    source,
                })?;
        let rows = statement
            .query_map(params_from_iter(params.iter()), |row| {
                row.get::<_, String>(0)
            })
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "distinct_paths",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "distinct_paths_row",
                source,
            })
        })
        .collect()
    }

    /// Count how many paths in a subset are matched by an indexed segment query.
    pub fn count_matching_paths_subset(
        connection: &Connection,
        query: &SearchSegmentQuery,
        paths: &[String],
    ) -> Result<u64, SearchSegmentRepositoryError> {
        if paths.is_empty() {
            return Ok(0);
        }

        let mut clauses = vec![
            "search_segments_fts MATCH ?".to_string(),
            format!(
                "s.normalized_path IN ({})",
                vec!["?"; paths.len()].join(", ")
            ),
        ];
        let mut params = vec![Value::Text(query.fts_query.clone())];
        params.extend(paths.iter().cloned().map(Value::Text));

        if !query.surfaces.is_empty() {
            clauses.push(format!(
                "s.surface IN ({})",
                vec!["?"; query.surfaces.len()].join(", ")
            ));
            params.extend(query.surfaces.iter().cloned().map(Value::Text));
        }
        if let Some(scope) = query.scope.as_ref().filter(|scope| !scope.is_empty()) {
            clauses.push(
                "(s.normalized_path = ? OR s.normalized_path LIKE ? ESCAPE '\\')".to_string(),
            );
            params.push(Value::Text(scope.clone()));
            params.push(Value::Text(format!("{}/%", escape_like(scope))));
        }
        if !query.extensions.is_empty() {
            clauses.push(format!(
                "s.extension IN ({})",
                vec!["?"; query.extensions.len()].join(", ")
            ));
            params.extend(query.extensions.iter().cloned().map(Value::Text));
        }

        let sql = format!(
            r#"
SELECT COUNT(DISTINCT s.normalized_path)
FROM search_segments_fts
JOIN search_segments s ON s.rowid = search_segments_fts.rowid
WHERE {}
"#,
            clauses.join(" AND ")
        );
        connection
            .query_row(&sql, params_from_iter(params.iter()), |row| row.get(0))
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "count_matching_paths_subset",
                source,
            })
    }

    /// Return materialized base row segment payloads for a normalized path.
    pub fn base_rows_for_path(
        connection: &Connection,
        path: &str,
        limit: u32,
    ) -> Result<Vec<SearchSegmentMatch>, SearchSegmentRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  segment_id,
  surface,
  file_id,
  normalized_path,
  extension,
  field,
  record_id,
  label,
  weight,
  payload_json,
  updated_at,
  0 AS rank_score
FROM search_segments
WHERE surface = 'bases'
  AND normalized_path = ?1
ORDER BY label ASC, segment_id ASC
LIMIT ?2
"#,
            )
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "prepare_base_rows_for_path",
                source,
            })?;
        let rows = statement
            .query_map(params![path, i64::from(limit)], row_to_segment_match)
            .map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "base_rows_for_path",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| SearchSegmentRepositoryError::Sql {
                operation: "base_rows_for_path_row",
                source,
            })
        })
        .collect()
    }
}

fn row_to_segment_match(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchSegmentMatch> {
    Ok(SearchSegmentMatch {
        segment_id: row.get("segment_id")?,
        surface: row.get("surface")?,
        file_id: row.get("file_id")?,
        normalized_path: row.get("normalized_path")?,
        extension: row.get("extension")?,
        field: row.get("field")?,
        record_id: row.get("record_id")?,
        label: row.get("label")?,
        weight: row.get("weight")?,
        payload_json: row.get("payload_json")?,
        updated_at: row.get("updated_at")?,
        rank_score: row.get("rank_score")?,
    })
}

fn row_to_segment_candidate(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchSegmentCandidate> {
    Ok(SearchSegmentCandidate {
        segment_id: row.get("segment_id")?,
        surface: row.get("surface")?,
        file_id: row.get("file_id")?,
        normalized_path: row.get("normalized_path")?,
        extension: row.get("extension")?,
        field: row.get("field")?,
        record_id: row.get("record_id")?,
        label: row.get("label")?,
        weight: row.get("weight")?,
        updated_at: row.get("updated_at")?,
        rank_score: row.get("rank_score")?,
    })
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// Search segment repository operation failures.
#[derive(Debug, Error)]
pub enum SearchSegmentRepositoryError {
    /// SQL error with operation context.
    #[error("search segment repository operation '{operation}' failed: {source}")]
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

    use super::{SearchSegmentInput, SearchSegmentQuery, SearchSegmentRepository};
    use crate::{FileRecordInput, FilesRepository, apply_initial_schema};

    #[test]
    fn fts_triggers_track_segment_insert_update_and_cascade_delete() {
        let connection = Connection::open_in_memory().expect("open database");
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .expect("enable foreign keys");
        apply_initial_schema(&connection).expect("apply schema");
        FilesRepository::insert(&connection, &fixture_file()).expect("insert file");

        SearchSegmentRepository::insert_many(
            &connection,
            &[SearchSegmentInput {
                segment_id: "segment-1".to_string(),
                surface: "docs".to_string(),
                file_id: "file-1".to_string(),
                normalized_path: "notes/alpha.md".to_string(),
                normalized_path_lc: "notes/alpha.md".to_string(),
                extension: "md".to_string(),
                field: "document".to_string(),
                record_id: None,
                label: "Alpha".to_string(),
                weight: 80,
                payload_json: "{}".to_string(),
                path_text: "notes alpha".to_string(),
                title_text: "Alpha".to_string(),
                alias_text: String::new(),
                body_text: "needle body".to_string(),
                property_text: String::new(),
                task_text: String::new(),
                link_text: String::new(),
                base_text: String::new(),
            }],
        )
        .expect("insert segment");

        let needle = SearchSegmentRepository::query(
            &connection,
            &SearchSegmentQuery {
                fts_query: "\"needle\"*".to_string(),
                surfaces: Vec::new(),
                scope: None,
                extensions: Vec::new(),
                limit: 10,
            },
        )
        .expect("query inserted segment");
        assert_eq!(needle.len(), 1);

        connection
            .execute(
                "UPDATE search_segments SET body_text = ?1 WHERE segment_id = ?2",
                params!["replacement body", "segment-1"],
            )
            .expect("update segment");

        let stale = SearchSegmentRepository::query(
            &connection,
            &SearchSegmentQuery {
                fts_query: "\"needle\"*".to_string(),
                surfaces: Vec::new(),
                scope: None,
                extensions: Vec::new(),
                limit: 10,
            },
        )
        .expect("query old token");
        assert!(stale.is_empty());

        let replacement = SearchSegmentRepository::query(
            &connection,
            &SearchSegmentQuery {
                fts_query: "\"replacement\"*".to_string(),
                surfaces: Vec::new(),
                scope: None,
                extensions: Vec::new(),
                limit: 10,
            },
        )
        .expect("query updated token");
        assert_eq!(replacement.len(), 1);
        let candidates = SearchSegmentRepository::query_candidates(
            &connection,
            &SearchSegmentQuery {
                fts_query: "\"replacement\"*".to_string(),
                surfaces: Vec::new(),
                scope: None,
                extensions: Vec::new(),
                limit: 10,
            },
        )
        .expect("query lightweight candidates");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].segment_id, "segment-1");
        let hydrated = SearchSegmentRepository::hydrate_by_segment_ids(
            &connection,
            &[candidates[0].segment_id.clone()],
        )
        .expect("hydrate selected candidate");
        assert_eq!(hydrated.len(), 1);
        assert_eq!(hydrated[0].payload_json, "{}");

        connection
            .execute("DELETE FROM files WHERE file_id = ?1", params!["file-1"])
            .expect("delete file");
        assert_eq!(
            SearchSegmentRepository::count(&connection).expect("count segments"),
            0
        );
        let after_delete = SearchSegmentRepository::query(
            &connection,
            &SearchSegmentQuery {
                fts_query: "\"replacement\"*".to_string(),
                surfaces: Vec::new(),
                scope: None,
                extensions: Vec::new(),
                limit: 10,
            },
        )
        .expect("query after cascade");
        assert!(after_delete.is_empty());
    }

    fn fixture_file() -> FileRecordInput {
        FileRecordInput {
            file_id: "file-1".to_string(),
            normalized_path: "notes/alpha.md".to_string(),
            match_key: "notes/alpha.md".to_string(),
            absolute_path: "/tmp/vault/notes/alpha.md".to_string(),
            size_bytes: 12,
            modified_unix_ms: 1,
            hash_blake3: "hash".to_string(),
            is_markdown: true,
        }
    }
}
