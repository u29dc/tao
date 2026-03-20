use rusqlite::{Connection, OptionalExtension, params, params_from_iter, types::Value};
use thiserror::Error;

/// Persisted row model for `links` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRecord {
    /// Stable link identifier.
    pub link_id: String,
    /// Source file id.
    pub source_file_id: String,
    /// Raw target payload from markdown.
    pub raw_target: String,
    /// Resolved file id when link target can be resolved.
    pub resolved_file_id: Option<String>,
    /// Optional heading slug target.
    pub heading_slug: Option<String>,
    /// Optional block id target.
    pub block_id: Option<String>,
    /// Unresolved marker.
    pub is_unresolved: bool,
    /// Stable unresolved reason code when unresolved.
    pub unresolved_reason: Option<String>,
    /// Link provenance field (`body`, `body:markdown`, `body:embed`, or `frontmatter:<field>`).
    pub source_field: String,
    /// Creation timestamp.
    pub created_at: String,
}

/// Input payload for inserting link rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRecordInput {
    /// Stable link identifier.
    pub link_id: String,
    /// Source file id.
    pub source_file_id: String,
    /// Raw target payload from markdown.
    pub raw_target: String,
    /// Resolved file id when available.
    pub resolved_file_id: Option<String>,
    /// Optional heading slug target.
    pub heading_slug: Option<String>,
    /// Optional block id target.
    pub block_id: Option<String>,
    /// Unresolved marker.
    pub is_unresolved: bool,
    /// Stable unresolved reason code when unresolved.
    pub unresolved_reason: Option<String>,
    /// Link provenance field (`body`, `body:markdown`, `body:embed`, or `frontmatter:<field>`).
    pub source_field: String,
}

/// Link row enriched with source/target normalized paths from join queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkWithPaths {
    /// Stable link identifier.
    pub link_id: String,
    /// Source file id.
    pub source_file_id: String,
    /// Source normalized path.
    pub source_path: String,
    /// Raw target payload from markdown.
    pub raw_target: String,
    /// Resolved file id when available.
    pub resolved_file_id: Option<String>,
    /// Resolved normalized path when available.
    pub resolved_path: Option<String>,
    /// Optional heading slug target.
    pub heading_slug: Option<String>,
    /// Optional block id target.
    pub block_id: Option<String>,
    /// Unresolved marker.
    pub is_unresolved: bool,
    /// Stable unresolved reason code when unresolved.
    pub unresolved_reason: Option<String>,
    /// Link provenance field (`body`, `body:markdown`, `body:embed`, or `frontmatter:<field>`).
    pub source_field: String,
}

/// Lightweight resolved link pair for graph component construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedLinkPair {
    /// Source file id.
    pub source_file_id: String,
    /// Resolved target file id.
    pub target_file_id: String,
}

/// One markdown node with resolved incoming/outgoing degree counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphNodeDegree {
    /// Stable file identifier.
    pub file_id: String,
    /// Normalized file path.
    pub path: String,
    /// Count of resolved incoming edges.
    pub incoming_resolved: u64,
    /// Count of resolved outgoing edges.
    pub outgoing_resolved: u64,
}

/// One floating-file row with resolved incoming/outgoing degree counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloatingFileDegree {
    /// Stable file identifier.
    pub file_id: String,
    /// Normalized file path.
    pub path: String,
    /// Whether file is markdown.
    pub is_markdown: bool,
    /// Count of resolved incoming edges.
    pub incoming_resolved: u64,
    /// Count of resolved outgoing edges.
    pub outgoing_resolved: u64,
}

/// Aggregate summary for strict floating-file scans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloatingFileSummary {
    /// Total floating files (strict disconnected).
    pub total_files: u64,
    /// Total floating markdown files.
    pub markdown_files: u64,
    /// Total floating non-markdown files.
    pub non_markdown_files: u64,
}

/// Scoped inbound-link row for attachment and file-audit workflows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedInboundRow {
    /// Stable file identifier.
    pub file_id: String,
    /// Normalized file path.
    pub path: String,
    /// Whether file is markdown.
    pub is_markdown: bool,
    /// Number of resolved inbound links.
    pub inbound_resolved: u64,
}

/// Scoped inbound-link aggregate summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedInboundSummary {
    /// Total files matched by scope/filter constraints.
    pub total_files: u64,
    /// Files with inbound link count > 0.
    pub linked_files: u64,
    /// Files with inbound link count == 0.
    pub unlinked_files: u64,
}

/// Repository operations over `links` table.
#[derive(Debug, Default, Clone, Copy)]
pub struct LinksRepository;

impl LinksRepository {
    /// List all links across the vault with joined source/target paths.
    pub fn list_all_with_paths(
        connection: &Connection,
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved,
  l.unresolved_reason,
  l.source_field
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
ORDER BY l.link_id ASC
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_list_all_with_paths",
                source,
            })?;

        let rows = statement
            .query_map([], row_to_link_with_paths)
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_all_with_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_all_with_paths_row",
                source,
            })
        })
        .collect()
    }

    /// Insert one link record.
    pub fn insert(
        connection: &Connection,
        record: &LinkRecordInput,
    ) -> Result<(), LinksRepositoryError> {
        connection
            .execute(
                r#"
INSERT INTO links (
  link_id,
  source_file_id,
  raw_target,
  resolved_file_id,
  heading_slug,
  block_id,
  is_unresolved,
  unresolved_reason,
  source_field
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
"#,
                params![
                    record.link_id,
                    record.source_file_id,
                    record.raw_target,
                    record.resolved_file_id,
                    record.heading_slug,
                    record.block_id,
                    i64::from(record.is_unresolved),
                    record.unresolved_reason,
                    record.source_field,
                ],
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "insert",
                source,
            })?;

        Ok(())
    }

    /// Fetch one link by id.
    pub fn get_by_id(
        connection: &Connection,
        link_id: &str,
    ) -> Result<Option<LinkRecord>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  link_id,
  source_file_id,
  raw_target,
  resolved_file_id,
  heading_slug,
  block_id,
  is_unresolved,
  unresolved_reason,
  source_field,
  created_at
FROM links
WHERE link_id = ?1
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_get_by_id",
                source,
            })?;

        statement
            .query_row(params![link_id], row_to_link_record)
            .optional()
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "get_by_id",
                source,
            })
    }

    /// List outgoing links for source file with joined file paths.
    pub fn list_outgoing_with_paths(
        connection: &Connection,
        source_file_id: &str,
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved,
  l.unresolved_reason,
  l.source_field
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
WHERE l.source_file_id = ?1
ORDER BY l.link_id ASC
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_list_outgoing_with_paths",
                source,
            })?;

        let rows = statement
            .query_map(params![source_file_id], row_to_link_with_paths)
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_outgoing_with_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_outgoing_with_paths_row",
                source,
            })
        })
        .collect()
    }

    /// List backlinks for resolved target file with joined file paths.
    pub fn list_backlinks_with_paths(
        connection: &Connection,
        target_file_id: &str,
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved,
  l.unresolved_reason,
  l.source_field
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
WHERE l.resolved_file_id = ?1
ORDER BY sf.normalized_path ASC, l.link_id ASC
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_list_backlinks_with_paths",
                source,
            })?;

        let rows = statement
            .query_map(params![target_file_id], row_to_link_with_paths)
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_backlinks_with_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_backlinks_with_paths_row",
                source,
            })
        })
        .collect()
    }

    /// List unresolved links across vault with joined source/target paths.
    pub fn list_unresolved_with_paths(
        connection: &Connection,
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved,
  l.unresolved_reason,
  l.source_field
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
WHERE l.is_unresolved = 1
ORDER BY sf.normalized_path ASC, l.link_id ASC
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_list_unresolved_with_paths",
                source,
            })?;

        let rows = statement
            .query_map([], row_to_link_with_paths)
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_unresolved_with_paths",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_unresolved_with_paths_row",
                source,
            })
        })
        .collect()
    }

    /// Count unresolved links across vault.
    pub fn count_unresolved(connection: &Connection) -> Result<u64, LinksRepositoryError> {
        connection
            .query_row(
                "SELECT COUNT(*) FROM links WHERE is_unresolved = 1",
                [],
                |row| row.get::<_, u64>(0),
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "count_unresolved",
                source,
            })
    }

    /// List one unresolved links window with joined source/target paths.
    pub fn list_unresolved_with_paths_window(
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved,
  l.unresolved_reason,
  l.source_field
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
WHERE l.is_unresolved = 1
ORDER BY l.link_id ASC
LIMIT ?1 OFFSET ?2
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_list_unresolved_with_paths_window",
                source,
            })?;

        let rows = statement
            .query_map(
                params![i64::from(limit), i64::from(offset)],
                row_to_link_with_paths,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_unresolved_with_paths_window",
                source,
            })?;

        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_unresolved_with_paths_window_row",
                source,
            })
        })
        .collect()
    }

    /// Count markdown notes that have at least one incoming resolved edge and zero outgoing resolved edges.
    pub fn count_deadends(connection: &Connection) -> Result<u64, LinksRepositoryError> {
        let sql = deadends_count_sql();
        connection
            .query_row(&sql, [], |row| row.get::<_, u64>("total"))
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "count_deadends",
                source,
            })
    }

    /// List one deadends page in deterministic path order.
    pub fn list_deadends_window(
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<GraphNodeDegree>, LinksRepositoryError> {
        let sql = deadends_window_sql();
        let mut statement =
            connection
                .prepare(&sql)
                .map_err(|source| LinksRepositoryError::Sql {
                    operation: "prepare_list_deadends_window",
                    source,
                })?;
        let rows = statement
            .query_map(
                params![i64::from(limit), i64::from(offset)],
                row_to_graph_node_degree,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_deadends_window",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_deadends_window_row",
                source,
            })
        })
        .collect()
    }

    /// Count markdown notes with zero incoming and zero outgoing resolved edges.
    pub fn count_orphans(connection: &Connection) -> Result<u64, LinksRepositoryError> {
        let sql = orphans_count_sql();
        connection
            .query_row(&sql, [], |row| row.get::<_, u64>("total"))
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "count_orphans",
                source,
            })
    }

    /// List one orphans page in deterministic path order.
    pub fn list_orphans_window(
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<GraphNodeDegree>, LinksRepositoryError> {
        let sql = orphans_window_sql();
        let mut statement =
            connection
                .prepare(&sql)
                .map_err(|source| LinksRepositoryError::Sql {
                    operation: "prepare_list_orphans_window",
                    source,
                })?;
        let rows = statement
            .query_map(
                params![i64::from(limit), i64::from(offset)],
                row_to_graph_node_degree,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_orphans_window",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_orphans_window_row",
                source,
            })
        })
        .collect()
    }

    /// Return strict floating-file summary using default graph-visible filters.
    pub fn summarize_floating_default(
        connection: &Connection,
    ) -> Result<FloatingFileSummary, LinksRepositoryError> {
        let sql = floating_default_summary_sql();
        connection
            .query_row(&sql, [], |row| {
                Ok(FloatingFileSummary {
                    total_files: row.get("total_files")?,
                    markdown_files: row.get("markdown_files")?,
                    non_markdown_files: row.get("non_markdown_files")?,
                })
            })
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "summarize_floating_default",
                source,
            })
    }

    /// Return one strict floating-file page in deterministic path order.
    pub fn list_floating_default_window(
        connection: &Connection,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<FloatingFileDegree>, LinksRepositoryError> {
        let sql = floating_default_window_sql();
        let mut statement =
            connection
                .prepare(&sql)
                .map_err(|source| LinksRepositoryError::Sql {
                    operation: "prepare_list_floating_default_window",
                    source,
                })?;
        let rows = statement
            .query_map(
                params![i64::from(limit), i64::from(offset)],
                row_to_floating_file_degree,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_floating_default_window",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_floating_default_window_row",
                source,
            })
        })
        .collect()
    }

    /// List resolved source-target file id pairs used by graph component services.
    pub fn list_resolved_pairs(
        connection: &Connection,
    ) -> Result<Vec<ResolvedLinkPair>, LinksRepositoryError> {
        let mut statement = connection
            .prepare(
                r#"
SELECT DISTINCT
  source_file_id,
  resolved_file_id AS target_file_id
FROM links
WHERE is_unresolved = 0
  AND resolved_file_id IS NOT NULL
ORDER BY source_file_id ASC, target_file_id ASC
"#,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "prepare_list_resolved_pairs",
                source,
            })?;
        let rows = statement
            .query_map([], |row| {
                Ok(ResolvedLinkPair {
                    source_file_id: row.get("source_file_id")?,
                    target_file_id: row.get("target_file_id")?,
                })
            })
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_resolved_pairs",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_resolved_pairs_row",
                source,
            })
        })
        .collect()
    }

    /// Fetch outgoing edges for a frontier of source ids.
    pub fn list_outgoing_for_sources_with_paths(
        connection: &Connection,
        source_file_ids: &[String],
        include_unresolved: bool,
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        if source_file_ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = in_clause_placeholders(source_file_ids.len(), 1);
        let unresolved_clause = if include_unresolved {
            ""
        } else {
            "AND l.is_unresolved = 0"
        };
        let query = format!(
            r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved,
  l.unresolved_reason,
  l.source_field
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
WHERE l.source_file_id IN ({placeholders})
  {unresolved_clause}
ORDER BY l.link_id ASC
"#
        );
        let mut statement =
            connection
                .prepare(&query)
                .map_err(|source| LinksRepositoryError::Sql {
                    operation: "prepare_list_outgoing_for_sources_with_paths",
                    source,
                })?;
        let rows = statement
            .query_map(
                params_from_iter(source_file_ids.iter()),
                row_to_link_with_paths,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_outgoing_for_sources_with_paths",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_outgoing_for_sources_with_paths_row",
                source,
            })
        })
        .collect()
    }

    /// Fetch incoming resolved edges for a frontier of target ids.
    pub fn list_incoming_for_targets_with_paths(
        connection: &Connection,
        target_file_ids: &[String],
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        if target_file_ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = in_clause_placeholders(target_file_ids.len(), 1);
        let query = format!(
            r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved,
  l.unresolved_reason,
  l.source_field
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
WHERE l.resolved_file_id IN ({placeholders})
  AND l.is_unresolved = 0
ORDER BY l.link_id ASC
"#
        );
        let mut statement =
            connection
                .prepare(&query)
                .map_err(|source| LinksRepositoryError::Sql {
                    operation: "prepare_list_incoming_for_targets_with_paths",
                    source,
                })?;
        let rows = statement
            .query_map(
                params_from_iter(target_file_ids.iter()),
                row_to_link_with_paths,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_incoming_for_targets_with_paths",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_incoming_for_targets_with_paths_row",
                source,
            })
        })
        .collect()
    }

    /// Return scoped inbound-link summary counts.
    pub fn summarize_scoped_inbound(
        connection: &Connection,
        scope_prefix: &str,
        include_markdown: bool,
        include_non_markdown: bool,
        exclude_prefixes: &[String],
    ) -> Result<ScopedInboundSummary, LinksRepositoryError> {
        let (scope_clause, mut parameters) = scoped_filter_clause(
            scope_prefix,
            include_markdown,
            include_non_markdown,
            exclude_prefixes,
        );
        let query = format!(
            r#"
WITH inbound AS (
  SELECT
    resolved_file_id AS file_id,
    COUNT(*) AS inbound_resolved
  FROM links
  WHERE is_unresolved = 0
    AND resolved_file_id IS NOT NULL
  GROUP BY resolved_file_id
),
scoped AS (
  SELECT
    f.file_id AS file_id,
    f.normalized_path AS path,
    f.is_markdown AS is_markdown,
    COALESCE(i.inbound_resolved, 0) AS inbound_resolved
  FROM files f
  LEFT JOIN inbound i ON i.file_id = f.file_id
  WHERE {scope_clause}
)
SELECT
  COUNT(*) AS total_files,
  COALESCE(SUM(CASE WHEN inbound_resolved > 0 THEN 1 ELSE 0 END), 0) AS linked_files,
  COALESCE(SUM(CASE WHEN inbound_resolved = 0 THEN 1 ELSE 0 END), 0) AS unlinked_files
FROM scoped
"#
        );
        let mut statement =
            connection
                .prepare(&query)
                .map_err(|source| LinksRepositoryError::Sql {
                    operation: "prepare_summarize_scoped_inbound",
                    source,
                })?;
        statement
            .query_row(params_from_iter(parameters.drain(..)), |row| {
                Ok(ScopedInboundSummary {
                    total_files: row.get("total_files")?,
                    linked_files: row.get("linked_files")?,
                    unlinked_files: row.get("unlinked_files")?,
                })
            })
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "summarize_scoped_inbound",
                source,
            })
    }

    /// Return one scoped inbound-link page in deterministic path order.
    pub fn list_scoped_inbound_window(
        connection: &Connection,
        scope_prefix: &str,
        include_markdown: bool,
        include_non_markdown: bool,
        exclude_prefixes: &[String],
        limit: u32,
        offset: u32,
    ) -> Result<Vec<ScopedInboundRow>, LinksRepositoryError> {
        let (scope_clause, mut parameters) = scoped_filter_clause(
            scope_prefix,
            include_markdown,
            include_non_markdown,
            exclude_prefixes,
        );
        parameters.push(Value::Integer(i64::from(limit)));
        parameters.push(Value::Integer(i64::from(offset)));

        let limit_index = parameters.len() - 1;
        let offset_index = parameters.len();
        let query = format!(
            r#"
WITH inbound AS (
  SELECT
    resolved_file_id AS file_id,
    COUNT(*) AS inbound_resolved
  FROM links
  WHERE is_unresolved = 0
    AND resolved_file_id IS NOT NULL
  GROUP BY resolved_file_id
),
scoped AS (
  SELECT
    f.file_id AS file_id,
    f.normalized_path AS path,
    f.is_markdown AS is_markdown,
    COALESCE(i.inbound_resolved, 0) AS inbound_resolved
  FROM files f
  LEFT JOIN inbound i ON i.file_id = f.file_id
  WHERE {scope_clause}
)
SELECT
  file_id,
  path,
  is_markdown,
  inbound_resolved
FROM scoped
ORDER BY path ASC
LIMIT ?{limit_index} OFFSET ?{offset_index}
"#
        );
        let mut statement =
            connection
                .prepare(&query)
                .map_err(|source| LinksRepositoryError::Sql {
                    operation: "prepare_list_scoped_inbound_window",
                    source,
                })?;
        let rows = statement
            .query_map(
                params_from_iter(parameters.drain(..)),
                row_to_scoped_inbound_row,
            )
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_scoped_inbound_window",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_scoped_inbound_window_row",
                source,
            })
        })
        .collect()
    }

    /// Load links potentially affected by a set of vault changes.
    ///
    /// Returns links from sources NOT in `excluded_source_paths` that satisfy
    /// at least one of:
    /// - Currently unresolved (may now resolve due to added/modified files)
    /// - Stale from FK SET NULL (resolved_file_id nulled by cascade but
    ///   is_unresolved column not yet updated)
    /// - Targeting a changed file path with a heading or block anchor
    ///
    /// This query is intentionally scoped to update/delete flows. Callers that
    /// introduce new candidate files must fall back to a full-link scan because
    /// basename/path tie-breakers can cause already-resolved links to prefer a
    /// different target even when their current resolved_file_id is unchanged.
    pub fn list_affected_by_changes_with_paths(
        connection: &Connection,
        excluded_source_paths: &[String],
        changed_target_paths: &[String],
        include_unresolved: bool,
    ) -> Result<Vec<LinkWithPaths>, LinksRepositoryError> {
        let mut or_conditions = Vec::new();
        let mut all_params = Vec::<String>::new();
        let mut param_idx = 1_usize;

        // Source exclusion: links from changed files are already being reindexed.
        let exclusion_clause = if excluded_source_paths.is_empty() {
            String::new()
        } else {
            let placeholders = in_clause_placeholders(excluded_source_paths.len(), param_idx);
            param_idx += excluded_source_paths.len();
            all_params.extend(excluded_source_paths.iter().cloned());
            format!("sf.normalized_path NOT IN ({placeholders})")
        };

        if include_unresolved {
            // Includes both explicitly-unresolved links and links left stale by
            // FK ON DELETE SET NULL (resolved_file_id is NULL but is_unresolved
            // column still reads 0).
            or_conditions.push("(l.is_unresolved = 1 OR l.resolved_file_id IS NULL)".to_string());
        }

        if !changed_target_paths.is_empty() {
            let placeholders = in_clause_placeholders(changed_target_paths.len(), param_idx);
            all_params.extend(changed_target_paths.iter().cloned());
            or_conditions.push(format!(
                "(tf.normalized_path IN ({placeholders}) AND (l.heading_slug IS NOT NULL OR l.block_id IS NOT NULL))"
            ));
        }

        if or_conditions.is_empty() {
            return Ok(Vec::new());
        }

        let or_clause = or_conditions.join("\n    OR ");
        let where_clause = if exclusion_clause.is_empty() {
            format!("WHERE ({or_clause})")
        } else {
            format!("WHERE {exclusion_clause}\n  AND ({or_clause})")
        };

        let query = format!(
            r#"
SELECT
  l.link_id,
  l.source_file_id,
  sf.normalized_path AS source_path,
  l.raw_target,
  l.resolved_file_id,
  tf.normalized_path AS resolved_path,
  l.heading_slug,
  l.block_id,
  l.is_unresolved,
  l.unresolved_reason,
  l.source_field
FROM links l
JOIN files sf ON sf.file_id = l.source_file_id
LEFT JOIN files tf ON tf.file_id = l.resolved_file_id
{where_clause}
ORDER BY l.link_id ASC
"#
        );

        let mut statement =
            connection
                .prepare(&query)
                .map_err(|source| LinksRepositoryError::Sql {
                    operation: "prepare_list_affected_by_changes",
                    source,
                })?;
        let rows = statement
            .query_map(params_from_iter(all_params.iter()), row_to_link_with_paths)
            .map_err(|source| LinksRepositoryError::Sql {
                operation: "list_affected_by_changes",
                source,
            })?;
        rows.map(|row| {
            row.map_err(|source| LinksRepositoryError::Sql {
                operation: "list_affected_by_changes_row",
                source,
            })
        })
        .collect()
    }
}

fn row_to_link_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<LinkRecord> {
    let is_unresolved: i64 = row.get("is_unresolved")?;

    Ok(LinkRecord {
        link_id: row.get("link_id")?,
        source_file_id: row.get("source_file_id")?,
        raw_target: row.get("raw_target")?,
        resolved_file_id: row.get("resolved_file_id")?,
        heading_slug: row.get("heading_slug")?,
        block_id: row.get("block_id")?,
        is_unresolved: is_unresolved != 0,
        unresolved_reason: row.get("unresolved_reason")?,
        source_field: row.get("source_field")?,
        created_at: row.get("created_at")?,
    })
}

fn row_to_link_with_paths(row: &rusqlite::Row<'_>) -> rusqlite::Result<LinkWithPaths> {
    let is_unresolved: i64 = row.get("is_unresolved")?;

    Ok(LinkWithPaths {
        link_id: row.get("link_id")?,
        source_file_id: row.get("source_file_id")?,
        source_path: row.get("source_path")?,
        raw_target: row.get("raw_target")?,
        resolved_file_id: row.get("resolved_file_id")?,
        resolved_path: row.get("resolved_path")?,
        heading_slug: row.get("heading_slug")?,
        block_id: row.get("block_id")?,
        is_unresolved: is_unresolved != 0,
        unresolved_reason: row.get("unresolved_reason")?,
        source_field: row.get("source_field")?,
    })
}

fn row_to_graph_node_degree(row: &rusqlite::Row<'_>) -> rusqlite::Result<GraphNodeDegree> {
    Ok(GraphNodeDegree {
        file_id: row.get("file_id")?,
        path: row.get("path")?,
        incoming_resolved: row.get("incoming_resolved")?,
        outgoing_resolved: row.get("outgoing_resolved")?,
    })
}

fn row_to_floating_file_degree(row: &rusqlite::Row<'_>) -> rusqlite::Result<FloatingFileDegree> {
    let is_markdown: i64 = row.get("is_markdown")?;
    Ok(FloatingFileDegree {
        file_id: row.get("file_id")?,
        path: row.get("path")?,
        is_markdown: is_markdown != 0,
        incoming_resolved: row.get("incoming_resolved")?,
        outgoing_resolved: row.get("outgoing_resolved")?,
    })
}

fn row_to_scoped_inbound_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ScopedInboundRow> {
    let is_markdown: i64 = row.get("is_markdown")?;
    Ok(ScopedInboundRow {
        file_id: row.get("file_id")?,
        path: row.get("path")?,
        is_markdown: is_markdown != 0,
        inbound_resolved: row.get("inbound_resolved")?,
    })
}

fn scoped_filter_clause(
    scope_prefix: &str,
    include_markdown: bool,
    include_non_markdown: bool,
    exclude_prefixes: &[String],
) -> (String, Vec<Value>) {
    let scope = normalize_scope_prefix(scope_prefix);
    let include_markdown_value = if include_markdown { 1_i64 } else { 0_i64 };
    let include_non_markdown_value = if include_non_markdown { 1_i64 } else { 0_i64 };

    let mut parameters = Vec::<Value>::new();
    let mut clause_parts = Vec::<String>::new();

    if scope.is_empty() {
        clause_parts.push("1 = 1".to_string());
    } else {
        let exact_index = parameters.len() + 1;
        parameters.push(Value::Text(scope.clone()));
        let nested_index = parameters.len() + 1;
        parameters.push(Value::Text(format!("{scope}/%")));
        clause_parts.push(format!(
            "(f.normalized_path = ?{exact_index} OR f.normalized_path LIKE ?{nested_index})"
        ));
    }

    let markdown_index = parameters.len() + 1;
    parameters.push(Value::Integer(include_markdown_value));
    let non_markdown_index = parameters.len() + 1;
    parameters.push(Value::Integer(include_non_markdown_value));
    clause_parts.push(format!(
        "((f.is_markdown = 1 AND ?{markdown_index} = 1) OR (f.is_markdown = 0 AND ?{non_markdown_index} = 1))"
    ));

    for prefix in exclude_prefixes {
        let normalized = normalize_scope_prefix(prefix);
        if normalized.is_empty() {
            continue;
        }
        let exact_index = parameters.len() + 1;
        parameters.push(Value::Text(normalized.clone()));
        let nested_index = parameters.len() + 1;
        parameters.push(Value::Text(format!("{normalized}/%")));
        clause_parts.push(format!(
            "(f.normalized_path != ?{exact_index} AND f.normalized_path NOT LIKE ?{nested_index})"
        ));
    }

    (clause_parts.join(" AND "), parameters)
}

fn normalize_scope_prefix(raw: &str) -> String {
    raw.trim().trim_matches('/').replace('\\', "/")
}

fn in_clause_placeholders(count: usize, start_index: usize) -> String {
    (0..count)
        .map(|index| format!("?{}", start_index + index))
        .collect::<Vec<_>>()
        .join(", ")
}

fn degree_cte_sql() -> &'static str {
    r#"
WITH incoming AS (
  SELECT
    resolved_file_id AS file_id,
    COUNT(*) AS incoming_resolved
  FROM links
  WHERE is_unresolved = 0
    AND resolved_file_id IS NOT NULL
  GROUP BY resolved_file_id
),
outgoing AS (
  SELECT
    source_file_id AS file_id,
    COUNT(*) AS outgoing_resolved
  FROM links
  WHERE is_unresolved = 0
    AND resolved_file_id IS NOT NULL
  GROUP BY source_file_id
),
degrees AS (
  SELECT
    f.file_id AS file_id,
    f.normalized_path AS path,
    COALESCE(i.incoming_resolved, 0) AS incoming_resolved,
    COALESCE(o.outgoing_resolved, 0) AS outgoing_resolved
  FROM files f
  LEFT JOIN incoming i ON i.file_id = f.file_id
  LEFT JOIN outgoing o ON o.file_id = f.file_id
  WHERE f.is_markdown = 1
)
"#
}

fn deadends_count_sql() -> String {
    format!(
        r#"
{}
SELECT COUNT(*) AS total
FROM degrees
WHERE incoming_resolved > 0
  AND outgoing_resolved = 0
"#,
        degree_cte_sql()
    )
}

fn deadends_window_sql() -> String {
    format!(
        r#"
{}
SELECT
  file_id,
  path,
  incoming_resolved,
  outgoing_resolved
FROM degrees
WHERE incoming_resolved > 0
  AND outgoing_resolved = 0
ORDER BY path ASC
LIMIT ?1 OFFSET ?2
"#,
        degree_cte_sql()
    )
}

fn orphans_count_sql() -> String {
    format!(
        r#"
{}
SELECT COUNT(*) AS total
FROM degrees
WHERE incoming_resolved = 0
  AND outgoing_resolved = 0
"#,
        degree_cte_sql()
    )
}

fn orphans_window_sql() -> String {
    format!(
        r#"
{}
SELECT
  file_id,
  path,
  incoming_resolved,
  outgoing_resolved
FROM degrees
WHERE incoming_resolved = 0
  AND outgoing_resolved = 0
ORDER BY path ASC
LIMIT ?1 OFFSET ?2
"#,
        degree_cte_sql()
    )
}

fn floating_default_degree_cte_sql() -> &'static str {
    r#"
WITH incoming AS (
  SELECT
    resolved_file_id AS file_id,
    COUNT(*) AS incoming_resolved
  FROM links
  WHERE is_unresolved = 0
    AND resolved_file_id IS NOT NULL
  GROUP BY resolved_file_id
),
outgoing AS (
  SELECT
    source_file_id AS file_id,
    COUNT(*) AS outgoing_resolved
  FROM links
  WHERE is_unresolved = 0
    AND resolved_file_id IS NOT NULL
  GROUP BY source_file_id
),
degrees AS (
  SELECT
    f.file_id AS file_id,
    f.normalized_path AS path,
    f.is_markdown AS is_markdown,
    COALESCE(i.incoming_resolved, 0) AS incoming_resolved,
    COALESCE(o.outgoing_resolved, 0) AS outgoing_resolved
  FROM files f
  LEFT JOIN incoming i ON i.file_id = f.file_id
  LEFT JOIN outgoing o ON o.file_id = f.file_id
  WHERE f.normalized_path != '.DS_Store'
    AND f.normalized_path != 'Thumbs.db'
    AND f.normalized_path != 'desktop.ini'
    AND f.normalized_path != '.gitkeep'
    AND f.normalized_path != '.gitignore'
    AND f.normalized_path NOT LIKE '%/.DS_Store'
    AND f.normalized_path NOT LIKE '%/Thumbs.db'
    AND f.normalized_path NOT LIKE '%/desktop.ini'
    AND f.normalized_path NOT LIKE '%/.gitkeep'
    AND f.normalized_path NOT LIKE '%/.gitignore'
)
"#
}

fn floating_default_summary_sql() -> String {
    format!(
        r#"
{}
SELECT
  COUNT(*) AS total_files,
  COALESCE(SUM(CASE WHEN is_markdown = 1 THEN 1 ELSE 0 END), 0) AS markdown_files,
  COALESCE(SUM(CASE WHEN is_markdown = 0 THEN 1 ELSE 0 END), 0) AS non_markdown_files
FROM degrees
WHERE incoming_resolved = 0
  AND outgoing_resolved = 0
"#,
        floating_default_degree_cte_sql()
    )
}

fn floating_default_window_sql() -> String {
    format!(
        r#"
{}
SELECT
  file_id,
  path,
  is_markdown,
  incoming_resolved,
  outgoing_resolved
FROM degrees
WHERE incoming_resolved = 0
  AND outgoing_resolved = 0
ORDER BY path ASC
LIMIT ?1 OFFSET ?2
"#,
        floating_default_degree_cte_sql()
    )
}

/// Links repository operation failures.
#[derive(Debug, Error)]
pub enum LinksRepositoryError {
    /// SQL error with operation context.
    #[error("links repository operation '{operation}' failed: {source}")]
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
    use rusqlite::Connection;

    use crate::{
        FileRecordInput, FilesRepository, LinkRecordInput, LinksRepository, run_migrations,
    };

    fn file(file_id: &str, path: &str) -> FileRecordInput {
        FileRecordInput {
            file_id: file_id.to_string(),
            normalized_path: path.to_string(),
            match_key: path.to_lowercase(),
            absolute_path: format!("/vault/{path}"),
            size_bytes: 10,
            modified_unix_ms: 1_700_000_000_000,
            hash_blake3: format!("hash-{file_id}"),
            is_markdown: path.ends_with(".md"),
        }
    }

    #[test]
    fn link_join_queries_return_source_and_target_paths() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &file("source", "notes/source.md"))
            .expect("insert source file");
        FilesRepository::insert(&connection, &file("target", "notes/target.md"))
            .expect("insert target file");

        let link = LinkRecordInput {
            link_id: "l1".to_string(),
            source_file_id: "source".to_string(),
            raw_target: "target".to_string(),
            resolved_file_id: Some("target".to_string()),
            heading_slug: Some("heading".to_string()),
            block_id: None,
            is_unresolved: false,
            unresolved_reason: None,
            source_field: "body".to_string(),
        };

        LinksRepository::insert(&connection, &link).expect("insert link");

        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, "source")
            .expect("list outgoing");
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].source_path, "notes/source.md");
        assert_eq!(
            outgoing[0].resolved_path.as_deref(),
            Some("notes/target.md")
        );

        let backlinks = LinksRepository::list_backlinks_with_paths(&connection, "target")
            .expect("list backlinks");
        assert_eq!(backlinks.len(), 1);
        assert_eq!(backlinks[0].source_path, "notes/source.md");
        assert_eq!(
            backlinks[0].resolved_path.as_deref(),
            Some("notes/target.md")
        );
    }

    #[test]
    fn unresolved_link_query_returns_only_unresolved_rows_in_stable_order() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &file("source-a", "notes/a.md"))
            .expect("insert source a");
        FilesRepository::insert(&connection, &file("source-b", "notes/b.md"))
            .expect("insert source b");

        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-resolved".to_string(),
                source_file_id: "source-a".to_string(),
                raw_target: "b".to_string(),
                resolved_file_id: Some("source-b".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert resolved");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-unresolved-b".to_string(),
                source_file_id: "source-b".to_string(),
                raw_target: "missing-b".to_string(),
                resolved_file_id: None,
                heading_slug: None,
                block_id: None,
                is_unresolved: true,
                unresolved_reason: Some("missing-note".to_string()),
                source_field: "frontmatter:related".to_string(),
            },
        )
        .expect("insert unresolved b");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-unresolved-a".to_string(),
                source_file_id: "source-a".to_string(),
                raw_target: "missing-a".to_string(),
                resolved_file_id: None,
                heading_slug: None,
                block_id: None,
                is_unresolved: true,
                unresolved_reason: Some("missing-note".to_string()),
                source_field: "body".to_string(),
            },
        )
        .expect("insert unresolved a");

        let unresolved =
            LinksRepository::list_unresolved_with_paths(&connection).expect("list unresolved");
        let unresolved_total =
            LinksRepository::count_unresolved(&connection).expect("count unresolved");
        let unresolved_window =
            LinksRepository::list_unresolved_with_paths_window(&connection, 1, 1)
                .expect("list unresolved window");

        assert_eq!(unresolved.len(), 2);
        assert_eq!(unresolved_total, 2);
        assert_eq!(unresolved_window.len(), 1);
        assert_eq!(unresolved_window[0].link_id, "l-unresolved-b");
        assert_eq!(unresolved[0].source_path, "notes/a.md");
        assert_eq!(unresolved[0].link_id, "l-unresolved-a");
        assert_eq!(unresolved[1].source_path, "notes/b.md");
        assert_eq!(unresolved[1].link_id, "l-unresolved-b");
        assert_eq!(
            unresolved[1].unresolved_reason.as_deref(),
            Some("missing-note")
        );
        assert_eq!(unresolved[1].source_field, "frontmatter:related");
        assert!(unresolved.iter().all(|row| row.is_unresolved));
    }

    #[test]
    fn scoped_inbound_queries_support_attachment_audits() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &file("note-index", "notes/index.md"))
            .expect("insert index note");
        FilesRepository::insert(&connection, &file("note-source", "notes/source.md"))
            .expect("insert source note");
        FilesRepository::insert(
            &connection,
            &file("asset-linked", "notes/assets/linked.pdf"),
        )
        .expect("insert linked asset");
        FilesRepository::insert(
            &connection,
            &file("asset-orphan", "notes/assets/orphan.pdf"),
        )
        .expect("insert orphan asset");
        FilesRepository::insert(&connection, &file("other", "archive/old.pdf"))
            .expect("insert out-of-scope asset");

        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-linked".to_string(),
                source_file_id: "note-source".to_string(),
                raw_target: "assets/linked.pdf".to_string(),
                resolved_file_id: Some("asset-linked".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body:markdown".to_string(),
            },
        )
        .expect("insert linked edge");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-index".to_string(),
                source_file_id: "note-index".to_string(),
                raw_target: "source".to_string(),
                resolved_file_id: Some("note-source".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert note edge");

        let summary =
            LinksRepository::summarize_scoped_inbound(&connection, "notes", false, true, &[])
                .expect("summarize scoped inbound");
        assert_eq!(summary.total_files, 2);
        assert_eq!(summary.linked_files, 1);
        assert_eq!(summary.unlinked_files, 1);

        let rows = LinksRepository::list_scoped_inbound_window(
            &connection,
            "notes",
            false,
            true,
            &[],
            10,
            0,
        )
        .expect("page scoped inbound");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].path, "notes/assets/linked.pdf");
        assert_eq!(rows[0].inbound_resolved, 1);
        assert_eq!(rows[1].path, "notes/assets/orphan.pdf");
        assert_eq!(rows[1].inbound_resolved, 0);
    }

    #[test]
    fn floating_queries_return_strict_disconnected_and_ignore_system_noise() {
        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        FilesRepository::insert(&connection, &file("note-source", "notes/source.md"))
            .expect("insert source note");
        FilesRepository::insert(&connection, &file("note-linked", "notes/linked.md"))
            .expect("insert linked note");
        FilesRepository::insert(&connection, &file("note-floating", "notes/floating.md"))
            .expect("insert floating note");
        FilesRepository::insert(
            &connection,
            &file("asset-linked", "notes/assets/linked.pdf"),
        )
        .expect("insert linked asset");
        FilesRepository::insert(
            &connection,
            &file("asset-floating", "notes/assets/floating.pdf"),
        )
        .expect("insert floating asset");

        FilesRepository::insert(&connection, &file("noise-root", ".DS_Store"))
            .expect("insert root noise");
        FilesRepository::insert(
            &connection,
            &file("noise-nested", "notes/assets/.gitignore"),
        )
        .expect("insert nested noise");

        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-note".to_string(),
                source_file_id: "note-source".to_string(),
                raw_target: "linked".to_string(),
                resolved_file_id: Some("note-linked".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body".to_string(),
            },
        )
        .expect("insert note link");
        LinksRepository::insert(
            &connection,
            &LinkRecordInput {
                link_id: "l-asset".to_string(),
                source_file_id: "note-source".to_string(),
                raw_target: "assets/linked.pdf".to_string(),
                resolved_file_id: Some("asset-linked".to_string()),
                heading_slug: None,
                block_id: None,
                is_unresolved: false,
                unresolved_reason: None,
                source_field: "body:markdown".to_string(),
            },
        )
        .expect("insert asset link");

        let summary =
            LinksRepository::summarize_floating_default(&connection).expect("summarize floating");
        assert_eq!(summary.total_files, 2);
        assert_eq!(summary.markdown_files, 1);
        assert_eq!(summary.non_markdown_files, 1);

        let rows =
            LinksRepository::list_floating_default_window(&connection, 50, 0).expect("list page");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].path, "notes/assets/floating.pdf");
        assert!(!rows[0].is_markdown);
        assert_eq!(rows[0].incoming_resolved, 0);
        assert_eq!(rows[0].outgoing_resolved, 0);
        assert_eq!(rows[1].path, "notes/floating.md");
        assert!(rows[1].is_markdown);
        assert_eq!(rows[1].incoming_resolved, 0);
        assert_eq!(rows[1].outgoing_resolved, 0);
    }
}
