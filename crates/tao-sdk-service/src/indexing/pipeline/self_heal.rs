use super::*;

/// Result payload for index self-heal workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSelfHealResult {
    /// Number of issues detected before repair.
    pub issues_detected: u64,
    /// Number of rows deleted while repairing issues.
    pub rows_deleted: u64,
    /// Number of rows updated while repairing issues.
    pub rows_updated: u64,
    /// Number of issues remaining after repair.
    pub remaining_issues: u64,
}

/// Self-heal service that repairs common consistency issues.
#[derive(Debug, Default, Clone, Copy)]
pub struct IndexSelfHealService {
    checker: IndexConsistencyChecker,
}

impl IndexSelfHealService {
    /// Detect and repair common index inconsistencies.
    pub fn heal(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
    ) -> Result<IndexSelfHealResult, IndexSelfHealError> {
        let before = self
            .checker
            .check(vault_root, connection)
            .map_err(|source| IndexSelfHealError::CheckBefore {
                source: Box::new(source),
            })?;

        if before.issues.is_empty() {
            return Ok(IndexSelfHealResult {
                issues_detected: 0,
                rows_deleted: 0,
                rows_updated: 0,
                remaining_issues: 0,
            });
        }

        let transaction =
            connection
                .transaction()
                .map_err(|source| IndexSelfHealError::BeginTransaction {
                    source: Box::new(source),
                })?;

        let mut rows_deleted = 0_u64;
        let mut rows_updated = 0_u64;

        for issue in &before.issues {
            match issue.kind {
                ConsistencyIssueKind::OrphanProperty => {
                    let changed = transaction
                        .execute(
                            "DELETE FROM properties WHERE property_id = ?1",
                            params![issue.record_id],
                        )
                        .map_err(|source| IndexSelfHealError::ExecuteSql {
                            operation: "delete_orphan_property",
                            record_id: issue.record_id.clone(),
                            source: Box::new(source),
                        })?;
                    rows_deleted += changed as u64;
                }
                ConsistencyIssueKind::OrphanBase => {
                    let changed = transaction
                        .execute(
                            "DELETE FROM bases WHERE base_id = ?1",
                            params![issue.record_id],
                        )
                        .map_err(|source| IndexSelfHealError::ExecuteSql {
                            operation: "delete_orphan_base",
                            record_id: issue.record_id.clone(),
                            source: Box::new(source),
                        })?;
                    rows_deleted += changed as u64;
                }
                ConsistencyIssueKind::OrphanRenderCache => {
                    let changed = transaction
                        .execute(
                            "DELETE FROM render_cache WHERE cache_key = ?1",
                            params![issue.record_id],
                        )
                        .map_err(|source| IndexSelfHealError::ExecuteSql {
                            operation: "delete_orphan_render_cache",
                            record_id: issue.record_id.clone(),
                            source: Box::new(source),
                        })?;
                    rows_deleted += changed as u64;
                }
                ConsistencyIssueKind::OrphanLinkSource => {
                    let changed = transaction
                        .execute(
                            "DELETE FROM links WHERE link_id = ?1",
                            params![issue.record_id],
                        )
                        .map_err(|source| IndexSelfHealError::ExecuteSql {
                            operation: "delete_orphan_link_source",
                            record_id: issue.record_id.clone(),
                            source: Box::new(source),
                        })?;
                    rows_deleted += changed as u64;
                }
                ConsistencyIssueKind::BrokenLinkTarget => {
                    let changed = transaction
                        .execute(
                            "UPDATE links SET resolved_file_id = NULL, is_unresolved = 1 WHERE link_id = ?1",
                            params![issue.record_id],
                        )
                        .map_err(|source| IndexSelfHealError::ExecuteSql {
                            operation: "repair_broken_link_target",
                            record_id: issue.record_id.clone(),
                            source: Box::new(source),
                        })?;
                    rows_updated += changed as u64;
                }
                ConsistencyIssueKind::LinkResolutionMismatch => {
                    let changed = transaction
                        .execute(
                            "UPDATE links SET is_unresolved = CASE WHEN resolved_file_id IS NULL THEN 1 ELSE 0 END WHERE link_id = ?1",
                            params![issue.record_id],
                        )
                        .map_err(|source| IndexSelfHealError::ExecuteSql {
                            operation: "repair_link_resolution_mismatch",
                            record_id: issue.record_id.clone(),
                            source: Box::new(source),
                        })?;
                    rows_updated += changed as u64;
                }
                ConsistencyIssueKind::OutsideVaultRoot
                | ConsistencyIssueKind::MissingOnDiskFile => {
                    let changed = transaction
                        .execute(
                            "DELETE FROM files WHERE file_id = ?1",
                            params![issue.record_id],
                        )
                        .map_err(|source| IndexSelfHealError::ExecuteSql {
                            operation: "delete_inconsistent_file_row",
                            record_id: issue.record_id.clone(),
                            source: Box::new(source),
                        })?;
                    rows_deleted += changed as u64;
                }
            }
        }

        transaction
            .commit()
            .map_err(|source| IndexSelfHealError::CommitTransaction {
                source: Box::new(source),
            })?;

        let after = self
            .checker
            .check(vault_root, connection)
            .map_err(|source| IndexSelfHealError::CheckAfter {
                source: Box::new(source),
            })?;

        Ok(IndexSelfHealResult {
            issues_detected: before.issues.len() as u64,
            rows_deleted,
            rows_updated,
            remaining_issues: after.issues.len() as u64,
        })
    }
}

pub(super) fn query_orphan_properties(
    connection: &Connection,
) -> Result<Vec<IndexConsistencyIssue>, IndexConsistencyError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT
  p.property_id,
  p.file_id
FROM properties p
LEFT JOIN files f ON f.file_id = p.file_id
WHERE f.file_id IS NULL
ORDER BY p.property_id ASC
"#,
        )
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "prepare_orphan_properties",
            source: Box::new(source),
        })?;

    let rows = statement
        .query_map([], |row| {
            Ok(IndexConsistencyIssue {
                kind: ConsistencyIssueKind::OrphanProperty,
                record_id: row.get("property_id")?,
                detail: format!(
                    "references missing file_id {}",
                    row.get::<_, String>("file_id")?
                ),
            })
        })
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "query_orphan_properties",
            source: Box::new(source),
        })?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "map_orphan_properties",
            source: Box::new(source),
        })
}

pub(super) fn query_orphan_bases(
    connection: &Connection,
) -> Result<Vec<IndexConsistencyIssue>, IndexConsistencyError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT
  b.base_id,
  b.file_id
FROM bases b
LEFT JOIN files f ON f.file_id = b.file_id
WHERE f.file_id IS NULL
ORDER BY b.base_id ASC
"#,
        )
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "prepare_orphan_bases",
            source: Box::new(source),
        })?;

    let rows = statement
        .query_map([], |row| {
            Ok(IndexConsistencyIssue {
                kind: ConsistencyIssueKind::OrphanBase,
                record_id: row.get("base_id")?,
                detail: format!(
                    "references missing file_id {}",
                    row.get::<_, String>("file_id")?
                ),
            })
        })
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "query_orphan_bases",
            source: Box::new(source),
        })?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "map_orphan_bases",
            source: Box::new(source),
        })
}

pub(super) fn query_orphan_render_cache(
    connection: &Connection,
) -> Result<Vec<IndexConsistencyIssue>, IndexConsistencyError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT
  r.cache_key,
  r.file_id
FROM render_cache r
LEFT JOIN files f ON f.file_id = r.file_id
WHERE r.file_id IS NOT NULL
  AND f.file_id IS NULL
ORDER BY r.cache_key ASC
"#,
        )
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "prepare_orphan_render_cache",
            source: Box::new(source),
        })?;

    let rows = statement
        .query_map([], |row| {
            Ok(IndexConsistencyIssue {
                kind: ConsistencyIssueKind::OrphanRenderCache,
                record_id: row.get("cache_key")?,
                detail: format!(
                    "references missing file_id {}",
                    row.get::<_, String>("file_id")?
                ),
            })
        })
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "query_orphan_render_cache",
            source: Box::new(source),
        })?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "map_orphan_render_cache",
            source: Box::new(source),
        })
}

pub(super) fn query_orphan_link_sources(
    connection: &Connection,
) -> Result<Vec<IndexConsistencyIssue>, IndexConsistencyError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT
  l.link_id,
  l.source_file_id
FROM links l
LEFT JOIN files f ON f.file_id = l.source_file_id
WHERE f.file_id IS NULL
ORDER BY l.link_id ASC
"#,
        )
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "prepare_orphan_link_sources",
            source: Box::new(source),
        })?;

    let rows = statement
        .query_map([], |row| {
            Ok(IndexConsistencyIssue {
                kind: ConsistencyIssueKind::OrphanLinkSource,
                record_id: row.get("link_id")?,
                detail: format!(
                    "references missing source_file_id {}",
                    row.get::<_, String>("source_file_id")?
                ),
            })
        })
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "query_orphan_link_sources",
            source: Box::new(source),
        })?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "map_orphan_link_sources",
            source: Box::new(source),
        })
}

pub(super) fn query_broken_link_targets(
    connection: &Connection,
) -> Result<Vec<IndexConsistencyIssue>, IndexConsistencyError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT
  l.link_id,
  l.resolved_file_id
FROM links l
LEFT JOIN files f ON f.file_id = l.resolved_file_id
WHERE l.resolved_file_id IS NOT NULL
  AND f.file_id IS NULL
ORDER BY l.link_id ASC
"#,
        )
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "prepare_broken_link_targets",
            source: Box::new(source),
        })?;

    let rows = statement
        .query_map([], |row| {
            Ok(IndexConsistencyIssue {
                kind: ConsistencyIssueKind::BrokenLinkTarget,
                record_id: row.get("link_id")?,
                detail: format!(
                    "references missing resolved_file_id {}",
                    row.get::<_, String>("resolved_file_id")?
                ),
            })
        })
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "query_broken_link_targets",
            source: Box::new(source),
        })?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "map_broken_link_targets",
            source: Box::new(source),
        })
}

pub(super) fn query_link_resolution_mismatches(
    connection: &Connection,
) -> Result<Vec<IndexConsistencyIssue>, IndexConsistencyError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT
  link_id,
  resolved_file_id,
  is_unresolved
FROM links
WHERE (is_unresolved = 1 AND resolved_file_id IS NOT NULL)
   OR (is_unresolved = 0 AND resolved_file_id IS NULL)
ORDER BY link_id ASC
"#,
        )
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "prepare_link_resolution_mismatches",
            source: Box::new(source),
        })?;

    let rows = statement
        .query_map([], |row| {
            let resolved_file_id: Option<String> = row.get("resolved_file_id")?;
            let is_unresolved = row.get::<_, i64>("is_unresolved")? != 0;
            Ok(IndexConsistencyIssue {
                kind: ConsistencyIssueKind::LinkResolutionMismatch,
                record_id: row.get("link_id")?,
                detail: format!(
                    "is_unresolved={} resolved_file_id={}",
                    is_unresolved,
                    resolved_file_id.unwrap_or_else(|| "<none>".to_string())
                ),
            })
        })
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "query_link_resolution_mismatches",
            source: Box::new(source),
        })?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| IndexConsistencyError::Sql {
            operation: "map_link_resolution_mismatches",
            source: Box::new(source),
        })
}

pub(super) fn query_filesystem_path_issues(
    connection: &Connection,
    canonical_vault_root: &Path,
) -> Result<Vec<IndexConsistencyIssue>, IndexConsistencyError> {
    let files = FilesRepository::list_all(connection).map_err(|source| {
        IndexConsistencyError::ListIndexedFiles {
            source: Box::new(source),
        }
    })?;

    let mut issues = Vec::new();
    for file in files {
        let absolute_path = PathBuf::from(&file.absolute_path);
        if !absolute_path.starts_with(canonical_vault_root) {
            issues.push(IndexConsistencyIssue {
                kind: ConsistencyIssueKind::OutsideVaultRoot,
                record_id: file.file_id.clone(),
                detail: format!(
                    "absolute path '{}' is outside vault root '{}'",
                    file.absolute_path,
                    canonical_vault_root.to_string_lossy()
                ),
            });
        }

        if let Err(source) = fs::metadata(&absolute_path) {
            issues.push(IndexConsistencyIssue {
                kind: ConsistencyIssueKind::MissingOnDiskFile,
                record_id: file.file_id,
                detail: format!(
                    "absolute path '{}' is not readable: {source}",
                    file.absolute_path
                ),
            });
        }
    }

    Ok(issues)
}
