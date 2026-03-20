use super::*;

/// Issue categories emitted by the index consistency checker.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConsistencyIssueKind {
    /// Property row references a missing file row.
    OrphanProperty,
    /// Base row references a missing file row.
    OrphanBase,
    /// Render cache row references a missing file row.
    OrphanRenderCache,
    /// Link row source file reference is missing.
    OrphanLinkSource,
    /// Link row resolved target reference is missing.
    BrokenLinkTarget,
    /// Link unresolved flag conflicts with resolved target presence.
    LinkResolutionMismatch,
    /// File row absolute path is outside configured vault root.
    OutsideVaultRoot,
    /// File row absolute path does not exist on disk.
    MissingOnDiskFile,
}

/// One consistency issue identified during index consistency checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConsistencyIssue {
    /// Issue category.
    pub kind: ConsistencyIssueKind,
    /// Stable row identifier associated with the issue.
    pub record_id: String,
    /// Human-readable issue context.
    pub detail: String,
}

/// Consistency check report over persisted index tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConsistencyReport {
    /// Wall-clock timestamp of report completion.
    pub checked_at_unix_ms: u128,
    /// All issues found; empty means no inconsistencies detected.
    pub issues: Vec<IndexConsistencyIssue>,
}

/// Service that validates index table referential and filesystem consistency.
#[derive(Debug, Default, Clone, Copy)]
pub struct IndexConsistencyChecker;

impl IndexConsistencyChecker {
    /// Run consistency checks and return an ordered issue report.
    pub fn check(
        &self,
        vault_root: &Path,
        connection: &Connection,
    ) -> Result<IndexConsistencyReport, IndexConsistencyError> {
        let canonical_vault_root = std::fs::canonicalize(vault_root).map_err(|source| {
            IndexConsistencyError::CanonicalizeVaultRoot {
                path: vault_root.to_path_buf(),
                source,
            }
        })?;

        let mut issues = Vec::new();

        issues.extend(self_heal::query_orphan_properties(connection)?);
        issues.extend(self_heal::query_orphan_bases(connection)?);
        issues.extend(self_heal::query_orphan_render_cache(connection)?);
        issues.extend(self_heal::query_orphan_link_sources(connection)?);
        issues.extend(self_heal::query_broken_link_targets(connection)?);
        issues.extend(self_heal::query_link_resolution_mismatches(connection)?);
        issues.extend(self_heal::query_filesystem_path_issues(
            connection,
            &canonical_vault_root,
        )?);

        issues.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then(left.record_id.cmp(&right.record_id))
        });

        let checked_at_unix_ms =
            current_unix_ms_raw().map_err(|source| IndexConsistencyError::Clock {
                source: Box::new(source),
            })?;

        Ok(IndexConsistencyReport {
            checked_at_unix_ms,
            issues,
        })
    }
}
