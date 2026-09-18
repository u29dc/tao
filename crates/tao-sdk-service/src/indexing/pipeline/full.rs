use super::*;

/// Result payload for full rebuild indexing workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullIndexResult {
    /// Total files indexed from vault scan.
    pub indexed_files: u64,
    /// Total markdown files indexed.
    pub markdown_files: u64,
    /// Total links indexed.
    pub links_total: u64,
    /// Total unresolved links indexed.
    pub unresolved_links: u64,
    /// Total properties indexed.
    pub properties_total: u64,
    /// Total bases indexed.
    pub bases_total: u64,
}

/// Full rebuild indexing service.
#[derive(Debug, Default, Clone, Copy)]
pub struct FullIndexService {
    parser: MarkdownParser,
}

impl FullIndexService {
    /// Rebuild canonical revisions and publish all projections atomically.
    pub fn rebuild(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
    ) -> Result<FullIndexResult, FullIndexError> {
        let _publication =
            crate::publication_lock::PublicationGuard::acquire(connection).map_err(|source| {
                FullIndexError::CanonicalState {
                    operation: "coordinate_full_publication",
                    message: source.to_string(),
                }
            })?;
        let generation = tao_sdk_storage::IndexGenerationRepository::get(connection)
            .map_err(|source| FullIndexError::CanonicalState {
                operation: "full_preparation_generation",
                message: source.to_string(),
            })?
            .canonical_generation;
        let manifest = VaultScanService::from_root(vault_root, case_policy)
            .map_err(|source| FullIndexError::CreateScanner {
                source: Box::new(source),
            })?
            .scan()
            .map_err(|source| FullIndexError::Scan {
                source: Box::new(source),
            })?;
        let paths = manifest
            .entries
            .iter()
            .map(|entry| entry.normalized.clone())
            .collect::<std::collections::HashSet<_>>();
        let mut changes = manifest
            .entries
            .into_iter()
            .map(|entry| IndexChange::Upsert {
                entry: Box::new(entry),
                captured: None,
            })
            .collect::<Vec<_>>();
        for existing in FilesRepository::list_all(connection).map_err(|source| {
            FullIndexError::UpsertFileMetadata {
                source: Box::new(source),
            }
        })? {
            if !paths.contains(&existing.normalized_path) {
                changes.push(IndexChange::Remove {
                    normalized_path: existing.normalized_path,
                });
            }
        }
        apply::apply_changes(
            vault_root,
            connection,
            changes,
            case_policy,
            self.parser,
            apply::PublicationOptions {
                force: true,
                force_full_corpus: true,
                expected_generation: Some(generation),
            },
        )?;
        let count = |sql: &str| {
            connection
                .query_row(sql, [], |row| row.get::<_, u64>(0))
                .map_err(|source| FullIndexError::CanonicalState {
                    operation: "full_index_totals",
                    message: source.to_string(),
                })
        };
        let result = FullIndexResult {
            indexed_files: count("SELECT COUNT(*) FROM files")?,
            markdown_files: count("SELECT COUNT(*) FROM files WHERE is_markdown=1")?,
            links_total: count("SELECT COUNT(*) FROM links")?,
            unresolved_links: count("SELECT COUNT(*) FROM links WHERE is_unresolved=1")?,
            properties_total: count("SELECT COUNT(*) FROM properties")?,
            bases_total: count("SELECT COUNT(*) FROM bases")?,
        };
        let summary = json!({"mode":"full_rebuild","indexed_files":result.indexed_files,
            "markdown_files":result.markdown_files,"links_total":result.links_total,
            "unresolved_links":result.unresolved_links,"properties_total":result.properties_total,
            "bases_total":result.bases_total,"completed_unix_ms":current_unix_ms()?});
        IndexStateRepository::upsert(
            connection,
            &IndexStateRecordInput {
                key: "last_full_index_summary".to_string(),
                value_json: summary.to_string(),
            },
        )
        .map_err(|source| FullIndexError::UpsertIndexState {
            source: Box::new(source),
        })?;
        Ok(result)
    }
}
