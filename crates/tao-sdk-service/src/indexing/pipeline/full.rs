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
    /// Rebuild all core index tables from the current vault filesystem state.
    pub fn rebuild(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
    ) -> Result<FullIndexResult, FullIndexError> {
        let scanner = VaultScanService::from_root(vault_root, case_policy).map_err(|source| {
            FullIndexError::CreateScanner {
                source: Box::new(source),
            }
        })?;
        let manifest = scanner.scan().map_err(|source| FullIndexError::Scan {
            source: Box::new(source),
        })?;

        let resolution_candidates: Vec<String> = manifest
            .entries
            .iter()
            .map(|entry| entry.normalized.clone())
            .collect();
        let markdown_candidates: Vec<String> = manifest
            .entries
            .iter()
            .filter(|entry| entry.normalized.ends_with(".md"))
            .map(|entry| entry.normalized.clone())
            .collect();

        let prepared_entries = manifest
            .entries
            .par_iter()
            .map(|entry| build_prepared_index_entry(entry, self.parser))
            .collect::<Result<Vec<_>, _>>()?;

        let mut file_records = Vec::with_capacity(prepared_entries.len());
        let mut file_id_by_path = HashMap::with_capacity(prepared_entries.len());
        let mut markdown_docs = Vec::new();
        let mut base_records = Vec::new();
        let mut search_records = Vec::new();

        for prepared in prepared_entries {
            file_id_by_path.insert(
                prepared.file_record.normalized_path.clone(),
                prepared.file_record.file_id.clone(),
            );
            file_records.push(prepared.file_record);
            if let Some(markdown_doc) = prepared.markdown_doc {
                markdown_docs.push(markdown_doc);
            }
            if let Some(base_record) = prepared.base_record {
                base_records.push(base_record);
            }
            if let Some(search_record) = prepared.search_record {
                search_records.push(search_record);
            }
        }

        markdown_docs.sort_by(|left, right| left.source_path.cmp(&right.source_path));
        file_records.sort_by(|left, right| left.normalized_path.cmp(&right.normalized_path));
        base_records.sort_by(|left, right| left.base_id.cmp(&right.base_id));
        search_records.sort_by(|left, right| left.file_id.cmp(&right.file_id));

        let mut property_records = markdown_docs
            .iter()
            .flat_map(|document| document.properties.iter().cloned())
            .collect::<Vec<_>>();
        let mut task_records = markdown_docs
            .iter()
            .flat_map(|document| document.tasks.iter().cloned())
            .collect::<Vec<_>>();
        property_records.sort_by(|left, right| left.property_id.cmp(&right.property_id));
        task_records.sort_by(|left, right| left.task_id.cmp(&right.task_id));

        let heading_index = markdown_docs
            .iter()
            .map(|document| (document.source_path.clone(), document.heading_slugs.clone()))
            .collect::<HashMap<_, _>>();
        let block_index = markdown_docs
            .iter()
            .map(|document| (document.source_path.clone(), document.block_ids.clone()))
            .collect::<HashMap<_, _>>();
        let mut unresolved_links = 0_u64;
        let mut link_records = markdown_docs
            .par_iter()
            .map(|document| {
                resolve_document_link_records(
                    document,
                    &resolution_candidates,
                    &file_id_by_path,
                    &heading_index,
                    &block_index,
                )
            })
            .collect::<Vec<_>>();
        let mut link_record_rows = Vec::new();
        for batch in link_records.drain(..) {
            unresolved_links += batch.unresolved_total;
            link_record_rows.extend(batch.records);
        }
        link_record_rows.sort_by(|left, right| left.link_id.cmp(&right.link_id));
        let link_records = link_record_rows;

        let transaction =
            connection
                .transaction()
                .map_err(|source| FullIndexError::BeginTransaction {
                    source: Box::new(source),
                })?;

        transaction
            .execute_batch(
                "DELETE FROM links;\
                 DELETE FROM properties;\
                 DELETE FROM bases;\
                 DELETE FROM render_cache;\
                 DELETE FROM search_index;\
                 DELETE FROM tasks;\
                 DELETE FROM files;",
            )
            .map_err(|source| FullIndexError::ClearTables {
                source: Box::new(source),
            })?;

        upsert_files_batch(&transaction, &file_records)?;
        upsert_properties_batch(&transaction, &property_records)?;
        upsert_tasks_batch(&transaction, &task_records)?;
        insert_links_batch(&transaction, &link_records)?;
        upsert_bases_batch(&transaction, &base_records)?;
        upsert_search_index_batch(&transaction, &search_records)?;

        let now_unix_ms = current_unix_ms()?;
        IndexStateRepository::upsert(
            &transaction,
            &IndexStateRecordInput {
                key: "last_index_at".to_string(),
                value_json: now_unix_ms.to_string(),
            },
        )
        .map_err(|source| FullIndexError::UpsertIndexState {
            source: Box::new(source),
        })?;
        IndexStateRepository::upsert(
            &transaction,
            &IndexStateRecordInput {
                key: LINK_RESOLUTION_VERSION_STATE_KEY.to_string(),
                value_json: CURRENT_LINK_RESOLUTION_VERSION.to_string(),
            },
        )
        .map_err(|source| FullIndexError::UpsertIndexState {
            source: Box::new(source),
        })?;

        let summary_json = serde_json::to_string(&json!({
            "mode": "full_rebuild",
            "indexed_files": file_records.len(),
            "markdown_files": markdown_candidates.len(),
            "links_total": link_records.len(),
            "unresolved_links": unresolved_links,
            "properties_total": property_records.len(),
            "tasks_total": task_records.len(),
            "bases_total": base_records.len(),
            "completed_unix_ms": now_unix_ms,
        }))
        .map_err(|source| FullIndexError::SerializeStateSummary {
            source: Box::new(source),
        })?;

        IndexStateRepository::upsert(
            &transaction,
            &IndexStateRecordInput {
                key: "last_full_index_summary".to_string(),
                value_json: summary_json,
            },
        )
        .map_err(|source| FullIndexError::UpsertIndexState {
            source: Box::new(source),
        })?;

        transaction
            .commit()
            .map_err(|source| FullIndexError::CommitTransaction {
                source: Box::new(source),
            })?;

        Ok(FullIndexResult {
            indexed_files: file_records.len() as u64,
            markdown_files: markdown_candidates.len() as u64,
            links_total: link_records.len() as u64,
            unresolved_links,
            properties_total: property_records.len() as u64,
            bases_total: base_records.len() as u64,
        })
    }
}
