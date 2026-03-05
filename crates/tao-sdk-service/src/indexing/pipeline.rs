use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rayon::prelude::*;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tao_sdk_links::{
    WikiLink, extract_block_ids, extract_markdown_links, extract_wikilinks, resolve_block_target,
    resolve_heading_target, resolve_target, slugify_heading,
};
use tao_sdk_markdown::{MarkdownParseError, MarkdownParseRequest, MarkdownParser};
use tao_sdk_properties::{
    FrontMatterStatus, PropertyProjectionError, TypedPropertyValue, extract_front_matter,
    project_typed_properties,
};
use tao_sdk_storage::{
    BaseRecordInput, BasesRepository, FileRecordInput, FilesRepository, IndexStateRecordInput,
    IndexStateRepository, LinkRecordInput, LinkWithPaths, PropertiesRepository,
    PropertyRecordInput, SearchIndexRecordInput, SearchIndexRepository, TaskRecordInput,
    TasksRepository,
};
use tao_sdk_vault::{
    CasePolicy, FileFingerprintError, FileFingerprintService, PathCanonicalizationError,
    VaultManifestEntry, VaultScanError, VaultScanService,
};
use thiserror::Error;

const CHECKPOINT_STATE_KEY: &str = "checkpoint.incremental_index";
const CHECKPOINT_SUMMARY_KEY: &str = "last_checkpointed_index_summary";

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

/// Result payload for incremental indexing workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalIndexResult {
    /// Number of changed paths processed.
    pub processed_paths: u64,
    /// Number of files inserted or updated.
    pub upserted_files: u64,
    /// Number of files removed from index.
    pub removed_files: u64,
    /// Number of links reindexed.
    pub links_reindexed: u64,
    /// Number of properties reindexed.
    pub properties_reindexed: u64,
    /// Number of bases reindexed.
    pub bases_reindexed: u64,
}

/// Result payload for coalesced batch indexing workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalescedBatchIndexResult {
    /// Number of raw path change events received.
    pub input_events: u64,
    /// Number of unique normalized paths after coalescing.
    pub unique_paths: u64,
    /// Number of batches applied.
    pub batches_applied: u64,
    /// Number of files inserted or updated.
    pub upserted_files: u64,
    /// Number of files removed from index.
    pub removed_files: u64,
    /// Number of links reindexed.
    pub links_reindexed: u64,
    /// Number of properties reindexed.
    pub properties_reindexed: u64,
    /// Number of bases reindexed.
    pub bases_reindexed: u64,
}

/// Incremental indexing service for targeted path updates.
#[derive(Debug, Default, Clone, Copy)]
pub struct IncrementalIndexService {
    parser: MarkdownParser,
}

impl IncrementalIndexService {
    /// Apply incremental indexing updates for one or more changed relative paths.
    pub fn apply_changes(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changed_paths: &[PathBuf],
        case_policy: CasePolicy,
    ) -> Result<IncrementalIndexResult, FullIndexError> {
        self.apply_changes_internal(vault_root, connection, changed_paths, case_policy, true)
    }

    /// Apply incremental indexing updates and always rebuild derived rows for provided paths.
    pub fn apply_changes_force(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changed_paths: &[PathBuf],
        case_policy: CasePolicy,
    ) -> Result<IncrementalIndexResult, FullIndexError> {
        self.apply_changes_internal(vault_root, connection, changed_paths, case_policy, false)
    }

    fn apply_changes_internal(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changed_paths: &[PathBuf],
        case_policy: CasePolicy,
        prefilter_unchanged: bool,
    ) -> Result<IncrementalIndexResult, FullIndexError> {
        let fingerprint_service = FileFingerprintService::from_root(vault_root, case_policy)
            .map_err(|source| FullIndexError::CreateFingerprintService {
                source: Box::new(source),
            })?;

        let transaction =
            connection
                .transaction()
                .map_err(|source| FullIndexError::BeginTransaction {
                    source: Box::new(source),
                })?;

        let mut upserted_files = 0_u64;
        let mut removed_files = 0_u64;
        let mut links_reindexed = 0_u64;
        let mut properties_reindexed = 0_u64;
        let mut bases_reindexed = 0_u64;
        let mut pending_link_records = Vec::<LinkRecordInput>::new();
        let existing_link_rows = tao_sdk_storage::LinksRepository::list_all_with_paths(
            &transaction,
        )
        .map_err(|source| FullIndexError::InsertLink {
            source: Box::new(source),
        })?;
        let mut changed_markdown_paths = std::collections::BTreeSet::<String>::new();

        let existing_records = FilesRepository::list_all(&transaction).map_err(|source| {
            FullIndexError::UpsertFileMetadata {
                source: Box::new(source),
            }
        })?;
        let mut markdown_candidates = Vec::new();
        let mut resolution_candidates = Vec::new();
        let mut file_id_by_path = HashMap::<String, String>::new();
        for record in existing_records {
            if record.is_markdown {
                markdown_candidates.push(record.normalized_path.clone());
            }
            resolution_candidates.push(record.normalized_path.clone());
            file_id_by_path.insert(record.normalized_path, record.file_id);
        }
        for changed_path in changed_paths {
            let normalized = normalize_changed_path(changed_path)?;
            let absolute = vault_root.join(changed_path);
            if absolute.exists() && normalized.ends_with(".md") {
                markdown_candidates.push(normalized.clone());
                let lookup_key = normalize_changed_path(changed_path)?;
                file_id_by_path
                    .entry(lookup_key.clone())
                    .or_insert_with(|| deterministic_id("file", &lookup_key));
            }
            if absolute.exists() {
                resolution_candidates.push(normalized);
            }
        }
        markdown_candidates.sort();
        markdown_candidates.dedup();
        resolution_candidates.sort();
        resolution_candidates.dedup();
        let mut heading_index =
            build_heading_index(vault_root, &markdown_candidates, &self.parser)?;
        let mut block_index = build_block_index(vault_root, &markdown_candidates, &self.parser)?;

        for changed_path in changed_paths {
            let normalized = normalize_changed_path(changed_path)?;
            let absolute = vault_root.join(changed_path);
            if normalized.ends_with(".md") {
                changed_markdown_paths.insert(normalized.clone());
            }
            let existing = FilesRepository::get_by_normalized_path(&transaction, &normalized)
                .map_err(|source| FullIndexError::UpsertFileMetadata {
                    source: Box::new(source),
                })?;

            if absolute.exists() {
                if prefilter_unchanged && let Some(existing_record) = existing.as_ref() {
                    let metadata =
                        fs::metadata(&absolute).map_err(|source| FullIndexError::ReadFile {
                            path: absolute.clone(),
                            source,
                        })?;
                    let modified_unix_ms = metadata_modified_unix_ms(&metadata, &absolute)?;
                    if existing_record.size_bytes == metadata.len()
                        && existing_record.modified_unix_ms == modified_unix_ms
                    {
                        // Fast unchanged prefilter: only skip when size/mtime/hash all match.
                        // This preserves correctness for rapid same-size rewrites on filesystems
                        // with coarse mtime granularity.
                        let hash_blake3 = hash_file_blake3(&absolute).map_err(|source| {
                            FullIndexError::ReadFile {
                                path: absolute.clone(),
                                source,
                            }
                        })?;
                        if existing_record.hash_blake3 == hash_blake3 {
                            continue;
                        }
                    }
                }

                let fingerprint =
                    fingerprint_service
                        .fingerprint(changed_path)
                        .map_err(|source| FullIndexError::Fingerprint {
                            path: absolute.clone(),
                            source: Box::new(source),
                        })?;
                let modified_unix_ms =
                    i64::try_from(fingerprint.modified_unix_ms).map_err(|_| {
                        FullIndexError::TimestampOverflow {
                            value: fingerprint.modified_unix_ms,
                        }
                    })?;
                let file_id = existing
                    .map(|record| record.file_id)
                    .unwrap_or_else(|| deterministic_id("file", &normalized));
                file_id_by_path.insert(normalized.clone(), file_id.clone());

                FilesRepository::upsert(
                    &transaction,
                    &FileRecordInput {
                        file_id: file_id.clone(),
                        normalized_path: normalized.clone(),
                        match_key: fingerprint.match_key,
                        absolute_path: fingerprint.absolute.to_string_lossy().to_string(),
                        size_bytes: fingerprint.size_bytes,
                        modified_unix_ms,
                        hash_blake3: fingerprint.hash_blake3,
                        is_markdown: normalized.ends_with(".md"),
                    },
                )
                .map_err(|source| FullIndexError::UpsertFileMetadata {
                    source: Box::new(source),
                })?;

                transaction
                    .execute(
                        "DELETE FROM links WHERE source_file_id = ?1",
                        params![file_id],
                    )
                    .map_err(|source| FullIndexError::ExecuteSql {
                        operation: "delete_links_for_file",
                        source: Box::new(source),
                    })?;
                transaction
                    .execute(
                        "DELETE FROM properties WHERE file_id = ?1",
                        params![file_id],
                    )
                    .map_err(|source| FullIndexError::ExecuteSql {
                        operation: "delete_properties_for_file",
                        source: Box::new(source),
                    })?;
                TasksRepository::delete_by_file_id(&transaction, &file_id).map_err(|source| {
                    FullIndexError::UpsertTask {
                        source: Box::new(source),
                    }
                })?;
                transaction
                    .execute("DELETE FROM bases WHERE file_id = ?1", params![file_id])
                    .map_err(|source| FullIndexError::ExecuteSql {
                        operation: "delete_bases_for_file",
                        source: Box::new(source),
                    })?;
                SearchIndexRepository::delete_by_file_id(&transaction, &file_id).map_err(
                    |source| FullIndexError::UpsertSearchIndex {
                        source: Box::new(source),
                    },
                )?;

                if normalized.ends_with(".md") {
                    let markdown = fs::read_to_string(&absolute).map_err(|source| {
                        FullIndexError::ReadFile {
                            path: absolute.clone(),
                            source,
                        }
                    })?;
                    let parsed = self
                        .parser
                        .parse(MarkdownParseRequest {
                            normalized_path: normalized.clone(),
                            raw: markdown.clone(),
                        })
                        .map_err(|source| FullIndexError::ParseMarkdown {
                            path: absolute.clone(),
                            source: Box::new(source),
                        })?;

                    let property_records =
                        build_property_records(&file_id, &normalized, &markdown, &absolute)?;
                    properties_reindexed += property_records.len() as u64;
                    for property in &property_records {
                        PropertiesRepository::upsert(&transaction, property).map_err(|source| {
                            FullIndexError::UpsertProperty {
                                source: Box::new(source),
                            }
                        })?;
                    }

                    SearchIndexRepository::upsert(
                        &transaction,
                        &SearchIndexRecordInput {
                            file_id: file_id.clone(),
                            normalized_path: normalized.clone(),
                            normalized_path_lc: normalized.to_lowercase(),
                            title_lc: title_from_normalized_path(&normalized).to_lowercase(),
                            content_lc: markdown.to_lowercase(),
                        },
                    )
                    .map_err(|source| FullIndexError::UpsertSearchIndex {
                        source: Box::new(source),
                    })?;
                    for task in build_task_records(&file_id, &normalized, &markdown) {
                        TasksRepository::upsert(&transaction, &task).map_err(|source| {
                            FullIndexError::UpsertTask {
                                source: Box::new(source),
                            }
                        })?;
                    }

                    if !markdown_candidates.iter().any(|path| path == &normalized) {
                        markdown_candidates.push(normalized.clone());
                        markdown_candidates.sort();
                    }
                    if !resolution_candidates.iter().any(|path| path == &normalized) {
                        resolution_candidates.push(normalized.clone());
                        resolution_candidates.sort();
                    }
                    let mut heading_slugs = parsed
                        .headings
                        .iter()
                        .map(|heading| slugify_heading(&heading.text))
                        .filter(|slug| !slug.is_empty())
                        .collect::<Vec<_>>();
                    heading_slugs.sort();
                    heading_slugs.dedup();
                    heading_index.insert(normalized.clone(), heading_slugs);
                    block_index.insert(normalized.clone(), extract_block_ids(&parsed.body));

                    let link_records = build_incremental_link_records(
                        &LinkResolutionContext {
                            resolution_candidates: &resolution_candidates,
                            file_id_by_path: &file_id_by_path,
                            heading_index: &heading_index,
                            block_index: &block_index,
                        },
                        &file_id,
                        &normalized,
                        &markdown,
                        &parsed.body,
                    );
                    links_reindexed += link_records.len() as u64;
                    pending_link_records.extend(link_records);
                } else if normalized.ends_with(".base") {
                    let raw = fs::read_to_string(&absolute).map_err(|source| {
                        FullIndexError::ReadFile {
                            path: absolute.clone(),
                            source,
                        }
                    })?;
                    let config_json =
                        serde_json::to_string(&json!({ "raw": raw })).map_err(|source| {
                            FullIndexError::SerializeBaseConfig {
                                path: absolute.clone(),
                                source,
                            }
                        })?;

                    BasesRepository::upsert(
                        &transaction,
                        &BaseRecordInput {
                            base_id: deterministic_id("base", &normalized),
                            file_id,
                            config_json,
                        },
                    )
                    .map_err(|source| FullIndexError::UpsertBase {
                        source: Box::new(source),
                    })?;
                    bases_reindexed += 1;
                }

                upserted_files += 1;
            } else if let Some(existing) = existing {
                FilesRepository::delete_by_id(&transaction, &existing.file_id).map_err(
                    |source| FullIndexError::UpsertFileMetadata {
                        source: Box::new(source),
                    },
                )?;
                if existing.is_markdown {
                    markdown_candidates.retain(|candidate| candidate != &normalized);
                    heading_index.remove(&normalized);
                    block_index.remove(&normalized);
                }
                resolution_candidates.retain(|candidate| candidate != &normalized);
                file_id_by_path.remove(&normalized);
                removed_files += 1;
            }
        }

        let affected_sources = existing_link_rows
            .iter()
            .filter(|link| !changed_markdown_paths.contains(&link.source_path))
            .filter(|link| {
                stored_link_requires_refresh(
                    link,
                    &resolution_candidates,
                    &file_id_by_path,
                    &heading_index,
                    &block_index,
                )
            })
            .map(|link| link.source_path.clone())
            .collect::<std::collections::BTreeSet<_>>();

        for source_path in affected_sources {
            let Some(source_record) =
                FilesRepository::get_by_normalized_path(&transaction, &source_path).map_err(
                    |source| FullIndexError::UpsertFileMetadata {
                        source: Box::new(source),
                    },
                )?
            else {
                continue;
            };
            let absolute = vault_root.join(&source_path);
            let markdown =
                fs::read_to_string(&absolute).map_err(|source| FullIndexError::ReadFile {
                    path: absolute.clone(),
                    source,
                })?;
            let parsed = self
                .parser
                .parse(MarkdownParseRequest {
                    normalized_path: source_path.clone(),
                    raw: markdown.clone(),
                })
                .map_err(|source| FullIndexError::ParseMarkdown {
                    path: absolute,
                    source: Box::new(source),
                })?;

            transaction
                .execute(
                    "DELETE FROM links WHERE source_file_id = ?1",
                    params![source_record.file_id],
                )
                .map_err(|source| FullIndexError::ExecuteSql {
                    operation: "delete_links_for_dependent_source",
                    source: Box::new(source),
                })?;
            let link_records = build_incremental_link_records(
                &LinkResolutionContext {
                    resolution_candidates: &resolution_candidates,
                    file_id_by_path: &file_id_by_path,
                    heading_index: &heading_index,
                    block_index: &block_index,
                },
                &source_record.file_id,
                &source_path,
                &markdown,
                &parsed.body,
            );
            links_reindexed += link_records.len() as u64;
            pending_link_records.extend(link_records);
        }

        insert_links_batch(&transaction, &pending_link_records)?;

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

        let summary_json = serde_json::to_string(&json!({
            "mode": "incremental",
            "processed_paths": changed_paths.len(),
            "upserted_files": upserted_files,
            "removed_files": removed_files,
            "links_reindexed": links_reindexed,
            "properties_reindexed": properties_reindexed,
            "bases_reindexed": bases_reindexed,
            "completed_unix_ms": now_unix_ms,
        }))
        .map_err(|source| FullIndexError::SerializeStateSummary {
            source: Box::new(source),
        })?;

        IndexStateRepository::upsert(
            &transaction,
            &IndexStateRecordInput {
                key: "last_incremental_index_summary".to_string(),
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

        Ok(IncrementalIndexResult {
            processed_paths: changed_paths.len() as u64,
            upserted_files,
            removed_files,
            links_reindexed,
            properties_reindexed,
            bases_reindexed,
        })
    }
}

/// Coalescing batch service for burst filesystem change events.
#[derive(Debug, Default, Clone, Copy)]
pub struct CoalescedBatchIndexService {
    incremental: IncrementalIndexService,
}

impl CoalescedBatchIndexService {
    /// Deduplicate changed paths and apply incremental indexing in bounded batches.
    pub fn apply_coalesced(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changed_paths: &[PathBuf],
        max_batch_size: usize,
        case_policy: CasePolicy,
    ) -> Result<CoalescedBatchIndexResult, FullIndexError> {
        if max_batch_size == 0 {
            return Err(FullIndexError::InvalidBatchSize { value: 0 });
        }

        let mut seen = std::collections::BTreeSet::new();
        let mut unique_paths = Vec::new();
        for path in changed_paths {
            let normalized = normalize_changed_path(path)?;
            if seen.insert(normalized.clone()) {
                unique_paths.push(PathBuf::from(normalized));
            }
        }

        let mut batches_applied = 0_u64;
        let mut upserted_files = 0_u64;
        let mut removed_files = 0_u64;
        let mut links_reindexed = 0_u64;
        let mut properties_reindexed = 0_u64;
        let mut bases_reindexed = 0_u64;

        for batch in unique_paths.chunks(max_batch_size) {
            let batch_result =
                self.incremental
                    .apply_changes(vault_root, connection, batch, case_policy)?;
            batches_applied += 1;
            upserted_files += batch_result.upserted_files;
            removed_files += batch_result.removed_files;
            links_reindexed += batch_result.links_reindexed;
            properties_reindexed += batch_result.properties_reindexed;
            bases_reindexed += batch_result.bases_reindexed;
        }

        Ok(CoalescedBatchIndexResult {
            input_events: changed_paths.len() as u64,
            unique_paths: unique_paths.len() as u64,
            batches_applied,
            upserted_files,
            removed_files,
            links_reindexed,
            properties_reindexed,
            bases_reindexed,
        })
    }
}

/// Result payload for stale metadata cleanup workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleCleanupResult {
    /// Number of files discovered in current vault scan.
    pub scanned_files: u64,
    /// Number of stale file rows removed.
    pub stale_files_removed: u64,
}

/// Service for removing stale file metadata rows not present in the vault scan.
#[derive(Debug, Default, Clone, Copy)]
pub struct StaleCleanupService;

impl StaleCleanupService {
    /// Remove stale file rows and dependent records for files no longer present on disk.
    pub fn cleanup(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
    ) -> Result<StaleCleanupResult, StaleCleanupError> {
        let scanner = VaultScanService::from_root(vault_root, case_policy).map_err(|source| {
            StaleCleanupError::CreateScanner {
                source: Box::new(source),
            }
        })?;
        let manifest = scanner.scan().map_err(|source| StaleCleanupError::Scan {
            source: Box::new(source),
        })?;

        let live_paths = manifest
            .entries
            .iter()
            .map(|entry| entry.normalized.clone())
            .collect::<std::collections::HashSet<_>>();
        let existing = FilesRepository::list_all(connection).map_err(|source| {
            StaleCleanupError::ListFiles {
                source: Box::new(source),
            }
        })?;
        let stale_ids = existing
            .into_iter()
            .filter(|record| !live_paths.contains(&record.normalized_path))
            .map(|record| record.file_id)
            .collect::<Vec<_>>();

        let transaction =
            connection
                .transaction()
                .map_err(|source| StaleCleanupError::BeginTransaction {
                    source: Box::new(source),
                })?;

        for file_id in &stale_ids {
            FilesRepository::delete_by_id(&transaction, file_id).map_err(|source| {
                StaleCleanupError::DeleteFileRow {
                    source: Box::new(source),
                }
            })?;
        }

        let summary_json = serde_json::to_string(&json!({
            "mode": "stale_cleanup",
            "scanned_files": manifest.entries.len(),
            "stale_files_removed": stale_ids.len(),
            "completed_unix_ms": current_unix_ms_raw().map_err(|source| StaleCleanupError::Clock {
                source: Box::new(source),
            })?,
        }))
        .map_err(|source| StaleCleanupError::SerializeSummary {
            source: Box::new(source),
        })?;

        IndexStateRepository::upsert(
            &transaction,
            &IndexStateRecordInput {
                key: "last_stale_cleanup_summary".to_string(),
                value_json: summary_json,
            },
        )
        .map_err(|source| StaleCleanupError::UpsertIndexState {
            source: Box::new(source),
        })?;

        transaction
            .commit()
            .map_err(|source| StaleCleanupError::CommitTransaction {
                source: Box::new(source),
            })?;

        Ok(StaleCleanupResult {
            scanned_files: manifest.entries.len() as u64,
            stale_files_removed: stale_ids.len() as u64,
        })
    }
}

/// Result payload for checkpointed incremental indexing workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointedIndexResult {
    /// Whether this run resumed from a previously persisted checkpoint.
    pub started_from_checkpoint: bool,
    /// Total unique paths tracked by the active checkpoint.
    pub total_paths: u64,
    /// Number of unique paths processed in this invocation.
    pub processed_paths: u64,
    /// Number of unique paths still pending in checkpoint state.
    pub remaining_paths: u64,
    /// Number of incremental batches applied in this invocation.
    pub batches_applied: u64,
    /// Number of file rows upserted in this invocation.
    pub upserted_files: u64,
    /// Number of file rows removed in this invocation.
    pub removed_files: u64,
    /// Number of link rows rebuilt in this invocation.
    pub links_reindexed: u64,
    /// Number of property rows rebuilt in this invocation.
    pub properties_reindexed: u64,
    /// Number of base rows rebuilt in this invocation.
    pub bases_reindexed: u64,
    /// Whether checkpoint state was fully consumed and removed.
    pub checkpoint_completed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncrementalCheckpointState {
    pending_paths: Vec<String>,
    next_offset: usize,
    max_batch_size: usize,
    case_policy: String,
    created_unix_ms: u128,
    updated_unix_ms: u128,
}

/// Service that persists incremental indexing checkpoints and resumes after restart.
#[derive(Debug, Default, Clone, Copy)]
pub struct CheckpointedIndexService {
    incremental: IncrementalIndexService,
}

impl CheckpointedIndexService {
    /// Apply incremental indexing with persisted checkpoints.
    ///
    /// When `changed_paths` is empty, this resumes from checkpoint state if present.
    /// When `changed_paths` is non-empty, this starts a new checkpoint run.
    pub fn apply_checkpointed(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changed_paths: &[PathBuf],
        max_batch_size: usize,
        max_batches_per_run: Option<usize>,
        case_policy: CasePolicy,
    ) -> Result<CheckpointedIndexResult, CheckpointedIndexError> {
        if max_batch_size == 0 {
            return Err(CheckpointedIndexError::InvalidBatchSize { value: 0 });
        }
        if matches!(max_batches_per_run, Some(0)) {
            return Err(CheckpointedIndexError::InvalidBatchLimit { value: 0 });
        }

        let (mut checkpoint, started_from_checkpoint, effective_case_policy) =
            if changed_paths.is_empty() {
                let Some(state) = load_checkpoint_state(connection)? else {
                    return Ok(CheckpointedIndexResult {
                        started_from_checkpoint: true,
                        total_paths: 0,
                        processed_paths: 0,
                        remaining_paths: 0,
                        batches_applied: 0,
                        upserted_files: 0,
                        removed_files: 0,
                        links_reindexed: 0,
                        properties_reindexed: 0,
                        bases_reindexed: 0,
                        checkpoint_completed: true,
                    });
                };
                let policy = parse_checkpoint_case_policy(&state.case_policy)?;
                (state, true, policy)
            } else {
                let mut seen = std::collections::BTreeSet::new();
                let mut pending_paths = Vec::new();
                for path in changed_paths {
                    let normalized = normalize_changed_path(path).map_err(|source| {
                        CheckpointedIndexError::NormalizeChangedPath {
                            source: Box::new(source),
                        }
                    })?;
                    if seen.insert(normalized.clone()) {
                        pending_paths.push(normalized);
                    }
                }

                let now_unix_ms =
                    current_unix_ms_raw().map_err(|source| CheckpointedIndexError::Clock {
                        source: Box::new(source),
                    })?;
                let state = IncrementalCheckpointState {
                    pending_paths,
                    next_offset: 0,
                    max_batch_size,
                    case_policy: checkpoint_case_policy_label(case_policy).to_string(),
                    created_unix_ms: now_unix_ms,
                    updated_unix_ms: now_unix_ms,
                };
                save_checkpoint_state(connection, &state)?;
                (state, false, case_policy)
            };

        let mut processed_paths = 0_u64;
        let mut batches_applied = 0_u64;
        let mut upserted_files = 0_u64;
        let mut removed_files = 0_u64;
        let mut links_reindexed = 0_u64;
        let mut properties_reindexed = 0_u64;
        let mut bases_reindexed = 0_u64;

        while checkpoint.next_offset < checkpoint.pending_paths.len() {
            if max_batches_per_run
                .is_some_and(|limit| batches_applied >= u64::try_from(limit).unwrap_or(u64::MAX))
            {
                break;
            }

            let batch_end = std::cmp::min(
                checkpoint.next_offset + checkpoint.max_batch_size,
                checkpoint.pending_paths.len(),
            );
            let batch_paths = checkpoint.pending_paths[checkpoint.next_offset..batch_end]
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<_>>();

            let batch_result = self
                .incremental
                .apply_changes(vault_root, connection, &batch_paths, effective_case_policy)
                .map_err(|source| CheckpointedIndexError::ApplyIncremental {
                    source: Box::new(source),
                })?;

            checkpoint.next_offset = batch_end;
            checkpoint.updated_unix_ms =
                current_unix_ms_raw().map_err(|source| CheckpointedIndexError::Clock {
                    source: Box::new(source),
                })?;
            save_checkpoint_state(connection, &checkpoint)?;

            processed_paths += batch_result.processed_paths;
            batches_applied += 1;
            upserted_files += batch_result.upserted_files;
            removed_files += batch_result.removed_files;
            links_reindexed += batch_result.links_reindexed;
            properties_reindexed += batch_result.properties_reindexed;
            bases_reindexed += batch_result.bases_reindexed;
        }

        let total_paths = checkpoint.pending_paths.len() as u64;
        let remaining_paths = total_paths - checkpoint.next_offset as u64;
        let checkpoint_completed = remaining_paths == 0;

        if checkpoint_completed {
            IndexStateRepository::delete_by_key(connection, CHECKPOINT_STATE_KEY).map_err(
                |source| CheckpointedIndexError::DeleteCheckpointState {
                    source: Box::new(source),
                },
            )?;
        }

        let completed_unix_ms =
            current_unix_ms_raw().map_err(|source| CheckpointedIndexError::Clock {
                source: Box::new(source),
            })?;
        let summary_json = serde_json::to_string(&json!({
            "mode": "checkpointed_incremental",
            "started_from_checkpoint": started_from_checkpoint,
            "total_paths": total_paths,
            "processed_paths": processed_paths,
            "remaining_paths": remaining_paths,
            "batches_applied": batches_applied,
            "upserted_files": upserted_files,
            "removed_files": removed_files,
            "links_reindexed": links_reindexed,
            "properties_reindexed": properties_reindexed,
            "bases_reindexed": bases_reindexed,
            "checkpoint_completed": checkpoint_completed,
            "completed_unix_ms": completed_unix_ms,
        }))
        .map_err(|source| CheckpointedIndexError::SerializeSummary {
            source: Box::new(source),
        })?;
        IndexStateRepository::upsert(
            connection,
            &IndexStateRecordInput {
                key: CHECKPOINT_SUMMARY_KEY.to_string(),
                value_json: summary_json,
            },
        )
        .map_err(|source| CheckpointedIndexError::UpsertIndexState {
            source: Box::new(source),
        })?;

        Ok(CheckpointedIndexResult {
            started_from_checkpoint,
            total_paths,
            processed_paths,
            remaining_paths,
            batches_applied,
            upserted_files,
            removed_files,
            links_reindexed,
            properties_reindexed,
            bases_reindexed,
            checkpoint_completed,
        })
    }
}

fn load_checkpoint_state(
    connection: &Connection,
) -> Result<Option<IncrementalCheckpointState>, CheckpointedIndexError> {
    let stored =
        IndexStateRepository::get_by_key(connection, CHECKPOINT_STATE_KEY).map_err(|source| {
            CheckpointedIndexError::GetCheckpointState {
                source: Box::new(source),
            }
        })?;
    let Some(record) = stored else {
        return Ok(None);
    };
    let checkpoint = serde_json::from_str::<IncrementalCheckpointState>(&record.value_json)
        .map_err(|source| CheckpointedIndexError::DeserializeCheckpoint {
            source: Box::new(source),
        })?;
    Ok(Some(checkpoint))
}

fn save_checkpoint_state(
    connection: &Connection,
    checkpoint: &IncrementalCheckpointState,
) -> Result<(), CheckpointedIndexError> {
    let value_json = serde_json::to_string(checkpoint).map_err(|source| {
        CheckpointedIndexError::SerializeCheckpoint {
            source: Box::new(source),
        }
    })?;
    IndexStateRepository::upsert(
        connection,
        &IndexStateRecordInput {
            key: CHECKPOINT_STATE_KEY.to_string(),
            value_json,
        },
    )
    .map_err(|source| CheckpointedIndexError::UpsertCheckpointState {
        source: Box::new(source),
    })
}

fn checkpoint_case_policy_label(case_policy: CasePolicy) -> &'static str {
    match case_policy {
        CasePolicy::Sensitive => "sensitive",
        CasePolicy::Insensitive => "insensitive",
    }
}

fn parse_checkpoint_case_policy(label: &str) -> Result<CasePolicy, CheckpointedIndexError> {
    match label {
        "sensitive" => Ok(CasePolicy::Sensitive),
        "insensitive" => Ok(CasePolicy::Insensitive),
        _ => Err(CheckpointedIndexError::InvalidCheckpointCasePolicy {
            value: label.to_string(),
        }),
    }
}

/// Result payload for drift reconciliation scanner workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationScanResult {
    /// Number of files discovered in current vault scan.
    pub scanned_files: u64,
    /// Number of paths detected as inserted.
    pub inserted_paths: u64,
    /// Number of paths detected as updated.
    pub updated_paths: u64,
    /// Number of paths detected as removed.
    pub removed_paths: u64,
    /// Total drift path count submitted for repair.
    pub drift_paths: u64,
    /// Number of incremental repair batches applied.
    pub batches_applied: u64,
    /// Number of file rows upserted by repair batches.
    pub upserted_files: u64,
    /// Number of file rows removed by repair batches.
    pub removed_files: u64,
    /// Number of links rebuilt by repair batches.
    pub links_reindexed: u64,
    /// Number of properties rebuilt by repair batches.
    pub properties_reindexed: u64,
    /// Number of bases rebuilt by repair batches.
    pub bases_reindexed: u64,
}

/// Scanner that detects drift and repairs it via bounded incremental index batches.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReconciliationScannerService {
    coalesced: CoalescedBatchIndexService,
}

impl ReconciliationScannerService {
    /// Scan vault vs index metadata and repair missed watcher events.
    pub fn scan_and_repair(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        case_policy: CasePolicy,
        max_batch_size: usize,
    ) -> Result<ReconciliationScanResult, ReconciliationScanError> {
        if max_batch_size == 0 {
            return Err(ReconciliationScanError::InvalidBatchSize { value: 0 });
        }

        let scanner = VaultScanService::from_root(vault_root, case_policy).map_err(|source| {
            ReconciliationScanError::CreateScanner {
                source: Box::new(source),
            }
        })?;
        let manifest = scanner
            .scan()
            .map_err(|source| ReconciliationScanError::Scan {
                source: Box::new(source),
            })?;

        let existing = FilesRepository::list_reconcile(connection).map_err(|source| {
            ReconciliationScanError::ListIndexedFiles {
                source: Box::new(source),
            }
        })?;
        let mut inserted_changed_paths = Vec::new();
        let mut updated_changed_paths = Vec::new();
        let mut removed_changed_paths = Vec::new();
        let mut inserted_paths = 0_u64;
        let mut updated_paths = 0_u64;
        let mut removed_paths = 0_u64;
        let mut scan_index = 0_usize;
        let mut existing_index = 0_usize;

        while scan_index < manifest.entries.len() && existing_index < existing.len() {
            let scanned = &manifest.entries[scan_index];
            let indexed = &existing[existing_index];
            let order = scanned
                .match_key
                .cmp(&indexed.match_key)
                .then(scanned.normalized.cmp(&indexed.normalized_path));

            match order {
                std::cmp::Ordering::Less => {
                    inserted_changed_paths.push(PathBuf::from(&scanned.normalized));
                    inserted_paths += 1;
                    scan_index += 1;
                }
                std::cmp::Ordering::Greater => {
                    removed_changed_paths.push(PathBuf::from(&indexed.normalized_path));
                    removed_paths += 1;
                    existing_index += 1;
                }
                std::cmp::Ordering::Equal => {
                    if !indexed_record_matches_manifest_entry(indexed, scanned) {
                        updated_changed_paths.push(PathBuf::from(&scanned.normalized));
                        updated_paths += 1;
                    }
                    scan_index += 1;
                    existing_index += 1;
                }
            }
        }

        for scanned in &manifest.entries[scan_index..] {
            inserted_changed_paths.push(PathBuf::from(&scanned.normalized));
            inserted_paths += 1;
        }

        for indexed in &existing[existing_index..] {
            removed_changed_paths.push(PathBuf::from(&indexed.normalized_path));
            removed_paths += 1;
        }

        let mut changed_paths = Vec::with_capacity(
            inserted_changed_paths.len()
                + updated_changed_paths.len()
                + removed_changed_paths.len(),
        );
        changed_paths.extend(inserted_changed_paths);
        changed_paths.extend(updated_changed_paths);
        changed_paths.extend(removed_changed_paths);

        if changed_paths.is_empty() {
            return Ok(ReconciliationScanResult {
                scanned_files: manifest.entries.len() as u64,
                inserted_paths,
                updated_paths,
                removed_paths,
                drift_paths: 0,
                batches_applied: 0,
                upserted_files: 0,
                removed_files: 0,
                links_reindexed: 0,
                properties_reindexed: 0,
                bases_reindexed: 0,
            });
        }

        let batch_result = self
            .coalesced
            .apply_coalesced(
                vault_root,
                connection,
                &changed_paths,
                max_batch_size,
                case_policy,
            )
            .map_err(|source| ReconciliationScanError::RepairBatch {
                source: Box::new(source),
            })?;

        Ok(ReconciliationScanResult {
            scanned_files: manifest.entries.len() as u64,
            inserted_paths,
            updated_paths,
            removed_paths,
            drift_paths: batch_result.unique_paths,
            batches_applied: batch_result.batches_applied,
            upserted_files: batch_result.upserted_files,
            removed_files: batch_result.removed_files,
            links_reindexed: batch_result.links_reindexed,
            properties_reindexed: batch_result.properties_reindexed,
            bases_reindexed: batch_result.bases_reindexed,
        })
    }
}

fn indexed_record_matches_manifest_entry(
    indexed: &tao_sdk_storage::FileReconcileRecord,
    entry: &tao_sdk_vault::VaultManifestEntry,
) -> bool {
    indexed.normalized_path == entry.normalized
        && indexed.match_key == entry.match_key
        && entry
            .absolute
            .to_str()
            .is_some_and(|absolute| indexed.absolute_path == absolute)
        && indexed.size_bytes == entry.size_bytes
        && indexed.modified_unix_ms == entry.modified_unix_ms
}

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

        issues.extend(query_orphan_properties(connection)?);
        issues.extend(query_orphan_bases(connection)?);
        issues.extend(query_orphan_render_cache(connection)?);
        issues.extend(query_orphan_link_sources(connection)?);
        issues.extend(query_broken_link_targets(connection)?);
        issues.extend(query_link_resolution_mismatches(connection)?);
        issues.extend(query_filesystem_path_issues(
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

fn query_orphan_properties(
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

fn query_orphan_bases(
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

fn query_orphan_render_cache(
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

fn query_orphan_link_sources(
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

fn query_broken_link_targets(
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

fn query_link_resolution_mismatches(
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

fn query_filesystem_path_issues(
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

#[derive(Debug, Clone)]
struct MarkdownIndexDocument {
    file_id: String,
    source_path: String,
    links: Vec<IndexedWikiLink>,
    heading_slugs: Vec<String>,
    block_ids: Vec<String>,
    properties: Vec<PropertyRecordInput>,
    tasks: Vec<TaskRecordInput>,
}

#[derive(Debug, Clone)]
struct PreparedIndexEntry {
    file_record: FileRecordInput,
    markdown_doc: Option<MarkdownIndexDocument>,
    base_record: Option<BaseRecordInput>,
    search_record: Option<SearchIndexRecordInput>,
}

#[derive(Debug, Clone)]
struct ResolvedLinkBatch {
    records: Vec<LinkRecordInput>,
    unresolved_total: u64,
}

#[derive(Debug, Clone)]
struct IndexedWikiLink {
    link: WikiLink,
    source: String,
    kind: IndexedLinkKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IndexedLinkKind {
    Wikilink,
    Markdown,
    Embed,
}

impl IndexedLinkKind {
    fn source_field(self, source: &str) -> String {
        match self {
            Self::Wikilink => source.to_string(),
            Self::Markdown => "body:markdown".to_string(),
            Self::Embed => "body:embed".to_string(),
        }
    }
}

fn hash_file_blake3(path: &Path) -> Result<String, std::io::Error> {
    const HASH_BUFFER_BYTES: usize = 64 * 1024;
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::with_capacity(HASH_BUFFER_BYTES, file);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

fn metadata_modified_unix_ms(metadata: &fs::Metadata, path: &Path) -> Result<i64, FullIndexError> {
    let modified_unix_ms = metadata
        .modified()
        .map_err(|source| FullIndexError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?
        .duration_since(UNIX_EPOCH)
        .map_err(|source| FullIndexError::Clock {
            source: Box::new(source),
        })?
        .as_millis();

    i64::try_from(modified_unix_ms).map_err(|_| FullIndexError::TimestampOverflow {
        value: modified_unix_ms,
    })
}

fn upsert_files_batch(
    connection: &Connection,
    records: &[FileRecordInput],
) -> Result<(), FullIndexError> {
    let mut statement = connection
        .prepare_cached(
            r#"
INSERT INTO files (
  file_id,
  normalized_path,
  match_key,
  absolute_path,
  size_bytes,
  modified_unix_ms,
  hash_blake3,
  is_markdown
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
ON CONFLICT(file_id)
DO UPDATE SET
  normalized_path = excluded.normalized_path,
  match_key = excluded.match_key,
  absolute_path = excluded.absolute_path,
  size_bytes = excluded.size_bytes,
  modified_unix_ms = excluded.modified_unix_ms,
  hash_blake3 = excluded.hash_blake3,
  is_markdown = excluded.is_markdown,
  indexed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
        )
        .map_err(|source| FullIndexError::ExecuteSql {
            operation: "prepare_bulk_upsert_files",
            source: Box::new(source),
        })?;

    for record in records {
        statement
            .execute(params![
                record.file_id,
                record.normalized_path,
                record.match_key,
                record.absolute_path,
                record.size_bytes,
                record.modified_unix_ms,
                record.hash_blake3,
                i64::from(record.is_markdown)
            ])
            .map_err(|source| FullIndexError::ExecuteSql {
                operation: "bulk_upsert_files",
                source: Box::new(source),
            })?;
    }

    Ok(())
}

fn upsert_properties_batch(
    connection: &Connection,
    records: &[PropertyRecordInput],
) -> Result<(), FullIndexError> {
    let mut statement = connection
        .prepare_cached(
            r#"
INSERT INTO properties (
  property_id,
  file_id,
  key,
  value_type,
  value_json
)
VALUES (?1, ?2, ?3, ?4, ?5)
ON CONFLICT(file_id, key)
DO UPDATE SET
  value_type = excluded.value_type,
  value_json = excluded.value_json,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
        )
        .map_err(|source| FullIndexError::ExecuteSql {
            operation: "prepare_bulk_upsert_properties",
            source: Box::new(source),
        })?;

    for record in records {
        statement
            .execute(params![
                record.property_id,
                record.file_id,
                record.key,
                record.value_type,
                record.value_json
            ])
            .map_err(|source| FullIndexError::ExecuteSql {
                operation: "bulk_upsert_properties",
                source: Box::new(source),
            })?;
    }

    Ok(())
}

fn upsert_tasks_batch(
    connection: &Connection,
    records: &[TaskRecordInput],
) -> Result<(), FullIndexError> {
    let mut statement = connection
        .prepare_cached(
            r#"
INSERT INTO tasks (
  task_id,
  file_id,
  file_path,
  file_path_lc,
  line_number,
  state,
  text,
  text_lc
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
ON CONFLICT(file_id, line_number)
DO UPDATE SET
  file_path = excluded.file_path,
  file_path_lc = excluded.file_path_lc,
  state = excluded.state,
  text = excluded.text,
  text_lc = excluded.text_lc,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
        )
        .map_err(|source| FullIndexError::ExecuteSql {
            operation: "prepare_bulk_upsert_tasks",
            source: Box::new(source),
        })?;

    for record in records {
        statement
            .execute(params![
                record.task_id,
                record.file_id,
                record.file_path,
                record.file_path_lc,
                record.line_number,
                record.state,
                record.text,
                record.text_lc
            ])
            .map_err(|source| FullIndexError::ExecuteSql {
                operation: "bulk_upsert_tasks",
                source: Box::new(source),
            })?;
    }

    Ok(())
}

fn insert_links_batch(
    connection: &Connection,
    records: &[LinkRecordInput],
) -> Result<(), FullIndexError> {
    let mut statement = connection
        .prepare_cached(
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
        )
        .map_err(|source| FullIndexError::ExecuteSql {
            operation: "prepare_bulk_insert_links",
            source: Box::new(source),
        })?;

    for record in records {
        statement
            .execute(params![
                record.link_id,
                record.source_file_id,
                record.raw_target,
                record.resolved_file_id,
                record.heading_slug,
                record.block_id,
                i64::from(record.is_unresolved),
                record.unresolved_reason,
                record.source_field
            ])
            .map_err(|source| FullIndexError::ExecuteSql {
                operation: "bulk_insert_links",
                source: Box::new(source),
            })?;
    }

    Ok(())
}

fn upsert_bases_batch(
    connection: &Connection,
    records: &[BaseRecordInput],
) -> Result<(), FullIndexError> {
    let mut statement = connection
        .prepare_cached(
            r#"
INSERT INTO bases (
  base_id,
  file_id,
  config_json
)
VALUES (?1, ?2, ?3)
ON CONFLICT(base_id)
DO UPDATE SET
  file_id = excluded.file_id,
  config_json = excluded.config_json,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
        )
        .map_err(|source| FullIndexError::ExecuteSql {
            operation: "prepare_bulk_upsert_bases",
            source: Box::new(source),
        })?;

    for record in records {
        statement
            .execute(params![record.base_id, record.file_id, record.config_json])
            .map_err(|source| FullIndexError::ExecuteSql {
                operation: "bulk_upsert_bases",
                source: Box::new(source),
            })?;
    }

    Ok(())
}

fn upsert_search_index_batch(
    connection: &Connection,
    records: &[SearchIndexRecordInput],
) -> Result<(), FullIndexError> {
    let mut statement = connection
        .prepare_cached(
            r#"
INSERT INTO search_index (
  file_id,
  normalized_path,
  normalized_path_lc,
  title_lc,
  content_lc
)
VALUES (?1, ?2, ?3, ?4, ?5)
ON CONFLICT(file_id)
DO UPDATE SET
  normalized_path = excluded.normalized_path,
  normalized_path_lc = excluded.normalized_path_lc,
  title_lc = excluded.title_lc,
  content_lc = excluded.content_lc,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
        )
        .map_err(|source| FullIndexError::ExecuteSql {
            operation: "prepare_bulk_upsert_search_index",
            source: Box::new(source),
        })?;

    for record in records {
        statement
            .execute(params![
                record.file_id,
                record.normalized_path,
                record.normalized_path_lc,
                record.title_lc,
                record.content_lc
            ])
            .map_err(|source| FullIndexError::ExecuteSql {
                operation: "bulk_upsert_search_index",
                source: Box::new(source),
            })?;
    }

    Ok(())
}

fn title_from_normalized_path(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .map(std::string::ToString::to_string)
        .unwrap_or_else(|| path.to_string())
}

fn build_property_records(
    file_id: &str,
    source_path: &str,
    markdown: &str,
    absolute_path: &Path,
) -> Result<Vec<PropertyRecordInput>, FullIndexError> {
    let extraction = extract_front_matter(markdown);
    let front_matter = match extraction.status {
        FrontMatterStatus::Parsed { value } => value,
        FrontMatterStatus::Malformed { .. } | FrontMatterStatus::Missing => return Ok(Vec::new()),
    };

    let projected = project_typed_properties(&front_matter).map_err(|source| {
        FullIndexError::ProjectProperties {
            path: absolute_path.to_path_buf(),
            source: Box::new(source),
        }
    })?;

    let mut records = Vec::with_capacity(projected.len());
    for property in projected {
        let value_json =
            serde_json::to_string(&typed_value_to_json(&property.value)).map_err(|source| {
                FullIndexError::SerializePropertyJson {
                    path: source_path.to_string(),
                    source: Box::new(source),
                }
            })?;

        records.push(PropertyRecordInput {
            property_id: deterministic_id("prop", &format!("{file_id}:{}", property.key)),
            file_id: file_id.to_string(),
            key: property.key,
            value_type: typed_value_kind(&property.value).to_string(),
            value_json,
        });
    }

    Ok(records)
}

fn build_task_records(file_id: &str, source_path: &str, markdown: &str) -> Vec<TaskRecordInput> {
    markdown
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let (state, text) = parse_task_line(line)?;
            let line_number = (index + 1) as i64;
            Some(TaskRecordInput {
                task_id: deterministic_id("task", &format!("{file_id}:{line_number}")),
                file_id: file_id.to_string(),
                file_path: source_path.to_string(),
                file_path_lc: source_path.to_lowercase(),
                line_number,
                state: state.to_string(),
                text: text.to_string(),
                text_lc: text.to_lowercase(),
            })
        })
        .collect()
}

struct LinkResolutionContext<'a> {
    resolution_candidates: &'a [String],
    file_id_by_path: &'a HashMap<String, String>,
    heading_index: &'a HashMap<String, Vec<String>>,
    block_index: &'a HashMap<String, Vec<String>>,
}

fn build_incremental_link_records(
    context: &LinkResolutionContext<'_>,
    file_id: &str,
    source_path: &str,
    markdown: &str,
    parsed_body: &str,
) -> Vec<LinkRecordInput> {
    let mut records = Vec::new();
    for (index, indexed_link) in extract_index_links(markdown, parsed_body)
        .iter()
        .enumerate()
    {
        let link = &indexed_link.link;
        let resolution = resolve_target(
            &link.target,
            Some(source_path),
            context.resolution_candidates,
        );
        let mut resolved_file_id = resolution
            .resolved_path
            .as_ref()
            .and_then(|path| context.file_id_by_path.get(path).cloned());
        let mut heading_slug = link.heading.as_deref().map(slugify_heading);
        let mut block_id = link.block.clone();
        let heading_resolution = resolve_heading_target(
            link.heading.as_deref(),
            resolution.resolved_path.as_deref(),
            context.heading_index,
        );
        if let Some(resolved_heading_slug) = heading_resolution.resolved_heading_slug {
            heading_slug = Some(resolved_heading_slug);
        }
        if link.heading.is_some() && !heading_resolution.is_resolved {
            resolved_file_id = None;
        }
        let block_resolution = resolve_block_target(
            link.block.as_deref(),
            resolution.resolved_path.as_deref(),
            context.block_index,
        );
        if let Some(resolved_block_id) = block_resolution.resolved_block_id {
            block_id = Some(resolved_block_id);
        }
        if link.block.is_some() && !block_resolution.is_resolved {
            resolved_file_id = None;
        }

        let is_unresolved = resolved_file_id.is_none();
        let unresolved_reason = if is_unresolved {
            classify_unresolved_reason(
                link,
                resolution.resolved_path.as_deref(),
                heading_resolution.is_resolved,
                block_resolution.is_resolved,
            )
        } else {
            None
        };

        records.push(LinkRecordInput {
            link_id: deterministic_id(
                "link",
                &format!("{file_id}:{index}:{}:{}", indexed_link.source, link.raw),
            ),
            source_file_id: file_id.to_string(),
            raw_target: link.target.clone(),
            resolved_file_id,
            heading_slug,
            block_id,
            is_unresolved,
            unresolved_reason,
            source_field: indexed_link.kind.source_field(&indexed_link.source),
        });
    }
    records
}

fn stored_link_requires_refresh(
    link: &LinkWithPaths,
    resolution_candidates: &[String],
    file_id_by_path: &HashMap<String, String>,
    heading_index: &HashMap<String, Vec<String>>,
    block_index: &HashMap<String, Vec<String>>,
) -> bool {
    let resolution = resolve_target(
        &link.raw_target,
        Some(&link.source_path),
        resolution_candidates,
    );
    let mut resolved_file_id = resolution
        .resolved_path
        .as_ref()
        .and_then(|path| file_id_by_path.get(path).cloned());
    let mut heading_slug = link.heading_slug.clone();
    let mut block_id = link.block_id.clone();

    if heading_slug.is_some() {
        let heading_resolution = resolve_heading_target(
            heading_slug.as_deref(),
            resolution.resolved_path.as_deref(),
            heading_index,
        );
        if let Some(resolved_heading_slug) = heading_resolution.resolved_heading_slug {
            heading_slug = Some(resolved_heading_slug);
        }
        if !heading_resolution.is_resolved {
            resolved_file_id = None;
        }
    }

    if block_id.is_some() {
        let block_resolution = resolve_block_target(
            block_id.as_deref(),
            resolution.resolved_path.as_deref(),
            block_index,
        );
        if let Some(resolved_block_id) = block_resolution.resolved_block_id {
            block_id = Some(resolved_block_id);
        }
        if !block_resolution.is_resolved {
            resolved_file_id = None;
        }
    }

    let is_unresolved = resolved_file_id.is_none();
    resolved_file_id != link.resolved_file_id
        || heading_slug != link.heading_slug
        || block_id != link.block_id
        || is_unresolved != link.is_unresolved
}

fn parse_task_line(line: &str) -> Option<(&'static str, &str)> {
    let trimmed = line.trim_start();
    let (state, remainder) = if let Some(rest) = trimmed.strip_prefix("- [ ] ") {
        ("open", rest)
    } else if let Some(rest) = trimmed
        .strip_prefix("- [x] ")
        .or_else(|| trimmed.strip_prefix("- [X] "))
    {
        ("done", rest)
    } else if let Some(rest) = trimmed.strip_prefix("- [-] ") {
        ("cancelled", rest)
    } else {
        return None;
    };

    Some((state, remainder.trim()))
}

fn extract_index_links(markdown: &str, body: &str) -> Vec<IndexedWikiLink> {
    let mut links = Vec::new();

    for link in extract_wikilinks(body) {
        links.push(IndexedWikiLink {
            link,
            source: "body".to_string(),
            kind: IndexedLinkKind::Wikilink,
        });
    }

    for markdown_link in extract_markdown_links(body) {
        links.push(IndexedWikiLink {
            link: WikiLink {
                raw: markdown_link.raw_target,
                target: markdown_link.target,
                display: None,
                heading: None,
                block: None,
                has_explicit_path: true,
            },
            source: "body".to_string(),
            kind: if markdown_link.is_embed {
                IndexedLinkKind::Embed
            } else {
                IndexedLinkKind::Markdown
            },
        });
    }

    let extraction = extract_front_matter(markdown);
    if let FrontMatterStatus::Parsed { value } = extraction.status {
        collect_frontmatter_links(&value, "", &mut links);
    }

    // Deterministic dedupe across body and frontmatter paths.
    links.sort_by(|left, right| {
        (
            left.source.as_str(),
            left.kind,
            left.link.raw.as_str(),
            left.link.target.as_str(),
            left.link.heading.as_deref().unwrap_or(""),
            left.link.block.as_deref().unwrap_or(""),
        )
            .cmp(&(
                right.source.as_str(),
                right.kind,
                right.link.raw.as_str(),
                right.link.target.as_str(),
                right.link.heading.as_deref().unwrap_or(""),
                right.link.block.as_deref().unwrap_or(""),
            ))
    });
    links.dedup_by(|left, right| {
        left.source == right.source
            && left.kind == right.kind
            && left.link.raw == right.link.raw
            && left.link.target == right.link.target
            && left.link.heading == right.link.heading
            && left.link.block == right.link.block
    });

    links
}

fn collect_frontmatter_links(
    value: &serde_yaml::Value,
    path: &str,
    links: &mut Vec<IndexedWikiLink>,
) {
    match value {
        serde_yaml::Value::String(raw) => {
            for link in extract_wikilinks(raw) {
                links.push(IndexedWikiLink {
                    link,
                    source: format!("frontmatter:{path}"),
                    kind: IndexedLinkKind::Wikilink,
                });
            }
        }
        serde_yaml::Value::Sequence(items) => {
            for (index, item) in items.iter().enumerate() {
                let nested_path = if path.is_empty() {
                    format!("[{index}]")
                } else {
                    format!("{path}[{index}]")
                };
                collect_frontmatter_links(item, &nested_path, links);
            }
        }
        serde_yaml::Value::Mapping(mapping) => {
            for (key, nested) in mapping {
                let key_label = match key {
                    serde_yaml::Value::String(raw) => raw.clone(),
                    other => serde_yaml::to_string(other)
                        .unwrap_or_else(|_| "<non-string-key>".to_string())
                        .replace('\n', "")
                        .trim()
                        .to_string(),
                };
                let nested_path = if path.is_empty() {
                    key_label
                } else {
                    format!("{path}.{key_label}")
                };
                collect_frontmatter_links(nested, &nested_path, links);
            }
        }
        serde_yaml::Value::Tagged(tagged) => {
            collect_frontmatter_links(&tagged.value, path, links);
        }
        serde_yaml::Value::Null | serde_yaml::Value::Bool(_) | serde_yaml::Value::Number(_) => {}
    }
}

fn build_prepared_index_entry(
    entry: &VaultManifestEntry,
    parser: MarkdownParser,
) -> Result<PreparedIndexEntry, FullIndexError> {
    let hash_blake3 =
        hash_file_blake3(&entry.absolute).map_err(|source| FullIndexError::ReadFile {
            path: entry.absolute.clone(),
            source,
        })?;

    let file_id = deterministic_id("file", &entry.normalized);
    let file_record = FileRecordInput {
        file_id: file_id.clone(),
        normalized_path: entry.normalized.clone(),
        match_key: entry.match_key.clone(),
        absolute_path: entry.absolute.to_string_lossy().to_string(),
        size_bytes: entry.size_bytes,
        modified_unix_ms: entry.modified_unix_ms,
        hash_blake3,
        is_markdown: entry.normalized.ends_with(".md"),
    };

    if entry.normalized.ends_with(".md") {
        let markdown =
            fs::read_to_string(&entry.absolute).map_err(|source| FullIndexError::ReadFile {
                path: entry.absolute.clone(),
                source,
            })?;

        let parsed = parser
            .parse(MarkdownParseRequest {
                normalized_path: entry.normalized.clone(),
                raw: markdown.clone(),
            })
            .map_err(|source| FullIndexError::ParseMarkdown {
                path: entry.absolute.clone(),
                source: Box::new(source),
            })?;

        let property_records =
            build_property_records(&file_id, &entry.normalized, &markdown, &entry.absolute)?;
        let task_records = build_task_records(&file_id, &entry.normalized, &markdown);
        let links = extract_index_links(&markdown, &parsed.body);
        let mut heading_slugs = parsed
            .headings
            .iter()
            .map(|heading| slugify_heading(&heading.text))
            .filter(|slug| !slug.is_empty())
            .collect::<Vec<_>>();
        heading_slugs.sort();
        heading_slugs.dedup();
        let block_ids = extract_block_ids(&parsed.body);
        let search_record = SearchIndexRecordInput {
            file_id: file_id.clone(),
            normalized_path: entry.normalized.clone(),
            normalized_path_lc: entry.normalized.to_lowercase(),
            title_lc: title_from_normalized_path(&entry.normalized).to_lowercase(),
            content_lc: markdown.to_lowercase(),
        };
        let markdown_doc = MarkdownIndexDocument {
            file_id,
            source_path: entry.normalized.clone(),
            links,
            heading_slugs,
            block_ids,
            properties: property_records,
            tasks: task_records,
        };

        return Ok(PreparedIndexEntry {
            file_record,
            markdown_doc: Some(markdown_doc),
            base_record: None,
            search_record: Some(search_record),
        });
    }

    if entry.normalized.ends_with(".base") {
        let raw =
            fs::read_to_string(&entry.absolute).map_err(|source| FullIndexError::ReadFile {
                path: entry.absolute.clone(),
                source,
            })?;
        let config_json = serde_json::to_string(&json!({ "raw": raw })).map_err(|source| {
            FullIndexError::SerializeBaseConfig {
                path: entry.absolute.clone(),
                source,
            }
        })?;

        return Ok(PreparedIndexEntry {
            file_record,
            markdown_doc: None,
            base_record: Some(BaseRecordInput {
                base_id: deterministic_id("base", &entry.normalized),
                file_id,
                config_json,
            }),
            search_record: None,
        });
    }

    Ok(PreparedIndexEntry {
        file_record,
        markdown_doc: None,
        base_record: None,
        search_record: None,
    })
}

fn resolve_document_link_records(
    document: &MarkdownIndexDocument,
    resolution_candidates: &[String],
    file_id_by_path: &HashMap<String, String>,
    heading_index: &HashMap<String, Vec<String>>,
    block_index: &HashMap<String, Vec<String>>,
) -> ResolvedLinkBatch {
    let mut records = Vec::with_capacity(document.links.len());
    let mut unresolved_total = 0_u64;

    for (index, indexed_link) in document.links.iter().enumerate() {
        let link = &indexed_link.link;
        let resolution = resolve_target(
            &link.target,
            Some(&document.source_path),
            resolution_candidates,
        );

        let mut resolved_file_id = resolution
            .resolved_path
            .as_ref()
            .and_then(|path| file_id_by_path.get(path).cloned());
        let mut heading_slug = link.heading.as_deref().map(slugify_heading);
        let mut block_id = link.block.clone();
        let heading_resolution = resolve_heading_target(
            link.heading.as_deref(),
            resolution.resolved_path.as_deref(),
            heading_index,
        );
        if let Some(resolved_heading_slug) = heading_resolution.resolved_heading_slug {
            heading_slug = Some(resolved_heading_slug);
        }
        if link.heading.is_some() && !heading_resolution.is_resolved {
            resolved_file_id = None;
        }
        let block_resolution = resolve_block_target(
            link.block.as_deref(),
            resolution.resolved_path.as_deref(),
            block_index,
        );
        if let Some(resolved_block_id) = block_resolution.resolved_block_id {
            block_id = Some(resolved_block_id);
        }
        if link.block.is_some() && !block_resolution.is_resolved {
            resolved_file_id = None;
        }

        let is_unresolved = resolved_file_id.is_none();
        let unresolved_reason = if is_unresolved {
            classify_unresolved_reason(
                link,
                resolution.resolved_path.as_deref(),
                heading_resolution.is_resolved,
                block_resolution.is_resolved,
            )
        } else {
            None
        };
        if is_unresolved {
            unresolved_total += 1;
        }
        records.push(LinkRecordInput {
            link_id: deterministic_id(
                "link",
                &format!(
                    "{}:{}:{}:{}",
                    document.file_id, index, indexed_link.source, link.raw
                ),
            ),
            source_file_id: document.file_id.clone(),
            raw_target: link.target.clone(),
            resolved_file_id,
            heading_slug,
            block_id,
            is_unresolved,
            unresolved_reason,
            source_field: indexed_link.kind.source_field(&indexed_link.source),
        });
    }

    ResolvedLinkBatch {
        records,
        unresolved_total,
    }
}

fn classify_unresolved_reason(
    link: &WikiLink,
    resolved_path: Option<&str>,
    heading_is_resolved: bool,
    block_is_resolved: bool,
) -> Option<String> {
    if link.block.is_some() && !block_is_resolved {
        return Some("bad-block".to_string());
    }
    if link.heading.is_some() && !heading_is_resolved {
        return Some("bad-anchor".to_string());
    }
    if resolved_path.is_none() {
        if is_malformed_link_target(&link.target) {
            return Some("malformed-target".to_string());
        }
        return Some("missing-note".to_string());
    }
    None
}

fn is_malformed_link_target(target: &str) -> bool {
    let trimmed = target.trim();
    if trimmed.is_empty() {
        return true;
    }

    trimmed
        .chars()
        .any(|ch| !(ch.is_alphanumeric() || matches!(ch, '/' | '_' | '-' | '.' | ' ' | '(' | ')')))
}

fn build_heading_index(
    vault_root: &Path,
    candidates: &[String],
    parser: &MarkdownParser,
) -> Result<HashMap<String, Vec<String>>, FullIndexError> {
    let mut heading_index = HashMap::new();

    for normalized in candidates {
        let absolute = vault_root.join(normalized);
        let markdown = match fs::read_to_string(&absolute) {
            Ok(markdown) => markdown,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(FullIndexError::ReadFile {
                    path: absolute,
                    source,
                });
            }
        };

        let parsed = parser
            .parse(MarkdownParseRequest {
                normalized_path: normalized.clone(),
                raw: markdown,
            })
            .map_err(|source| FullIndexError::ParseMarkdown {
                path: absolute.clone(),
                source: Box::new(source),
            })?;

        let mut heading_slugs = parsed
            .headings
            .iter()
            .map(|heading| slugify_heading(&heading.text))
            .filter(|slug| !slug.is_empty())
            .collect::<Vec<_>>();
        heading_slugs.sort();
        heading_slugs.dedup();

        heading_index.insert(normalized.clone(), heading_slugs);
    }

    Ok(heading_index)
}

fn build_block_index(
    vault_root: &Path,
    candidates: &[String],
    parser: &MarkdownParser,
) -> Result<HashMap<String, Vec<String>>, FullIndexError> {
    let mut block_index = HashMap::new();

    for normalized in candidates {
        let absolute = vault_root.join(normalized);
        let markdown = match fs::read_to_string(&absolute) {
            Ok(markdown) => markdown,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(FullIndexError::ReadFile {
                    path: absolute,
                    source,
                });
            }
        };

        let parsed = parser
            .parse(MarkdownParseRequest {
                normalized_path: normalized.clone(),
                raw: markdown,
            })
            .map_err(|source| FullIndexError::ParseMarkdown {
                path: absolute.clone(),
                source: Box::new(source),
            })?;

        block_index.insert(normalized.clone(), extract_block_ids(&parsed.body));
    }

    Ok(block_index)
}

fn typed_value_kind(value: &TypedPropertyValue) -> &'static str {
    match value {
        TypedPropertyValue::Bool(_) => "bool",
        TypedPropertyValue::Number(_) => "number",
        TypedPropertyValue::Date(_) => "date",
        TypedPropertyValue::String(_) => "string",
        TypedPropertyValue::List(_) => "list",
        TypedPropertyValue::Null => "null",
    }
}

fn typed_value_to_json(value: &TypedPropertyValue) -> serde_json::Value {
    match value {
        TypedPropertyValue::Bool(value) => serde_json::Value::Bool(*value),
        TypedPropertyValue::Number(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        TypedPropertyValue::Date(value) | TypedPropertyValue::String(value) => {
            serde_json::Value::String(value.clone())
        }
        TypedPropertyValue::List(values) => {
            serde_json::Value::Array(values.iter().map(typed_value_to_json).collect())
        }
        TypedPropertyValue::Null => serde_json::Value::Null,
    }
}

fn deterministic_id(prefix: &str, input: &str) -> String {
    let hash = blake3::hash(input.as_bytes()).to_hex();
    format!("{prefix}_{}", &hash[..16])
}

fn normalize_changed_path(path: &Path) -> Result<String, FullIndexError> {
    if path.is_absolute() {
        return Err(FullIndexError::InvalidChangedPath {
            path: path.to_path_buf(),
            reason: "path must be relative".to_string(),
        });
    }

    let mut segments = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(segment) => {
                let segment =
                    segment
                        .to_str()
                        .ok_or_else(|| FullIndexError::InvalidChangedPath {
                            path: path.to_path_buf(),
                            reason: "path component is not utf-8".to_string(),
                        })?;
                segments.push(segment.to_string());
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                return Err(FullIndexError::InvalidChangedPath {
                    path: path.to_path_buf(),
                    reason: "path must not contain parent traversal".to_string(),
                });
            }
            std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                return Err(FullIndexError::InvalidChangedPath {
                    path: path.to_path_buf(),
                    reason: "unsupported path component".to_string(),
                });
            }
        }
    }

    Ok(segments.join("/"))
}

fn current_unix_ms() -> Result<u128, FullIndexError> {
    current_unix_ms_raw().map_err(|source| FullIndexError::Clock {
        source: Box::new(source),
    })
}

fn current_unix_ms_raw() -> Result<u128, std::time::SystemTimeError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}

/// Full index rebuild failures.
#[derive(Debug, Error)]
pub enum FullIndexError {
    /// Scanner initialization failed.
    #[error("failed to initialize full index scanner: {source}")]
    CreateScanner {
        /// Scanner path error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Vault scan failed.
    #[error("failed to scan vault during full index: {source}")]
    Scan {
        /// Scan error.
        #[source]
        source: Box<VaultScanError>,
    },
    /// Fingerprint service initialization failed.
    #[error("failed to initialize fingerprint service for full index: {source}")]
    CreateFingerprintService {
        /// Fingerprint service path error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Fingerprinting one file failed.
    #[error("failed to fingerprint file '{path}': {source}")]
    Fingerprint {
        /// Absolute file path.
        path: PathBuf,
        /// Fingerprint error.
        #[source]
        source: Box<FileFingerprintError>,
    },
    /// Reading file contents failed.
    #[error("failed to read file '{path}': {source}")]
    ReadFile {
        /// Absolute file path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Markdown parse failed.
    #[error("failed to parse markdown file '{path}': {source}")]
    ParseMarkdown {
        /// Absolute file path.
        path: PathBuf,
        /// Parse error.
        #[source]
        source: Box<MarkdownParseError>,
    },
    /// Typed property projection failed.
    #[error("failed to project typed properties for '{path}': {source}")]
    ProjectProperties {
        /// Absolute file path.
        path: PathBuf,
        /// Projection error.
        #[source]
        source: Box<PropertyProjectionError>,
    },
    /// Property JSON serialization failed.
    #[error("failed to serialize property json for '{path}': {source}")]
    SerializePropertyJson {
        /// Normalized path.
        path: String,
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Base config serialization failed.
    #[error("failed to serialize base config payload for '{path}': {source}")]
    SerializeBaseConfig {
        /// Absolute base path.
        path: PathBuf,
        /// JSON serialization error.
        #[source]
        source: serde_json::Error,
    },
    /// Fingerprint modified timestamp overflows storage integer type.
    #[error("fingerprint modified timestamp overflows i64: {value}")]
    TimestampOverflow {
        /// Raw timestamp value.
        value: u128,
    },
    /// Changed path input is invalid for incremental indexing.
    #[error("invalid changed path '{path}': {reason}")]
    InvalidChangedPath {
        /// Invalid changed path.
        path: PathBuf,
        /// Validation reason.
        reason: String,
    },
    /// Provided batch size is invalid.
    #[error("invalid coalesced batch size: {value}")]
    InvalidBatchSize {
        /// Invalid batch size value.
        value: usize,
    },
    /// Beginning sqlite transaction failed.
    #[error("failed to begin full index transaction: {source}")]
    BeginTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Clearing index tables failed.
    #[error("failed to clear index tables before rebuild: {source}")]
    ClearTables {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Executing incremental maintenance SQL failed.
    #[error("failed to execute sql operation '{operation}': {source}")]
    ExecuteSql {
        /// SQL operation identifier.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Upserting files table rows failed.
    #[error("failed to upsert file metadata during full index: {source}")]
    UpsertFileMetadata {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Upserting properties rows failed.
    #[error("failed to upsert properties during full index: {source}")]
    UpsertProperty {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::PropertiesRepositoryError>,
    },
    /// Upserting task rows failed.
    #[error("failed to upsert tasks during full index: {source}")]
    UpsertTask {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::TasksRepositoryError>,
    },
    /// Inserting links rows failed.
    #[error("failed to insert links during full index: {source}")]
    InsertLink {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::LinksRepositoryError>,
    },
    /// Upserting bases rows failed.
    #[error("failed to upsert bases during full index: {source}")]
    UpsertBase {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::BasesRepositoryError>,
    },
    /// Upserting search index rows failed.
    #[error("failed to upsert search index rows during indexing: {source}")]
    UpsertSearchIndex {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::SearchIndexRepositoryError>,
    },
    /// Upserting index state failed.
    #[error("failed to upsert index state during full index: {source}")]
    UpsertIndexState {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Serializing index summary state failed.
    #[error("failed to serialize index summary state: {source}")]
    SerializeStateSummary {
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Transaction commit failed.
    #[error("failed to commit full index transaction: {source}")]
    CommitTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Reading system clock failed.
    #[error("failed to read current time during full index: {source}")]
    Clock {
        /// System time conversion error.
        #[source]
        source: Box<std::time::SystemTimeError>,
    },
}

/// Stale cleanup workflow failures.
#[derive(Debug, Error)]
pub enum StaleCleanupError {
    /// Scanner initialization failed.
    #[error("failed to initialize stale cleanup scanner: {source}")]
    CreateScanner {
        /// Scanner path error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Vault scan failed.
    #[error("failed to scan vault for stale cleanup: {source}")]
    Scan {
        /// Scan error.
        #[source]
        source: Box<VaultScanError>,
    },
    /// Listing existing file rows failed.
    #[error("failed to list file rows for stale cleanup: {source}")]
    ListFiles {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Beginning sqlite transaction failed.
    #[error("failed to begin stale cleanup transaction: {source}")]
    BeginTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Deleting stale file row failed.
    #[error("failed to delete stale file row: {source}")]
    DeleteFileRow {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Serializing cleanup summary failed.
    #[error("failed to serialize stale cleanup summary: {source}")]
    SerializeSummary {
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Updating index state failed.
    #[error("failed to persist stale cleanup summary state: {source}")]
    UpsertIndexState {
        /// Repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Transaction commit failed.
    #[error("failed to commit stale cleanup transaction: {source}")]
    CommitTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Reading system clock failed.
    #[error("failed to read current time during stale cleanup: {source}")]
    Clock {
        /// System time conversion error.
        #[source]
        source: Box<std::time::SystemTimeError>,
    },
}

/// Checkpointed incremental indexing failures.
#[derive(Debug, Error)]
pub enum CheckpointedIndexError {
    /// Provided batch size is invalid.
    #[error("invalid checkpoint batch size: {value}")]
    InvalidBatchSize {
        /// Invalid batch size value.
        value: usize,
    },
    /// Provided per-run batch processing limit is invalid.
    #[error("invalid max-batches-per-run value: {value}")]
    InvalidBatchLimit {
        /// Invalid max-batches-per-run value.
        value: usize,
    },
    /// Changed path input is invalid.
    #[error("invalid changed path while creating checkpoint: {source}")]
    NormalizeChangedPath {
        /// Path normalization error.
        #[source]
        source: Box<FullIndexError>,
    },
    /// Stored checkpoint JSON payload cannot be parsed.
    #[error("failed to deserialize checkpoint state payload: {source}")]
    DeserializeCheckpoint {
        /// JSON deserialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Checkpoint JSON payload serialization failed.
    #[error("failed to serialize checkpoint state payload: {source}")]
    SerializeCheckpoint {
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Checkpoint summary serialization failed.
    #[error("failed to serialize checkpoint summary payload: {source}")]
    SerializeSummary {
        /// JSON serialization error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// Checkpoint case policy label is invalid.
    #[error("invalid checkpoint case policy '{value}'")]
    InvalidCheckpointCasePolicy {
        /// Unknown case policy label.
        value: String,
    },
    /// Reading checkpoint state row failed.
    #[error("failed to read checkpoint state row: {source}")]
    GetCheckpointState {
        /// Index state repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Persisting checkpoint state row failed.
    #[error("failed to persist checkpoint state row: {source}")]
    UpsertCheckpointState {
        /// Index state repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Deleting consumed checkpoint state row failed.
    #[error("failed to delete consumed checkpoint state row: {source}")]
    DeleteCheckpointState {
        /// Index state repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Persisting checkpoint summary state row failed.
    #[error("failed to persist checkpoint summary state row: {source}")]
    UpsertIndexState {
        /// Index state repository error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Applying incremental index batch failed.
    #[error("failed to apply incremental batch from checkpoint: {source}")]
    ApplyIncremental {
        /// Incremental indexing error.
        #[source]
        source: Box<FullIndexError>,
    },
    /// Reading system clock failed.
    #[error("failed to read current time during checkpointed indexing: {source}")]
    Clock {
        /// System time conversion error.
        #[source]
        source: Box<std::time::SystemTimeError>,
    },
}

/// Reconciliation scanner failures.
#[derive(Debug, Error)]
pub enum ReconciliationScanError {
    /// Provided batch size is invalid.
    #[error("invalid reconciliation scan batch size: {value}")]
    InvalidBatchSize {
        /// Invalid batch size value.
        value: usize,
    },
    /// Scanner initialization failed.
    #[error("failed to initialize reconciliation scanner: {source}")]
    CreateScanner {
        /// Scanner path error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Vault scan failed.
    #[error("failed to scan vault during reconciliation: {source}")]
    Scan {
        /// Scan error.
        #[source]
        source: Box<VaultScanError>,
    },
    /// Loading current indexed file rows failed.
    #[error("failed to list indexed file rows during reconciliation: {source}")]
    ListIndexedFiles {
        /// Files repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Applying incremental repair batches failed.
    #[error("failed to repair reconciliation drift via incremental batches: {source}")]
    RepairBatch {
        /// Incremental indexing error.
        #[source]
        source: Box<FullIndexError>,
    },
}

/// Index consistency checker failures.
#[derive(Debug, Error)]
pub enum IndexConsistencyError {
    /// Vault root canonicalization failed.
    #[error("failed to canonicalize vault root '{path}': {source}")]
    CanonicalizeVaultRoot {
        /// Input vault root path.
        path: PathBuf,
        /// Filesystem canonicalization error.
        #[source]
        source: std::io::Error,
    },
    /// Listing indexed file rows failed.
    #[error("failed to list indexed file rows for consistency checks: {source}")]
    ListIndexedFiles {
        /// Files repository error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// SQL query operation failed.
    #[error("consistency checker sql operation '{operation}' failed: {source}")]
    Sql {
        /// SQL operation identifier.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Reading system clock failed.
    #[error("failed to read current time during consistency check: {source}")]
    Clock {
        /// System time conversion error.
        #[source]
        source: Box<std::time::SystemTimeError>,
    },
}

/// Index self-heal workflow failures.
#[derive(Debug, Error)]
pub enum IndexSelfHealError {
    /// Running pre-repair consistency check failed.
    #[error("failed to run pre-repair consistency check: {source}")]
    CheckBefore {
        /// Consistency checker error.
        #[source]
        source: Box<IndexConsistencyError>,
    },
    /// Starting self-heal transaction failed.
    #[error("failed to begin index self-heal transaction: {source}")]
    BeginTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Executing one repair SQL operation failed.
    #[error("failed to execute self-heal sql '{operation}' for record '{record_id}': {source}")]
    ExecuteSql {
        /// SQL operation identifier.
        operation: &'static str,
        /// Record identifier targeted for repair.
        record_id: String,
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Committing self-heal transaction failed.
    #[error("failed to commit index self-heal transaction: {source}")]
    CommitTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Running post-repair consistency check failed.
    #[error("failed to run post-repair consistency check: {source}")]
    CheckAfter {
        /// Consistency checker error.
        #[source]
        source: Box<IndexConsistencyError>,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use rusqlite::Connection;
    use serde_json::Value as JsonValue;
    use tao_sdk_storage::{
        BasesRepository, FilesRepository, IndexStateRepository, LinksRepository,
        PropertiesRepository, run_migrations,
    };
    use tempfile::tempdir;

    use super::{
        CasePolicy, CheckpointedIndexService, CoalescedBatchIndexService, ConsistencyIssueKind,
        FullIndexService, IncrementalIndexService, IndexConsistencyChecker, IndexSelfHealService,
        ReconciliationScannerService, StaleCleanupService,
    };

    #[test]
    fn rebuild_populates_core_tables_for_files_links_properties_bases_and_state() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::create_dir_all(temp.path().join("views")).expect("create views dir");
        fs::create_dir_all(temp.path().join("assets")).expect("create assets dir");

        fs::write(
            temp.path().join("notes/a.md"),
            "---\nstatus: draft\n---\n# A\n[[b]]\n[[missing]]",
        )
        .expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");
        fs::write(temp.path().join("views/projects.base"), "views:\n  - table")
            .expect("write base");
        fs::write(temp.path().join("assets/logo.png"), "png").expect("write asset");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let result = FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("full rebuild");

        assert_eq!(result.indexed_files, 4);
        assert_eq!(result.markdown_files, 2);
        assert_eq!(result.links_total, 2);
        assert_eq!(result.unresolved_links, 1);
        assert_eq!(result.properties_total, 1);
        assert_eq!(result.bases_total, 1);

        let all_files = FilesRepository::list_all(&connection).expect("list files");
        assert_eq!(all_files.len(), 4);

        let source = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get source")
            .expect("source exists");
        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
            .expect("list outgoing");
        assert_eq!(outgoing.len(), 2);
        assert_eq!(outgoing.iter().filter(|row| row.is_unresolved).count(), 1);

        let properties =
            PropertiesRepository::list_for_file_with_path(&connection, &source.file_id)
                .expect("list properties");
        assert_eq!(properties.len(), 1);
        assert_eq!(properties[0].key, "status");

        let base_file = FilesRepository::get_by_normalized_path(&connection, "views/projects.base")
            .expect("get base file")
            .expect("base file exists");
        let base = BasesRepository::get_by_file_id(&connection, &base_file.file_id)
            .expect("get base row")
            .expect("base exists");
        assert!(base.config_json.contains("views"));

        assert!(
            IndexStateRepository::get_by_key(&connection, "last_index_at")
                .expect("get index state")
                .is_some()
        );
        assert!(
            IndexStateRepository::get_by_key(&connection, "last_full_index_summary")
                .expect("get summary state")
                .is_some()
        );
    }

    #[test]
    fn rebuild_produces_deterministic_link_rows_across_repeated_runs() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes/projects")).expect("create notes dir");
        fs::write(
            temp.path().join("notes/a.md"),
            "---\nrelated: [\"[[projects/b]]\", \"[[projects/c]]\"]\n---\n# A\n[[projects/b]]\n[[missing]]",
        )
        .expect("write a");
        fs::write(
            temp.path().join("notes/projects/b.md"),
            "# B\n[[../a]]\n[[c]]",
        )
        .expect("write b");
        fs::write(temp.path().join("notes/projects/c.md"), "# C").expect("write c");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let mut expected_link_ids: Option<Vec<String>> = None;
        for _ in 0..5 {
            FullIndexService::default()
                .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
                .expect("full rebuild");

            let mut statement = connection
                .prepare("SELECT link_id FROM links ORDER BY link_id ASC")
                .expect("prepare link id query");
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .expect("query link ids");
            let link_ids = rows
                .map(|row| row.expect("map link id row"))
                .collect::<Vec<_>>();
            match expected_link_ids.as_ref() {
                Some(expected) => assert_eq!(&link_ids, expected),
                None => {
                    expected_link_ids = Some(link_ids);
                }
            }
        }
    }

    #[test]
    fn heading_fragment_links_only_resolve_when_target_heading_exists() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(
            temp.path().join("notes/a.md"),
            "# A\n[[b#Project Plan]]\n[[b#Missing Heading]]",
        )
        .expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# Project Plan").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a")
            .expect("a exists");
        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
            .expect("list outgoing");
        assert_eq!(outgoing.len(), 2);

        let resolved_heading = outgoing
            .iter()
            .find(|row| row.heading_slug.as_deref() == Some("project-plan"))
            .expect("resolved heading link");
        assert!(!resolved_heading.is_unresolved);
        assert_eq!(
            resolved_heading.resolved_path.as_deref(),
            Some("notes/b.md")
        );

        let missing_heading = outgoing
            .iter()
            .find(|row| row.heading_slug.as_deref() == Some("missing-heading"))
            .expect("missing heading link");
        assert!(missing_heading.is_unresolved);
        assert_eq!(missing_heading.resolved_path, None);
        assert_eq!(
            missing_heading.unresolved_reason.as_deref(),
            Some("bad-anchor")
        );
    }

    #[test]
    fn block_fragment_links_only_resolve_when_target_block_exists() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(
            temp.path().join("notes/a.md"),
            "# A\n[[b#^block-a]]\n[[b#^missing-block]]",
        )
        .expect("write a");
        fs::write(temp.path().join("notes/b.md"), "Paragraph ^block-a").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a")
            .expect("a exists");
        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
            .expect("list outgoing");
        assert_eq!(outgoing.len(), 2);

        let resolved_block = outgoing
            .iter()
            .find(|row| row.block_id.as_deref() == Some("block-a"))
            .expect("resolved block link");
        assert!(!resolved_block.is_unresolved);
        assert_eq!(resolved_block.resolved_path.as_deref(), Some("notes/b.md"));

        let missing_block = outgoing
            .iter()
            .find(|row| row.block_id.as_deref() == Some("missing-block"))
            .expect("missing block link");
        assert!(missing_block.is_unresolved);
        assert_eq!(missing_block.resolved_path, None);
        assert_eq!(
            missing_block.unresolved_reason.as_deref(),
            Some("bad-block")
        );
    }

    #[test]
    fn unresolved_links_include_reason_codes_and_provenance() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(
            temp.path().join("notes/a.md"),
            "---\nup: \"[[frontmatter-missing]]\"\n---\n# A\n[[missing-note]]\n[[b#Missing Heading]]\n[[b#^missing-block]]\n[[bad??target]]",
        )
        .expect("write a");
        fs::write(
            temp.path().join("notes/b.md"),
            "# Known Heading\nParagraph ^known-block",
        )
        .expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a")
            .expect("a exists");
        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
            .expect("list outgoing");

        let unresolved = outgoing
            .iter()
            .filter(|row| row.is_unresolved)
            .collect::<Vec<_>>();
        assert_eq!(unresolved.len(), 5);
        assert!(unresolved.iter().any(|row| {
            row.unresolved_reason.as_deref() == Some("missing-note")
                && row.source_field == "body"
                && row.raw_target == "missing-note"
        }));
        assert!(unresolved.iter().any(|row| {
            row.unresolved_reason.as_deref() == Some("missing-note")
                && row.source_field.starts_with("frontmatter:")
                && row.raw_target == "frontmatter-missing"
        }));
        assert!(unresolved.iter().any(|row| {
            row.unresolved_reason.as_deref() == Some("bad-anchor") && row.raw_target == "b"
        }));
        assert!(unresolved.iter().any(|row| {
            row.unresolved_reason.as_deref() == Some("bad-block") && row.raw_target == "b"
        }));
        assert!(unresolved.iter().any(|row| {
            row.unresolved_reason.as_deref() == Some("malformed-target")
                && row.raw_target == "bad??target"
        }));
    }

    #[test]
    fn malformed_front_matter_documents_do_not_break_indexing() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(
            temp.path().join("notes/a.md"),
            "---\nstatus: [broken\n# A\n[[b]]",
        )
        .expect("write malformed a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let result = FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("full index should tolerate malformed front matter");
        assert_eq!(result.indexed_files, 2);
        assert_eq!(result.markdown_files, 2);

        let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a")
            .expect("a exists");
        let properties =
            PropertiesRepository::list_for_file_with_path(&connection, &source_a.file_id)
                .expect("list properties");
        assert!(properties.is_empty());

        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
            .expect("list outgoing links");
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].resolved_path.as_deref(), Some("notes/b.md"));
        assert!(!outgoing[0].is_unresolved);
    }

    #[test]
    fn frontmatter_only_wikilinks_are_indexed_for_outgoing_and_backlinks() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(
            temp.path().join("notes/a.md"),
            "---\nup: \"[[b]]\"\nchildren:\n  - \"[[c]]\"\n---\n# A\n",
        )
        .expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B\n").expect("write b");
        fs::write(temp.path().join("notes/c.md"), "# C\n").expect("write c");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        let result = FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        assert_eq!(result.links_total, 2);
        assert_eq!(result.unresolved_links, 0);

        let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a")
            .expect("a exists");
        let mut outgoing =
            LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
                .expect("list outgoing");
        outgoing.sort_by(|left, right| left.raw_target.cmp(&right.raw_target));
        assert_eq!(outgoing.len(), 2);
        assert_eq!(outgoing[0].resolved_path.as_deref(), Some("notes/b.md"));
        assert_eq!(outgoing[1].resolved_path.as_deref(), Some("notes/c.md"));
        assert!(
            outgoing
                .iter()
                .all(|row| row.source_field.starts_with("frontmatter:"))
        );

        let target_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
            .expect("get b")
            .expect("b exists");
        let backlinks_b =
            LinksRepository::list_backlinks_with_paths(&connection, &target_b.file_id)
                .expect("list b backlinks");
        assert_eq!(backlinks_b.len(), 1);
        assert_eq!(backlinks_b[0].source_path, "notes/a.md");
    }

    #[test]
    fn markdown_links_and_embeds_resolve_to_non_markdown_targets() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes/assets")).expect("create notes/assets");
        fs::write(
            temp.path().join("notes/index.md"),
            "# Index\n[Deck](assets/company%20deck.pdf#page=2)\n![Photo](assets/image.png)",
        )
        .expect("write index");
        fs::write(temp.path().join("notes/assets/company deck.pdf"), "pdf").expect("write pdf");
        fs::write(temp.path().join("notes/assets/image.png"), "png").expect("write png");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("full index");

        let source = FilesRepository::get_by_normalized_path(&connection, "notes/index.md")
            .expect("get source")
            .expect("source exists");
        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
            .expect("outgoing");
        assert_eq!(outgoing.len(), 2);
        assert!(outgoing.iter().all(|row| !row.is_unresolved));
        assert!(outgoing.iter().any(|row| {
            row.source_field == "body:markdown"
                && row.resolved_path.as_deref() == Some("notes/assets/company deck.pdf")
        }));
        assert!(outgoing.iter().any(|row| {
            row.source_field == "body:embed"
                && row.resolved_path.as_deref() == Some("notes/assets/image.png")
        }));

        let linked_pdf =
            FilesRepository::get_by_normalized_path(&connection, "notes/assets/company deck.pdf")
                .expect("get pdf")
                .expect("pdf exists");
        let backlinks_pdf =
            LinksRepository::list_backlinks_with_paths(&connection, &linked_pdf.file_id)
                .expect("pdf backlinks");
        assert_eq!(backlinks_pdf.len(), 1);
        assert_eq!(backlinks_pdf[0].source_path, "notes/index.md");
    }

    #[test]
    fn incremental_reindex_refreshes_markdown_attachment_links() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes/assets")).expect("create notes/assets");
        fs::write(
            temp.path().join("notes/index.md"),
            "# Index\n[Deck](assets/company-deck.pdf)",
        )
        .expect("write index");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let source = FilesRepository::get_by_normalized_path(&connection, "notes/index.md")
            .expect("get source")
            .expect("source exists");
        let outgoing_before =
            LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
                .expect("list outgoing before");
        assert_eq!(outgoing_before.len(), 1);
        assert!(outgoing_before[0].is_unresolved);

        fs::write(temp.path().join("notes/assets/company-deck.pdf"), "pdf").expect("write pdf");
        IncrementalIndexService::default()
            .apply_changes(
                temp.path(),
                &mut connection,
                &[PathBuf::from("notes/assets/company-deck.pdf")],
                CasePolicy::Sensitive,
            )
            .expect("reindex attachment");

        let outgoing_after =
            LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
                .expect("list outgoing after");
        assert_eq!(outgoing_after.len(), 1);
        assert!(!outgoing_after[0].is_unresolved);
        assert_eq!(
            outgoing_after[0].resolved_path.as_deref(),
            Some("notes/assets/company-deck.pdf")
        );
        assert_eq!(outgoing_after[0].source_field, "body:markdown");
    }

    #[test]
    fn incremental_reindex_updates_frontmatter_only_wikilinks() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(
            temp.path().join("notes/a.md"),
            "---\nup: \"[[b]]\"\n---\n# A\n",
        )
        .expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B\n").expect("write b");
        fs::write(temp.path().join("notes/c.md"), "# C\n").expect("write c");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        fs::write(
            temp.path().join("notes/a.md"),
            "---\nup: \"[[c]]\"\n---\n# A updated\n",
        )
        .expect("update a");

        IncrementalIndexService::default()
            .apply_changes(
                temp.path(),
                &mut connection,
                &[PathBuf::from("notes/a.md")],
                CasePolicy::Sensitive,
            )
            .expect("incremental update");

        let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a")
            .expect("a exists");
        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
            .expect("list outgoing");
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].resolved_path.as_deref(), Some("notes/c.md"));
    }

    #[test]
    fn incremental_apply_changes_reindexes_only_changed_markdown_file() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A\n[[b]]").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let before_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
            .expect("get b before")
            .expect("b exists before");

        fs::write(
            temp.path().join("notes/a.md"),
            "---\nstatus: done\n---\n# A updated\n[[b]]\n[[missing]]",
        )
        .expect("update a");

        let result = IncrementalIndexService::default()
            .apply_changes(
                temp.path(),
                &mut connection,
                &[PathBuf::from("notes/a.md")],
                CasePolicy::Sensitive,
            )
            .expect("incremental update");

        assert_eq!(result.processed_paths, 1);
        assert_eq!(result.upserted_files, 1);
        assert_eq!(result.removed_files, 0);

        let after_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
            .expect("get b after")
            .expect("b exists after");
        assert_eq!(before_b.file_id, after_b.file_id);
        assert_eq!(before_b.hash_blake3, after_b.hash_blake3);

        let source = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get source")
            .expect("source exists");
        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
            .expect("list outgoing");
        assert_eq!(outgoing.len(), 2);
        assert_eq!(outgoing.iter().filter(|row| row.is_unresolved).count(), 1);

        let properties =
            PropertiesRepository::list_for_file_with_path(&connection, &source.file_id)
                .expect("list properties");
        assert_eq!(properties.len(), 1);
        assert_eq!(properties[0].key, "status");
    }

    #[test]
    fn incremental_apply_changes_resolves_forward_links_within_same_batch() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A\n[[b]]").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B\n[[c]]").expect("write b");
        fs::write(temp.path().join("notes/c.md"), "# C").expect("write c");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        IncrementalIndexService::default()
            .apply_changes(
                temp.path(),
                &mut connection,
                &[
                    PathBuf::from("notes/a.md"),
                    PathBuf::from("notes/b.md"),
                    PathBuf::from("notes/c.md"),
                ],
                CasePolicy::Sensitive,
            )
            .expect("incremental apply on fresh db");

        let file_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a")
            .expect("a exists");
        let file_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
            .expect("get b")
            .expect("b exists");
        let outgoing_a = LinksRepository::list_outgoing_with_paths(&connection, &file_a.file_id)
            .expect("list outgoing a");
        let outgoing_b = LinksRepository::list_outgoing_with_paths(&connection, &file_b.file_id)
            .expect("list outgoing b");
        assert_eq!(outgoing_a.len(), 1);
        assert_eq!(outgoing_a[0].resolved_path.as_deref(), Some("notes/b.md"));
        assert!(!outgoing_a[0].is_unresolved);
        assert_eq!(outgoing_b.len(), 1);
        assert_eq!(outgoing_b[0].resolved_path.as_deref(), Some("notes/c.md"));
        assert!(!outgoing_b[0].is_unresolved);
    }

    #[test]
    fn incremental_apply_changes_skips_unchanged_paths_using_metadata_prefilter() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A\n[[b]]").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let before_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a before")
            .expect("a exists before");

        let result = IncrementalIndexService::default()
            .apply_changes(
                temp.path(),
                &mut connection,
                &[PathBuf::from("notes/a.md")],
                CasePolicy::Sensitive,
            )
            .expect("incremental unchanged path");

        assert_eq!(result.processed_paths, 1);
        assert_eq!(result.upserted_files, 0);
        assert_eq!(result.links_reindexed, 0);
        assert_eq!(result.properties_reindexed, 0);

        let after_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a after")
            .expect("a exists after");
        assert_eq!(before_a.hash_blake3, after_a.hash_blake3);
        assert_eq!(before_a.modified_unix_ms, after_a.modified_unix_ms);
    }

    #[test]
    fn incremental_apply_changes_removes_deleted_file_metadata() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        fs::remove_file(temp.path().join("notes/a.md")).expect("remove a");

        let result = IncrementalIndexService::default()
            .apply_changes(
                temp.path(),
                &mut connection,
                &[PathBuf::from("notes/a.md")],
                CasePolicy::Sensitive,
            )
            .expect("incremental delete");

        assert_eq!(result.processed_paths, 1);
        assert_eq!(result.upserted_files, 0);
        assert_eq!(result.removed_files, 1);
        assert!(
            FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
                .expect("get deleted file")
                .is_none()
        );
    }

    #[test]
    fn incremental_apply_changes_refreshes_links_when_target_note_is_created() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/source.md"), "# Source\n[[target]]")
            .expect("write source");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let source = FilesRepository::get_by_normalized_path(&connection, "notes/source.md")
            .expect("get source")
            .expect("source exists");
        let before = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
            .expect("list outgoing before");
        assert_eq!(before.len(), 1);
        assert!(before[0].is_unresolved);

        fs::write(temp.path().join("notes/target.md"), "# Target").expect("write target");
        IncrementalIndexService::default()
            .apply_changes(
                temp.path(),
                &mut connection,
                &[PathBuf::from("notes/target.md")],
                CasePolicy::Sensitive,
            )
            .expect("incremental create target");

        let after = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
            .expect("list outgoing after");
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].resolved_path.as_deref(), Some("notes/target.md"));
        assert!(!after[0].is_unresolved);
    }

    #[test]
    fn incremental_apply_changes_marks_backlinks_unresolved_when_target_is_deleted() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/source.md"), "# Source\n[[target]]")
            .expect("write source");
        fs::write(temp.path().join("notes/target.md"), "# Target").expect("write target");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let source = FilesRepository::get_by_normalized_path(&connection, "notes/source.md")
            .expect("get source")
            .expect("source exists");
        fs::remove_file(temp.path().join("notes/target.md")).expect("remove target");

        IncrementalIndexService::default()
            .apply_changes(
                temp.path(),
                &mut connection,
                &[PathBuf::from("notes/target.md")],
                CasePolicy::Sensitive,
            )
            .expect("incremental delete target");

        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
            .expect("list outgoing after delete");
        assert_eq!(outgoing.len(), 1);
        assert!(outgoing[0].is_unresolved);
        assert_eq!(outgoing[0].resolved_path, None);
        assert_eq!(
            outgoing[0].unresolved_reason.as_deref(),
            Some("missing-note")
        );
        assert_eq!(
            LinksRepository::count_unresolved(&connection).expect("count unresolved"),
            1
        );
    }

    #[test]
    fn incremental_apply_changes_refreshes_anchor_links_when_target_heading_changes() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(
            temp.path().join("notes/source.md"),
            "# Source\n[[target#Known Heading]]",
        )
        .expect("write source");
        fs::write(temp.path().join("notes/target.md"), "# Known Heading\nbody")
            .expect("write target");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let source = FilesRepository::get_by_normalized_path(&connection, "notes/source.md")
            .expect("get source")
            .expect("source exists");
        fs::write(
            temp.path().join("notes/target.md"),
            "# Renamed Heading\nbody",
        )
        .expect("rewrite target");

        IncrementalIndexService::default()
            .apply_changes(
                temp.path(),
                &mut connection,
                &[PathBuf::from("notes/target.md")],
                CasePolicy::Sensitive,
            )
            .expect("incremental heading change");

        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source.file_id)
            .expect("list outgoing after heading change");
        assert_eq!(outgoing.len(), 1);
        assert!(outgoing[0].is_unresolved);
        assert_eq!(outgoing[0].resolved_path, None);
        assert_eq!(outgoing[0].heading_slug.as_deref(), Some("known-heading"));
        assert_eq!(outgoing[0].unresolved_reason.as_deref(), Some("bad-anchor"));
    }

    #[test]
    fn coalesced_batch_apply_deduplicates_events_and_respects_batch_size() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        fs::write(temp.path().join("notes/a.md"), "# A changed").expect("update a");
        fs::write(temp.path().join("notes/b.md"), "# B changed").expect("update b");

        let result = CoalescedBatchIndexService::default()
            .apply_coalesced(
                temp.path(),
                &mut connection,
                &[
                    PathBuf::from("notes/a.md"),
                    PathBuf::from("notes/a.md"),
                    PathBuf::from("notes/b.md"),
                ],
                1,
                CasePolicy::Sensitive,
            )
            .expect("apply coalesced batches");

        assert_eq!(result.input_events, 3);
        assert_eq!(result.unique_paths, 2);
        assert_eq!(result.batches_applied, 2);
        assert_eq!(result.upserted_files, 2);
    }

    #[test]
    fn coalesced_batch_apply_rejects_zero_batch_size() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let error = CoalescedBatchIndexService::default()
            .apply_coalesced(
                temp.path(),
                &mut connection,
                &[PathBuf::from("notes/a.md")],
                0,
                CasePolicy::Sensitive,
            )
            .expect_err("zero batch size should fail");

        assert!(matches!(
            error,
            super::FullIndexError::InvalidBatchSize { .. }
        ));
    }

    #[test]
    fn checkpointed_apply_persists_progress_and_resume_finishes_remaining_paths() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");
        fs::write(temp.path().join("notes/c.md"), "# C").expect("write c");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let before_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
            .expect("get b before")
            .expect("b exists before");
        let before_c = FilesRepository::get_by_normalized_path(&connection, "notes/c.md")
            .expect("get c before")
            .expect("c exists before");

        fs::write(temp.path().join("notes/a.md"), "# A changed").expect("update a");
        fs::write(temp.path().join("notes/b.md"), "# B changed").expect("update b");
        fs::write(temp.path().join("notes/c.md"), "# C changed").expect("update c");

        let first = CheckpointedIndexService::default()
            .apply_checkpointed(
                temp.path(),
                &mut connection,
                &[
                    PathBuf::from("notes/a.md"),
                    PathBuf::from("notes/b.md"),
                    PathBuf::from("notes/c.md"),
                ],
                1,
                Some(1),
                CasePolicy::Sensitive,
            )
            .expect("first checkpointed run");

        assert!(!first.started_from_checkpoint);
        assert_eq!(first.total_paths, 3);
        assert_eq!(first.processed_paths, 1);
        assert_eq!(first.remaining_paths, 2);
        assert_eq!(first.batches_applied, 1);
        assert!(!first.checkpoint_completed);
        assert!(
            IndexStateRepository::get_by_key(&connection, "checkpoint.incremental_index")
                .expect("get checkpoint state")
                .is_some()
        );

        let mid_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
            .expect("get b mid")
            .expect("b exists mid");
        assert_eq!(mid_b.hash_blake3, before_b.hash_blake3);

        let resumed = CheckpointedIndexService::default()
            .apply_checkpointed(
                temp.path(),
                &mut connection,
                &[],
                8,
                None,
                CasePolicy::Insensitive,
            )
            .expect("resume checkpointed run");

        assert!(resumed.started_from_checkpoint);
        assert_eq!(resumed.total_paths, 3);
        assert_eq!(resumed.processed_paths, 2);
        assert_eq!(resumed.remaining_paths, 0);
        assert_eq!(resumed.batches_applied, 2);
        assert!(resumed.checkpoint_completed);
        assert!(
            IndexStateRepository::get_by_key(&connection, "checkpoint.incremental_index")
                .expect("get consumed checkpoint state")
                .is_none()
        );

        let after_b = FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
            .expect("get b after")
            .expect("b exists after");
        let after_c = FilesRepository::get_by_normalized_path(&connection, "notes/c.md")
            .expect("get c after")
            .expect("c exists after");
        assert_ne!(after_b.hash_blake3, before_b.hash_blake3);
        assert_ne!(after_c.hash_blake3, before_c.hash_blake3);

        let summary =
            IndexStateRepository::get_by_key(&connection, "last_checkpointed_index_summary")
                .expect("get checkpoint summary")
                .expect("checkpoint summary exists");
        let summary_json: JsonValue =
            serde_json::from_str(&summary.value_json).expect("parse checkpoint summary");
        assert_eq!(
            summary_json
                .get("checkpoint_completed")
                .and_then(JsonValue::as_bool),
            Some(true)
        );
    }

    #[test]
    fn checkpointed_apply_returns_noop_when_no_checkpoint_exists() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");

        let result = CheckpointedIndexService::default()
            .apply_checkpointed(
                temp.path(),
                &mut connection,
                &[],
                32,
                None,
                CasePolicy::Sensitive,
            )
            .expect("resume with no checkpoint");

        assert!(result.started_from_checkpoint);
        assert_eq!(result.total_paths, 0);
        assert_eq!(result.processed_paths, 0);
        assert_eq!(result.remaining_paths, 0);
        assert!(result.checkpoint_completed);
    }

    #[test]
    fn reconciliation_scanner_repairs_missed_add_update_delete_events() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A\n[[b]]").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        fs::write(temp.path().join("notes/a.md"), "# A updated\n[[c]]").expect("update a");
        fs::remove_file(temp.path().join("notes/b.md")).expect("remove b");
        fs::write(temp.path().join("notes/c.md"), "# C").expect("write c");

        let result = ReconciliationScannerService::default()
            .scan_and_repair(temp.path(), &mut connection, CasePolicy::Sensitive, 2)
            .expect("scan and repair");

        assert_eq!(result.scanned_files, 2);
        assert_eq!(result.inserted_paths, 1);
        assert_eq!(result.updated_paths, 1);
        assert_eq!(result.removed_paths, 1);
        assert_eq!(result.drift_paths, 3);

        assert!(
            FilesRepository::get_by_normalized_path(&connection, "notes/b.md")
                .expect("get removed b")
                .is_none()
        );
        let c_file = FilesRepository::get_by_normalized_path(&connection, "notes/c.md")
            .expect("get c")
            .expect("c exists");
        assert_eq!(c_file.normalized_path, "notes/c.md");

        let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a")
            .expect("a exists");
        let outgoing = LinksRepository::list_outgoing_with_paths(&connection, &source_a.file_id)
            .expect("list outgoing links");
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].raw_target, "c");
        assert_eq!(outgoing[0].resolved_path.as_deref(), Some("notes/c.md"));
        assert!(!outgoing[0].is_unresolved);
    }

    #[test]
    fn reconciliation_scanner_returns_noop_when_no_drift_detected() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let result = ReconciliationScannerService::default()
            .scan_and_repair(temp.path(), &mut connection, CasePolicy::Sensitive, 4)
            .expect("scan without drift");

        assert_eq!(result.scanned_files, 1);
        assert_eq!(result.inserted_paths, 0);
        assert_eq!(result.updated_paths, 0);
        assert_eq!(result.removed_paths, 0);
        assert_eq!(result.drift_paths, 0);
        assert_eq!(result.batches_applied, 0);
    }

    #[test]
    fn reconciliation_scanner_handles_burst_changes_consistently() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");

        for index in 0..40_u64 {
            let path = temp.path().join(format!("notes/n{index:02}.md"));
            let next = (index + 1) % 40;
            fs::write(path, format!("# Note {index}\n[[n{next:02}]]")).expect("write seed note");
        }

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        for index in 0..10_u64 {
            let path = temp.path().join(format!("notes/n{index:02}.md"));
            fs::write(path, format!("# Note {index} updated\n[[n{index:02}]]"))
                .expect("update existing note");
        }
        for index in 10..20_u64 {
            let path = temp.path().join(format!("notes/n{index:02}.md"));
            fs::remove_file(path).expect("remove existing note");
        }
        for index in 40..55_u64 {
            let path = temp.path().join(format!("notes/n{index:02}.md"));
            fs::write(path, format!("# New Note {index}\n[[n00]]")).expect("write inserted note");
        }

        let result = ReconciliationScannerService::default()
            .scan_and_repair(temp.path(), &mut connection, CasePolicy::Sensitive, 4)
            .expect("run reconciliation repair for burst changes");

        assert_eq!(result.inserted_paths, 15);
        assert_eq!(result.updated_paths, 10);
        assert_eq!(result.removed_paths, 10);
        assert_eq!(result.drift_paths, 35);
        assert_eq!(result.batches_applied, 9);

        let files = FilesRepository::list_all(&connection).expect("list reconciled files");
        assert_eq!(files.len(), 45);

        let report = IndexConsistencyChecker
            .check(temp.path(), &connection)
            .expect("run consistency checker");
        assert!(report.issues.is_empty());

        let second = ReconciliationScannerService::default()
            .scan_and_repair(temp.path(), &mut connection, CasePolicy::Sensitive, 4)
            .expect("run reconciliation after stabilization");
        assert_eq!(second.drift_paths, 0);
        assert_eq!(second.inserted_paths, 0);
        assert_eq!(second.updated_paths, 0);
        assert_eq!(second.removed_paths, 0);
    }

    #[test]
    fn consistency_checker_reports_orphans_and_broken_references() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        fs::remove_file(temp.path().join("notes/b.md")).expect("remove b from disk");

        let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a file row")
            .expect("a file exists");

        connection
            .execute_batch("PRAGMA foreign_keys = OFF;")
            .expect("disable foreign key checks for injected corruption");
        connection
            .execute(
                "INSERT INTO properties (property_id, file_id, key, value_type, value_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params!["prop_orphan_1", "file_missing_1", "status", "string", "\"draft\""],
            )
            .expect("insert orphan property");
        connection
            .execute(
                "INSERT INTO render_cache (cache_key, file_id, html, content_hash) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params!["cache_orphan_1", "file_missing_2", "<p>x</p>", "abc123"],
            )
            .expect("insert orphan render cache");
        connection
            .execute(
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved, unresolved_reason, source_field) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, NULL, ?6)",
                rusqlite::params!["link_broken_target", source_a.file_id, "missing-target", "file_missing_3", 0_i64, "body"],
            )
            .expect("insert broken target link");
        connection
            .execute(
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved, unresolved_reason, source_field) VALUES (?1, ?2, ?3, NULL, NULL, NULL, ?4, NULL, ?5)",
                rusqlite::params!["link_resolution_mismatch", source_a.file_id, "mismatch", 0_i64, "body"],
            )
            .expect("insert resolution mismatch link");

        let report = IndexConsistencyChecker
            .check(temp.path(), &connection)
            .expect("run consistency checker");

        assert!(report.checked_at_unix_ms > 0);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.kind == ConsistencyIssueKind::OrphanProperty)
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.kind == ConsistencyIssueKind::OrphanRenderCache)
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.kind == ConsistencyIssueKind::BrokenLinkTarget)
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.kind == ConsistencyIssueKind::LinkResolutionMismatch)
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.kind == ConsistencyIssueKind::MissingOnDiskFile)
        );
    }

    #[test]
    fn consistency_checker_returns_empty_report_for_healthy_index() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let report = IndexConsistencyChecker
            .check(temp.path(), &connection)
            .expect("run consistency checker");
        assert!(report.issues.is_empty());
    }

    #[test]
    fn self_heal_repairs_common_consistency_issues() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");
        fs::write(temp.path().join("notes/b.md"), "# B").expect("write b");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        fs::remove_file(temp.path().join("notes/b.md")).expect("remove b from disk");

        let source_a = FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
            .expect("get a file row")
            .expect("a file exists");

        connection
            .execute_batch("PRAGMA foreign_keys = OFF;")
            .expect("disable foreign key checks for injected corruption");
        connection
            .execute(
                "INSERT INTO properties (property_id, file_id, key, value_type, value_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params!["prop_orphan_2", "file_missing_x", "status", "string", "\"draft\""],
            )
            .expect("insert orphan property");
        connection
            .execute(
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved, unresolved_reason, source_field) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, NULL, ?6)",
                rusqlite::params!["link_broken_target_2", source_a.file_id, "missing-target", "file_missing_y", 0_i64, "body"],
            )
            .expect("insert broken target link");
        connection
            .execute(
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved, unresolved_reason, source_field) VALUES (?1, ?2, ?3, NULL, NULL, NULL, ?4, NULL, ?5)",
                rusqlite::params!["link_resolution_mismatch_2", source_a.file_id, "mismatch", 0_i64, "body"],
            )
            .expect("insert resolution mismatch link");

        let heal_result = IndexSelfHealService::default()
            .heal(temp.path(), &mut connection)
            .expect("run self-heal");

        assert!(heal_result.issues_detected > 0);
        assert!(heal_result.rows_deleted > 0);
        assert!(heal_result.rows_updated > 0);
        assert_eq!(heal_result.remaining_issues, 0);

        let report_after = IndexConsistencyChecker
            .check(temp.path(), &connection)
            .expect("run consistency checker after heal");
        assert!(report_after.issues.is_empty());
    }

    #[test]
    fn self_heal_is_noop_for_consistent_index() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write a");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let heal_result = IndexSelfHealService::default()
            .heal(temp.path(), &mut connection)
            .expect("run self-heal");

        assert_eq!(heal_result.issues_detected, 0);
        assert_eq!(heal_result.rows_deleted, 0);
        assert_eq!(heal_result.rows_updated, 0);
        assert_eq!(heal_result.remaining_issues, 0);
    }

    #[test]
    fn stale_cleanup_removes_rows_for_files_no_longer_in_vault() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::create_dir_all(temp.path().join("views")).expect("create views dir");
        fs::write(temp.path().join("notes/live.md"), "# Live").expect("write live note");
        fs::write(temp.path().join("notes/stale.md"), "# Stale").expect("write stale note");
        fs::write(temp.path().join("views/old.base"), "views:\n  - table").expect("write base");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        fs::remove_file(temp.path().join("notes/stale.md")).expect("remove stale note");
        fs::remove_file(temp.path().join("views/old.base")).expect("remove stale base");

        let result = StaleCleanupService
            .cleanup(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("run stale cleanup");

        assert_eq!(result.scanned_files, 1);
        assert_eq!(result.stale_files_removed, 2);

        let files = FilesRepository::list_all(&connection).expect("list files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].normalized_path, "notes/live.md");

        let base_rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM bases", [], |row| row.get(0))
            .expect("count bases");
        assert_eq!(base_rows, 0);

        let summary_state =
            IndexStateRepository::get_by_key(&connection, "last_stale_cleanup_summary")
                .expect("get stale cleanup summary")
                .expect("summary exists");
        let summary_json: JsonValue =
            serde_json::from_str(&summary_state.value_json).expect("parse summary json");
        assert_eq!(
            summary_json.get("mode").and_then(JsonValue::as_str),
            Some("stale_cleanup")
        );
        assert_eq!(
            summary_json
                .get("scanned_files")
                .and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            summary_json
                .get("stale_files_removed")
                .and_then(JsonValue::as_u64),
            Some(2)
        );
    }

    #[test]
    fn stale_cleanup_is_noop_when_index_and_vault_match() {
        let temp = tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join("notes")).expect("create notes dir");
        fs::write(temp.path().join("notes/a.md"), "# A").expect("write note");

        let mut connection = Connection::open_in_memory().expect("open db");
        run_migrations(&mut connection).expect("run migrations");
        FullIndexService::default()
            .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("seed full index");

        let result = StaleCleanupService
            .cleanup(temp.path(), &mut connection, CasePolicy::Sensitive)
            .expect("run stale cleanup");

        assert_eq!(result.scanned_files, 1);
        assert_eq!(result.stale_files_removed, 0);
        assert!(
            FilesRepository::get_by_normalized_path(&connection, "notes/a.md")
                .expect("get note")
                .is_some()
        );
    }
}
