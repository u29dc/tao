use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use obs_sdk_links::{WikiLink, extract_wikilinks, resolve_target};
use obs_sdk_markdown::{MarkdownParseError, MarkdownParseRequest, MarkdownParser};
use obs_sdk_properties::{
    FrontMatterStatus, PropertyProjectionError, TypedPropertyValue, extract_front_matter,
    project_typed_properties,
};
use obs_sdk_storage::{
    BaseRecordInput, BasesRepository, FileRecordInput, FilesRepository, IndexStateRecordInput,
    IndexStateRepository, LinkRecordInput, LinksRepository, PropertiesRepository,
    PropertyRecordInput,
};
use obs_sdk_vault::{
    CasePolicy, FileFingerprint, FileFingerprintError, FileFingerprintService,
    PathCanonicalizationError, VaultScanError, VaultScanService,
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
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

        let fingerprint_service = FileFingerprintService::from_root(vault_root, case_policy)
            .map_err(|source| FullIndexError::CreateFingerprintService {
                source: Box::new(source),
            })?;

        let markdown_candidates: Vec<String> = manifest
            .entries
            .iter()
            .filter(|entry| entry.normalized.ends_with(".md"))
            .map(|entry| entry.normalized.clone())
            .collect();

        let mut file_records = Vec::new();
        let mut file_id_by_path = HashMap::new();
        let mut markdown_docs = Vec::new();
        let mut base_records = Vec::new();

        for entry in &manifest.entries {
            let fingerprint =
                fingerprint_service
                    .fingerprint(&entry.relative)
                    .map_err(|source| FullIndexError::Fingerprint {
                        path: entry.absolute.clone(),
                        source: Box::new(source),
                    })?;

            let modified_unix_ms = i64::try_from(fingerprint.modified_unix_ms).map_err(|_| {
                FullIndexError::TimestampOverflow {
                    value: fingerprint.modified_unix_ms,
                }
            })?;

            let file_id = deterministic_id("file", &fingerprint.normalized);
            file_id_by_path.insert(fingerprint.normalized.clone(), file_id.clone());

            file_records.push(FileRecordInput {
                file_id: file_id.clone(),
                normalized_path: fingerprint.normalized.clone(),
                match_key: fingerprint.match_key,
                absolute_path: fingerprint.absolute.to_string_lossy().to_string(),
                size_bytes: fingerprint.size_bytes,
                modified_unix_ms,
                hash_blake3: fingerprint.hash_blake3,
                is_markdown: fingerprint.normalized.ends_with(".md"),
            });

            if fingerprint.normalized.ends_with(".md") {
                let markdown = fs::read_to_string(&entry.absolute).map_err(|source| {
                    FullIndexError::ReadFile {
                        path: entry.absolute.clone(),
                        source,
                    }
                })?;

                let parsed = self
                    .parser
                    .parse(MarkdownParseRequest {
                        normalized_path: fingerprint.normalized.clone(),
                        raw: markdown.clone(),
                    })
                    .map_err(|source| FullIndexError::ParseMarkdown {
                        path: entry.absolute.clone(),
                        source: Box::new(source),
                    })?;

                let property_records = build_property_records(
                    &file_id,
                    &fingerprint.normalized,
                    &markdown,
                    &entry.absolute,
                )?;
                let links = extract_wikilinks(&parsed.body);

                markdown_docs.push(MarkdownIndexDocument {
                    file_id,
                    source_path: fingerprint.normalized,
                    links,
                    properties: property_records,
                });
            } else if fingerprint.normalized.ends_with(".base") {
                let raw = fs::read_to_string(&entry.absolute).map_err(|source| {
                    FullIndexError::ReadFile {
                        path: entry.absolute.clone(),
                        source,
                    }
                })?;
                let config_json =
                    serde_json::to_string(&json!({ "raw": raw })).map_err(|source| {
                        FullIndexError::SerializeBaseConfig {
                            path: entry.absolute.clone(),
                            source,
                        }
                    })?;

                base_records.push(BaseRecordInput {
                    base_id: deterministic_id("base", &fingerprint.normalized),
                    file_id,
                    config_json,
                });
            }
        }

        let mut link_records = Vec::new();
        let mut unresolved_links = 0_u64;
        let mut property_records = Vec::new();

        for markdown_doc in markdown_docs {
            property_records.extend(markdown_doc.properties.clone());

            for (index, link) in markdown_doc.links.iter().enumerate() {
                let resolution = resolve_target(
                    &link.raw,
                    Some(&markdown_doc.source_path),
                    &markdown_candidates,
                );

                let resolved_file_id = resolution
                    .resolved_path
                    .and_then(|path| file_id_by_path.get(&path).cloned());
                let is_unresolved = resolved_file_id.is_none();
                if is_unresolved {
                    unresolved_links += 1;
                }

                link_records.push(LinkRecordInput {
                    link_id: deterministic_id(
                        "link",
                        &format!("{}:{}:{}", markdown_doc.file_id, index, link.raw),
                    ),
                    source_file_id: markdown_doc.file_id.clone(),
                    raw_target: link.target.clone(),
                    resolved_file_id,
                    heading_slug: link.heading.clone(),
                    block_id: link.block.clone(),
                    is_unresolved,
                });
            }
        }

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
                 DELETE FROM files;",
            )
            .map_err(|source| FullIndexError::ClearTables {
                source: Box::new(source),
            })?;

        for file in &file_records {
            FilesRepository::upsert(&transaction, file).map_err(|source| {
                FullIndexError::UpsertFileMetadata {
                    source: Box::new(source),
                }
            })?;
        }

        for property in &property_records {
            PropertiesRepository::upsert(&transaction, property).map_err(|source| {
                FullIndexError::UpsertProperty {
                    source: Box::new(source),
                }
            })?;
        }

        for link in &link_records {
            LinksRepository::insert(&transaction, link).map_err(|source| {
                FullIndexError::InsertLink {
                    source: Box::new(source),
                }
            })?;
        }

        for base in &base_records {
            BasesRepository::upsert(&transaction, base).map_err(|source| {
                FullIndexError::UpsertBase {
                    source: Box::new(source),
                }
            })?;
        }

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

        for changed_path in changed_paths {
            let normalized = normalize_changed_path(changed_path)?;
            let absolute = vault_root.join(changed_path);

            if absolute.exists() {
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

                let existing = FilesRepository::get_by_normalized_path(&transaction, &normalized)
                    .map_err(|source| FullIndexError::UpsertFileMetadata {
                    source: Box::new(source),
                })?;
                let file_id = existing
                    .map(|record| record.file_id)
                    .unwrap_or_else(|| deterministic_id("file", &normalized));

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
                transaction
                    .execute("DELETE FROM bases WHERE file_id = ?1", params![file_id])
                    .map_err(|source| FullIndexError::ExecuteSql {
                        operation: "delete_bases_for_file",
                        source: Box::new(source),
                    })?;

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

                    let candidates = FilesRepository::list_all(&transaction)
                        .map_err(|source| FullIndexError::UpsertFileMetadata {
                            source: Box::new(source),
                        })?
                        .into_iter()
                        .filter(|record| record.is_markdown)
                        .map(|record| record.normalized_path)
                        .collect::<Vec<_>>();

                    for (index, link) in extract_wikilinks(&parsed.body).iter().enumerate() {
                        let resolution = resolve_target(&link.raw, Some(&normalized), &candidates);
                        let resolved_file_id = resolution
                            .resolved_path
                            .and_then(|path| {
                                FilesRepository::get_by_normalized_path(&transaction, &path)
                                    .ok()
                                    .flatten()
                            })
                            .map(|record| record.file_id);
                        let is_unresolved = resolved_file_id.is_none();

                        LinksRepository::insert(
                            &transaction,
                            &LinkRecordInput {
                                link_id: deterministic_id(
                                    "link",
                                    &format!("{file_id}:{index}:{}", link.raw),
                                ),
                                source_file_id: file_id.clone(),
                                raw_target: link.target.clone(),
                                resolved_file_id,
                                heading_slug: link.heading.clone(),
                                block_id: link.block.clone(),
                                is_unresolved,
                            },
                        )
                        .map_err(|source| FullIndexError::InsertLink {
                            source: Box::new(source),
                        })?;
                        links_reindexed += 1;
                    }
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
            } else if let Some(existing) =
                FilesRepository::get_by_normalized_path(&transaction, &normalized).map_err(
                    |source| FullIndexError::UpsertFileMetadata {
                        source: Box::new(source),
                    },
                )?
            {
                FilesRepository::delete_by_id(&transaction, &existing.file_id).map_err(
                    |source| FullIndexError::UpsertFileMetadata {
                        source: Box::new(source),
                    },
                )?;
                removed_files += 1;
            }
        }

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

        let fingerprint_service = FileFingerprintService::from_root(vault_root, case_policy)
            .map_err(|source| ReconciliationScanError::CreateFingerprintService {
                source: Box::new(source),
            })?;

        let existing = FilesRepository::list_all(connection).map_err(|source| {
            ReconciliationScanError::ListIndexedFiles {
                source: Box::new(source),
            }
        })?;
        let mut existing_by_path = HashMap::new();
        for record in existing {
            existing_by_path.insert(record.normalized_path.clone(), record);
        }

        let mut seen_paths = std::collections::HashSet::new();
        let mut inserted_changed_paths = Vec::new();
        let mut updated_changed_paths = Vec::new();
        let mut removed_changed_paths = Vec::new();
        let mut inserted_paths = 0_u64;
        let mut updated_paths = 0_u64;

        for entry in &manifest.entries {
            let fingerprint =
                fingerprint_service
                    .fingerprint(&entry.relative)
                    .map_err(|source| ReconciliationScanError::Fingerprint {
                        path: entry.absolute.clone(),
                        source: Box::new(source),
                    })?;

            seen_paths.insert(fingerprint.normalized.clone());

            if let Some(indexed) = existing_by_path.get(&fingerprint.normalized) {
                if !indexed_record_matches_fingerprint(indexed, &fingerprint) {
                    updated_changed_paths.push(PathBuf::from(fingerprint.normalized));
                    updated_paths += 1;
                }
            } else {
                inserted_changed_paths.push(PathBuf::from(fingerprint.normalized));
                inserted_paths += 1;
            }
        }

        let mut removed_paths = 0_u64;
        for normalized_path in existing_by_path.keys() {
            if !seen_paths.contains(normalized_path) {
                removed_changed_paths.push(PathBuf::from(normalized_path));
                removed_paths += 1;
            }
        }

        let mut changed_paths = Vec::new();
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

fn indexed_record_matches_fingerprint(
    indexed: &obs_sdk_storage::FileRecord,
    fingerprint: &FileFingerprint,
) -> bool {
    let Ok(modified_unix_ms) = i64::try_from(fingerprint.modified_unix_ms) else {
        return false;
    };

    indexed.normalized_path == fingerprint.normalized
        && indexed.match_key == fingerprint.match_key
        && indexed.absolute_path == fingerprint.absolute.to_string_lossy()
        && indexed.size_bytes == fingerprint.size_bytes
        && indexed.modified_unix_ms == modified_unix_ms
        && indexed.hash_blake3 == fingerprint.hash_blake3
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
    links: Vec<WikiLink>,
    properties: Vec<PropertyRecordInput>,
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
        source: Box<obs_sdk_storage::FilesRepositoryError>,
    },
    /// Upserting properties rows failed.
    #[error("failed to upsert properties during full index: {source}")]
    UpsertProperty {
        /// Repository error.
        #[source]
        source: Box<obs_sdk_storage::PropertiesRepositoryError>,
    },
    /// Inserting links rows failed.
    #[error("failed to insert links during full index: {source}")]
    InsertLink {
        /// Repository error.
        #[source]
        source: Box<obs_sdk_storage::LinksRepositoryError>,
    },
    /// Upserting bases rows failed.
    #[error("failed to upsert bases during full index: {source}")]
    UpsertBase {
        /// Repository error.
        #[source]
        source: Box<obs_sdk_storage::BasesRepositoryError>,
    },
    /// Upserting index state failed.
    #[error("failed to upsert index state during full index: {source}")]
    UpsertIndexState {
        /// Repository error.
        #[source]
        source: Box<obs_sdk_storage::IndexStateRepositoryError>,
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
        source: Box<obs_sdk_storage::FilesRepositoryError>,
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
        source: Box<obs_sdk_storage::FilesRepositoryError>,
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
        source: Box<obs_sdk_storage::IndexStateRepositoryError>,
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
        source: Box<obs_sdk_storage::IndexStateRepositoryError>,
    },
    /// Persisting checkpoint state row failed.
    #[error("failed to persist checkpoint state row: {source}")]
    UpsertCheckpointState {
        /// Index state repository error.
        #[source]
        source: Box<obs_sdk_storage::IndexStateRepositoryError>,
    },
    /// Deleting consumed checkpoint state row failed.
    #[error("failed to delete consumed checkpoint state row: {source}")]
    DeleteCheckpointState {
        /// Index state repository error.
        #[source]
        source: Box<obs_sdk_storage::IndexStateRepositoryError>,
    },
    /// Persisting checkpoint summary state row failed.
    #[error("failed to persist checkpoint summary state row: {source}")]
    UpsertIndexState {
        /// Index state repository error.
        #[source]
        source: Box<obs_sdk_storage::IndexStateRepositoryError>,
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
    /// Fingerprint service initialization failed.
    #[error("failed to initialize fingerprint service during reconciliation: {source}")]
    CreateFingerprintService {
        /// Fingerprint service path error.
        #[source]
        source: Box<PathCanonicalizationError>,
    },
    /// Fingerprinting one file failed.
    #[error("failed to fingerprint file during reconciliation '{path}': {source}")]
    Fingerprint {
        /// Absolute file path.
        path: PathBuf,
        /// Fingerprint error.
        #[source]
        source: Box<FileFingerprintError>,
    },
    /// Loading current indexed file rows failed.
    #[error("failed to list indexed file rows during reconciliation: {source}")]
    ListIndexedFiles {
        /// Files repository error.
        #[source]
        source: Box<obs_sdk_storage::FilesRepositoryError>,
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
        source: Box<obs_sdk_storage::FilesRepositoryError>,
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use obs_sdk_storage::{
        BasesRepository, FilesRepository, IndexStateRepository, LinksRepository,
        PropertiesRepository, run_migrations,
    };
    use rusqlite::Connection;
    use serde_json::Value as JsonValue;
    use tempfile::tempdir;

    use super::{
        CasePolicy, CheckpointedIndexService, CoalescedBatchIndexService, ConsistencyIssueKind,
        FullIndexService, IncrementalIndexService, IndexConsistencyChecker,
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
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5)",
                rusqlite::params!["link_broken_target", source_a.file_id, "missing-target", "file_missing_3", 0_i64],
            )
            .expect("insert broken target link");
        connection
            .execute(
                "INSERT INTO links (link_id, source_file_id, raw_target, resolved_file_id, heading_slug, block_id, is_unresolved) VALUES (?1, ?2, ?3, NULL, NULL, NULL, ?4)",
                rusqlite::params!["link_resolution_mismatch", source_a.file_id, "mismatch", 0_i64],
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
