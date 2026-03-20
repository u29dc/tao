use super::*;

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
        let mut changed_markdown_paths = std::collections::BTreeSet::<String>::new();
        let mut has_new_files = false;
        let mut removed_file_ids = Vec::<String>::new();

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
                if existing.is_none() {
                    has_new_files = true;
                }
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
                removed_file_ids.push(existing.file_id.clone());
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

        // Newly-added files can change deterministic basename/path tie-breakers
        // for already-resolved links, so preserve correctness by falling back to
        // the full link scan in that case.
        let candidate_link_rows = if has_new_files {
            tao_sdk_storage::LinksRepository::list_all_with_paths(&transaction).map_err(
                |source| FullIndexError::InsertLink {
                    source: Box::new(source),
                },
            )?
        } else {
            // Otherwise, only fetch links that could have changed resolution
            // state due to the vault changes processed above. This avoids the
            // unconditional O(total_links) scan for common update/delete flows.
            let include_unresolved =
                !removed_file_ids.is_empty() || !changed_markdown_paths.is_empty();
            let excluded_source_paths: Vec<String> =
                changed_markdown_paths.iter().cloned().collect();
            let changed_target_paths: Vec<String> =
                changed_markdown_paths.iter().cloned().collect();
            tao_sdk_storage::LinksRepository::list_affected_by_changes_with_paths(
                &transaction,
                &excluded_source_paths,
                &changed_target_paths,
                include_unresolved,
            )
            .map_err(|source| FullIndexError::InsertLink {
                source: Box::new(source),
            })?
        };

        let affected_sources = candidate_link_rows
            .iter()
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
