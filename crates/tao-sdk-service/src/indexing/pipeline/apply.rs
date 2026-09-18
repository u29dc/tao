//! One final-inventory publication shared by full and incremental indexing.
use super::budget::{self, IndexWork};
use super::incremental::{add_link_input_corpus_file_ids, add_link_with_paths_corpus_file_ids};
use super::*;
use std::collections::BTreeSet;

fn state_error(operation: &'static str, error: impl std::fmt::Display) -> FullIndexError {
    FullIndexError::CanonicalState {
        operation,
        message: error.to_string(),
    }
}

pub(super) fn changes_for_paths(
    vault_root: &Path,
    paths: &[PathBuf],
    case_policy: CasePolicy,
) -> Result<Vec<IndexChange>, FullIndexError> {
    let manifest = VaultScanService::from_root(vault_root, case_policy)
        .map_err(|source| FullIndexError::CreateScanner {
            source: Box::new(source),
        })?
        .scan()
        .map_err(|source| FullIndexError::Scan {
            source: Box::new(source),
        })?;
    let mut entries = manifest
        .entries
        .into_iter()
        .map(|entry| (entry.normalized.clone(), entry))
        .collect::<HashMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut changes = Vec::new();
    for path in paths {
        let normalized = normalize_changed_path(path)?;
        if !seen.insert(normalized.clone()) {
            continue;
        }
        changes.push(match entries.remove(&normalized) {
            Some(entry) => IndexChange::Upsert {
                entry: Box::new(entry),
                captured: None,
            },
            None => IndexChange::Remove {
                normalized_path: normalized,
            },
        });
    }
    Ok(changes)
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct PublicationOptions {
    pub(super) force: bool,
    pub(super) force_full_corpus: bool,
    pub(super) expected_generation: Option<i64>,
}

/// Prepare source revisions before locking SQLite, then publish all dependent rows together.
pub(super) fn apply_changes(
    vault_root: &Path,
    connection: &mut Connection,
    changes: Vec<IndexChange>,
    case_policy: CasePolicy,
    parser: MarkdownParser,
    options: PublicationOptions,
) -> Result<IncrementalIndexResult, FullIndexError> {
    apply_changes_with_budget(
        vault_root,
        connection,
        changes,
        case_policy,
        parser,
        options,
        budget::MAX_PREPARATION_BYTES,
    )
}

pub(super) fn apply_changes_with_budget(
    vault_root: &Path,
    connection: &mut Connection,
    changes: Vec<IndexChange>,
    case_policy: CasePolicy,
    parser: MarkdownParser,
    options: PublicationOptions,
    preparation_limit: usize,
) -> Result<IncrementalIndexResult, FullIndexError> {
    tao_sdk_vault::check_index_cancellation()?;
    let _publication = crate::publication_lock::PublicationGuard::acquire(connection)
        .map_err(|error| state_error("coordinate_publication", error))?;
    let PublicationOptions {
        force,
        force_full_corpus,
        expected_generation,
    } = options;
    let processed_paths = changes.len() as u64;
    let fingerprints =
        FileFingerprintService::from_root(vault_root, case_policy).map_err(|source| {
            FullIndexError::CreateFingerprintService {
                source: Box::new(source),
            }
        })?;
    let prepared_generation = tao_sdk_storage::IndexGenerationRepository::get(connection)
        .map_err(|error| state_error("read_preparation_generation", error))?
        .canonical_generation;
    if let Some(expected) = expected_generation
        && expected != prepared_generation
    {
        return Err(FullIndexError::ConcurrentPublication {
            expected,
            actual: prepared_generation,
        });
    }
    let prior = FilesRepository::list_all(connection)
        .map_err(|source| FullIndexError::UpsertFileMetadata {
            source: Box::new(source),
        })?
        .into_iter()
        .map(|file| (file.normalized_path.clone(), file))
        .collect::<HashMap<_, _>>();
    let mut work = IndexWork::default();
    let change_slots = changes.capacity() * size_of::<IndexChange>();
    let mut remaining_changes = changes.iter().map(budget::change_bytes).sum::<usize>();
    let mut remaining_captures = changes
        .iter()
        .filter_map(|change| match change {
            IndexChange::Upsert { captured, .. } => {
                captured.as_ref().map(|capture| capture.bytes.capacity())
            }
            IndexChange::Remove { .. } => None,
        })
        .sum::<usize>();
    work.peak_retained_capture_bytes = remaining_captures;
    let mut retained_projections = 0_usize;
    budget::admit(
        change_slots.saturating_add(remaining_changes),
        preparation_limit,
        &mut work,
    )?;
    let mut prepared = Vec::new();
    let mut removed = BTreeSet::new();
    let mut diagnostics = Vec::new();
    let mut recovered = Vec::new();
    let mut unchanged_readable = Vec::new();
    let mut content_batch = crate::PreparedContentBatch::default();
    let spool_root = crate::content_spool_root(connection, vault_root);
    for change in changes {
        tao_sdk_vault::check_index_cancellation()?;
        let current_change_bytes = budget::change_bytes(&change);
        remaining_changes = remaining_changes.saturating_sub(current_change_bytes);
        let retained = change_slots
            .saturating_add(remaining_changes)
            .saturating_add(retained_projections);
        budget::admit(
            retained.saturating_add(current_change_bytes),
            preparation_limit,
            &mut work,
        )?;
        match change {
            IndexChange::Remove { normalized_path } => {
                retained_projections += normalized_path.capacity() + size_of::<String>() * 2;
                removed.insert(normalized_path);
            }
            IndexChange::Upsert {
                entry,
                mut captured,
            } => {
                let previous_capture_bytes = captured.as_ref().map_or(0, budget::capture_bytes);
                remaining_captures = remaining_captures.saturating_sub(
                    captured
                        .as_ref()
                        .map_or(0, |capture| capture.bytes.capacity()),
                );
                let entry_charge = current_change_bytes.saturating_sub(previous_capture_bytes);
                let mut asset_captures = HashMap::new();
                let existing = prior.get(&entry.normalized);
                let content_needs_refresh = match existing {
                    Some(file) => crate::ContentIndexService
                        .needs_refresh(connection, &file.file_id, &entry.normalized)
                        .map_err(|error| state_error("check_content_identity", error))?,
                    None => false,
                };
                let kind = file_kind(&entry.relative);
                if captured.is_none() && matches!(kind, FileKind::Markdown | FileKind::Base) {
                    captured = fingerprints.capture(&entry.absolute).ok();
                }
                if !force {
                    if matches!(
                        kind,
                        FileKind::Markdown | FileKind::Base | FileKind::PlainText | FileKind::Pdf
                    ) && captured.is_none()
                    {
                        let limit = if kind == FileKind::Pdf {
                            64 * 1024 * 1024
                        } else {
                            32 * 1024 * 1024
                        };
                        if entry.size_bytes <= limit {
                            captured = fingerprints.capture_with_limit(&entry.absolute, limit).ok();
                        }
                    }
                    tao_sdk_vault::check_index_cancellation()?;
                    let mut candidate = inventory_record(&entry);
                    if let Some(capture) = captured.as_ref() {
                        candidate
                            .hash_blake3
                            .clone_from(&capture.fingerprint.hash_blake3);
                        candidate.size_bytes = capture.fingerprint.size_bytes;
                        candidate.modified_unix_ms =
                            i64::try_from(capture.fingerprint.modified_unix_ms).map_err(|_| {
                                FullIndexError::TimestampOverflow {
                                    value: capture.fingerprint.modified_unix_ms,
                                }
                            })?;
                    }
                    if !content_needs_refresh
                        && existing.is_some_and(|file| file_record_unchanged(file, &candidate))
                        && (!matches!(kind, FileKind::Markdown | FileKind::Base)
                            || captured.is_some())
                    {
                        if let Some(capture) = &captured {
                            work.source_captures += 1;
                            work.source_bytes_captured += capture.bytes.len() as u64;
                            work.peak_retained_capture_bytes = work
                                .peak_retained_capture_bytes
                                .max(remaining_captures + capture.bytes.capacity());
                        }
                        retained_projections +=
                            entry.normalized.capacity() + size_of::<String>() * 2;
                        unchanged_readable.push(entry.normalized.clone());
                        budget::admit(
                            retained
                                + entry_charge
                                + captured.as_ref().map_or(0, budget::capture_bytes),
                            preparation_limit,
                            &mut work,
                        )?;
                        continue;
                    }
                }
                let counted_capture = captured.is_some();
                if let Some(capture) = &captured {
                    work.source_captures += 1;
                    work.source_bytes_captured += capture.bytes.len() as u64;
                    work.peak_retained_capture_bytes = work
                        .peak_retained_capture_bytes
                        .max(remaining_captures + capture.bytes.capacity());
                }
                budget::admit(
                    retained + entry_charge + captured.as_ref().map_or(0, budget::capture_bytes),
                    preparation_limit,
                    &mut work,
                )?;
                if matches!(kind, FileKind::PlainText | FileKind::Pdf)
                    && let Some(capture) = captured.take()
                {
                    asset_captures.insert(entry.normalized.clone(), capture);
                }

                match build_prepared_index_entry(&entry, parser, &fingerprints, captured) {
                    Ok(mut value) => {
                        work.markdown_parses += u64::from(value.markdown_doc.is_some());
                        if !counted_capture && matches!(kind, FileKind::Markdown | FileKind::Base) {
                            work.source_captures += 1;
                            work.source_bytes_captured += value.file_record.size_bytes;
                        }
                        if let Some(capture) = asset_captures.get(&entry.normalized) {
                            value
                                .file_record
                                .hash_blake3
                                .clone_from(&capture.fingerprint.hash_blake3);
                            value.file_record.size_bytes = capture.fingerprint.size_bytes;
                            value.file_record.modified_unix_ms = i64::try_from(
                                capture.fingerprint.modified_unix_ms,
                            )
                            .map_err(|_| FullIndexError::TimestampOverflow {
                                value: capture.fingerprint.modified_unix_ms,
                            })?;
                        }
                        retained_projections +=
                            entry.normalized.capacity() + size_of::<String>() * 2;
                        recovered.push(entry.normalized.clone());
                        if let Some(existing) = existing {
                            replace_prepared_id(&mut value, &existing.file_id);
                            if !force
                                && !content_needs_refresh
                                && file_record_unchanged(existing, &value.file_record)
                            {
                                continue;
                            }
                        }
                        if let Some(diagnostic) = value.diagnostic.as_ref() {
                            retained_projections += budget::diagnostic_bytes(diagnostic);
                            diagnostics.push(diagnostic.clone());
                        }
                        let asset_bytes = asset_captures
                            .values()
                            .map(budget::capture_bytes)
                            .sum::<usize>();
                        budget::admit(
                            change_slots
                                + remaining_changes
                                + retained_projections
                                + entry_charge
                                + budget::prepared_bytes(&value)
                                + asset_bytes,
                            preparation_limit,
                            &mut work,
                        )?;
                        let batch = crate::ContentIndexService
                            .prepare_with_captures(
                                connection,
                                vault_root,
                                &spool_root,
                                std::slice::from_ref(&value.file_record),
                                &asset_captures,
                            )
                            .map_err(|error| state_error("prepare_asset_content", error))?;
                        if let Some(revision) = batch.revision_for(&value.file_record.file_id) {
                            value.file_record.hash_blake3 = revision.to_string();
                            if !counted_capture
                                && matches!(kind, FileKind::PlainText | FileKind::Pdf)
                            {
                                work.source_captures += 1;
                                work.source_bytes_captured += batch
                                    .metadata_for(&value.file_record.file_id)
                                    .map_or(value.file_record.size_bytes, |(size, _)| size);
                            }
                        }
                        if let Some((size, modified)) =
                            batch.metadata_for(&value.file_record.file_id)
                        {
                            value.file_record.size_bytes = size;
                            value.file_record.modified_unix_ms = modified;
                        }
                        // Two times the new batch charge covers destination Vec growth
                        // without rescanning all previous TXT segments on every source.
                        retained_projections = retained_projections
                            .saturating_add(budget::prepared_bytes(&value))
                            .saturating_add(batch.retained_bytes().saturating_mul(2));
                        budget::admit(
                            change_slots
                                + remaining_changes
                                + retained_projections
                                + entry_charge
                                + asset_bytes,
                            preparation_limit,
                            &mut work,
                        )?;
                        content_batch.extend(batch);
                        prepared.push(value);
                    }
                    Err(error) => {
                        // A failed read is not a deletion. Preserve a known-good revision;
                        // new paths still participate in inventory with an explicit failure.
                        let file_id = existing.map_or_else(
                            || deterministic_id("file", &entry.normalized),
                            |file| file.file_id.clone(),
                        );
                        let diagnostic = FileDiagnosticInput {
                            path: entry.normalized.clone(),
                            file_id: Some(file_id),
                            kind: "source_capture_failed".to_string(),
                            message: error.to_string(),
                        };
                        retained_projections += budget::diagnostic_bytes(&diagnostic);
                        diagnostics.push(diagnostic);
                        if existing.is_none() {
                            let value = PreparedIndexEntry {
                                file_record: inventory_record(&entry),
                                markdown_doc: None,
                                base_record: None,
                                document_record: None,
                                diagnostic: None,
                            };
                            retained_projections += budget::prepared_bytes(&value);
                            prepared.push(value);
                        }
                    }
                }
            }
        }
        budget::admit(
            change_slots + remaining_changes + retained_projections,
            preparation_limit,
            &mut work,
        )?;
    }

    tao_sdk_vault::check_index_cancellation()?;
    let transaction = connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(|source| FullIndexError::BeginTransaction {
            source: Box::new(source),
        })?;
    let current_generation = tao_sdk_storage::IndexGenerationRepository::get(&transaction)
        .map_err(|error| state_error("read_publication_generation", error))?
        .canonical_generation;
    if current_generation != prepared_generation {
        return Err(FullIndexError::ConcurrentPublication {
            expected: prepared_generation,
            actual: current_generation,
        });
    }
    let before_status = crate::SearchCorpusService
        .status(&transaction)
        .map_err(|source| FullIndexError::RebuildSearchCorpus {
            source: Box::new(source),
        })?;
    let mut corpus_ids = BTreeSet::<String>::new();
    let mut changed_paths = BTreeSet::<String>::new();
    let mut source_paths = BTreeSet::<String>::new();
    let mut inventory_changed = false;
    let mut removed_files = 0;
    let mut properties_reindexed = 0;
    let mut bases_reindexed = 0;
    let prior_aliases = load_aliases(&transaction)?;
    let existing_links = tao_sdk_storage::LinksRepository::list_all_with_paths(&transaction)
        .map_err(|source| FullIndexError::InsertLink {
            source: Box::new(source),
        })?;
    for path in &removed {
        changed_paths.insert(path.clone());
        if let Some(file) = prior.get(path) {
            corpus_ids.insert(file.file_id.clone());
            FilesRepository::delete_by_id(&transaction, &file.file_id).map_err(|source| {
                FullIndexError::UpsertFileMetadata {
                    source: Box::new(source),
                }
            })?;
            inventory_changed = true;
            removed_files += 1;
        }
        DiagnosticsRepository::clear_for_path(&transaction, path)
            .map_err(|source| state_error("clear_removed_diagnostic", source))?;
    }
    // Reserve every final inventory ID before creating any edge, including attachments.
    for entry in &prepared {
        let file = &entry.file_record;
        inventory_changed |= !prior.contains_key(&file.normalized_path);
        changed_paths.insert(file.normalized_path.clone());
        corpus_ids.insert(file.file_id.clone());
        FilesRepository::upsert(&transaction, file).map_err(|source| {
            FullIndexError::UpsertFileMetadata {
                source: Box::new(source),
            }
        })?;
    }
    for path in unchanged_readable {
        transaction
            .execute(
                "DELETE FROM file_diagnostics WHERE path=?1 AND kind='source_capture_failed'",
                [&path],
            )
            .map_err(|source| state_error("clear_recovered_capture_diagnostic", source))?;
    }
    for path in recovered {
        DiagnosticsRepository::clear_for_path(&transaction, &path)
            .map_err(|source| state_error("clear_recovered_diagnostic", source))?;
    }
    for diagnostic in &diagnostics {
        DiagnosticsRepository::upsert(&transaction, diagnostic)
            .map_err(|source| state_error("record_source_diagnostic", source))?;
    }
    for entry in &prepared {
        let file_id = &entry.file_record.file_id;
        if entry.document_record.is_none() && entry.base_record.is_none() {
            continue;
        }
        transaction
            .execute("DELETE FROM properties WHERE file_id=?1", [file_id])
            .map_err(|source| state_error("clear_properties", source))?;
        TasksRepository::delete_by_file_id(&transaction, file_id).map_err(|source| {
            FullIndexError::UpsertTask {
                source: Box::new(source),
            }
        })?;
        transaction
            .execute("DELETE FROM bases WHERE file_id=?1", [file_id])
            .map_err(|source| state_error("clear_base", source))?;
        if let Some(document) = &entry.document_record {
            DocumentsRepository::upsert(&transaction, document)
                .map_err(|source| state_error("persist_source_revision", source))?;
        }
        if let Some(document) = &entry.markdown_doc {
            source_paths.insert(document.source_path.clone());
            upsert_properties_batch(&transaction, &document.properties)?;
            upsert_tasks_batch(&transaction, &document.tasks)?;
            properties_reindexed += document.properties.len() as u64;
        }
        if let Some(base) = &entry.base_record {
            BasesRepository::upsert(&transaction, base).map_err(|source| {
                FullIndexError::UpsertBase {
                    source: Box::new(source),
                }
            })?;
            bases_reindexed += 1;
        }
    }
    let files = FilesRepository::list_all(&transaction).map_err(|source| {
        FullIndexError::UpsertFileMetadata {
            source: Box::new(source),
        }
    })?;
    let file_id_by_path = files
        .iter()
        .map(|file| (file.normalized_path.clone(), file.file_id.clone()))
        .collect::<HashMap<_, _>>();
    let path_by_id = files
        .iter()
        .map(|file| (file.file_id.clone(), file.normalized_path.clone()))
        .collect::<HashMap<_, _>>();
    let candidates = files
        .iter()
        .map(|file| file.normalized_path.clone())
        .collect::<Vec<_>>();
    let aliases = load_aliases(&transaction)?;
    let resolution_index =
        LinkResolutionIndex::with_case_policy(&candidates, link_case_policy(case_policy))
            .with_aliases(&aliases);
    let mut models = HashMap::new();
    let mut heading_index = HashMap::new();
    let mut block_index = HashMap::new();
    for record in DocumentsRepository::list_structures(&transaction)
        .map_err(|source| state_error("load_canonical_structures", source))?
    {
        work.canonical_structures_loaded += 1;
        let Some(path) = path_by_id.get(&record.file_id) else {
            continue;
        };
        let structure = match serde_json::from_str::<CanonicalStructure>(&record.structure_json) {
            Ok(structure) if structure.version == CANONICAL_STRUCTURE_VERSION => structure,
            _ => {
                let mut stored = DocumentsRepository::get_by_file_id(&transaction, &record.file_id)
                    .map_err(|source| state_error("load_canonical_revision", source))?
                    .ok_or_else(|| {
                        state_error("load_canonical_revision", "missing source revision")
                    })?;
                if blake3::hash(stored.raw_text.as_bytes()).to_hex().as_str() != stored.source_hash
                {
                    return Err(state_error(
                        "verify_canonical_revision",
                        "stored source hash mismatch; explicit reindex required",
                    ));
                }
                work.canonical_structures_reparsed += 1;
                let parsed = MarkdownParser
                    .parse(MarkdownParseRequest {
                        normalized_path: path.clone(),
                        raw: stored.raw_text.clone(),
                    })
                    .map_err(|source| state_error("repair_canonical_structure", source))?;
                let mut headings = parsed
                    .headings
                    .iter()
                    .map(|heading| slugify_heading(&heading.text))
                    .collect::<Vec<_>>();
                headings.sort();
                headings.dedup();
                let structure = CanonicalStructure {
                    version: CANONICAL_STRUCTURE_VERSION,
                    links: extract_index_links(&parsed),
                    heading_slugs: headings,
                    block_ids: parsed.block_ids,
                };
                stored.structure_json = serde_json::to_string(&structure)
                    .map_err(|source| state_error("encode_canonical_structure", source))?;
                stored.parser_version = CANONICAL_STRUCTURE_VERSION;
                DocumentsRepository::upsert(&transaction, &stored)
                    .map_err(|source| state_error("repair_canonical_structure", source))?;
                source_paths.insert(path.clone());
                structure
            }
        };
        heading_index.insert(path.clone(), structure.heading_slugs.clone());
        block_index.insert(path.clone(), structure.block_ids.clone());
        models.insert(
            path.clone(),
            MarkdownIndexDocument {
                file_id: record.file_id,
                source_path: path.clone(),
                links: structure.links,
                properties: Vec::new(),
                tasks: Vec::new(),
            },
        );
    }
    // New candidates may change basename tie-breakers. Other edits only invalidate
    // direct dependents and unresolved references; persisted structure avoids rereads.
    for link in &existing_links {
        if inventory_changed
            || (!changed_paths.is_empty()
                && (link.is_unresolved
                    || aliases
                        .iter()
                        .chain(prior_aliases.iter())
                        .any(|(alias, _)| {
                            tao_sdk_vault::path_match_key(alias, case_policy)
                                == tao_sdk_vault::path_match_key(&link.raw_target, case_policy)
                        })))
            || link
                .resolved_path
                .as_ref()
                .is_some_and(|path| changed_paths.contains(path))
        {
            source_paths.insert(link.source_path.clone());
        }
        if changed_paths.contains(&link.source_path)
            || link
                .resolved_path
                .as_ref()
                .is_some_and(|path| changed_paths.contains(path))
        {
            add_link_with_paths_corpus_file_ids(&mut corpus_ids, link);
        }
    }
    let mut links_by_source = HashMap::<&str, Vec<&LinkWithPaths>>::new();
    for link in &existing_links {
        links_by_source
            .entry(&link.source_path)
            .or_default()
            .push(link);
    }
    let mut links_reindexed = 0;
    for path in source_paths {
        let Some(document) = models.get(&path) else {
            continue;
        };
        work.graph_sources_resolved += 1;
        let resolved = resolve_document_link_records(
            document,
            &resolution_index,
            &file_id_by_path,
            &heading_index,
            &block_index,
        );
        if let Some(old_links) = links_by_source.get(path.as_str()) {
            for link in old_links {
                add_link_with_paths_corpus_file_ids(&mut corpus_ids, link);
            }
        }
        add_link_input_corpus_file_ids(&mut corpus_ids, &resolved.records);
        transaction
            .execute(
                "DELETE FROM links WHERE source_file_id=?1",
                [&document.file_id],
            )
            .map_err(|source| state_error("replace_source_links", source))?;
        insert_links_batch(&transaction, &resolved.records)?;
        for evidence in &resolved.evidence {
            LinkEvidenceRepository::upsert(&transaction, evidence)
                .map_err(|source| state_error("persist_link_evidence", source))?;
        }
        links_reindexed += resolved.records.len() as u64;
    }
    crate::ContentIndexService
        .publish(&transaction, &content_batch)
        .map_err(|source| state_error("publish_asset_content", source))?;
    restore_pdf_page_statuses(&transaction, &files)?;
    corpus_ids.extend(
        tao_sdk_storage::IndexGenerationRepository::dirty_files(&transaction)
            .map_err(|error| state_error("load_publication_dependencies", error))?,
    );
    let generations = tao_sdk_storage::IndexGenerationRepository::get(&transaction)
        .map_err(|error| state_error("read_derived_generation", error))?;
    let canonical_changed = generations.canonical_generation != generations.search_generation;
    let corpus_ids = corpus_ids.into_iter().collect::<Vec<_>>();
    let full_corpus = force_full_corpus
        || before_status.search_index_stale
        || inventory_changed
        || bases_reindexed > 0;
    let refresh = if full_corpus {
        crate::SearchCorpusService
            .rebuild_in_transaction(&transaction, case_policy)
            .map_err(|source| FullIndexError::RebuildSearchCorpus {
                source: Box::new(source),
            })?;
        SearchCorpusRefreshMode::Full
    } else if !corpus_ids.is_empty() || canonical_changed {
        crate::SearchCorpusService
            .refresh_files_in_transaction(&transaction, &corpus_ids, case_policy)
            .map_err(|source| FullIndexError::RebuildSearchCorpus {
                source: Box::new(source),
            })?;
        SearchCorpusRefreshMode::Partial
    } else {
        SearchCorpusRefreshMode::None
    };
    work.publication_transactions = 1;
    let now = current_unix_ms()?;
    for (key, value) in [
        ("last_index_at", now.to_string()),
        (LINK_RESOLUTION_VERSION_STATE_KEY, CURRENT_LINK_RESOLUTION_VERSION.to_string()),
        ("fingerprint_policy_version",tao_sdk_vault::FINGERPRINT_POLICY_VERSION.to_string()),
        ("index_case_policy", match case_policy{CasePolicy::Sensitive=>"sensitive",CasePolicy::Insensitive=>"insensitive"}.to_string()),
        ("last_incremental_index_summary", json!({"mode":"incremental","processed_paths":processed_paths,
            "upserted_files":prepared.len(),"removed_files":removed_files,"links_reindexed":links_reindexed,
            "properties_reindexed":properties_reindexed,"bases_reindexed":bases_reindexed,
            "source_errors":diagnostics.len(),"search_corpus_refresh":refresh.as_str(),"completed_unix_ms":now,"work":work}).to_string()),
    ] {
        IndexStateRepository::upsert(&transaction, &IndexStateRecordInput { key:key.to_string(), value_json:value })
            .map_err(|source| FullIndexError::UpsertIndexState { source: Box::new(source) })?;
    }
    tao_sdk_vault::check_index_cancellation()?;
    transaction
        .commit()
        .map_err(|source| FullIndexError::CommitTransaction {
            source: Box::new(source),
        })?;
    Ok(IncrementalIndexResult {
        processed_paths,
        upserted_files: prepared.len() as u64,
        removed_files,
        links_reindexed,
        properties_reindexed,
        bases_reindexed,
        search_corpus_refresh: refresh,
        search_corpus_refresh_file_ids: corpus_ids,
        requires_full_search_corpus_refresh: false,
    })
}

fn file_record_unchanged(old: &tao_sdk_storage::FileRecord, new: &FileRecordInput) -> bool {
    old.normalized_path == new.normalized_path
        && old.match_key == new.match_key
        && old.absolute_path == new.absolute_path
        && old.size_bytes == new.size_bytes
        && old.modified_unix_ms == new.modified_unix_ms
        && old.hash_blake3 == new.hash_blake3
        && old.is_markdown == new.is_markdown
}

fn replace_prepared_id(entry: &mut PreparedIndexEntry, file_id: &str) {
    if entry.file_record.file_id == file_id {
        return;
    }
    entry.file_record.file_id = file_id.to_string();
    if let Some(diagnostic) = entry.diagnostic.as_mut() {
        diagnostic.file_id = Some(file_id.to_string());
    }
    if let Some(record) = entry.document_record.as_mut() {
        record.file_id = file_id.to_string();
    }
    if let Some(base) = entry.base_record.as_mut() {
        base.file_id = file_id.to_string();
    }
    if let Some(document) = entry.markdown_doc.as_mut() {
        document.file_id = file_id.to_string();
        for property in &mut document.properties {
            property.file_id = file_id.to_string();
            property.property_id = deterministic_id("prop", &format!("{file_id}:{}", property.key));
        }
        for task in &mut document.tasks {
            task.file_id = file_id.to_string();
            task.task_id = deterministic_id("task", &format!("{file_id}:{}", task.line_number));
        }
    }
}

fn load_aliases(connection: &Connection) -> Result<Vec<(String, String)>, FullIndexError> {
    let mut statement=connection.prepare("SELECT f.normalized_path,p.value_json FROM properties p JOIN files f ON f.file_id=p.file_id WHERE lower(p.key) IN ('alias','aliases') ORDER BY f.normalized_path,p.key")
        .map_err(|error|state_error("load_link_aliases",error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| state_error("load_link_aliases", error))?;
    let mut aliases = Vec::new();
    for row in rows {
        let (path, json) = row.map_err(|error| state_error("read_link_alias", error))?;
        let value: serde_json::Value =
            serde_json::from_str(&json).map_err(|error| state_error("decode_link_alias", error))?;
        match value {
            serde_json::Value::String(alias) => aliases.push((alias, path)),
            serde_json::Value::Array(values) => {
                for value in values {
                    if let Some(alias) = value.as_str() {
                        aliases.push((alias.to_string(), path.clone()));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(aliases)
}

/// Repair graph projections from committed source revisions within the caller's
/// transaction; self-healing never has to reopen vault content.
pub(super) fn repair_canonical_links(
    connection: &Connection,
    case_policy: CasePolicy,
) -> Result<(), FullIndexError> {
    let files = FilesRepository::list_all(connection).map_err(|source| {
        FullIndexError::UpsertFileMetadata {
            source: Box::new(source),
        }
    })?;
    let ids = files
        .iter()
        .map(|file| (file.normalized_path.clone(), file.file_id.clone()))
        .collect::<HashMap<_, _>>();
    let paths = files
        .iter()
        .map(|file| (file.file_id.clone(), file.normalized_path.clone()))
        .collect::<HashMap<_, _>>();
    let candidates = files
        .iter()
        .map(|file| file.normalized_path.clone())
        .collect::<Vec<_>>();
    let index = LinkResolutionIndex::with_case_policy(&candidates, link_case_policy(case_policy))
        .with_aliases(&load_aliases(connection)?);
    let mut headings = HashMap::new();
    let mut blocks = HashMap::new();
    let mut documents = Vec::new();
    for record in DocumentsRepository::list_structures(connection)
        .map_err(|error| state_error("load_repair_structures", error))?
    {
        let Some(path) = paths.get(&record.file_id) else {
            continue;
        };
        let structure = serde_json::from_str::<CanonicalStructure>(&record.structure_json)
            .map_err(|error| state_error("decode_repair_structure", error))?;
        headings.insert(path.clone(), structure.heading_slugs);
        blocks.insert(path.clone(), structure.block_ids);
        documents.push(MarkdownIndexDocument {
            file_id: record.file_id,
            source_path: path.clone(),
            links: structure.links,
            properties: Vec::new(),
            tasks: Vec::new(),
        });
    }
    for document in documents {
        let resolved = resolve_document_link_records(&document, &index, &ids, &headings, &blocks);
        connection
            .execute(
                "DELETE FROM links WHERE source_file_id=?1",
                [&document.file_id],
            )
            .map_err(|error| state_error("clear_repaired_links", error))?;
        insert_links_batch(connection, &resolved.records)?;
        for evidence in resolved.evidence {
            LinkEvidenceRepository::upsert(connection, &evidence)
                .map_err(|error| state_error("publish_repaired_link", error))?;
        }
    }
    restore_pdf_page_statuses(connection, &files)?;
    Ok(())
}

fn restore_pdf_page_statuses(
    connection: &Connection,
    files: &[tao_sdk_storage::FileRecord],
) -> Result<(), FullIndexError> {
    for file in files
        .iter()
        .filter(|file| file_kind(Path::new(&file.normalized_path)) == FileKind::Pdf)
    {
        let content = tao_sdk_storage::ContentRepository::get(connection, &file.file_id)
            .map_err(|error| state_error("load_pdf_page_metadata", error))?;
        let count = content
            .filter(|doc| {
                doc.served_revision.as_deref() == Some(doc.desired_revision.as_str())
                    && doc.served_extractor_identity.as_deref()
                        == Some(doc.extractor_identity.as_str())
            })
            .and_then(|doc| serde_json::from_str::<serde_json::Value>(&doc.metadata_json).ok())
            .and_then(|value| value.get("page_count").and_then(serde_json::Value::as_u64))
            .and_then(|count| u32::try_from(count).ok());
        crate::revalidate_pdf_page_links(connection, &file.file_id, count)
            .map_err(|error| state_error("validate_pdf_page_links", error))?;
    }
    Ok(())
}
