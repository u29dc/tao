//! Derived unified search corpus built from canonical index tables.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use rusqlite::{Connection, Transaction};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tao_sdk_bases::{BaseCoercionMode, BaseTableQueryPlanner, BaseViewRegistry};
use tao_sdk_core::note_title_from_path;
use tao_sdk_storage::{
    BasesRepository, ContentRepository, DiagnosticsRepository, DocumentsRepository,
    FileDiagnosticInput, FileRecord, FilesRepository, IndexGenerationRepository,
    IndexStateRecordInput, IndexStateRepository, LinkWithPaths, LinksRepository,
    PropertiesRepository, PropertyWithPath, SearchAliasInput, SearchAliasRepository,
    SearchSegmentInput, SearchSegmentRepository, TaskWithPath, TasksRepository,
};
use tao_sdk_vault::CasePolicy;
use thiserror::Error;

use crate::{BaseTableExecutionContext, BaseTableExecutionOptions, BaseTableExecutorService};

/// Current derived search corpus schema/build version.
pub const SEARCH_CORPUS_SCHEMA_VERSION: u32 = 3;
/// Index state key for derived search corpus schema version.
pub const SEARCH_CORPUS_SCHEMA_VERSION_STATE_KEY: &str = "search_corpus_schema_version";
/// Index state key for the canonical-table fingerprint used to build the corpus.
pub const SEARCH_CORPUS_SOURCE_FINGERPRINT_STATE_KEY: &str = "search_corpus_source_fingerprint";
/// Index state key for derived search corpus build completion time.
pub const SEARCH_CORPUS_BUILT_AT_STATE_KEY: &str = "search_corpus_built_at";

const DOC_SEGMENT_WEIGHT: i64 = 80;
const FILE_SEGMENT_WEIGHT: i64 = 50;
const PROPERTY_SEGMENT_WEIGHT: i64 = 45;
const TASK_SEGMENT_WEIGHT: i64 = 25;
const GRAPH_SEGMENT_WEIGHT: i64 = 30;
const BASE_SEGMENT_WEIGHT: i64 = 55;
const BASE_ROW_PAGE_SIZE: u32 = 512;

/// Derived search corpus status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCorpusStatus {
    /// Number of materialized search segments.
    pub search_segments_total: u64,
    /// Number of materialized exact aliases.
    pub search_aliases_total: u64,
    /// Whether the corpus is missing or stale relative to canonical index tables.
    pub search_index_stale: bool,
    /// Whether a rebuild would be needed.
    pub would_rebuild_search_index: bool,
    /// Current source fingerprint from canonical tables.
    pub source_fingerprint: String,
    /// Fingerprint recorded by the last corpus build.
    pub recorded_source_fingerprint: Option<String>,
    /// Recorded corpus schema version.
    pub schema_version: Option<u32>,
}

/// Result from rebuilding the derived search corpus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCorpusRebuildResult {
    /// Number of materialized search segments.
    pub search_segments_total: u64,
    /// Number of materialized exact aliases.
    pub search_aliases_total: u64,
    /// Source fingerprint recorded for this rebuild.
    pub source_fingerprint: String,
    /// Views actually evaluated, excluding unaffected scoped views.
    pub base_views_evaluated: u64,
}

/// Builds and inspects the unified search corpus.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchCorpusService;

impl SearchCorpusService {
    /// Inspect whether the derived search corpus matches canonical index tables.
    pub fn status(&self, connection: &Connection) -> Result<SearchCorpusStatus, SearchCorpusError> {
        let transaction = if connection.is_autocommit() {
            Some(connection.unchecked_transaction().map_err(|source| {
                SearchCorpusError::BeginTransaction {
                    source: Box::new(source),
                }
            })?)
        } else {
            None
        };
        let result = self.status_snapshot(connection);
        if let Some(transaction) = transaction {
            transaction
                .commit()
                .map_err(|source| SearchCorpusError::CommitTransaction {
                    source: Box::new(source),
                })?;
        }
        result
    }

    fn status_snapshot(
        &self,
        connection: &Connection,
    ) -> Result<SearchCorpusStatus, SearchCorpusError> {
        let recorded_source_fingerprint = IndexStateRepository::get_by_key(
            connection,
            SEARCH_CORPUS_SOURCE_FINGERPRINT_STATE_KEY,
        )
        .map_err(|source| SearchCorpusError::IndexState {
            source: Box::new(source),
        })?
        .and_then(|record| serde_json::from_str::<String>(&record.value_json).ok());
        let schema_version =
            IndexStateRepository::get_by_key(connection, SEARCH_CORPUS_SCHEMA_VERSION_STATE_KEY)
                .map_err(|source| SearchCorpusError::IndexState {
                    source: Box::new(source),
                })?
                .and_then(|record| serde_json::from_str::<u32>(&record.value_json).ok());
        let generations = IndexGenerationRepository::get(connection).map_err(|source| {
            SearchCorpusError::Sql {
                operation: "generation_status",
                source: Box::new(source),
            }
        })?;
        let source_fingerprint = format!("generation:{}", generations.canonical_generation);
        let search_segments_total = generations.segments_total;
        let search_aliases_total = generations.aliases_total;
        let files_total = generations.files_total;

        let schema_complete =
            SearchSegmentRepository::schema_complete(connection).map_err(|source| {
                SearchCorpusError::SearchSegments {
                    source: Box::new(source),
                }
            })?;
        let search_index_stale = !schema_complete
            || generations.canonical_generation != generations.search_generation
            || generations.derived_generation != generations.published_derived_generation
            || generations.segments_total != generations.published_segments
            || generations.aliases_total != generations.published_aliases
            || schema_version != Some(SEARCH_CORPUS_SCHEMA_VERSION)
            || recorded_source_fingerprint.as_deref() != Some(source_fingerprint.as_str())
            || (files_total > 0 && search_segments_total == 0)
            || (files_total > 0 && search_aliases_total == 0);

        Ok(SearchCorpusStatus {
            search_segments_total,
            search_aliases_total,
            search_index_stale,
            would_rebuild_search_index: search_index_stale,
            source_fingerprint,
            recorded_source_fingerprint,
            schema_version,
        })
    }

    /// Rebuild the derived search corpus from canonical index tables atomically.
    pub fn rebuild_atomic(
        &self,
        connection: &mut Connection,
        case_policy: CasePolicy,
    ) -> Result<SearchCorpusRebuildResult, SearchCorpusError> {
        let transaction =
            connection
                .transaction()
                .map_err(|source| SearchCorpusError::BeginTransaction {
                    source: Box::new(source),
                })?;
        let result = self.rebuild_on_connection(&transaction, case_policy)?;
        transaction
            .commit()
            .map_err(|source| SearchCorpusError::CommitTransaction {
                source: Box::new(source),
            })?;
        Ok(result)
    }

    /// Rebuild the derived search corpus inside an existing index transaction.
    pub fn rebuild_in_transaction(
        &self,
        transaction: &Transaction<'_>,
        case_policy: CasePolicy,
    ) -> Result<SearchCorpusRebuildResult, SearchCorpusError> {
        self.rebuild_on_connection(transaction, case_policy)
    }

    /// Refresh derived search corpus rows for selected file ids atomically.
    pub fn refresh_files_atomic(
        &self,
        connection: &mut Connection,
        file_ids: &[String],
        case_policy: CasePolicy,
    ) -> Result<SearchCorpusRebuildResult, SearchCorpusError> {
        let transaction =
            connection
                .transaction()
                .map_err(|source| SearchCorpusError::BeginTransaction {
                    source: Box::new(source),
                })?;
        let result = self.refresh_files_on_connection(&transaction, file_ids, case_policy)?;
        transaction
            .commit()
            .map_err(|source| SearchCorpusError::CommitTransaction {
                source: Box::new(source),
            })?;
        Ok(result)
    }

    /// Refresh derived search corpus rows for selected file ids inside an existing transaction.
    pub fn refresh_files_in_transaction(
        &self,
        transaction: &Transaction<'_>,
        file_ids: &[String],
        case_policy: CasePolicy,
    ) -> Result<SearchCorpusRebuildResult, SearchCorpusError> {
        self.refresh_files_on_connection(transaction, file_ids, case_policy)
    }

    fn rebuild_on_connection(
        &self,
        connection: &Connection,
        case_policy: CasePolicy,
    ) -> Result<SearchCorpusRebuildResult, SearchCorpusError> {
        if !SearchSegmentRepository::schema_complete(connection).map_err(|source| {
            SearchCorpusError::SearchSegments {
                source: Box::new(source),
            }
        })? {
            SearchSegmentRepository::repair_schema(connection).map_err(|source| {
                SearchCorpusError::SearchSegments {
                    source: Box::new(source),
                }
            })?;
        }
        let files =
            FilesRepository::list_all(connection).map_err(|source| SearchCorpusError::Files {
                source: Box::new(source),
            })?;
        let properties =
            PropertiesRepository::list_all_with_paths(connection).map_err(|source| {
                SearchCorpusError::Properties {
                    source: Box::new(source),
                }
            })?;
        let tasks = TasksRepository::list_all_with_paths(connection).map_err(|source| {
            SearchCorpusError::Tasks {
                source: Box::new(source),
            }
        })?;
        let links = LinksRepository::list_all_with_paths(connection).map_err(|source| {
            SearchCorpusError::Links {
                source: Box::new(source),
            }
        })?;
        let bases = BasesRepository::list_with_paths(connection).map_err(|source| {
            SearchCorpusError::Bases {
                source: Box::new(source),
            }
        })?;

        let file_by_id = files
            .iter()
            .map(|file| (file.file_id.clone(), file))
            .collect::<HashMap<_, _>>();
        SearchAliasRepository::clear(connection).map_err(|source| {
            SearchCorpusError::SearchAliases {
                source: Box::new(source),
            }
        })?;
        SearchSegmentRepository::clear(connection).map_err(|source| {
            SearchCorpusError::SearchSegments {
                source: Box::new(source),
            }
        })?;
        let mut builder = SearchCorpusBuilder::default();
        for file in &files {
            builder.add_file(file)?;
            if file.is_markdown {
                builder.add_doc_file(connection, file)?;
            } else {
                builder.add_extracted_file(connection, file)?;
            }
            builder.flush_if_ready(connection)?;
        }
        for property in &properties {
            builder.add_property(property)?;
            builder.flush_if_ready(connection)?;
        }
        for task in &tasks {
            builder.add_task(task)?;
            builder.flush_if_ready(connection)?;
        }
        for link in &links {
            builder.add_link(link, &file_by_id)?;
            builder.flush_if_ready(connection)?;
        }
        for base in &bases {
            builder.add_base_definition(base)?;
            builder.add_base_rows(connection, base, case_policy, None)?;
        }

        builder.flush(connection)?;
        let source_fingerprint = source_fingerprint(connection)?;
        record_search_corpus_state(connection, &source_fingerprint)?;

        Ok(SearchCorpusRebuildResult {
            search_segments_total: builder.segments_written,
            search_aliases_total: builder.aliases_written,
            source_fingerprint,
            base_views_evaluated: builder.base_views_evaluated,
        })
    }

    fn refresh_files_on_connection(
        &self,
        connection: &Connection,
        file_ids: &[String],
        case_policy: CasePolicy,
    ) -> Result<SearchCorpusRebuildResult, SearchCorpusError> {
        let version =
            IndexStateRepository::get_by_key(connection, SEARCH_CORPUS_SCHEMA_VERSION_STATE_KEY)
                .map_err(|source| SearchCorpusError::IndexState {
                    source: Box::new(source),
                })?
                .and_then(|record| serde_json::from_str::<u32>(&record.value_json).ok());
        if version != Some(SEARCH_CORPUS_SCHEMA_VERSION)
            || !SearchSegmentRepository::schema_complete(connection).map_err(|source| {
                SearchCorpusError::SearchSegments {
                    source: Box::new(source),
                }
            })?
        {
            return self.rebuild_on_connection(connection, case_policy);
        }
        let dirty_ids = IndexGenerationRepository::dirty_files(connection).map_err(|source| {
            SearchCorpusError::Sql {
                operation: "dirty_source_ids",
                source: Box::new(source),
            }
        })?;
        let file_ids = file_ids
            .iter()
            .chain(&dirty_ids)
            .filter(|file_id| !file_id.trim().is_empty())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let derived_dirty =
            IndexGenerationRepository::derived_dirty_files(connection).map_err(|source| {
                SearchCorpusError::Sql {
                    operation: "derived_dirty_source_ids",
                    source: Box::new(source),
                }
            })?;
        if derived_dirty
            .iter()
            .any(|file_id| file_ids.binary_search(file_id).is_err())
        {
            return self.rebuild_on_connection(connection, case_policy);
        }
        if file_ids.is_empty() {
            let status = self.status(connection)?;
            if status.search_index_stale {
                return self.rebuild_on_connection(connection, case_policy);
            }
            return Ok(SearchCorpusRebuildResult {
                search_segments_total: status.search_segments_total,
                search_aliases_total: status.search_aliases_total,
                source_fingerprint: status.source_fingerprint,
                base_views_evaluated: 0,
            });
        }

        let files = FilesRepository::list_by_ids(connection, &file_ids).map_err(|source| {
            SearchCorpusError::Files {
                source: Box::new(source),
            }
        })?;
        let changes = BaseRefreshScope::capture(connection, &file_ids, &files)?;

        for file_id in &file_ids {
            SearchAliasRepository::delete_by_file_id(connection, file_id).map_err(|source| {
                SearchCorpusError::SearchAliases {
                    source: Box::new(source),
                }
            })?;
            SearchSegmentRepository::delete_by_file_id(connection, file_id).map_err(|source| {
                SearchCorpusError::SearchSegments {
                    source: Box::new(source),
                }
            })?;
        }

        let file_by_id = files
            .iter()
            .map(|file| (file.file_id.clone(), file))
            .collect::<HashMap<_, _>>();
        let mut properties = Vec::new();
        for file_id in &file_ids {
            properties.extend(
                PropertiesRepository::list_for_file_with_path(connection, file_id).map_err(
                    |source| SearchCorpusError::Properties {
                        source: Box::new(source),
                    },
                )?,
            );
        }
        let tasks = TasksRepository::list_for_file_ids_with_paths(connection, &file_ids).map_err(
            |source| SearchCorpusError::Tasks {
                source: Box::new(source),
            },
        )?;
        let mut link_by_id = BTreeMap::<String, LinkWithPaths>::new();
        for link in
            LinksRepository::list_outgoing_for_sources_with_paths(connection, &file_ids, true)
                .map_err(|source| SearchCorpusError::Links {
                    source: Box::new(source),
                })?
        {
            link_by_id.insert(link.link_id.clone(), link);
        }
        let bases = BasesRepository::list_with_paths(connection).map_err(|source| {
            SearchCorpusError::Bases {
                source: Box::new(source),
            }
        })?;

        let mut builder = SearchCorpusBuilder::default();
        for file in &files {
            builder.add_file(file)?;
            if file.is_markdown {
                builder.add_doc_file(connection, file)?;
            } else {
                builder.add_extracted_file(connection, file)?;
            }
            builder.flush_if_ready(connection)?;
        }
        for property in &properties {
            builder.add_property(property)?;
            builder.flush_if_ready(connection)?;
        }
        for task in &tasks {
            builder.add_task(task)?;
            builder.flush_if_ready(connection)?;
        }
        for link in link_by_id.values() {
            builder.add_link(link, &file_by_id)?;
            builder.flush_if_ready(connection)?;
        }
        remove_orphaned_base_projections(connection)?;
        for base in &bases {
            if changes.file_ids.contains(&base.file_id) {
                invalidate_base_projection(connection, &base.base_id, None)?;
                builder.add_base_definition(base)?;
            }
            builder.add_base_rows(connection, base, case_policy, Some(&changes))?;
        }

        builder.flush(connection)?;

        let source_fingerprint = source_fingerprint(connection)?;
        record_search_corpus_state(connection, &source_fingerprint)?;
        let generations = IndexGenerationRepository::get(connection).map_err(|source| {
            SearchCorpusError::Sql {
                operation: "read_published_counts",
                source: Box::new(source),
            }
        })?;
        let search_segments_total = generations.segments_total;
        let search_aliases_total = generations.aliases_total;

        Ok(SearchCorpusRebuildResult {
            search_segments_total,
            search_aliases_total,
            source_fingerprint,
            base_views_evaluated: builder.base_views_evaluated,
        })
    }
}

/// Current and previously projected paths cover renames without trusting only the new location.
struct BaseRefreshScope {
    file_ids: BTreeSet<String>,
    paths: BTreeSet<String>,
    unknown_removed_path: bool,
}

impl BaseRefreshScope {
    fn capture(
        connection: &Connection,
        ids: &[String],
        files: &[FileRecord],
    ) -> Result<Self, SearchCorpusError> {
        let mut scope = Self {
            file_ids: ids.iter().cloned().collect(),
            paths: BTreeSet::new(),
            unknown_removed_path: false,
        };
        let current = files
            .iter()
            .map(|file| (file.file_id.as_str(), file.normalized_path.as_str()))
            .collect::<HashMap<_, _>>();
        let mut statement = connection
            .prepare(
                "SELECT normalized_path FROM search_segments WHERE file_id=?1 AND surface='files'",
            )
            .map_err(|source| SearchCorpusError::Sql {
                operation: "prepare_previous_source_paths",
                source: Box::new(source),
            })?;
        for id in ids {
            let previous = statement
                .query_map([id], |row| row.get::<_, String>(0))
                .map_err(|source| SearchCorpusError::Sql {
                    operation: "query_previous_source_paths",
                    source: Box::new(source),
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| SearchCorpusError::Sql {
                    operation: "read_previous_source_paths",
                    source: Box::new(source),
                })?;
            if previous.is_empty() && !current.contains_key(id.as_str()) {
                scope.unknown_removed_path = true;
            }
            scope.paths.extend(previous);
            if let Some(path) = current.get(id.as_str()) {
                scope.paths.insert((*path).to_string());
            }
        }
        Ok(scope)
    }

    fn affects(
        &self,
        base: &tao_sdk_storage::BaseWithPath,
        plan: &tao_sdk_bases::TableQueryPlan,
        policy: CasePolicy,
    ) -> bool {
        if self.unknown_removed_path
            || self.file_ids.contains(&base.file_id)
            || !plan.relations.is_empty()
            || !plan.rollups.is_empty()
        {
            return true;
        }
        let Some(prefix) = plan
            .source_prefix
            .as_deref()
            .filter(|value| !value.is_empty())
        else {
            return true;
        };
        let prefix = tao_sdk_vault::path_match_key(prefix, policy);
        let folder = format!("{prefix}/");
        self.paths.iter().any(|path| {
            let path = tao_sdk_vault::path_match_key(path, policy);
            path == prefix || path.starts_with(&folder)
        })
    }
}

fn invalidate_base_projection(
    connection: &Connection,
    base_id: &str,
    view: Option<&str>,
) -> Result<(), SearchCorpusError> {
    connection.execute(
        "DELETE FROM search_segments WHERE surface='bases' AND json_extract(payload_json,'$.base_id')=?1 AND (?2 IS NULL OR json_extract(payload_json,'$.view_name')=?2)",
        rusqlite::params![base_id, view],
    ).map_err(|source| SearchCorpusError::Sql { operation: "invalidate_selected_base_segments", source: Box::new(source) })?;
    if let Some(view) = view {
        connection
            .execute(
                "DELETE FROM search_aliases WHERE surface='bases' AND source=?1",
                [json!([base_id, view]).to_string()],
            )
            .map_err(|source| SearchCorpusError::Sql {
                operation: "invalidate_selected_base_aliases",
                source: Box::new(source),
            })?;
    } else {
        connection.execute("DELETE FROM search_aliases WHERE surface='bases' AND CASE WHEN json_valid(source) THEN json_extract(source,'$[0]')=?1 ELSE file_id IN (SELECT file_id FROM bases WHERE base_id=?1) END", [base_id])
            .map_err(|source| SearchCorpusError::Sql { operation: "invalidate_base_definition_aliases", source: Box::new(source) })?;
    }
    Ok(())
}

fn remove_orphaned_base_projections(connection: &Connection) -> Result<(), SearchCorpusError> {
    connection.execute("DELETE FROM search_segments WHERE surface='bases' AND NOT EXISTS(SELECT 1 FROM bases b WHERE b.base_id=json_extract(search_segments.payload_json,'$.base_id'))", [])
        .map_err(|source| SearchCorpusError::Sql { operation: "remove_deleted_base_segments", source: Box::new(source) })?;
    connection.execute("DELETE FROM search_aliases WHERE surface='bases' AND CASE WHEN json_valid(source) THEN NOT EXISTS(SELECT 1 FROM bases b WHERE b.base_id=json_extract(search_aliases.source,'$[0]')) ELSE NOT EXISTS(SELECT 1 FROM bases b WHERE b.file_id=search_aliases.file_id) END", [])
        .map_err(|source| SearchCorpusError::Sql { operation: "remove_deleted_base_aliases", source: Box::new(source) })?;
    Ok(())
}

#[derive(Debug, Default)]
struct SearchCorpusBuilder {
    segments: Vec<SearchSegmentInput>,
    aliases: Vec<SearchAliasInput>,
    seen_aliases: BTreeSet<String>,
    segments_written: u64,
    aliases_written: u64,
    base_context: BaseTableExecutionContext,
    base_views_evaluated: u64,
}

impl SearchCorpusBuilder {
    fn flush_if_ready(&mut self, connection: &Connection) -> Result<(), SearchCorpusError> {
        if self.segments.len() >= 128 || self.aliases.len() >= 512 {
            self.flush(connection)?;
        }
        Ok(())
    }
    fn flush(&mut self, connection: &Connection) -> Result<(), SearchCorpusError> {
        SearchSegmentRepository::insert_many(connection, &self.segments).map_err(|source| {
            SearchCorpusError::SearchSegments {
                source: Box::new(source),
            }
        })?;
        SearchAliasRepository::insert_many(connection, &self.aliases).map_err(|source| {
            SearchCorpusError::SearchAliases {
                source: Box::new(source),
            }
        })?;
        self.segments_written += self.segments.len() as u64;
        self.aliases_written += self.aliases.len() as u64;
        self.segments.clear();
        self.aliases.clear();
        Ok(())
    }
    fn add_file(&mut self, file: &FileRecord) -> Result<(), SearchCorpusError> {
        let title = note_title_from_path(&file.normalized_path);
        let extension = extension_for_path(&file.normalized_path);
        let payload = json!({
            "file_id": file.file_id,
            "path": file.normalized_path,
            "extension": extension,
            "size": file.size_bytes,
            "modified_unix_ms": file.modified_unix_ms,
            "indexed_at": file.indexed_at,
            "is_markdown": file.is_markdown,
        });
        self.push_segment(SearchSegmentDraft {
            surface: "files",
            file_id: &file.file_id,
            path: &file.normalized_path,
            extension: &extension,
            field: "file",
            record_id: Some(&file.file_id),
            label: &title,
            weight: FILE_SEGMENT_WEIGHT,
            payload,
            path_text: path_search_text(&file.normalized_path),
            title_text: search_text_with_variants(&title),
            alias_text: alias_search_text(&file.normalized_path, &title),
            body_text: String::new(),
            property_text: String::new(),
            task_text: String::new(),
            link_text: String::new(),
            base_text: String::new(),
        })?;
        self.add_path_aliases(
            "files",
            &file.file_id,
            &file.normalized_path,
            &extension,
            115,
        );
        Ok(())
    }

    fn add_doc_file(
        &mut self,
        connection: &Connection,
        file: &FileRecord,
    ) -> Result<(), SearchCorpusError> {
        let Some(document) = DocumentsRepository::get_by_file_id(connection, &file.file_id)
            .map_err(|source| SearchCorpusError::Sql {
                operation: "read_canonical_document",
                source: Box::new(source),
            })?
        else {
            // Inventory remains searchable when a source has no successful revision yet.
            return Ok(());
        };
        let stale: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM file_diagnostics WHERE path=?1)",
                [&file.normalized_path],
                |row| row.get(0),
            )
            .map_err(|source| SearchCorpusError::Sql {
                operation: "read_document_diagnostics",
                source: Box::new(source),
            })?;
        let content_lc = document.raw_text.to_lowercase();
        let title = document.title;
        let extension = extension_for_path(&file.normalized_path);
        let payload = json!({
            "file_id": file.file_id,
            "path": file.normalized_path,
            "title": title,
            "indexed_at": file.indexed_at,
            "revision": document.source_hash,
            "coverage": "complete",
            "stale": stale,
        });
        self.push_segment(SearchSegmentDraft {
            surface: "docs",
            file_id: &file.file_id,
            path: &file.normalized_path,
            extension: &extension,
            field: "document",
            record_id: Some(&file.file_id),
            label: &title,
            weight: DOC_SEGMENT_WEIGHT,
            payload,
            path_text: path_search_text(&file.normalized_path),
            title_text: search_text_with_variants(&title),
            alias_text: alias_search_text(&file.normalized_path, &title),
            body_text: content_lc,
            property_text: String::new(),
            task_text: String::new(),
            link_text: String::new(),
            base_text: String::new(),
        })?;
        self.add_path_aliases(
            "docs",
            &file.file_id,
            &file.normalized_path,
            &extension,
            170,
        );
        Ok(())
    }

    fn add_extracted_file(
        &mut self,
        connection: &Connection,
        file: &FileRecord,
    ) -> Result<(), SearchCorpusError> {
        let Some(document) =
            ContentRepository::get(connection, &file.file_id).map_err(|source| {
                SearchCorpusError::Sql {
                    operation: "content_document",
                    source: Box::new(source),
                }
            })?
        else {
            return Ok(());
        };
        let title = note_title_from_path(&file.normalized_path);
        let extension = extension_for_path(&file.normalized_path);
        let stale = document.served_revision.as_deref() != Some(document.desired_revision.as_str())
            || document.served_extractor_identity.as_deref()
                != Some(document.extractor_identity.as_str());
        let mut offset = 0;
        loop {
            let rows = ContentRepository::segments(connection, &file.file_id, offset, 128)
                .map_err(|source| SearchCorpusError::Sql {
                    operation: "content_segments",
                    source: Box::new(source),
                })?;
            if rows.is_empty() {
                break;
            }
            offset += rows.len();
            for row in rows {
                if row.text.trim().is_empty() {
                    continue;
                }
                let record_id = format!("{}:{}", file.file_id, row.ordinal);
                self.push_segment(SearchSegmentDraft {
                    surface:"docs",file_id:&file.file_id,path:&file.normalized_path,extension:&extension,field:"document",record_id:Some(&record_id),label:&title,weight:DOC_SEGMENT_WEIGHT,
                    payload:json!({"file_id":file.file_id,"path":file.normalized_path,"title":title,"indexed_at":file.indexed_at,"revision":document.served_revision,"coverage":row.coverage,"stale":stale,"locator":{"kind":row.locator_kind,"start":row.source_start,"end":row.source_end,"ordinal":row.ordinal},"excerpt_text":row.text.chars().take(4096).collect::<String>()}),
                    path_text:path_search_text(&file.normalized_path),title_text:search_text_with_variants(&title),alias_text:alias_search_text(&file.normalized_path,&title),body_text:row.text,property_text:String::new(),task_text:String::new(),link_text:String::new(),base_text:String::new(),
                })?;
                self.flush_if_ready(connection)?;
            }
        }
        self.add_path_aliases(
            "docs",
            &file.file_id,
            &file.normalized_path,
            &extension,
            170,
        );
        Ok(())
    }

    fn add_property(&mut self, property: &PropertyWithPath) -> Result<(), SearchCorpusError> {
        let extension = extension_for_path(&property.file_path);
        let value_text = property_value_text(&property.value_json);
        let payload = json!({
            "property_id": property.property_id,
            "file_id": property.file_id,
            "path": property.file_path,
            "key": property.key,
            "value_type": property.value_type,
            "value_json": property.value_json,
            "updated_at": property.updated_at,
        });
        self.push_segment(SearchSegmentDraft {
            surface: "properties",
            file_id: &property.file_id,
            path: &property.file_path,
            extension: &extension,
            field: &property.key,
            record_id: Some(&property.property_id),
            label: &property.key,
            weight: PROPERTY_SEGMENT_WEIGHT,
            payload,
            path_text: path_search_text(&property.file_path),
            title_text: String::new(),
            alias_text: String::new(),
            body_text: String::new(),
            property_text: format!("{} {}", property.key, value_text),
            task_text: String::new(),
            link_text: String::new(),
            base_text: String::new(),
        })?;
        for alias in property_alias_values(&property.value_json) {
            self.push_alias(SearchAliasDraft {
                surface: "properties",
                file_id: &property.file_id,
                path: &property.file_path,
                extension: &extension,
                alias: &alias,
                source: &format!("frontmatter:{}", property.key),
                weight: 105,
            });
        }
        Ok(())
    }

    fn add_task(&mut self, task: &TaskWithPath) -> Result<(), SearchCorpusError> {
        let extension = extension_for_path(&task.file_path);
        let payload = json!({
            "task_id": task.task_id,
            "file_id": task.file_id,
            "path": task.file_path,
            "line": task.line_number,
            "state": task.state,
            "text": task.text,
            "updated_at": task.updated_at,
        });
        self.push_segment(SearchSegmentDraft {
            surface: "tasks",
            file_id: &task.file_id,
            path: &task.file_path,
            extension: &extension,
            field: "task",
            record_id: Some(&task.task_id),
            label: &task.text,
            weight: TASK_SEGMENT_WEIGHT,
            payload,
            path_text: path_search_text(&task.file_path),
            title_text: String::new(),
            alias_text: String::new(),
            body_text: String::new(),
            property_text: String::new(),
            task_text: format!("{} {}", task.state, task.text),
            link_text: String::new(),
            base_text: String::new(),
        })?;
        Ok(())
    }

    fn add_link(
        &mut self,
        link: &LinkWithPaths,
        file_by_id: &HashMap<String, &FileRecord>,
    ) -> Result<(), SearchCorpusError> {
        // Source ownership is independent of the ranked target path: removing or
        // changing a source must invalidate all of its derived link records.
        let file_id = link.source_file_id.as_str();
        let path = link.resolved_path.as_deref().unwrap_or(&link.source_path);
        if !file_by_id.contains_key(file_id) {
            return Ok(());
        }
        let extension = extension_for_path(path);
        let payload = json!({
            "link_id": link.link_id,
            "source_file_id": link.source_file_id,
            "source_path": link.source_path,
            "raw_target": link.raw_target,
            "resolved_file_id": link.resolved_file_id,
            "target_path": link.resolved_path,
            "source_field": link.source_field,
            "resolved": !link.is_unresolved,
            "unresolved_reason": link.unresolved_reason,
        });
        self.push_segment(SearchSegmentDraft {
            surface: "graph",
            file_id,
            path,
            extension: &extension,
            field: "link",
            record_id: Some(&link.link_id),
            label: &link.raw_target,
            weight: GRAPH_SEGMENT_WEIGHT,
            payload,
            path_text: String::new(),
            title_text: String::new(),
            alias_text: String::new(),
            body_text: String::new(),
            property_text: String::new(),
            task_text: String::new(),
            link_text: format!(
                "{} {} {}",
                link.raw_target,
                link.source_path,
                link.resolved_path.as_deref().unwrap_or_default()
            ),
            base_text: String::new(),
        })?;
        Ok(())
    }

    fn add_base_definition(
        &mut self,
        base: &tao_sdk_storage::BaseWithPath,
    ) -> Result<(), SearchCorpusError> {
        let extension = extension_for_path(&base.file_path);
        let title = note_title_from_path(&base.file_path);
        let payload = json!({
            "base_id": base.base_id,
            "base_path": base.file_path,
            "file_id": base.file_id,
            "updated_at": base.updated_at,
        });
        self.push_segment(SearchSegmentDraft {
            surface: "bases",
            file_id: &base.file_id,
            path: &base.file_path,
            extension: &extension,
            field: "base",
            record_id: Some(&base.base_id),
            label: &title,
            weight: BASE_SEGMENT_WEIGHT,
            payload,
            path_text: path_search_text(&base.file_path),
            title_text: search_text_with_variants(&title),
            alias_text: alias_search_text(&base.file_path, &title),
            body_text: String::new(),
            property_text: String::new(),
            task_text: String::new(),
            link_text: String::new(),
            base_text: title.clone(),
        })?;
        self.add_path_aliases("bases", &base.file_id, &base.file_path, &extension, 100);
        Ok(())
    }

    fn add_base_rows(
        &mut self,
        connection: &Connection,
        base: &tao_sdk_storage::BaseWithPath,
        case_policy: CasePolicy,
        changes: Option<&BaseRefreshScope>,
    ) -> Result<(), SearchCorpusError> {
        let document = match tao_sdk_bases::decode_base_config_json(&base.config_json) {
            Ok(document) => document,
            Err(error) => return record_base_diagnostics(connection, base, &[error.to_string()]),
        };
        let registry = match BaseViewRegistry::from_document(&document) {
            Ok(registry) => registry,
            Err(error) => return record_base_diagnostics(connection, base, &[error.to_string()]),
        };
        let mut failures = Vec::new();
        let retry_failed = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM file_diagnostics WHERE path=?1 AND kind='base_view_failed')",
            [&base.file_path], |row| row.get::<_, bool>(0),
        ).map_err(|source| SearchCorpusError::Sql { operation: "read_base_view_diagnostic", source: Box::new(source) })?;
        for view in document.views {
            let plan = match BaseTableQueryPlanner.compile(
                &registry,
                &tao_sdk_bases::TableQueryPlanRequest {
                    view_name: view.name.clone(),
                    page: 1,
                    page_size: BASE_ROW_PAGE_SIZE,
                },
            ) {
                Ok(plan) => plan,
                Err(error) => {
                    if changes.is_some() {
                        invalidate_base_projection(connection, &base.base_id, Some(&view.name))?;
                    }
                    failures.push(format!("view '{}': {error}", view.name));
                    continue;
                }
            };
            if let Some(changes) = changes {
                if !retry_failed && !changes.affects(base, &plan, case_policy) {
                    continue;
                }
                invalidate_base_projection(connection, &base.base_id, Some(&view.name))?;
            }
            self.base_views_evaluated += 1;
            let page = match BaseTableExecutorService.execute_all_with_context(
                connection,
                &plan,
                BaseTableExecutionOptions {
                    include_summaries: false,
                    coercion_mode: BaseCoercionMode::Permissive,
                    case_policy,
                },
                &mut self.base_context,
            ) {
                Ok(page) => page,
                Err(source @ crate::BaseTableExecutorError::Sql { .. }) => {
                    return Err(SearchCorpusError::BaseExecute {
                        source: Box::new(source),
                    });
                }
                Err(error) => {
                    failures.push(format!("view '{}': {error}", view.name));
                    continue;
                }
            };
            for mut row in page.rows {
                let record_identity = row.file_id.clone();
                if row.file_path.is_empty() {
                    row.file_path = base.file_path.clone();
                    row.file_id = base.file_id.clone();
                }
                let view_name = view.name.clone();
                let extension = extension_for_path(&row.file_path);
                let values_text = serde_json::to_string(&row.values).map_err(|source| {
                    SearchCorpusError::Serialize {
                        source: Box::new(source),
                    }
                })?;
                let values = row.values.clone();
                let record_key =
                    json!([base.base_id, view_name, row.file_path, record_identity]).to_string();
                let payload = json!({
                    "base_id": base.base_id,
                    "base_path": base.file_path,
                    "view_name": view_name,
                    "file_id": row.file_id,
                    "path": row.file_path,
                    "values": values,
                });
                self.push_segment(SearchSegmentDraft {
                    surface: "bases",
                    file_id: &row.file_id,
                    path: &row.file_path,
                    extension: &extension,
                    field: "base_row",
                    record_id: Some(&record_key),
                    label: &view_name,
                    weight: BASE_SEGMENT_WEIGHT,
                    payload,
                    path_text: path_search_text(&row.file_path),
                    title_text: search_text_with_variants(&note_title_from_path(&row.file_path)),
                    alias_text: String::new(),
                    body_text: String::new(),
                    property_text: String::new(),
                    task_text: String::new(),
                    link_text: String::new(),
                    base_text: values_text.clone(),
                })?;
                let alias_owner = json!([base.base_id, view_name]).to_string();
                for alias in value_aliases(&JsonValue::Object(row.values)) {
                    self.push_alias(SearchAliasDraft {
                        surface: "bases",
                        file_id: &row.file_id,
                        path: &row.file_path,
                        extension: &extension,
                        alias: &alias,
                        source: &alias_owner,
                        weight: 115,
                    });
                }
                self.flush_if_ready(connection)?;
            }
        }
        record_base_diagnostics(connection, base, &failures)
    }

    fn push_segment(&mut self, draft: SearchSegmentDraft<'_>) -> Result<(), SearchCorpusError> {
        let segment_id = deterministic_id(
            "seg",
            &json!([
                draft.surface,
                draft.file_id,
                draft.field,
                draft.record_id.unwrap_or(draft.path)
            ])
            .to_string(),
        );
        let mut payload = draft.payload;
        if let Some(object) = payload.as_object_mut() {
            object.insert("schema_version".into(), json!(1));
        }
        let payload_json =
            serde_json::to_string(&payload).map_err(|source| SearchCorpusError::Serialize {
                source: Box::new(source),
            })?;
        self.segments.push(SearchSegmentInput {
            segment_id,
            surface: draft.surface.to_string(),
            file_id: draft.file_id.to_string(),
            normalized_path: draft.path.to_string(),
            normalized_path_lc: tao_sdk_vault::path_match_key(draft.path, CasePolicy::Insensitive),
            extension: draft.extension.to_string(),
            field: draft.field.to_string(),
            record_id: draft.record_id.map(ToString::to_string),
            label: draft.label.to_string(),
            weight: draft.weight,
            payload_json,
            path_text: draft.path_text,
            title_text: draft.title_text,
            alias_text: draft.alias_text,
            body_text: draft.body_text,
            property_text: draft.property_text,
            task_text: draft.task_text,
            link_text: draft.link_text,
            base_text: draft.base_text,
        });
        Ok(())
    }

    fn add_path_aliases(
        &mut self,
        surface: &str,
        file_id: &str,
        path: &str,
        extension: &str,
        weight: i64,
    ) {
        let title = note_title_from_path(path);
        let stem = file_stem(path);
        for (source, alias, alias_weight) in [
            ("path", path.to_string(), weight - 25),
            ("title", title, weight),
            ("stem", stem, weight - 5),
        ] {
            self.push_alias(SearchAliasDraft {
                surface,
                file_id,
                path,
                extension,
                alias: &alias,
                source,
                weight: alias_weight,
            });
        }
    }

    fn push_alias(&mut self, draft: SearchAliasDraft<'_>) {
        let alias_norm = normalize_alias(draft.alias);
        let alias_compact = compact_alias(draft.alias);
        if alias_norm.is_empty() || alias_compact.is_empty() {
            return;
        }
        let dedupe_key = format!(
            "{}\0{}\0{}\0{}\0{}",
            draft.surface, draft.path, draft.source, alias_norm, alias_compact
        );
        if !self.seen_aliases.insert(dedupe_key) {
            return;
        }
        let alias_id = deterministic_id(
            "alias",
            &json!([
                draft.surface,
                draft.file_id,
                draft.source,
                alias_norm,
                alias_compact
            ])
            .to_string(),
        );
        self.aliases.push(SearchAliasInput {
            alias_id,
            file_id: draft.file_id.to_string(),
            normalized_path: draft.path.to_string(),
            normalized_path_lc: tao_sdk_vault::path_match_key(draft.path, CasePolicy::Insensitive),
            extension: draft.extension.to_string(),
            surface: draft.surface.to_string(),
            alias_norm,
            alias_compact,
            source: draft.source.to_string(),
            weight: draft.weight.max(1),
        });
    }
}

struct SearchSegmentDraft<'a> {
    surface: &'a str,
    file_id: &'a str,
    path: &'a str,
    extension: &'a str,
    field: &'a str,
    record_id: Option<&'a str>,
    label: &'a str,
    weight: i64,
    payload: JsonValue,
    path_text: String,
    title_text: String,
    alias_text: String,
    body_text: String,
    property_text: String,
    task_text: String,
    link_text: String,
    base_text: String,
}

struct SearchAliasDraft<'a> {
    surface: &'a str,
    file_id: &'a str,
    path: &'a str,
    extension: &'a str,
    alias: &'a str,
    source: &'a str,
    weight: i64,
}

fn record_base_diagnostics(
    connection: &Connection,
    base: &tao_sdk_storage::BaseWithPath,
    failures: &[String],
) -> Result<(), SearchCorpusError> {
    if failures.is_empty() {
        connection
            .execute(
                "DELETE FROM file_diagnostics WHERE path=?1 AND kind='base_view_failed'",
                [&base.file_path],
            )
            .map_err(|source| SearchCorpusError::Sql {
                operation: "clear_base_view_diagnostic",
                source: Box::new(source),
            })?;
    } else {
        DiagnosticsRepository::upsert(
            connection,
            &FileDiagnosticInput {
                path: base.file_path.clone(),
                file_id: Some(base.file_id.clone()),
                kind: "base_view_failed".into(),
                message: failures.join("; "),
            },
        )
        .map_err(|source| SearchCorpusError::Sql {
            operation: "record_base_view_diagnostic",
            source: Box::new(source),
        })?;
    }
    Ok(())
}

fn source_fingerprint(connection: &Connection) -> Result<String, SearchCorpusError> {
    IndexGenerationRepository::get(connection)
        .map(|state| format!("generation:{}", state.canonical_generation))
        .map_err(|source| SearchCorpusError::Sql {
            operation: "canonical_generation",
            source: Box::new(source),
        })
}

fn record_search_corpus_state(
    connection: &Connection,
    source_fingerprint: &str,
) -> Result<(), SearchCorpusError> {
    let now_unix_ms = current_unix_ms_raw()?;
    IndexStateRepository::upsert(
        connection,
        &IndexStateRecordInput {
            key: SEARCH_CORPUS_SCHEMA_VERSION_STATE_KEY.to_string(),
            value_json: SEARCH_CORPUS_SCHEMA_VERSION.to_string(),
        },
    )
    .map_err(|source| SearchCorpusError::IndexState {
        source: Box::new(source),
    })?;
    IndexStateRepository::upsert(
        connection,
        &IndexStateRecordInput {
            key: SEARCH_CORPUS_SOURCE_FINGERPRINT_STATE_KEY.to_string(),
            value_json: serde_json::to_string(source_fingerprint).map_err(|source| {
                SearchCorpusError::Serialize {
                    source: Box::new(source),
                }
            })?,
        },
    )
    .map_err(|source| SearchCorpusError::IndexState {
        source: Box::new(source),
    })?;
    IndexStateRepository::upsert(
        connection,
        &IndexStateRecordInput {
            key: SEARCH_CORPUS_BUILT_AT_STATE_KEY.to_string(),
            value_json: now_unix_ms.to_string(),
        },
    )
    .map_err(|source| SearchCorpusError::IndexState {
        source: Box::new(source),
    })?;
    IndexGenerationRepository::publish_search(connection).map_err(|source| {
        SearchCorpusError::Sql {
            operation: "publish_search_generation",
            source: Box::new(source),
        }
    })?;
    Ok(())
}

fn path_search_text(path: &str) -> String {
    let title = note_title_from_path(path);
    let stem = file_stem(path);
    join_unique([
        path.to_string(),
        path.replace(['_', '-', '/', '.'], " "),
        title,
        stem,
    ])
}

fn search_text_with_variants(value: &str) -> String {
    join_unique([
        value.to_string(),
        value.replace(['_', '-', '/', '.'], " "),
        normalize_alias(value),
        compact_alias(value),
    ])
}

fn alias_search_text(path: &str, title: &str) -> String {
    join_unique([
        path.to_string(),
        note_title_from_path(path),
        file_stem(path),
        title.to_string(),
        normalize_alias(title),
        compact_alias(title),
    ])
}

fn join_unique(values: impl IntoIterator<Item = String>) -> String {
    let mut unique = BTreeSet::new();
    for value in values {
        let value = value.trim();
        if !value.is_empty() {
            unique.insert(value.to_string());
        }
    }
    unique.into_iter().collect::<Vec<_>>().join(" ")
}

fn property_value_text(value_json: &str) -> String {
    serde_json::from_str::<JsonValue>(value_json)
        .map(|value| value_to_search_text(&value))
        .unwrap_or_else(|_| value_json.to_string())
}

fn property_alias_values(value_json: &str) -> Vec<String> {
    serde_json::from_str::<JsonValue>(value_json)
        .map(|value| value_aliases(&value))
        .unwrap_or_default()
}

fn value_aliases(value: &JsonValue) -> Vec<String> {
    let mut aliases = BTreeSet::new();
    collect_value_aliases(value, &mut aliases);
    aliases.into_iter().collect()
}

fn collect_value_aliases(value: &JsonValue, aliases: &mut BTreeSet<String>) {
    match value {
        JsonValue::String(value) => {
            if value.split_whitespace().count() <= 12 && value.len() <= 160 {
                aliases.insert(value.clone());
            }
        }
        JsonValue::Array(values) => {
            for value in values {
                collect_value_aliases(value, aliases);
            }
        }
        JsonValue::Object(values) => {
            for value in values.values() {
                collect_value_aliases(value, aliases);
            }
        }
        JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::Null => {}
    }
}

fn value_to_search_text(value: &JsonValue) -> String {
    match value {
        JsonValue::String(value) => value.clone(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Array(values) => values
            .iter()
            .map(value_to_search_text)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
        JsonValue::Object(values) => object_to_search_text(values),
        JsonValue::Null => String::new(),
    }
}

fn object_to_search_text(values: &JsonMap<String, JsonValue>) -> String {
    let mut parts = BTreeMap::new();
    for (key, value) in values {
        let text = value_to_search_text(value);
        if !text.is_empty() {
            parts.insert(key, text);
        }
    }
    parts
        .into_iter()
        .flat_map(|(key, value)| [key.clone(), value])
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_alias(value: &str) -> String {
    tao_sdk_search::parser::normalize_query_text(value)
}

fn compact_alias(value: &str) -> String {
    value
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|character| character.is_alphanumeric())
        .collect()
}

fn extension_for_path(path: &str) -> String {
    tao_sdk_vault::normalized_extension(std::path::Path::new(path)).unwrap_or_default()
}

fn file_stem(path: &str) -> String {
    std::path::Path::new(path)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| path.to_string())
}

fn deterministic_id(prefix: &str, input: &str) -> String {
    let hash = blake3::hash(input.as_bytes()).to_hex();
    format!("{prefix}_{}", &hash[..16])
}

fn current_unix_ms_raw() -> Result<u128, SearchCorpusError> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|source| SearchCorpusError::Clock {
            source: Box::new(source),
        })?
        .as_millis())
}

/// Unified search corpus errors.
#[derive(Debug, Error)]
pub enum SearchCorpusError {
    /// Starting an atomic rebuild transaction failed.
    #[error("failed to begin search corpus rebuild transaction: {source}")]
    BeginTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Committing an atomic rebuild transaction failed.
    #[error("failed to commit search corpus rebuild transaction: {source}")]
    CommitTransaction {
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// SQL operation failed.
    #[error("search corpus sql operation '{operation}' failed: {source}")]
    Sql {
        /// Operation name.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: Box<rusqlite::Error>,
    },
    /// Files repository failed.
    #[error("failed to read files for search corpus: {source}")]
    Files {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::FilesRepositoryError>,
    },
    /// Properties repository failed.
    #[error("failed to read properties for search corpus: {source}")]
    Properties {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::PropertiesRepositoryError>,
    },
    /// Tasks repository failed.
    #[error("failed to read tasks for search corpus: {source}")]
    Tasks {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::TasksRepositoryError>,
    },
    /// Links repository failed.
    #[error("failed to read links for search corpus: {source}")]
    Links {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::LinksRepositoryError>,
    },
    /// Bases repository failed.
    #[error("failed to read bases for search corpus: {source}")]
    Bases {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::BasesRepositoryError>,
    },
    /// Search segment repository failed.
    #[error("failed to write search segments: {source}")]
    SearchSegments {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::SearchSegmentRepositoryError>,
    },
    /// Search alias repository failed.
    #[error("failed to write search aliases: {source}")]
    SearchAliases {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::SearchAliasRepositoryError>,
    },
    /// Index state repository failed.
    #[error("failed to update search corpus index state: {source}")]
    IndexState {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::IndexStateRepositoryError>,
    },
    /// Base row materialization failed.
    #[error("failed to materialize base rows for search corpus: {source}")]
    BaseExecute {
        /// Source error.
        #[source]
        source: Box<crate::BaseTableExecutorError>,
    },
    /// JSON serialization failed.
    #[error("failed to serialize search corpus payload: {source}")]
    Serialize {
        /// Source error.
        #[source]
        source: Box<serde_json::Error>,
    },
    /// System clock failed.
    #[error("failed to read system clock for search corpus: {source}")]
    Clock {
        /// Source error.
        #[source]
        source: Box<std::time::SystemTimeError>,
    },
}
