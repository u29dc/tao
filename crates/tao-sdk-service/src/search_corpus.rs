//! Derived unified search corpus built from canonical index tables.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use rusqlite::Connection;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tao_sdk_bases::{BaseCoercionMode, BaseTableQueryPlanner, BaseViewRegistry};
use tao_sdk_core::note_title_from_path;
use tao_sdk_storage::{
    BasesRepository, FileRecord, FilesRepository, IndexStateRecordInput, IndexStateRepository,
    LinkWithPaths, LinksRepository, PropertiesRepository, PropertyWithPath, SearchAliasInput,
    SearchAliasRepository, SearchIndexRecord, SearchIndexRepository, SearchSegmentInput,
    SearchSegmentRepository, TaskWithPath, TasksRepository,
};
use tao_sdk_vault::CasePolicy;
use thiserror::Error;

use crate::{BaseTableExecutionOptions, BaseTableExecutorService};

/// Current derived search corpus schema/build version.
pub const SEARCH_CORPUS_SCHEMA_VERSION: u32 = 1;
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
}

/// Builds and inspects the unified search corpus.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchCorpusService;

impl SearchCorpusService {
    /// Inspect whether the derived search corpus matches canonical index tables.
    pub fn status(&self, connection: &Connection) -> Result<SearchCorpusStatus, SearchCorpusError> {
        let source_fingerprint = source_fingerprint(connection)?;
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
        let search_segments_total =
            SearchSegmentRepository::count(connection).map_err(|source| {
                SearchCorpusError::SearchSegments {
                    source: Box::new(source),
                }
            })?;
        let search_aliases_total = SearchAliasRepository::count(connection).map_err(|source| {
            SearchCorpusError::SearchAliases {
                source: Box::new(source),
            }
        })?;
        let files_total: u64 = scalar_count(connection, "files")?;

        let search_index_stale = schema_version != Some(SEARCH_CORPUS_SCHEMA_VERSION)
            || recorded_source_fingerprint.as_deref() != Some(source_fingerprint.as_str())
            || (files_total > 0 && search_segments_total == 0);

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

    /// Rebuild the derived search corpus from canonical index tables.
    pub fn rebuild(
        &self,
        connection: &Connection,
        case_policy: CasePolicy,
    ) -> Result<SearchCorpusRebuildResult, SearchCorpusError> {
        let source_fingerprint = source_fingerprint(connection)?;
        let files =
            FilesRepository::list_all(connection).map_err(|source| SearchCorpusError::Files {
                source: Box::new(source),
            })?;
        let docs = SearchIndexRepository::list_all(connection).map_err(|source| {
            SearchCorpusError::SearchIndex {
                source: Box::new(source),
            }
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
            .map(|file| (file.file_id.clone(), file.clone()))
            .collect::<HashMap<_, _>>();
        let file_by_path = files
            .iter()
            .map(|file| (file.normalized_path.clone(), file.clone()))
            .collect::<HashMap<_, _>>();

        let mut builder = SearchCorpusBuilder::default();
        for file in &files {
            builder.add_file(file)?;
        }
        for doc in &docs {
            builder.add_doc(doc)?;
        }
        for property in &properties {
            builder.add_property(property)?;
        }
        for task in &tasks {
            builder.add_task(task)?;
        }
        for link in &links {
            builder.add_link(link, &file_by_id)?;
        }
        for base in &bases {
            builder.add_base_definition(base)?;
            builder.add_base_rows(connection, base, &file_by_path, case_policy)?;
        }

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
        SearchSegmentRepository::insert_many(connection, &builder.segments).map_err(|source| {
            SearchCorpusError::SearchSegments {
                source: Box::new(source),
            }
        })?;
        SearchAliasRepository::insert_many(connection, &builder.aliases).map_err(|source| {
            SearchCorpusError::SearchAliases {
                source: Box::new(source),
            }
        })?;

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
                value_json: serde_json::to_string(&source_fingerprint).map_err(|source| {
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

        Ok(SearchCorpusRebuildResult {
            search_segments_total: builder.segments.len() as u64,
            search_aliases_total: builder.aliases.len() as u64,
            source_fingerprint,
        })
    }
}

#[derive(Debug, Default)]
struct SearchCorpusBuilder {
    segments: Vec<SearchSegmentInput>,
    aliases: Vec<SearchAliasInput>,
    seen_aliases: BTreeSet<String>,
}

impl SearchCorpusBuilder {
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

    fn add_doc(&mut self, doc: &SearchIndexRecord) -> Result<(), SearchCorpusError> {
        let title = note_title_from_path(&doc.normalized_path);
        let extension = extension_for_path(&doc.normalized_path);
        let payload = json!({
            "file_id": doc.file_id,
            "path": doc.normalized_path,
            "title": title,
            "indexed_at": doc.updated_at,
        });
        let suffixes = long_token_suffixes(&doc.content_lc);
        let body_text = if suffixes.is_empty() {
            doc.content_lc.clone()
        } else {
            format!("{} {}", doc.content_lc, suffixes)
        };
        self.push_segment(SearchSegmentDraft {
            surface: "docs",
            file_id: &doc.file_id,
            path: &doc.normalized_path,
            extension: &extension,
            field: "document",
            record_id: Some(&doc.file_id),
            label: &title,
            weight: DOC_SEGMENT_WEIGHT,
            payload,
            path_text: path_search_text(&doc.normalized_path),
            title_text: search_text_with_variants(&title),
            alias_text: alias_search_text(&doc.normalized_path, &title),
            body_text,
            property_text: String::new(),
            task_text: String::new(),
            link_text: String::new(),
            base_text: String::new(),
        })?;
        self.add_path_aliases("docs", &doc.file_id, &doc.normalized_path, &extension, 170);
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
        file_by_id: &HashMap<String, FileRecord>,
    ) -> Result<(), SearchCorpusError> {
        let (file_id, path) = link
            .resolved_file_id
            .as_ref()
            .and_then(|file_id| {
                link.resolved_path
                    .as_ref()
                    .map(|path| (file_id.as_str(), path.as_str()))
            })
            .unwrap_or((&link.source_file_id, link.source_path.as_str()));
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
        file_by_path: &HashMap<String, FileRecord>,
        case_policy: CasePolicy,
    ) -> Result<(), SearchCorpusError> {
        let Ok(document) = tao_sdk_bases::decode_base_config_json(&base.config_json) else {
            return Ok(());
        };
        let Ok(registry) = BaseViewRegistry::from_document(&document) else {
            return Ok(());
        };
        for view in document.views {
            let mut page_number = 1_u32;
            while let Ok(plan) = BaseTableQueryPlanner.compile(
                &registry,
                &tao_sdk_bases::TableQueryPlanRequest {
                    view_name: view.name.clone(),
                    page: page_number,
                    page_size: BASE_ROW_PAGE_SIZE,
                },
            ) {
                let page = BaseTableExecutorService
                    .execute_with_options(
                        connection,
                        &plan,
                        BaseTableExecutionOptions {
                            include_summaries: false,
                            coercion_mode: BaseCoercionMode::Permissive,
                            case_policy,
                        },
                    )
                    .map_err(|source| SearchCorpusError::BaseExecute {
                        source: Box::new(source),
                    })?;
                let total = page.total;
                for row in page.rows {
                    let Some(file) = file_by_path.get(&row.file_path) else {
                        continue;
                    };
                    let view_name = view.name.clone();
                    let extension = extension_for_path(&row.file_path);
                    let values_text = serde_json::to_string(&row.values).map_err(|source| {
                        SearchCorpusError::Serialize {
                            source: Box::new(source),
                        }
                    })?;
                    let values = row.values.clone();
                    let record_key = format!("{}:{}:{}", base.base_id, view_name, row.file_path);
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
                        file_id: &file.file_id,
                        path: &row.file_path,
                        extension: &extension,
                        field: "base_row",
                        record_id: Some(&record_key),
                        label: &view_name,
                        weight: BASE_SEGMENT_WEIGHT,
                        payload,
                        path_text: path_search_text(&row.file_path),
                        title_text: search_text_with_variants(&note_title_from_path(
                            &row.file_path,
                        )),
                        alias_text: String::new(),
                        body_text: String::new(),
                        property_text: String::new(),
                        task_text: String::new(),
                        link_text: String::new(),
                        base_text: values_text.clone(),
                    })?;
                    for alias in value_aliases(&JsonValue::Object(row.values)) {
                        self.push_alias(SearchAliasDraft {
                            surface: "bases",
                            file_id: &file.file_id,
                            path: &row.file_path,
                            extension: &extension,
                            alias: &alias,
                            source: "base_row",
                            weight: 115,
                        });
                    }
                }
                if u64::from(page_number) * u64::from(BASE_ROW_PAGE_SIZE) >= total {
                    break;
                }
                page_number = page_number.saturating_add(1);
            }
        }
        Ok(())
    }

    fn push_segment(&mut self, draft: SearchSegmentDraft<'_>) -> Result<(), SearchCorpusError> {
        let segment_id = deterministic_id(
            "seg",
            &format!(
                "{}:{}:{}:{}",
                draft.surface,
                draft.file_id,
                draft.field,
                draft.record_id.unwrap_or(draft.path)
            ),
        );
        let payload_json = serde_json::to_string(&draft.payload).map_err(|source| {
            SearchCorpusError::Serialize {
                source: Box::new(source),
            }
        })?;
        self.segments.push(SearchSegmentInput {
            segment_id,
            surface: draft.surface.to_string(),
            file_id: draft.file_id.to_string(),
            normalized_path: draft.path.to_string(),
            normalized_path_lc: draft.path.to_ascii_lowercase(),
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
            &format!(
                "{}:{}:{}:{}:{}",
                draft.surface, draft.file_id, draft.source, alias_norm, alias_compact
            ),
        );
        self.aliases.push(SearchAliasInput {
            alias_id,
            file_id: draft.file_id.to_string(),
            normalized_path: draft.path.to_string(),
            normalized_path_lc: draft.path.to_ascii_lowercase(),
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

fn source_fingerprint(connection: &Connection) -> Result<String, SearchCorpusError> {
    let mut parts = Vec::new();
    for (table, timestamp_column) in [
        ("files", "indexed_at"),
        ("search_index", "updated_at"),
        ("properties", "updated_at"),
        ("tasks", "updated_at"),
        ("links", "created_at"),
        ("bases", "updated_at"),
    ] {
        let sql = format!("SELECT COUNT(*), COALESCE(MAX({timestamp_column}), '') FROM {table}");
        let part = connection
            .query_row(&sql, [], |row| {
                let count: u64 = row.get(0)?;
                let max_timestamp: String = row.get(1)?;
                Ok(format!("{table}:{count}:{max_timestamp}"))
            })
            .map_err(|source| SearchCorpusError::Sql {
                operation: "source_fingerprint",
                source: Box::new(source),
            })?;
        parts.push(part);
    }
    Ok(blake3::hash(parts.join("|").as_bytes())
        .to_hex()
        .to_string())
}

fn scalar_count(connection: &Connection, table: &'static str) -> Result<u64, SearchCorpusError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    connection
        .query_row(&sql, [], |row| row.get(0))
        .map_err(|source| SearchCorpusError::Sql {
            operation: "scalar_count",
            source: Box::new(source),
        })
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

fn long_token_suffixes(text: &str) -> String {
    const MIN_LONG_TOKEN_CHARS: usize = 48;
    const MAX_SUFFIXES_PER_TEXT: usize = 256;
    const MAX_SUFFIX_CHARS: usize = 48;

    let mut suffixes = Vec::new();
    let mut token = String::new();
    for character in text.chars().chain(std::iter::once(' ')) {
        if character.is_alphanumeric() {
            token.push(character);
            continue;
        }
        if token.chars().count() >= MIN_LONG_TOKEN_CHARS {
            for (byte_index, _) in token.char_indices().skip(1) {
                if suffixes.len() >= MAX_SUFFIXES_PER_TEXT {
                    return suffixes.join(" ");
                }
                let suffix = token[byte_index..]
                    .chars()
                    .take(MAX_SUFFIX_CHARS)
                    .collect::<String>();
                if !suffix.is_empty() {
                    suffixes.push(suffix);
                }
            }
        }
        token.clear();
    }
    suffixes.join(" ")
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
    let mut output = String::new();
    let mut last_was_space = true;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_alphanumeric() {
            output.push(character);
            last_was_space = false;
        } else if !last_was_space {
            output.push(' ');
            last_was_space = true;
        }
    }
    output.trim().to_string()
}

fn compact_alias(value: &str) -> String {
    value
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|character| character.is_alphanumeric())
        .collect()
}

fn extension_for_path(path: &str) -> String {
    std::path::Path::new(path)
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .map(str::to_ascii_lowercase)
        .unwrap_or_default()
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
    /// Legacy document search index repository failed.
    #[error("failed to read document search index for search corpus: {source}")]
    SearchIndex {
        /// Source error.
        #[source]
        source: Box<tao_sdk_storage::SearchIndexRepositoryError>,
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
