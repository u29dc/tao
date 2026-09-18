use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tao_sdk_links::{
    LinkCasePolicy, LinkFragment, LinkKind, LinkResolutionIndex, LinkTarget, WikiLink,
    extract_wikilinks, parse_link_occurrence, parse_link_target, slugify_heading,
    validate_fragment,
};
use tao_sdk_markdown::{
    LinkSyntax, MarkdownParseError, MarkdownParseRequest, MarkdownParseResult, MarkdownParser,
    SourceSpan,
};
use tao_sdk_properties::{
    FrontMatterStatus, MAX_FRONT_MATTER_DEPTH, PropertyProjectionError, TypedPropertyValue,
    project_typed_properties,
};
use tao_sdk_storage::{
    BaseRecordInput, BasesRepository, DiagnosticsRepository, DocumentRecordInput,
    DocumentsRepository, FileDiagnosticInput, FileRecordInput, FilesRepository,
    IndexStateRecordInput, IndexStateRepository, LinkEvidenceInput, LinkEvidenceRepository,
    LinkRecordInput, LinkWithPaths, PropertyRecordInput, TaskRecordInput, TasksRepository,
};
use tao_sdk_vault::{
    CapturedFile, CasePolicy, FileFingerprintError, FileFingerprintService, FileKind,
    PathCanonicalizationError, VaultManifestEntry, VaultScanError, VaultScanService, file_kind,
};
use thiserror::Error;

const CHECKPOINT_STATE_KEY: &str = "checkpoint.incremental_index";
const CHECKPOINT_SUMMARY_KEY: &str = "last_checkpointed_index_summary";
pub const LINK_RESOLUTION_VERSION_STATE_KEY: &str = "link_resolution_version";
pub const CURRENT_LINK_RESOLUTION_VERSION: u32 = 3;
const CANONICAL_STRUCTURE_VERSION: u32 = 1;

mod apply;
mod budget;
mod checkpoint;
mod consistency;
mod errors;
mod full;
mod incremental;
mod reconcile_scan;
mod self_heal;

pub use checkpoint::{CheckpointedIndexResult, CheckpointedIndexService};
pub use consistency::{
    ConsistencyIssueKind, IndexConsistencyChecker, IndexConsistencyIssue, IndexConsistencyReport,
};
pub use errors::{
    CheckpointedIndexError, FullIndexError, IndexConsistencyError, IndexSelfHealError,
    ReconciliationScanError, StaleCleanupError,
};
pub use full::{FullIndexResult, FullIndexService};
pub use incremental::{
    CoalescedBatchIndexResult, CoalescedBatchIndexService, IncrementalIndexResult,
    IncrementalIndexService, SearchCorpusRefreshMode, StaleCleanupResult, StaleCleanupService,
};
pub(crate) use reconcile_scan::IndexChange;
pub use reconcile_scan::{
    ReconciliationScanMode, ReconciliationScanResult, ReconciliationScannerService,
};
pub use self_heal::{IndexSelfHealResult, IndexSelfHealService};

#[derive(Debug, Clone)]
struct MarkdownIndexDocument {
    file_id: String,
    source_path: String,
    links: Vec<IndexedWikiLink>,
    properties: Vec<PropertyRecordInput>,
    tasks: Vec<TaskRecordInput>,
}

#[derive(Debug, Clone)]
struct PreparedIndexEntry {
    file_record: FileRecordInput,
    markdown_doc: Option<MarkdownIndexDocument>,
    base_record: Option<BaseRecordInput>,
    document_record: Option<DocumentRecordInput>,
    diagnostic: Option<FileDiagnosticInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CanonicalStructure {
    version: u32,
    links: Vec<IndexedWikiLink>,
    heading_slugs: Vec<String>,
    block_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct ResolvedLinkBatch {
    records: Vec<LinkRecordInput>,
    evidence: Vec<LinkEvidenceInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IndexedWikiLink {
    link: WikiLink,
    source: String,
    kind: IndexedLinkKind,
    target: LinkTarget,
    span: Option<SourceSpan>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

fn link_case_policy(case_policy: CasePolicy) -> LinkCasePolicy {
    match case_policy {
        CasePolicy::Sensitive => LinkCasePolicy::Sensitive,
        CasePolicy::Insensitive => LinkCasePolicy::Insensitive,
    }
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

fn build_property_records(
    file_id: &str,
    source_path: &str,
    status: &FrontMatterStatus,
    absolute_path: &Path,
) -> Result<Vec<PropertyRecordInput>, FullIndexError> {
    let front_matter = match status {
        FrontMatterStatus::Parsed { value } => value,
        FrontMatterStatus::Malformed { .. } | FrontMatterStatus::Missing => return Ok(Vec::new()),
    };

    let projected = project_typed_properties(front_matter).map_err(|source| {
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

fn build_task_records(
    file_id: &str,
    source_path: &str,
    parsed: &MarkdownParseResult,
) -> Vec<TaskRecordInput> {
    parsed
        .tasks
        .iter()
        .map(|task| {
            let line_number = task.line as i64;
            TaskRecordInput {
                task_id: deterministic_id("task", &format!("{file_id}:{line_number}")),
                file_id: file_id.to_string(),
                file_path: source_path.to_string(),
                file_path_lc: source_path.to_lowercase(),
                line_number,
                state: task.state.clone(),
                text: task.text.clone(),
                text_lc: task.text.to_lowercase(),
            }
        })
        .collect()
}

fn extract_index_links(parsed: &MarkdownParseResult) -> Vec<IndexedWikiLink> {
    let mut links = parsed
        .links
        .iter()
        .filter_map(parse_link_occurrence)
        .map(|occurrence| IndexedWikiLink {
            link: occurrence.link,
            source: "body".to_string(),
            kind: match occurrence.kind {
                LinkKind::Wikilink => IndexedLinkKind::Wikilink,
                LinkKind::Markdown => IndexedLinkKind::Markdown,
                LinkKind::Embed => IndexedLinkKind::Embed,
            },
            target: occurrence.target,
            span: Some(occurrence.span),
        })
        .collect::<Vec<_>>();
    if let FrontMatterStatus::Parsed { value } = &parsed.front_matter_status {
        collect_frontmatter_links(value, "", &mut links);
    }
    // Occurrences retain source order and duplicates; IDs include occurrence ordinals.
    links
}

fn collect_frontmatter_links(
    value: &serde_yaml::Value,
    path: &str,
    links: &mut Vec<IndexedWikiLink>,
) {
    collect_frontmatter_links_at_depth(value, path, links, 0);
}

fn collect_frontmatter_links_at_depth(
    value: &serde_yaml::Value,
    path: &str,
    links: &mut Vec<IndexedWikiLink>,
    depth: usize,
) {
    if depth > MAX_FRONT_MATTER_DEPTH {
        return;
    }

    match value {
        serde_yaml::Value::String(raw) => {
            for link in extract_wikilinks(raw) {
                if let Some(target) = parse_link_target(&link.raw, LinkSyntax::Wiki) {
                    links.push(IndexedWikiLink {
                        link,
                        target,
                        span: None,
                        source: format!("frontmatter:{path}"),
                        kind: IndexedLinkKind::Wikilink,
                    });
                }
            }
        }
        serde_yaml::Value::Sequence(items) => {
            for (index, item) in items.iter().enumerate() {
                let nested_path = if path.is_empty() {
                    format!("[{index}]")
                } else {
                    format!("{path}[{index}]")
                };
                collect_frontmatter_links_at_depth(item, &nested_path, links, depth + 1);
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
                collect_frontmatter_links_at_depth(nested, &nested_path, links, depth + 1);
            }
        }
        serde_yaml::Value::Tagged(tagged) => {
            collect_frontmatter_links_at_depth(&tagged.value, path, links, depth + 1);
        }
        serde_yaml::Value::Null | serde_yaml::Value::Bool(_) | serde_yaml::Value::Number(_) => {}
    }
}

fn build_prepared_index_entry(
    entry: &VaultManifestEntry,
    parser: MarkdownParser,
    fingerprints: &FileFingerprintService,
    captured: Option<CapturedFile>,
) -> Result<PreparedIndexEntry, FullIndexError> {
    let kind = file_kind(&entry.relative);
    let mut file_record = inventory_record(entry);
    if !matches!(kind, FileKind::Markdown | FileKind::Base) {
        return Ok(PreparedIndexEntry {
            file_record,
            markdown_doc: None,
            base_record: None,
            document_record: None,
            diagnostic: None,
        });
    }
    let captured = captured
        .map_or_else(|| fingerprints.capture(&entry.absolute), Ok)
        .map_err(|source| FullIndexError::Fingerprint {
            path: entry.absolute.clone(),
            source: Box::new(source),
        })?;
    file_record.size_bytes = captured.fingerprint.size_bytes;
    file_record.modified_unix_ms =
        i64::try_from(captured.fingerprint.modified_unix_ms).map_err(|_| {
            FullIndexError::TimestampOverflow {
                value: captured.fingerprint.modified_unix_ms,
            }
        })?;
    file_record.hash_blake3 = captured.fingerprint.hash_blake3;
    let raw = String::from_utf8(captured.bytes).map_err(|source| FullIndexError::ReadFile {
        path: entry.absolute.clone(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })?;
    if kind == FileKind::Base {
        let config_json = serde_json::to_string(&json!({ "raw": raw })).map_err(|source| {
            FullIndexError::SerializeBaseConfig {
                path: entry.absolute.clone(),
                source,
            }
        })?;
        return Ok(PreparedIndexEntry {
            base_record: Some(BaseRecordInput {
                base_id: deterministic_id("base", &entry.normalized),
                file_id: file_record.file_id.clone(),
                config_json,
            }),
            file_record,
            markdown_doc: None,
            document_record: None,
            diagnostic: None,
        });
    }
    let parsed = parser
        .parse(MarkdownParseRequest {
            normalized_path: entry.normalized.clone(),
            raw: raw.clone(),
        })
        .map_err(|source| FullIndexError::ParseMarkdown {
            path: entry.absolute.clone(),
            source: Box::new(source),
        })?;
    let diagnostic = if matches!(
        parsed.front_matter_status,
        FrontMatterStatus::Malformed { .. }
    ) {
        Some(FileDiagnosticInput {
            path: entry.normalized.clone(),
            file_id: Some(file_record.file_id.clone()),
            kind: "frontmatter_invalid".to_string(),
            message: format!("{:?}", parsed.front_matter_status),
        })
    } else {
        None
    };
    let properties = build_property_records(
        &file_record.file_id,
        &entry.normalized,
        &parsed.front_matter_status,
        &entry.absolute,
    )?;
    let tasks = build_task_records(&file_record.file_id, &entry.normalized, &parsed);
    let links = extract_index_links(&parsed);
    let mut heading_slugs = parsed
        .headings
        .iter()
        .map(|heading| slugify_heading(&heading.text))
        .filter(|slug| !slug.is_empty())
        .collect::<Vec<_>>();
    heading_slugs.sort();
    heading_slugs.dedup();
    let block_ids = parsed.block_ids.clone();
    let structure = CanonicalStructure {
        version: CANONICAL_STRUCTURE_VERSION,
        links: links.clone(),
        heading_slugs: heading_slugs.clone(),
        block_ids: block_ids.clone(),
    };
    let document_record = DocumentRecordInput {
        file_id: file_record.file_id.clone(),
        source_hash: file_record.hash_blake3.clone(),
        parser_version: CANONICAL_STRUCTURE_VERSION,
        raw_text: raw,
        body_text: parsed.body,
        title: parsed.title,
        structure_json: serde_json::to_string(&structure).map_err(|source| {
            FullIndexError::CanonicalState {
                operation: "serialize_document_structure",
                message: source.to_string(),
            }
        })?,
    };
    Ok(PreparedIndexEntry {
        markdown_doc: Some(MarkdownIndexDocument {
            file_id: file_record.file_id.clone(),
            source_path: entry.normalized.clone(),
            links,
            properties,
            tasks,
        }),
        file_record,
        base_record: None,
        document_record: Some(document_record),
        diagnostic,
    })
}

fn inventory_record(entry: &VaultManifestEntry) -> FileRecordInput {
    FileRecordInput {
        file_id: deterministic_id("file", &entry.normalized),
        normalized_path: entry.normalized.clone(),
        match_key: entry.match_key.clone(),
        absolute_path: entry.absolute.to_string_lossy().into_owned(),
        size_bytes: entry.size_bytes,
        modified_unix_ms: entry.modified_unix_ms,
        // Empty is explicitly an uncomputed content digest for inventory-only files.
        hash_blake3: String::new(),
        is_markdown: file_kind(&entry.relative) == FileKind::Markdown,
    }
}

fn resolve_document_link_records(
    document: &MarkdownIndexDocument,
    resolution_index: &LinkResolutionIndex,
    file_id_by_path: &HashMap<String, String>,
    heading_index: &HashMap<String, Vec<String>>,
    block_index: &HashMap<String, Vec<String>>,
) -> ResolvedLinkBatch {
    let mut records = Vec::with_capacity(document.links.len());
    let mut evidence = Vec::with_capacity(document.links.len());
    for (index, indexed) in document.links.iter().enumerate() {
        let resolution =
            resolution_index.resolve_link(&indexed.target, Some(&document.source_path));
        let resolved_file_id = resolution
            .resolved_path
            .as_ref()
            .and_then(|path| file_id_by_path.get(path))
            .cloned();
        let fragment_status = validate_fragment(
            indexed.target.fragment.as_ref(),
            resolution.resolved_path.as_deref(),
            heading_index,
            block_index,
            None,
        );
        let heading_slug = match &indexed.target.fragment {
            Some(LinkFragment::Heading(value)) => Some(slugify_heading(value)),
            _ => None,
        };
        let block_id = match &indexed.target.fragment {
            Some(LinkFragment::Block(value)) => Some(value.clone()),
            _ => None,
        };
        let is_unresolved = resolved_file_id.is_none();
        let unresolved_reason = if is_unresolved {
            Some(
                if indexed.target.invalid_reason.is_some() {
                    "malformed-target"
                } else {
                    "missing-note"
                }
                .to_string(),
            )
        } else {
            None
        };
        let link_id = deterministic_id(
            "link",
            &format!(
                "{}:{index}:{}:{}",
                document.file_id, indexed.source, indexed.link.raw
            ),
        );
        evidence.push(LinkEvidenceInput {
            link_id: link_id.clone(),
            raw_expression: indexed.link.raw.clone(),
            source_start: indexed.span.as_ref().map_or(0, |span| span.start as u64),
            source_end: indexed.span.as_ref().map_or(0, |span| span.end as u64),
            line: indexed.span.as_ref().map_or(0, |span| span.line as u64),
            end_line: indexed.span.as_ref().map_or(0, |span| span.end_line as u64),
            syntax: match indexed.target.syntax {
                LinkSyntax::Wiki => "wiki",
                LinkSyntax::Markdown => "markdown",
            }
            .to_string(),
            fragment_json: serde_json::to_string(&indexed.target.fragment)
                .unwrap_or_else(|_| "null".to_string()),
            fragment_status: serde_json::to_value(fragment_status)
                .ok()
                .and_then(|value| value.as_str().map(str::to_string))
                .unwrap_or_default(),
            resolution_rule: serde_json::to_value(resolution.rule)
                .ok()
                .and_then(|value| value.as_str().map(str::to_string))
                .unwrap_or_default(),
            candidates_json: serde_json::to_string(&resolution.matched_candidates)
                .unwrap_or_else(|_| "[]".to_string()),
        });
        records.push(LinkRecordInput {
            link_id,
            source_file_id: document.file_id.clone(),
            raw_target: indexed.target.path.clone(),
            resolved_file_id,
            heading_slug,
            block_id,
            is_unresolved,
            unresolved_reason,
            source_field: indexed.kind.source_field(&indexed.source),
        });
    }
    ResolvedLinkBatch { records, evidence }
}

fn typed_value_kind(value: &TypedPropertyValue) -> &'static str {
    match value {
        TypedPropertyValue::Bool(_) => "bool",
        TypedPropertyValue::Number(_)
        | TypedPropertyValue::Integer(_)
        | TypedPropertyValue::UnsignedInteger(_) => "number",
        TypedPropertyValue::Date(_) => "date",
        TypedPropertyValue::String(_) => "string",
        TypedPropertyValue::List(_) => "list",
        TypedPropertyValue::Null => "null",
    }
}

fn typed_value_to_json(value: &TypedPropertyValue) -> serde_json::Value {
    match value {
        TypedPropertyValue::Bool(value) => serde_json::Value::Bool(*value),
        TypedPropertyValue::Integer(value) => serde_json::Value::Number((*value).into()),
        TypedPropertyValue::UnsignedInteger(value) => serde_json::Value::Number((*value).into()),
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
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(FullIndexError::InvalidChangedPath {
            path: path.to_path_buf(),
            reason: "path must be relative without parent traversal".to_string(),
        });
    }
    let normalized = tao_sdk_vault::normalize_relative_path(path).map_err(|source| {
        FullIndexError::InvalidChangedPath {
            path: path.to_path_buf(),
            reason: source.to_string(),
        }
    })?;
    if normalized.is_empty() {
        return Err(FullIndexError::InvalidChangedPath {
            path: path.to_path_buf(),
            reason: "path must identify a file".to_string(),
        });
    }
    Ok(normalized)
}

fn current_unix_ms() -> Result<u128, FullIndexError> {
    current_unix_ms_raw().map_err(|source| FullIndexError::Clock {
        source: Box::new(source),
    })
}

fn current_unix_ms_raw() -> Result<u128, std::time::SystemTimeError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}

#[cfg(test)]
#[path = "pipeline/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "pipeline/regression_tests.rs"]
mod regression_tests;

#[cfg(test)]
#[path = "pipeline/work_tests.rs"]
mod work_tests;
