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
pub const LINK_RESOLUTION_VERSION_STATE_KEY: &str = "link_resolution_version";
pub const CURRENT_LINK_RESOLUTION_VERSION: u32 = 2;

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
    IncrementalIndexService, StaleCleanupResult, StaleCleanupService,
};
pub use reconcile_scan::{ReconciliationScanResult, ReconciliationScannerService};
pub use self_heal::{IndexSelfHealResult, IndexSelfHealService};

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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[cfg(test)]
#[path = "pipeline/tests.rs"]
mod tests;
