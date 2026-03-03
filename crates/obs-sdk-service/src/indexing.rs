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
    CasePolicy, FileFingerprintError, FileFingerprintService, PathCanonicalizationError,
    VaultScanError, VaultScanService,
};
use rusqlite::Connection;
use serde_json::json;
use thiserror::Error;

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

fn current_unix_ms() -> Result<u128, FullIndexError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| FullIndexError::Clock {
            source: Box::new(source),
        })?
        .as_millis())
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

#[cfg(test)]
mod tests {
    use std::fs;

    use obs_sdk_storage::{
        BasesRepository, FilesRepository, IndexStateRepository, LinksRepository,
        PropertiesRepository, run_migrations,
    };
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::{CasePolicy, FullIndexService};

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
}
