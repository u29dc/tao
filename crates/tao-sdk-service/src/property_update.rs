//! Typed property update service for note front matter.

use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde_json::Value as JsonValue;
use tao_sdk_markdown::{
    MarkdownParseError, MarkdownParseRequest, MarkdownParseResult, MarkdownParser,
};
use tao_sdk_properties::{FrontMatterStatus, TypedPropertyValue, extract_front_matter};
use tao_sdk_storage::{FilesRepository, PropertiesRepository, PropertyRecordInput};
use thiserror::Error;

use super::{NoteCrudError, NoteCrudService, ServiceTraceContext};

/// Result payload for typed property update operations.
#[derive(Debug, Clone, PartialEq)]
pub struct PropertyUpdateResult {
    /// File id that was updated.
    pub file_id: String,
    /// Property key that was set.
    pub key: String,
    /// Typed value persisted.
    pub value: TypedPropertyValue,
    /// Parsed markdown result after update.
    pub parsed: MarkdownParseResult,
}

/// Service that applies typed property updates into note front matter and storage.
#[derive(Clone)]
pub struct PropertyUpdateService {
    note_crud: NoteCrudService,
    parser: MarkdownParser,
}

impl Default for PropertyUpdateService {
    fn default() -> Self {
        Self {
            note_crud: NoteCrudService::default(),
            parser: MarkdownParser,
        }
    }
}

impl PropertyUpdateService {
    /// Set one typed property on a note, persist metadata, and parse updated markdown.
    pub fn set_property(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        key: &str,
        value: TypedPropertyValue,
    ) -> Result<PropertyUpdateResult, PropertyUpdateError> {
        let existing = FilesRepository::get_by_id(connection, file_id)
            .map_err(|source| PropertyUpdateError::Repository { source })?;
        let Some(existing) = existing else {
            return Err(PropertyUpdateError::MissingFileRecord {
                file_id: file_id.to_string(),
            });
        };

        let absolute = vault_root.join(&existing.normalized_path);
        let markdown =
            fs::read_to_string(&absolute).map_err(|source| PropertyUpdateError::ReadFile {
                path: absolute.clone(),
                source,
            })?;

        let extraction = extract_front_matter(&markdown);
        let body = extraction.body;
        let mut mapping = match extraction.status {
            FrontMatterStatus::Parsed { value } => match value {
                serde_yaml::Value::Mapping(mapping) => mapping,
                _ => serde_yaml::Mapping::new(),
            },
            FrontMatterStatus::Malformed { .. } | FrontMatterStatus::Missing => {
                serde_yaml::Mapping::new()
            }
        };

        mapping.insert(
            serde_yaml::Value::String(key.to_string()),
            typed_value_to_yaml(&value),
        );
        let yaml = serde_yaml::to_string(&serde_yaml::Value::Mapping(mapping))
            .map_err(|source| PropertyUpdateError::SerializeYaml { source })?;

        let mut updated_markdown = String::new();
        updated_markdown.push_str("---\n");
        updated_markdown.push_str(&yaml);
        updated_markdown.push_str("---\n");
        if !body.is_empty() {
            updated_markdown.push_str(&body);
        }

        self.note_crud
            .update_note(
                vault_root,
                connection,
                file_id,
                Path::new(&existing.normalized_path),
                &updated_markdown,
            )
            .map_err(|source| PropertyUpdateError::NoteUpdate {
                source: Box::new(source),
            })?;

        let parsed = self
            .parser
            .parse(MarkdownParseRequest {
                normalized_path: existing.normalized_path.clone(),
                raw: updated_markdown,
            })
            .map_err(|source| PropertyUpdateError::Parse { source })?;

        let property_input = PropertyRecordInput {
            property_id: format!("{file_id}:{key}"),
            file_id: file_id.to_string(),
            key: key.to_string(),
            value_type: typed_value_kind(&value).to_string(),
            value_json: serde_json::to_string(&typed_value_to_json(&value))
                .map_err(|source| PropertyUpdateError::SerializeJson { source })?,
        };
        PropertiesRepository::upsert(connection, &property_input)
            .map_err(|source| PropertyUpdateError::PropertyRepository { source })?;

        Ok(PropertyUpdateResult {
            file_id: file_id.to_string(),
            key: key.to_string(),
            value,
            parsed,
        })
    }

    /// Tracing hook wrapper for `set_property` with explicit correlation context.
    pub fn set_property_with_trace_context(
        &self,
        trace_context: &ServiceTraceContext,
        vault_root: &Path,
        connection: &mut Connection,
        file_id: &str,
        key: &str,
        value: TypedPropertyValue,
    ) -> Result<PropertyUpdateResult, PropertyUpdateError> {
        let span = trace_context.span();
        let _entered = span.enter();
        trace_context.emit_start();

        let result = self.set_property(vault_root, connection, file_id, key, value);
        match &result {
            Ok(_) => trace_context.emit_success(),
            Err(error) => trace_context.emit_failure(error),
        }
        result
    }
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

fn typed_value_to_yaml(value: &TypedPropertyValue) -> serde_yaml::Value {
    match value {
        TypedPropertyValue::Bool(value) => serde_yaml::Value::Bool(*value),
        TypedPropertyValue::Number(value) => {
            serde_yaml::to_value(*value).unwrap_or(serde_yaml::Value::Null)
        }
        TypedPropertyValue::Date(value) | TypedPropertyValue::String(value) => {
            serde_yaml::Value::String(value.clone())
        }
        TypedPropertyValue::List(values) => {
            serde_yaml::Value::Sequence(values.iter().map(typed_value_to_yaml).collect())
        }
        TypedPropertyValue::Null => serde_yaml::Value::Null,
    }
}

fn typed_value_to_json(value: &TypedPropertyValue) -> JsonValue {
    match value {
        TypedPropertyValue::Bool(value) => JsonValue::Bool(*value),
        TypedPropertyValue::Number(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        TypedPropertyValue::Date(value) | TypedPropertyValue::String(value) => {
            JsonValue::String(value.clone())
        }
        TypedPropertyValue::List(values) => {
            JsonValue::Array(values.iter().map(typed_value_to_json).collect())
        }
        TypedPropertyValue::Null => JsonValue::Null,
    }
}

/// Errors returned by typed property update operations.
#[derive(Debug, Error)]
pub enum PropertyUpdateError {
    /// File metadata row missing for requested file id.
    #[error("no file metadata found for file id '{file_id}'")]
    MissingFileRecord {
        /// Missing file id.
        file_id: String,
    },
    /// Reading note file failed.
    #[error("failed to read note file '{path}': {source}")]
    ReadFile {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Note update flow failed.
    #[error("note update failed while setting property: {source}")]
    NoteUpdate {
        /// Note update error.
        #[source]
        source: Box<NoteCrudError>,
    },
    /// Parsing updated markdown failed.
    #[error("failed to parse updated markdown after property set: {source}")]
    Parse {
        /// Markdown parser error.
        #[source]
        source: MarkdownParseError,
    },
    /// YAML serialization failed.
    #[error("failed to serialize front matter yaml: {source}")]
    SerializeYaml {
        /// YAML serializer error.
        #[source]
        source: serde_yaml::Error,
    },
    /// JSON serialization failed.
    #[error("failed to serialize property json payload: {source}")]
    SerializeJson {
        /// JSON serializer error.
        #[source]
        source: serde_json::Error,
    },
    /// Files repository query failed.
    #[error("file repository operation failed: {source}")]
    Repository {
        /// Files repository error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
    /// Properties repository update failed.
    #[error("property repository operation failed: {source}")]
    PropertyRepository {
        /// Properties repository error.
        #[source]
        source: tao_sdk_storage::PropertiesRepositoryError,
    },
}
