//! `.base` parsing and typed document models.

use serde::{Deserialize, Serialize};
use serde_json::Map as JsonMap;
use serde_yaml::{Mapping, Value};
use thiserror::Error;

/// Parsed `.base` document model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseDocument {
    /// All views defined in the `.base` file.
    pub views: Vec<BaseViewDefinition>,
}

/// One parsed view definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseViewDefinition {
    /// Stable user-facing view name.
    pub name: String,
    /// View kind.
    pub kind: BaseViewKind,
    /// Optional source path/pattern for the view.
    pub source: Option<String>,
    /// Filter clauses.
    pub filters: Vec<BaseFilterClause>,
    /// Sort clauses.
    pub sorts: Vec<BaseSortClause>,
    /// Column configuration.
    pub columns: Vec<BaseColumnConfig>,
    /// Unknown keys preserved for forward compatibility.
    pub extras: JsonMap<String, serde_json::Value>,
}

/// Supported base view kinds in v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseViewKind {
    /// Table view.
    Table,
}

impl BaseViewKind {
    fn parse(raw: &str, view_index: usize, field: &str) -> Result<Self, BaseParseError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "table" => Ok(Self::Table),
            unsupported => Err(BaseParseError::UnsupportedValue {
                view_index,
                field: field.to_string(),
                value: unsupported.to_string(),
            }),
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Table => "table",
        }
    }
}

/// One filter clause in a table view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseFilterClause {
    /// Property key to filter on.
    pub key: String,
    /// Filter operator.
    pub op: BaseFilterOp,
    /// JSON value payload for the operator.
    pub value: serde_json::Value,
}

/// Supported filter operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseFilterOp {
    /// Equal.
    Eq,
    /// Not equal.
    NotEq,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Gte,
    /// Less than.
    Lt,
    /// Less than or equal.
    Lte,
    /// Substring/list contains.
    Contains,
    /// Any-of inclusion.
    In,
    /// Not-in inclusion.
    NotIn,
    /// Field existence check.
    Exists,
}

impl BaseFilterOp {
    fn parse(raw: &str, view_index: usize) -> Result<Self, BaseParseError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "eq" | "=" => Ok(Self::Eq),
            "neq" | "ne" | "not_eq" | "!=" | "<>" => Ok(Self::NotEq),
            "gt" | ">" => Ok(Self::Gt),
            "gte" | ">=" => Ok(Self::Gte),
            "lt" | "<" => Ok(Self::Lt),
            "lte" | "<=" => Ok(Self::Lte),
            "contains" => Ok(Self::Contains),
            "in" => Ok(Self::In),
            "not_in" | "notin" | "not-in" => Ok(Self::NotIn),
            "exists" => Ok(Self::Exists),
            unsupported => Err(BaseParseError::UnsupportedValue {
                view_index,
                field: "filters[].op".to_string(),
                value: unsupported.to_string(),
            }),
        }
    }
}

/// One sort clause in a table view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseSortClause {
    /// Property key to sort by.
    pub key: String,
    /// Sort direction.
    pub direction: BaseSortDirection,
}

/// Supported sort directions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseSortDirection {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

impl BaseSortDirection {
    fn parse(raw: &str, view_index: usize) -> Result<Self, BaseParseError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            unsupported => Err(BaseParseError::UnsupportedValue {
                view_index,
                field: "sorts[].direction".to_string(),
                value: unsupported.to_string(),
            }),
        }
    }
}

/// Column configuration in a table view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseColumnConfig {
    /// Property key rendered by the column.
    pub key: String,
    /// Optional user-facing label override.
    pub label: Option<String>,
    /// Optional width in character columns.
    pub width: Option<u16>,
    /// Hidden column marker.
    pub hidden: bool,
}

/// Parse one `.base` document from YAML text.
pub fn parse_base_document(input: &str) -> Result<BaseDocument, BaseParseError> {
    if input.trim().is_empty() {
        return Err(BaseParseError::EmptyInput);
    }

    let yaml = serde_yaml::from_str::<Value>(input)
        .map_err(|source| BaseParseError::DeserializeYaml { source })?;
    let Value::Mapping(root) = yaml else {
        return Err(BaseParseError::RootMustBeMapping);
    };

    let views_value = mapping_get(&root, "views").ok_or(BaseParseError::MissingViews)?;
    let Value::Sequence(view_values) = views_value else {
        return Err(BaseParseError::InvalidRootFieldType {
            field: "views".to_string(),
            expected: "sequence",
        });
    };

    let mut views = Vec::with_capacity(view_values.len());
    for (index, view_value) in view_values.iter().enumerate() {
        views.push(parse_view(view_value, index + 1)?);
    }

    Ok(BaseDocument { views })
}

fn parse_view(value: &Value, view_index: usize) -> Result<BaseViewDefinition, BaseParseError> {
    match value {
        Value::String(kind) => {
            let kind = BaseViewKind::parse(kind, view_index, "type")?;
            Ok(BaseViewDefinition {
                name: default_view_name(kind, view_index),
                kind,
                source: None,
                filters: Vec::new(),
                sorts: Vec::new(),
                columns: Vec::new(),
                extras: JsonMap::new(),
            })
        }
        Value::Mapping(mapping) => parse_view_mapping(mapping, view_index),
        _ => Err(BaseParseError::InvalidViewEntry { view_index }),
    }
}

fn parse_view_mapping(
    mapping: &Mapping,
    view_index: usize,
) -> Result<BaseViewDefinition, BaseParseError> {
    let kind = match mapping_get(mapping, "type") {
        Some(Value::String(kind)) => BaseViewKind::parse(kind, view_index, "type")?,
        Some(_) => {
            return Err(BaseParseError::InvalidFieldType {
                view_index,
                field: "type".to_string(),
                expected: "string",
            });
        }
        None => BaseViewKind::Table,
    };

    let name = match mapping_get(mapping, "name") {
        Some(Value::String(name)) => normalize_non_empty_string(name, view_index, "name")?,
        Some(_) => {
            return Err(BaseParseError::InvalidFieldType {
                view_index,
                field: "name".to_string(),
                expected: "string",
            });
        }
        None => default_view_name(kind, view_index),
    };

    let source = match mapping_get(mapping, "source") {
        Some(Value::String(source)) => {
            Some(normalize_non_empty_string(source, view_index, "source")?)
        }
        Some(_) => {
            return Err(BaseParseError::InvalidFieldType {
                view_index,
                field: "source".to_string(),
                expected: "string",
            });
        }
        None => None,
    };

    let filters = parse_filters(mapping, view_index)?;
    let sorts = parse_sorts(mapping, view_index)?;
    let columns = parse_columns(mapping, view_index)?;
    let extras = parse_extras(mapping, view_index)?;

    Ok(BaseViewDefinition {
        name,
        kind,
        source,
        filters,
        sorts,
        columns,
        extras,
    })
}

fn parse_filters(
    mapping: &Mapping,
    view_index: usize,
) -> Result<Vec<BaseFilterClause>, BaseParseError> {
    let Some(raw_filters) = mapping_get(mapping, "filters") else {
        return Ok(Vec::new());
    };
    let Value::Sequence(filters) = raw_filters else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "filters".to_string(),
            expected: "sequence",
        });
    };

    let mut parsed = Vec::with_capacity(filters.len());
    for (filter_index, filter_value) in filters.iter().enumerate() {
        let Value::Mapping(filter_map) = filter_value else {
            return Err(BaseParseError::InvalidFieldType {
                view_index,
                field: format!("filters[{filter_index}]"),
                expected: "mapping",
            });
        };

        let key = required_string_field(filter_map, view_index, "key", "filters[]")?;
        let op_raw = required_string_field(filter_map, view_index, "op", "filters[]")?;
        let op = BaseFilterOp::parse(&op_raw, view_index)?;
        let value = filter_map
            .get("value")
            .ok_or_else(|| BaseParseError::MissingField {
                view_index,
                field: format!("filters[{filter_index}].value"),
            })?;
        let value_json =
            serde_json::to_value(value).map_err(|source| BaseParseError::JsonConversion {
                view_index,
                field: format!("filters[{filter_index}].value"),
                source,
            })?;

        parsed.push(BaseFilterClause {
            key,
            op,
            value: value_json,
        });
    }

    Ok(parsed)
}

fn parse_sorts(
    mapping: &Mapping,
    view_index: usize,
) -> Result<Vec<BaseSortClause>, BaseParseError> {
    let Some(raw_sorts) = mapping_get(mapping, "sorts") else {
        return Ok(Vec::new());
    };
    let Value::Sequence(sorts) = raw_sorts else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "sorts".to_string(),
            expected: "sequence",
        });
    };

    let mut parsed = Vec::with_capacity(sorts.len());
    for (sort_index, sort_value) in sorts.iter().enumerate() {
        let Value::Mapping(sort_map) = sort_value else {
            return Err(BaseParseError::InvalidFieldType {
                view_index,
                field: format!("sorts[{sort_index}]"),
                expected: "mapping",
            });
        };

        let key = required_string_field(sort_map, view_index, "key", "sorts[]")?;
        let direction = match sort_map.get("direction") {
            Some(Value::String(direction)) => BaseSortDirection::parse(direction, view_index)?,
            Some(_) => {
                return Err(BaseParseError::InvalidFieldType {
                    view_index,
                    field: format!("sorts[{sort_index}].direction"),
                    expected: "string",
                });
            }
            None => BaseSortDirection::Asc,
        };

        parsed.push(BaseSortClause { key, direction });
    }

    Ok(parsed)
}

fn parse_columns(
    mapping: &Mapping,
    view_index: usize,
) -> Result<Vec<BaseColumnConfig>, BaseParseError> {
    let Some(raw_columns) = mapping_get(mapping, "columns") else {
        return Ok(Vec::new());
    };
    let Value::Sequence(columns) = raw_columns else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "columns".to_string(),
            expected: "sequence",
        });
    };

    let mut parsed = Vec::with_capacity(columns.len());
    for (column_index, column_value) in columns.iter().enumerate() {
        match column_value {
            Value::String(key) => parsed.push(BaseColumnConfig {
                key: normalize_non_empty_string(
                    key,
                    view_index,
                    &format!("columns[{column_index}]"),
                )?,
                label: None,
                width: None,
                hidden: false,
            }),
            Value::Mapping(column_map) => {
                let key = required_string_field(
                    column_map,
                    view_index,
                    "key",
                    &format!("columns[{column_index}]"),
                )?;
                let label = match column_map.get("label") {
                    Some(Value::String(label)) => Some(normalize_non_empty_string(
                        label,
                        view_index,
                        &format!("columns[{column_index}].label"),
                    )?),
                    Some(_) => {
                        return Err(BaseParseError::InvalidFieldType {
                            view_index,
                            field: format!("columns[{column_index}].label"),
                            expected: "string",
                        });
                    }
                    None => None,
                };
                let width = match column_map.get("width") {
                    Some(width) => Some(parse_column_width(width, view_index, column_index)?),
                    None => None,
                };
                let hidden = match column_map.get("hidden") {
                    Some(Value::Bool(hidden)) => *hidden,
                    Some(_) => {
                        return Err(BaseParseError::InvalidFieldType {
                            view_index,
                            field: format!("columns[{column_index}].hidden"),
                            expected: "boolean",
                        });
                    }
                    None => false,
                };

                parsed.push(BaseColumnConfig {
                    key,
                    label,
                    width,
                    hidden,
                });
            }
            _ => {
                return Err(BaseParseError::InvalidFieldType {
                    view_index,
                    field: format!("columns[{column_index}]"),
                    expected: "string or mapping",
                });
            }
        }
    }

    Ok(parsed)
}

fn parse_column_width(
    value: &Value,
    view_index: usize,
    column_index: usize,
) -> Result<u16, BaseParseError> {
    let Value::Number(width) = value else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: format!("columns[{column_index}].width"),
            expected: "unsigned integer",
        });
    };

    let Some(raw_width) = width.as_u64() else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: format!("columns[{column_index}].width"),
            expected: "unsigned integer",
        });
    };
    u16::try_from(raw_width).map_err(|_| BaseParseError::UnsupportedValue {
        view_index,
        field: format!("columns[{column_index}].width"),
        value: raw_width.to_string(),
    })
}

fn parse_extras(
    mapping: &Mapping,
    view_index: usize,
) -> Result<JsonMap<String, serde_json::Value>, BaseParseError> {
    let mut extras = JsonMap::new();
    for (key, value) in mapping {
        let Value::String(key) = key else {
            continue;
        };
        if matches!(
            key.as_str(),
            "name" | "type" | "source" | "filters" | "sorts" | "columns"
        ) {
            continue;
        }

        let value_json =
            serde_json::to_value(value).map_err(|source| BaseParseError::JsonConversion {
                view_index,
                field: key.clone(),
                source,
            })?;
        extras.insert(key.clone(), value_json);
    }
    Ok(extras)
}

fn mapping_get<'a>(mapping: &'a Mapping, key: &str) -> Option<&'a Value> {
    mapping.get(key)
}

fn required_string_field(
    mapping: &Mapping,
    view_index: usize,
    key: &str,
    context: &str,
) -> Result<String, BaseParseError> {
    let Some(value) = mapping.get(key) else {
        return Err(BaseParseError::MissingField {
            view_index,
            field: format!("{context}.{key}"),
        });
    };
    let Value::String(value) = value else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: format!("{context}.{key}"),
            expected: "string",
        });
    };

    normalize_non_empty_string(value, view_index, &format!("{context}.{key}"))
}

fn normalize_non_empty_string(
    raw: &str,
    view_index: usize,
    field: &str,
) -> Result<String, BaseParseError> {
    let normalized = raw.trim();
    if normalized.is_empty() {
        return Err(BaseParseError::EmptyField {
            view_index,
            field: field.to_string(),
        });
    }
    Ok(normalized.to_string())
}

fn default_view_name(kind: BaseViewKind, view_index: usize) -> String {
    format!("{}-{view_index}", kind.as_str())
}

/// `.base` parser failures.
#[derive(Debug, Error)]
pub enum BaseParseError {
    /// Input content was empty.
    #[error(".base document is empty")]
    EmptyInput,
    /// YAML parse failure.
    #[error("failed to parse .base yaml: {source}")]
    DeserializeYaml {
        /// Underlying parser error.
        #[source]
        source: serde_yaml::Error,
    },
    /// Root value was not a mapping.
    #[error(".base root must be a yaml mapping")]
    RootMustBeMapping,
    /// Required views field was absent.
    #[error(".base document missing required 'views' field")]
    MissingViews,
    /// Root field had invalid type.
    #[error("invalid root field '{field}': expected {expected}")]
    InvalidRootFieldType {
        /// Field name.
        field: String,
        /// Expected type.
        expected: &'static str,
    },
    /// View entry was not a string shorthand or mapping.
    #[error("view {view_index} must be a string shorthand or mapping")]
    InvalidViewEntry {
        /// One-based view index.
        view_index: usize,
    },
    /// Missing required field.
    #[error("view {view_index} missing required field '{field}'")]
    MissingField {
        /// One-based view index.
        view_index: usize,
        /// Field path.
        field: String,
    },
    /// Field had unexpected type.
    #[error("view {view_index} field '{field}' expected {expected}")]
    InvalidFieldType {
        /// One-based view index.
        view_index: usize,
        /// Field path.
        field: String,
        /// Expected type.
        expected: &'static str,
    },
    /// Field value was unsupported for current schema.
    #[error("view {view_index} field '{field}' has unsupported value '{value}'")]
    UnsupportedValue {
        /// One-based view index.
        view_index: usize,
        /// Field path.
        field: String,
        /// Raw unsupported value.
        value: String,
    },
    /// Field value was empty after normalization.
    #[error("view {view_index} field '{field}' must not be empty")]
    EmptyField {
        /// One-based view index.
        view_index: usize,
        /// Field path.
        field: String,
    },
    /// YAML to JSON conversion failed.
    #[error("failed to convert view {view_index} field '{field}' to json: {source}")]
    JsonConversion {
        /// One-based view index.
        view_index: usize,
        /// Field path.
        field: String,
        /// Conversion error.
        #[source]
        source: serde_json::Error,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        BaseColumnConfig, BaseFilterClause, BaseFilterOp, BaseParseError, BaseSortClause,
        BaseSortDirection, BaseViewKind, parse_base_document,
    };

    #[test]
    fn parse_base_document_supports_shorthand_and_typed_table_config() {
        let document = parse_base_document(
            r#"
views:
  - table
  - name: Projects
    type: table
    source: notes/projects
    filters:
      - key: status
        op: eq
        value: active
      - key: priority
        op: gte
        value: 2
    sorts:
      - key: due
        direction: desc
    columns:
      - title
      - key: status
        label: Status
        width: 120
        hidden: false
    sticky: true
"#,
        )
        .expect("parse base document");

        assert_eq!(document.views.len(), 2);

        let shorthand = &document.views[0];
        assert_eq!(shorthand.name, "table-1");
        assert_eq!(shorthand.kind, BaseViewKind::Table);
        assert!(shorthand.filters.is_empty());

        let table = &document.views[1];
        assert_eq!(table.name, "Projects");
        assert_eq!(table.kind, BaseViewKind::Table);
        assert_eq!(table.source.as_deref(), Some("notes/projects"));
        assert_eq!(
            table.filters,
            vec![
                BaseFilterClause {
                    key: "status".to_string(),
                    op: BaseFilterOp::Eq,
                    value: json!("active"),
                },
                BaseFilterClause {
                    key: "priority".to_string(),
                    op: BaseFilterOp::Gte,
                    value: json!(2),
                },
            ]
        );
        assert_eq!(
            table.sorts,
            vec![BaseSortClause {
                key: "due".to_string(),
                direction: BaseSortDirection::Desc,
            }]
        );
        assert_eq!(
            table.columns,
            vec![
                BaseColumnConfig {
                    key: "title".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
                BaseColumnConfig {
                    key: "status".to_string(),
                    label: Some("Status".to_string()),
                    width: Some(120),
                    hidden: false,
                },
            ]
        );
        assert_eq!(table.extras.get("sticky"), Some(&json!(true)));
    }

    #[test]
    fn parse_base_document_rejects_missing_views() {
        let error = parse_base_document("name: Projects").expect_err("missing views should fail");
        assert!(matches!(error, BaseParseError::MissingViews));
    }

    #[test]
    fn parse_base_document_rejects_unsupported_view_kind() {
        let error = parse_base_document(
            r#"
views:
  - type: board
"#,
        )
        .expect_err("unsupported view kind should fail");

        assert!(matches!(
            error,
            BaseParseError::UnsupportedValue {
                view_index: 1,
                field,
                value
            } if field == "type" && value == "board"
        ));
    }

    #[test]
    fn parse_base_document_rejects_invalid_column_width_type() {
        let error = parse_base_document(
            r#"
views:
  - type: table
    columns:
      - key: status
        width: wide
"#,
        )
        .expect_err("invalid column width type should fail");

        assert!(matches!(
            error,
            BaseParseError::InvalidFieldType {
                view_index: 1,
                field,
                expected
            } if field == "columns[0].width" && expected == "unsigned integer"
        ));
    }
}
