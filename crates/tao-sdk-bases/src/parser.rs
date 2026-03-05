use std::collections::HashMap;

use serde_json::Map as JsonMap;
use serde_yaml::{Mapping, Value};
use thiserror::Error;

use crate::ast::{
    BaseAggregateOp, BaseAggregateSpec, BaseColumnConfig, BaseDocument, BaseFilterClause,
    BaseFilterOp, BaseNullOrder, BaseRelationSpec, BaseRollupOp, BaseRollupSpec, BaseSortClause,
    BaseSortDirection, BaseViewDefinition, BaseViewKind,
};
use crate::lexer::{normalize_obsidian_field_key, parse_function_argument};

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
    /// Root-level Obsidian filter expression is unsupported.
    #[error("unsupported root filter expression: {expression}")]
    UnsupportedRootFilter {
        /// Raw expression text.
        expression: String,
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
    let defaults = parse_obsidian_root_defaults(&root)?;

    let mut views = Vec::with_capacity(view_values.len());
    for (index, view_value) in view_values.iter().enumerate() {
        views.push(parse_view(view_value, index + 1, &defaults)?);
    }

    Ok(BaseDocument { views })
}

#[derive(Debug, Clone, Default)]
struct ObsidianRootDefaults {
    source: Option<String>,
    filters: Vec<BaseFilterClause>,
}

fn parse_view(
    value: &Value,
    view_index: usize,
    defaults: &ObsidianRootDefaults,
) -> Result<BaseViewDefinition, BaseParseError> {
    match value {
        Value::String(kind) => {
            let kind = parse_view_kind(kind, view_index, "type")?;
            Ok(BaseViewDefinition {
                name: default_view_name(kind, view_index),
                kind,
                source: defaults.source.clone(),
                filters: defaults.filters.clone(),
                sorts: Vec::new(),
                columns: Vec::new(),
                group_by: Vec::new(),
                aggregates: Vec::new(),
                relations: Vec::new(),
                rollups: Vec::new(),
                extras: JsonMap::new(),
            })
        }
        Value::Mapping(mapping) => parse_view_mapping(mapping, view_index, defaults),
        _ => Err(BaseParseError::InvalidViewEntry { view_index }),
    }
}

fn parse_view_mapping(
    mapping: &Mapping,
    view_index: usize,
    defaults: &ObsidianRootDefaults,
) -> Result<BaseViewDefinition, BaseParseError> {
    let kind = match mapping_get(mapping, "type") {
        Some(Value::String(kind)) => parse_view_kind(kind, view_index, "type")?,
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
        None => defaults.source.clone(),
    };

    let mut filters = defaults.filters.clone();
    filters.extend(parse_filters(mapping, view_index)?);
    let sorts = parse_sorts(mapping, view_index)?;
    let columns = parse_columns(mapping, view_index)?;
    let group_by = parse_group_by(mapping, view_index)?;
    let aggregates = parse_aggregates(mapping, view_index)?;
    let relations = parse_relations(mapping, view_index)?;
    let rollups = parse_rollups(mapping, view_index)?;
    let extras = parse_extras(mapping, view_index)?;

    Ok(BaseViewDefinition {
        name,
        kind,
        source,
        filters,
        sorts,
        columns,
        group_by,
        aggregates,
        relations,
        rollups,
        extras,
    })
}

fn parse_view_kind(
    raw: &str,
    view_index: usize,
    field: &str,
) -> Result<BaseViewKind, BaseParseError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "table" => Ok(BaseViewKind::Table),
        unsupported => Err(BaseParseError::UnsupportedValue {
            view_index,
            field: field.to_string(),
            value: unsupported.to_string(),
        }),
    }
}

fn parse_filter_op(raw: &str, view_index: usize) -> Result<BaseFilterOp, BaseParseError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "eq" | "=" => Ok(BaseFilterOp::Eq),
        "neq" | "ne" | "not_eq" | "!=" | "<>" => Ok(BaseFilterOp::NotEq),
        "gt" | ">" => Ok(BaseFilterOp::Gt),
        "gte" | ">=" => Ok(BaseFilterOp::Gte),
        "lt" | "<" => Ok(BaseFilterOp::Lt),
        "lte" | "<=" => Ok(BaseFilterOp::Lte),
        "contains" => Ok(BaseFilterOp::Contains),
        "in" => Ok(BaseFilterOp::In),
        "not_in" | "notin" | "not-in" => Ok(BaseFilterOp::NotIn),
        "exists" => Ok(BaseFilterOp::Exists),
        "starts_with" | "startswith" => Ok(BaseFilterOp::StartsWith),
        "not_starts_with" | "notstartswith" | "not-starts-with" => Ok(BaseFilterOp::NotStartsWith),
        "ends_with" | "endswith" => Ok(BaseFilterOp::EndsWith),
        unsupported => Err(BaseParseError::UnsupportedValue {
            view_index,
            field: "filters[].op".to_string(),
            value: unsupported.to_string(),
        }),
    }
}

fn parse_sort_direction(raw: &str, view_index: usize) -> Result<BaseSortDirection, BaseParseError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "asc" => Ok(BaseSortDirection::Asc),
        "desc" => Ok(BaseSortDirection::Desc),
        unsupported => Err(BaseParseError::UnsupportedValue {
            view_index,
            field: "sorts[].direction".to_string(),
            value: unsupported.to_string(),
        }),
    }
}

fn parse_null_order(raw: &str, view_index: usize) -> Result<BaseNullOrder, BaseParseError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "first" | "nulls_first" | "nullsfirst" => Ok(BaseNullOrder::First),
        "last" | "nulls_last" | "nullslast" => Ok(BaseNullOrder::Last),
        unsupported => Err(BaseParseError::UnsupportedValue {
            view_index,
            field: "sorts[].nulls".to_string(),
            value: unsupported.to_string(),
        }),
    }
}

fn parse_aggregate_op(
    raw: &str,
    view_index: usize,
    field: &str,
) -> Result<BaseAggregateOp, BaseParseError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "count" => Ok(BaseAggregateOp::Count),
        "sum" => Ok(BaseAggregateOp::Sum),
        "min" => Ok(BaseAggregateOp::Min),
        "max" => Ok(BaseAggregateOp::Max),
        unsupported => Err(BaseParseError::UnsupportedValue {
            view_index,
            field: field.to_string(),
            value: unsupported.to_string(),
        }),
    }
}

fn parse_rollup_op(
    raw: &str,
    view_index: usize,
    field: &str,
) -> Result<BaseRollupOp, BaseParseError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "count" => Ok(BaseRollupOp::Count),
        "sum" => Ok(BaseRollupOp::Sum),
        "min" => Ok(BaseRollupOp::Min),
        "max" => Ok(BaseRollupOp::Max),
        unsupported => Err(BaseParseError::UnsupportedValue {
            view_index,
            field: field.to_string(),
            value: unsupported.to_string(),
        }),
    }
}

fn parse_obsidian_root_defaults(root: &Mapping) -> Result<ObsidianRootDefaults, BaseParseError> {
    let (source, filters) = parse_obsidian_root_filters(root)?;
    Ok(ObsidianRootDefaults { source, filters })
}

fn parse_obsidian_root_filters(
    root: &Mapping,
) -> Result<(Option<String>, Vec<BaseFilterClause>), BaseParseError> {
    let Some(raw_filters) = mapping_get(root, "filters") else {
        return Ok((None, Vec::new()));
    };
    let Value::Mapping(filters_map) = raw_filters else {
        return Err(BaseParseError::InvalidRootFieldType {
            field: "filters".to_string(),
            expected: "mapping",
        });
    };
    let Some(and_filters) = mapping_get(filters_map, "and") else {
        return Ok((None, Vec::new()));
    };
    let Value::Sequence(expressions) = and_filters else {
        return Err(BaseParseError::InvalidRootFieldType {
            field: "filters.and".to_string(),
            expected: "sequence",
        });
    };

    let mut source = None;
    let mut clauses = Vec::new();
    for expression in expressions {
        let Value::String(raw_expression) = expression else {
            return Err(BaseParseError::InvalidRootFieldType {
                field: "filters.and[]".to_string(),
                expected: "string",
            });
        };
        let parsed = parse_obsidian_filter_expression(raw_expression)?;
        match parsed {
            ObsidianFilterExpr::InFolder(path) => {
                if source.is_some() && source.as_deref() != Some(path.as_str()) {
                    return Err(BaseParseError::UnsupportedRootFilter {
                        expression: raw_expression.clone(),
                    });
                }
                source = Some(path);
            }
            ObsidianFilterExpr::Clause(clause) => clauses.push(clause),
        }
    }

    Ok((source, clauses))
}

enum ObsidianFilterExpr {
    InFolder(String),
    Clause(BaseFilterClause),
}

fn parse_obsidian_filter_expression(
    expression: &str,
) -> Result<ObsidianFilterExpr, BaseParseError> {
    let trimmed = expression.trim();
    if trimmed.is_empty() {
        return Err(BaseParseError::UnsupportedRootFilter {
            expression: expression.to_string(),
        });
    }

    let (negated, body) = if let Some(stripped) = trimmed.strip_prefix('!') {
        (true, stripped.trim())
    } else {
        (false, trimmed)
    };

    if let Some(argument) = parse_function_argument(body, "file.inFolder(", ')') {
        if negated {
            return Err(BaseParseError::UnsupportedRootFilter {
                expression: expression.to_string(),
            });
        }
        return Ok(ObsidianFilterExpr::InFolder(argument));
    }

    if let Some(argument) = parse_function_argument(body, "file.name.startsWith(", ')') {
        let clause = BaseFilterClause {
            key: "title".to_string(),
            op: if negated {
                BaseFilterOp::NotStartsWith
            } else {
                BaseFilterOp::StartsWith
            },
            value: serde_json::Value::String(argument),
        };
        return Ok(ObsidianFilterExpr::Clause(clause));
    }

    if let Some(field_expr) = body.strip_suffix(".isEmpty()") {
        let key = normalize_obsidian_field_key(field_expr);
        let clause = BaseFilterClause {
            key,
            op: BaseFilterOp::Exists,
            value: serde_json::Value::Bool(negated),
        };
        return Ok(ObsidianFilterExpr::Clause(clause));
    }

    Err(BaseParseError::UnsupportedRootFilter {
        expression: expression.to_string(),
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
        let op = parse_filter_op(&op_raw, view_index)?;
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
    let (raw_sorts, field_name) = if let Some(raw) = mapping_get(mapping, "sorts") {
        (raw, "sorts")
    } else if let Some(raw) = mapping_get(mapping, "sort") {
        (raw, "sort")
    } else {
        return Ok(Vec::new());
    };
    let Value::Sequence(sorts) = raw_sorts else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: field_name.to_string(),
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

        let key = if sort_map.contains_key("key") {
            required_string_field(sort_map, view_index, "key", "sorts[]")?
        } else {
            required_string_field(sort_map, view_index, "property", "sorts[]")?
        };
        let direction = match sort_map.get("direction") {
            Some(Value::String(direction)) => parse_sort_direction(direction, view_index)?,
            Some(_) => {
                return Err(BaseParseError::InvalidFieldType {
                    view_index,
                    field: format!("sorts[{sort_index}].direction"),
                    expected: "string",
                });
            }
            None => BaseSortDirection::Asc,
        };
        let null_order = match sort_map.get("nulls") {
            Some(Value::String(order)) => parse_null_order(order, view_index)?,
            Some(_) => {
                return Err(BaseParseError::InvalidFieldType {
                    view_index,
                    field: format!("sorts[{sort_index}].nulls"),
                    expected: "string",
                });
            }
            None => BaseNullOrder::First,
        };

        parsed.push(BaseSortClause {
            key: normalize_obsidian_field_key(&key),
            direction,
            null_order,
        });
    }

    Ok(parsed)
}

fn parse_columns(
    mapping: &Mapping,
    view_index: usize,
) -> Result<Vec<BaseColumnConfig>, BaseParseError> {
    if let Some(raw_columns) = mapping_get(mapping, "columns") {
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
                    key: normalize_obsidian_field_key(&normalize_non_empty_string(
                        key,
                        view_index,
                        &format!("columns[{column_index}]"),
                    )?),
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
                        key: normalize_obsidian_field_key(&key),
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

        return Ok(parsed);
    }

    let Some(raw_order) = mapping_get(mapping, "order") else {
        return Ok(Vec::new());
    };
    let Value::Sequence(order_columns) = raw_order else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "order".to_string(),
            expected: "sequence",
        });
    };
    let width_by_key = parse_obsidian_column_widths(mapping, view_index)?;

    let mut parsed = Vec::with_capacity(order_columns.len());
    for (column_index, column_value) in order_columns.iter().enumerate() {
        match column_value {
            Value::String(key) => {
                let normalized_key = normalize_obsidian_field_key(&normalize_non_empty_string(
                    key,
                    view_index,
                    &format!("order[{column_index}]"),
                )?);
                parsed.push(BaseColumnConfig {
                    key: normalized_key.clone(),
                    label: None,
                    width: width_by_key.get(&normalized_key).copied(),
                    hidden: false,
                });
            }
            _ => {
                return Err(BaseParseError::InvalidFieldType {
                    view_index,
                    field: format!("order[{column_index}]"),
                    expected: "string",
                });
            }
        }
    }

    Ok(parsed)
}

fn parse_group_by(mapping: &Mapping, view_index: usize) -> Result<Vec<String>, BaseParseError> {
    let Some(raw_group_by) = mapping_get(mapping, "group_by") else {
        return Ok(Vec::new());
    };
    let Value::Sequence(entries) = raw_group_by else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "group_by".to_string(),
            expected: "sequence",
        });
    };

    entries
        .iter()
        .enumerate()
        .map(|(index, value)| match value {
            Value::String(key) => Ok(normalize_obsidian_field_key(&normalize_non_empty_string(
                key,
                view_index,
                &format!("group_by[{index}]"),
            )?)),
            _ => Err(BaseParseError::InvalidFieldType {
                view_index,
                field: format!("group_by[{index}]"),
                expected: "string",
            }),
        })
        .collect()
}

fn parse_aggregates(
    mapping: &Mapping,
    view_index: usize,
) -> Result<Vec<BaseAggregateSpec>, BaseParseError> {
    let Some(raw_aggregates) = mapping_get(mapping, "aggregates") else {
        return Ok(Vec::new());
    };
    let Value::Sequence(entries) = raw_aggregates else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "aggregates".to_string(),
            expected: "sequence",
        });
    };

    entries
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let Value::Mapping(entry) = value else {
                return Err(BaseParseError::InvalidFieldType {
                    view_index,
                    field: format!("aggregates[{index}]"),
                    expected: "mapping",
                });
            };

            let alias = required_string_field(entry, view_index, "alias", "aggregates[]")?;
            let op_raw = required_string_field(entry, view_index, "op", "aggregates[]")?;
            let op = parse_aggregate_op(&op_raw, view_index, "aggregates[].op")?;
            let key = match entry.get("key") {
                Some(Value::String(key)) => {
                    Some(normalize_obsidian_field_key(&normalize_non_empty_string(
                        key,
                        view_index,
                        &format!("aggregates[{index}].key"),
                    )?))
                }
                Some(_) => {
                    return Err(BaseParseError::InvalidFieldType {
                        view_index,
                        field: format!("aggregates[{index}].key"),
                        expected: "string",
                    });
                }
                None => None,
            };

            Ok(BaseAggregateSpec { alias, op, key })
        })
        .collect()
}

fn parse_relations(
    mapping: &Mapping,
    view_index: usize,
) -> Result<Vec<BaseRelationSpec>, BaseParseError> {
    let Some(raw_relations) = mapping_get(mapping, "relations") else {
        return Ok(Vec::new());
    };
    let Value::Sequence(entries) = raw_relations else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "relations".to_string(),
            expected: "sequence",
        });
    };

    entries
        .iter()
        .enumerate()
        .map(|(index, value)| match value {
            Value::String(key) => Ok(BaseRelationSpec {
                key: normalize_obsidian_field_key(&normalize_non_empty_string(
                    key,
                    view_index,
                    &format!("relations[{index}]"),
                )?),
            }),
            Value::Mapping(entry) => Ok(BaseRelationSpec {
                key: normalize_obsidian_field_key(&required_string_field(
                    entry,
                    view_index,
                    "key",
                    "relations[]",
                )?),
            }),
            _ => Err(BaseParseError::InvalidFieldType {
                view_index,
                field: format!("relations[{index}]"),
                expected: "string or mapping",
            }),
        })
        .collect()
}

fn parse_rollups(
    mapping: &Mapping,
    view_index: usize,
) -> Result<Vec<BaseRollupSpec>, BaseParseError> {
    let Some(raw_rollups) = mapping_get(mapping, "rollups") else {
        return Ok(Vec::new());
    };
    let Value::Sequence(entries) = raw_rollups else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "rollups".to_string(),
            expected: "sequence",
        });
    };

    entries
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let Value::Mapping(entry) = value else {
                return Err(BaseParseError::InvalidFieldType {
                    view_index,
                    field: format!("rollups[{index}]"),
                    expected: "mapping",
                });
            };

            let alias = required_string_field(entry, view_index, "alias", "rollups[]")?;
            let relation_key = normalize_obsidian_field_key(&required_string_field(
                entry,
                view_index,
                "relation",
                "rollups[]",
            )?);
            let target_key = normalize_obsidian_field_key(&required_string_field(
                entry,
                view_index,
                "target",
                "rollups[]",
            )?);
            let op_raw = required_string_field(entry, view_index, "op", "rollups[]")?;
            let op = parse_rollup_op(&op_raw, view_index, "rollups[].op")?;

            Ok(BaseRollupSpec {
                alias,
                relation_key,
                target_key,
                op,
            })
        })
        .collect()
}

fn parse_obsidian_column_widths(
    mapping: &Mapping,
    view_index: usize,
) -> Result<HashMap<String, u16>, BaseParseError> {
    let Some(raw_column_size) = mapping_get(mapping, "columnSize") else {
        return Ok(HashMap::new());
    };
    let Value::Mapping(column_size) = raw_column_size else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "columnSize".to_string(),
            expected: "mapping",
        });
    };

    let mut parsed = HashMap::new();
    for (key, value) in column_size {
        let Value::String(key) = key else {
            continue;
        };
        let parsed_width = parse_column_width(value, view_index, 0)?;
        parsed.insert(normalize_obsidian_field_key(key), parsed_width);
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
            "name"
                | "type"
                | "source"
                | "filters"
                | "sorts"
                | "sort"
                | "columns"
                | "order"
                | "columnSize"
                | "group_by"
                | "aggregates"
                | "relations"
                | "rollups"
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::parse_base_document;
    use crate::ast::{
        BaseAggregateOp, BaseFilterClause, BaseFilterOp, BaseNullOrder, BaseRelationSpec,
        BaseRollupOp, BaseRollupSpec, BaseSortClause, BaseSortDirection,
    };

    #[test]
    fn parser_supports_group_aggregate_relation_rollup_fields() {
        let document = parse_base_document(
            r#"
views:
  - name: Projects
    type: table
    group_by:
      - status
    aggregates:
      - alias: total
        op: count
      - alias: priority_sum
        op: sum
        key: priority
    relations:
      - key: meetings
    rollups:
      - alias: meeting_count
        relation: meetings
        target: priority
        op: count
"#,
        )
        .expect("parse");

        let view = &document.views[0];
        assert_eq!(view.group_by, vec!["status"]);
        assert_eq!(
            view.aggregates,
            vec![
                crate::ast::BaseAggregateSpec {
                    alias: "total".to_string(),
                    op: BaseAggregateOp::Count,
                    key: None,
                },
                crate::ast::BaseAggregateSpec {
                    alias: "priority_sum".to_string(),
                    op: BaseAggregateOp::Sum,
                    key: Some("priority".to_string()),
                }
            ]
        );
        assert_eq!(
            view.relations,
            vec![BaseRelationSpec {
                key: "meetings".to_string()
            }]
        );
        assert_eq!(
            view.rollups,
            vec![BaseRollupSpec {
                alias: "meeting_count".to_string(),
                relation_key: "meetings".to_string(),
                target_key: "priority".to_string(),
                op: BaseRollupOp::Count,
            }]
        );
    }

    #[test]
    fn parser_maps_obsidian_filters() {
        let document = parse_base_document(
            r#"
filters:
  and:
    - file.inFolder("WORK/13-RELATIONS/Communications")
    - '!file.name.startsWith("hub_work_")'
views:
  - type: table
    name: Table
    order:
      - file.name
      - date
    sort:
      - property: date
        direction: DESC
"#,
        )
        .expect("parse");

        let table = &document.views[0];
        assert_eq!(
            table.source.as_deref(),
            Some("WORK/13-RELATIONS/Communications")
        );
        assert_eq!(
            table.filters,
            vec![BaseFilterClause {
                key: "title".to_string(),
                op: BaseFilterOp::NotStartsWith,
                value: json!("hub_work_"),
            }]
        );
        assert_eq!(
            table.sorts,
            vec![BaseSortClause {
                key: "date".to_string(),
                direction: BaseSortDirection::Desc,
                null_order: BaseNullOrder::First,
            }]
        );
    }
}
