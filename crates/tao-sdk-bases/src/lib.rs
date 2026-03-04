//! `.base` parsing and typed document models.

use std::collections::HashSet;

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
    /// String starts with prefix.
    StartsWith,
    /// String does not start with prefix.
    NotStartsWith,
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
            "starts_with" | "startswith" => Ok(Self::StartsWith),
            "not_starts_with" | "notstartswith" | "not-starts-with" => Ok(Self::NotStartsWith),
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

/// One registered base view entry with typed kind and serialized config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseViewRegistryEntry {
    /// Stable view name.
    pub name: String,
    /// View kind.
    pub kind: BaseViewKind,
    /// Normalized configuration payload.
    pub config: serde_json::Value,
}

/// Registry over parsed base views.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BaseViewRegistry {
    views: Vec<BaseViewRegistryEntry>,
}

impl BaseViewRegistry {
    /// Build one registry from a parsed `.base` document.
    pub fn from_document(document: &BaseDocument) -> Result<Self, BaseViewRegistryError> {
        let mut views = Vec::with_capacity(document.views.len());
        let mut seen_names = HashSet::new();

        for view in &document.views {
            let dedupe_key = view.name.to_ascii_lowercase();
            if !seen_names.insert(dedupe_key) {
                return Err(BaseViewRegistryError::DuplicateViewName {
                    name: view.name.clone(),
                });
            }

            views.push(BaseViewRegistryEntry {
                name: view.name.clone(),
                kind: view.kind,
                config: serialize_view_config(view)?,
            });
        }

        Ok(Self { views })
    }

    /// Return all views in deterministic definition order.
    #[must_use]
    pub fn list(&self) -> &[BaseViewRegistryEntry] {
        &self.views
    }

    /// Lookup one view by name using case-insensitive matching.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&BaseViewRegistryEntry> {
        self.views
            .iter()
            .find(|entry| entry.name.eq_ignore_ascii_case(name))
    }
}

fn serialize_view_config(
    view: &BaseViewDefinition,
) -> Result<serde_json::Value, BaseViewRegistryError> {
    let mut config = JsonMap::new();
    if let Some(source) = &view.source {
        config.insert(
            "source".to_string(),
            serde_json::Value::String(source.clone()),
        );
    }

    config.insert(
        "filters".to_string(),
        serde_json::to_value(&view.filters).map_err(|source| {
            BaseViewRegistryError::SerializeConfig {
                view_name: view.name.clone(),
                source,
            }
        })?,
    );
    config.insert(
        "sorts".to_string(),
        serde_json::to_value(&view.sorts).map_err(|source| {
            BaseViewRegistryError::SerializeConfig {
                view_name: view.name.clone(),
                source,
            }
        })?,
    );
    config.insert(
        "columns".to_string(),
        serde_json::to_value(&view.columns).map_err(|source| {
            BaseViewRegistryError::SerializeConfig {
                view_name: view.name.clone(),
                source,
            }
        })?,
    );
    if !view.extras.is_empty() {
        config.insert(
            "extras".to_string(),
            serde_json::Value::Object(view.extras.clone()),
        );
    }

    Ok(serde_json::Value::Object(config))
}

/// Query planner request for one table view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableQueryPlanRequest {
    /// Target view name.
    pub view_name: String,
    /// One-based page number.
    pub page: u32,
    /// Page size.
    pub page_size: u32,
}

/// Property query request hint derived from a base table plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyQueryPlanRequest {
    /// Property key to query.
    pub key: String,
    /// Optional substring filter.
    pub value_contains: Option<String>,
    /// Sort strategy.
    pub sort: PropertyQuerySortHint,
    /// Optional row limit.
    pub limit: Option<usize>,
    /// Pagination row offset.
    pub offset: usize,
}

/// Sort hints that map to service-level property query sorts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyQuerySortHint {
    /// File path ascending.
    FilePathAsc,
    /// File path descending.
    FilePathDesc,
    /// Updated timestamp ascending.
    UpdatedAtAsc,
    /// Updated timestamp descending.
    UpdatedAtDesc,
    /// Value ascending.
    ValueAsc,
    /// Value descending.
    ValueDesc,
}

/// Compiled table query plan for one base view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableQueryPlan {
    /// View name.
    pub view_name: String,
    /// Optional path source prefix.
    pub source_prefix: Option<String>,
    /// Unique property keys required by filter/sort/column planning.
    pub required_property_keys: Vec<String>,
    /// Normalized filter predicates.
    pub filters: Vec<BaseFilterClause>,
    /// Normalized sort clauses.
    pub sorts: Vec<BaseSortClause>,
    /// Normalized column list.
    pub columns: Vec<BaseColumnConfig>,
    /// Query limit.
    pub limit: usize,
    /// Query offset.
    pub offset: usize,
    /// Per-key property query hints for executor layer wiring.
    pub property_queries: Vec<PropertyQueryPlanRequest>,
}

/// Query planner that compiles base table definitions into executable plan metadata.
#[derive(Debug, Default, Clone, Copy)]
pub struct BaseTableQueryPlanner;

impl BaseTableQueryPlanner {
    /// Compile one view query plan from registry metadata and paging settings.
    pub fn compile(
        &self,
        registry: &BaseViewRegistry,
        request: &TableQueryPlanRequest,
    ) -> Result<TableQueryPlan, BaseTableQueryPlanError> {
        if request.view_name.trim().is_empty() {
            return Err(BaseTableQueryPlanError::MissingViewName);
        }
        if request.page == 0 || request.page_size == 0 {
            return Err(BaseTableQueryPlanError::InvalidPagination {
                page: request.page,
                page_size: request.page_size,
            });
        }

        let view = registry.get(&request.view_name).ok_or_else(|| {
            BaseTableQueryPlanError::ViewNotFound {
                view_name: request.view_name.clone(),
            }
        })?;

        let config: RegistryTableConfig =
            serde_json::from_value(view.config.clone()).map_err(|source| {
                BaseTableQueryPlanError::InvalidViewConfig {
                    view_name: view.name.clone(),
                    source,
                }
            })?;

        let source_prefix = config.source.and_then(|source| {
            let normalized = source.trim().trim_matches('/').to_string();
            (!normalized.is_empty()).then_some(normalized)
        });
        let filters = config.filters;
        let sorts = config.sorts;
        let mut columns = config.columns;

        let required_property_keys = collect_required_property_keys(&filters, &sorts, &columns);
        if columns.is_empty() {
            columns = required_property_keys
                .iter()
                .map(|key| BaseColumnConfig {
                    key: key.clone(),
                    label: None,
                    width: None,
                    hidden: false,
                })
                .collect();
        }

        let limit = request.page_size as usize;
        let page_offset = (request.page - 1) as usize;
        let offset =
            page_offset
                .checked_mul(limit)
                .ok_or(BaseTableQueryPlanError::PaginationOverflow {
                    page: request.page,
                    page_size: request.page_size,
                })?;
        let property_queries =
            build_property_query_hints(&required_property_keys, &filters, &sorts, limit, offset);

        Ok(TableQueryPlan {
            view_name: view.name.clone(),
            source_prefix,
            required_property_keys,
            filters,
            sorts,
            columns,
            limit,
            offset,
            property_queries,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct RegistryTableConfig {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    filters: Vec<BaseFilterClause>,
    #[serde(default)]
    sorts: Vec<BaseSortClause>,
    #[serde(default)]
    columns: Vec<BaseColumnConfig>,
}

fn collect_required_property_keys(
    filters: &[BaseFilterClause],
    sorts: &[BaseSortClause],
    columns: &[BaseColumnConfig],
) -> Vec<String> {
    let mut keys = Vec::new();
    let mut dedupe = HashSet::new();

    for key in filters
        .iter()
        .map(|filter| filter.key.as_str())
        .chain(sorts.iter().map(|sort| sort.key.as_str()))
        .chain(columns.iter().map(|column| column.key.as_str()))
    {
        let normalized = key.trim();
        if normalized.is_empty() {
            continue;
        }
        let dedupe_key = normalized.to_ascii_lowercase();
        if dedupe.insert(dedupe_key) {
            keys.push(normalized.to_string());
        }
    }

    keys
}

fn build_property_query_hints(
    required_keys: &[String],
    filters: &[BaseFilterClause],
    sorts: &[BaseSortClause],
    limit: usize,
    offset: usize,
) -> Vec<PropertyQueryPlanRequest> {
    required_keys
        .iter()
        .map(|key| {
            let sort = sorts
                .iter()
                .find(|sort_clause| sort_clause.key.eq_ignore_ascii_case(key))
                .map(|sort_clause| match sort_clause.direction {
                    BaseSortDirection::Asc => PropertyQuerySortHint::ValueAsc,
                    BaseSortDirection::Desc => PropertyQuerySortHint::ValueDesc,
                })
                .unwrap_or(PropertyQuerySortHint::FilePathAsc);
            let value_contains = filters
                .iter()
                .find(|filter| {
                    filter.key.eq_ignore_ascii_case(key) && filter.op == BaseFilterOp::Contains
                })
                .and_then(|filter| json_scalar_to_string(&filter.value));

            PropertyQueryPlanRequest {
                key: key.clone(),
                value_contains,
                sort,
                limit: Some(limit),
                offset,
            }
        })
        .collect()
}

fn json_scalar_to_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
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
            let kind = BaseViewKind::parse(kind, view_index, "type")?;
            Ok(BaseViewDefinition {
                name: default_view_name(kind, view_index),
                kind,
                source: defaults.source.clone(),
                filters: defaults.filters.clone(),
                sorts: Vec::new(),
                columns: Vec::new(),
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
        None => defaults.source.clone(),
    };

    let mut filters = defaults.filters.clone();
    filters.extend(parse_filters(mapping, view_index)?);
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

fn parse_function_argument(expression: &str, prefix: &str, suffix: char) -> Option<String> {
    let body = expression.strip_prefix(prefix)?;
    let body = body.strip_suffix(suffix)?;
    let body = body.trim();

    if body.len() >= 2 && body.starts_with('"') && body.ends_with('"') {
        return Some(body[1..body.len() - 1].to_string());
    }
    if body.len() >= 2 && body.starts_with('\'') && body.ends_with('\'') {
        return Some(body[1..body.len() - 1].to_string());
    }

    None
}

fn normalize_obsidian_field_key(raw: &str) -> String {
    let normalized = raw.trim();
    if normalized.eq_ignore_ascii_case("file.name") {
        return "title".to_string();
    }
    if normalized.eq_ignore_ascii_case("file.path") {
        return "path".to_string();
    }
    if normalized.eq_ignore_ascii_case("file.folder") {
        return "file_folder".to_string();
    }
    if let Some(rest) = normalized.strip_prefix("note.") {
        return rest.to_string();
    }

    normalized.to_string()
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

        parsed.push(BaseSortClause {
            key: normalize_obsidian_field_key(&key),
            direction,
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

fn parse_obsidian_column_widths(
    mapping: &Mapping,
    view_index: usize,
) -> Result<std::collections::HashMap<String, u16>, BaseParseError> {
    let Some(raw_column_size) = mapping_get(mapping, "columnSize") else {
        return Ok(std::collections::HashMap::new());
    };
    let Value::Mapping(column_size) = raw_column_size else {
        return Err(BaseParseError::InvalidFieldType {
            view_index,
            field: "columnSize".to_string(),
            expected: "mapping",
        });
    };

    let mut parsed = std::collections::HashMap::new();
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

/// View registry construction failures.
#[derive(Debug, Error)]
pub enum BaseViewRegistryError {
    /// Duplicate names are not allowed in one `.base` document.
    #[error("duplicate base view name '{name}'")]
    DuplicateViewName {
        /// Duplicated name.
        name: String,
    },
    /// View config serialization failed.
    #[error("failed to serialize config for base view '{view_name}': {source}")]
    SerializeConfig {
        /// View name.
        view_name: String,
        /// JSON serialization error.
        #[source]
        source: serde_json::Error,
    },
}

/// Table query planner failures.
#[derive(Debug, Error)]
pub enum BaseTableQueryPlanError {
    /// Request view name was empty.
    #[error("table query plan requires a non-empty view name")]
    MissingViewName,
    /// Pagination values were invalid.
    #[error("invalid table query pagination page={page} page_size={page_size}")]
    InvalidPagination {
        /// One-based page number.
        page: u32,
        /// Page size.
        page_size: u32,
    },
    /// Pagination overflow occurred during offset computation.
    #[error("table query pagination overflow page={page} page_size={page_size}")]
    PaginationOverflow {
        /// One-based page number.
        page: u32,
        /// Page size.
        page_size: u32,
    },
    /// Requested view does not exist in registry.
    #[error("base view '{view_name}' not found in registry")]
    ViewNotFound {
        /// Requested view name.
        view_name: String,
    },
    /// Registry config payload could not be decoded.
    #[error("invalid config payload for base view '{view_name}': {source}")]
    InvalidViewConfig {
        /// View name.
        view_name: String,
        /// JSON deserialization error.
        #[source]
        source: serde_json::Error,
    },
}

/// Diagnostic severity for base validation messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseDiagnosticSeverity {
    /// Validation error that should block execution.
    Error,
    /// Non-blocking validation warning.
    Warning,
}

/// One base validation diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseDiagnostic {
    /// Stable machine-readable diagnostic code.
    pub code: String,
    /// Severity level.
    pub severity: BaseDiagnosticSeverity,
    /// Human-readable message.
    pub message: String,
    /// Optional field path for pinpointing issue location.
    pub field: Option<String>,
}

/// Validate raw `.base` YAML content and return diagnostics.
#[must_use]
pub fn validate_base_yaml(input: &str) -> Vec<BaseDiagnostic> {
    match parse_base_document(input) {
        Ok(document) => validate_base_document(&document),
        Err(error) => vec![parse_error_diagnostic(error)],
    }
}

/// Validate persisted base config JSON payload and return diagnostics.
#[must_use]
pub fn validate_base_config_json(config_json: &str) -> Vec<BaseDiagnostic> {
    match serde_json::from_str::<BaseDocument>(config_json) {
        Ok(document) => validate_base_document(&document),
        Err(source) => vec![BaseDiagnostic {
            code: "bases.parse.invalid_schema".to_string(),
            severity: BaseDiagnosticSeverity::Error,
            message: format!("failed to decode base config json: {source}"),
            field: None,
        }],
    }
}

/// Validate a parsed base document and return normalized diagnostics.
#[must_use]
pub fn validate_base_document(document: &BaseDocument) -> Vec<BaseDiagnostic> {
    let mut diagnostics = Vec::new();
    let mut seen_view_names = HashSet::new();

    for (view_index, view) in document.views.iter().enumerate() {
        let view_name_key = view.name.to_ascii_lowercase();
        if !seen_view_names.insert(view_name_key) {
            diagnostics.push(BaseDiagnostic {
                code: "bases.view.duplicate_name".to_string(),
                severity: BaseDiagnosticSeverity::Error,
                message: format!("duplicate view name '{}'", view.name),
                field: Some(format!("views[{view_index}].name")),
            });
        }

        if matches!(view.kind, BaseViewKind::Table) {
            if view.columns.is_empty() {
                diagnostics.push(BaseDiagnostic {
                    code: "bases.table.missing_columns".to_string(),
                    severity: BaseDiagnosticSeverity::Warning,
                    message: format!("table view '{}' has no configured columns", view.name),
                    field: Some(format!("views[{view_index}].columns")),
                });
            }

            let mut seen_columns = HashSet::new();
            for (column_index, column) in view.columns.iter().enumerate() {
                let column_key = column.key.to_ascii_lowercase();
                if !seen_columns.insert(column_key) {
                    diagnostics.push(BaseDiagnostic {
                        code: "bases.column.duplicate_key".to_string(),
                        severity: BaseDiagnosticSeverity::Warning,
                        message: format!(
                            "table view '{}' has duplicate column key '{}'",
                            view.name, column.key
                        ),
                        field: Some(format!("views[{view_index}].columns[{column_index}].key")),
                    });
                }
            }

            if !view.columns.is_empty() && view.columns.iter().all(|column| column.hidden) {
                diagnostics.push(BaseDiagnostic {
                    code: "bases.table.all_columns_hidden".to_string(),
                    severity: BaseDiagnosticSeverity::Warning,
                    message: format!("table view '{}' hides all configured columns", view.name),
                    field: Some(format!("views[{view_index}].columns")),
                });
            }
        }
    }

    diagnostics.sort_by(|left, right| {
        severity_rank(left.severity)
            .cmp(&severity_rank(right.severity))
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.field.cmp(&right.field))
            .then_with(|| left.message.cmp(&right.message))
    });
    diagnostics
}

fn parse_error_diagnostic(error: BaseParseError) -> BaseDiagnostic {
    BaseDiagnostic {
        code: "bases.parse.invalid_schema".to_string(),
        severity: BaseDiagnosticSeverity::Error,
        message: error.to_string(),
        field: parse_error_field(&error),
    }
}

fn parse_error_field(error: &BaseParseError) -> Option<String> {
    match error {
        BaseParseError::InvalidRootFieldType { field, .. } => Some(field.clone()),
        BaseParseError::MissingField { field, .. }
        | BaseParseError::InvalidFieldType { field, .. }
        | BaseParseError::UnsupportedValue { field, .. }
        | BaseParseError::EmptyField { field, .. }
        | BaseParseError::JsonConversion { field, .. } => Some(field.clone()),
        BaseParseError::MissingViews => Some("views".to_string()),
        BaseParseError::InvalidViewEntry { view_index } => Some(format!("views[{view_index}]")),
        BaseParseError::UnsupportedRootFilter { .. } => Some("filters.and".to_string()),
        BaseParseError::EmptyInput
        | BaseParseError::DeserializeYaml { .. }
        | BaseParseError::RootMustBeMapping => None,
    }
}

fn severity_rank(severity: BaseDiagnosticSeverity) -> u8 {
    match severity {
        BaseDiagnosticSeverity::Error => 0,
        BaseDiagnosticSeverity::Warning => 1,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        BaseColumnConfig, BaseDiagnosticSeverity, BaseFilterClause, BaseFilterOp, BaseParseError,
        BaseSortClause, BaseSortDirection, BaseTableQueryPlanError, BaseTableQueryPlanner,
        BaseViewKind, BaseViewRegistry, BaseViewRegistryError, PropertyQuerySortHint,
        TableQueryPlanRequest, parse_base_document, validate_base_config_json,
        validate_base_document, validate_base_yaml,
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

    #[test]
    fn parse_base_document_maps_obsidian_root_filters_to_source_and_clauses() {
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
        .expect("parse document");

        assert_eq!(document.views.len(), 1);
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
                    key: "date".to_string(),
                    label: None,
                    width: None,
                    hidden: false,
                },
            ]
        );
    }

    #[test]
    fn parse_base_document_maps_obsidian_order_sort_and_column_size() {
        let document = parse_base_document(
            r#"
filters:
  and:
    - file.inFolder("WORK/14-PROJECTS")
    - "!prod_folder.isEmpty()"
views:
  - type: table
    name: Table
    order:
      - file.name
      - client
      - service
      - status
      - start_date
      - end_date
      - prod_folder
    sort:
      - property: formula.Untitled
        direction: ASC
      - property: file.name
        direction: ASC
    columnSize:
      file.name: 230
      note.client: 220
"#,
        )
        .expect("parse document");

        let table = &document.views[0];
        assert_eq!(table.source.as_deref(), Some("WORK/14-PROJECTS"));
        assert_eq!(
            table.filters,
            vec![BaseFilterClause {
                key: "prod_folder".to_string(),
                op: BaseFilterOp::Exists,
                value: json!(true),
            }]
        );
        assert_eq!(
            table.sorts,
            vec![
                BaseSortClause {
                    key: "formula.Untitled".to_string(),
                    direction: BaseSortDirection::Asc,
                },
                BaseSortClause {
                    key: "title".to_string(),
                    direction: BaseSortDirection::Asc,
                },
            ]
        );
        assert_eq!(table.columns.len(), 7);
        assert_eq!(table.columns[0].key, "title");
        assert_eq!(table.columns[0].width, Some(230));
        assert_eq!(table.columns[1].key, "client");
        assert_eq!(table.columns[1].width, Some(220));
    }

    #[test]
    fn view_registry_lists_views_with_kind_and_config() {
        let document = parse_base_document(
            r#"
views:
  - name: Projects
    type: table
    source: notes/projects
    filters:
      - key: status
        op: eq
        value: active
    sorts:
      - key: due
        direction: asc
    columns:
      - title
    sticky: true
  - table
"#,
        )
        .expect("parse document");

        let registry = BaseViewRegistry::from_document(&document).expect("build registry");
        assert_eq!(registry.list().len(), 2);

        let projects = registry.get("projects").expect("lookup projects");
        assert_eq!(projects.kind, BaseViewKind::Table);
        assert_eq!(
            projects.config.get("source"),
            Some(&json!("notes/projects"))
        );
        assert_eq!(projects.config["filters"][0]["key"], json!("status"));
        assert_eq!(projects.config["sorts"][0]["direction"], json!("asc"));
        assert_eq!(projects.config["columns"][0]["key"], json!("title"));
        assert_eq!(projects.config["extras"]["sticky"], json!(true));

        let shorthand = registry.get("table-2").expect("lookup shorthand view");
        assert_eq!(shorthand.kind, BaseViewKind::Table);
        assert_eq!(shorthand.config["filters"], json!([]));
    }

    #[test]
    fn view_registry_rejects_duplicate_view_names_case_insensitively() {
        let document = parse_base_document(
            r#"
views:
  - name: Projects
    type: table
  - name: projects
    type: table
"#,
        )
        .expect("parse document");

        let error =
            BaseViewRegistry::from_document(&document).expect_err("duplicate names should fail");
        assert!(matches!(
            error,
            BaseViewRegistryError::DuplicateViewName { name } if name == "projects"
        ));
    }

    #[test]
    fn table_query_planner_compiles_registry_view_into_query_plan() {
        let document = parse_base_document(
            r#"
views:
  - name: Projects
    type: table
    source: notes/projects
    filters:
      - key: status
        op: eq
        value: active
      - key: assignee
        op: contains
        value: han
    sorts:
      - key: due
        direction: desc
    columns:
      - status
      - due
"#,
        )
        .expect("parse document");
        let registry = BaseViewRegistry::from_document(&document).expect("build registry");

        let plan = BaseTableQueryPlanner
            .compile(
                &registry,
                &TableQueryPlanRequest {
                    view_name: "projects".to_string(),
                    page: 2,
                    page_size: 25,
                },
            )
            .expect("compile query plan");

        assert_eq!(plan.view_name, "Projects");
        assert_eq!(plan.source_prefix.as_deref(), Some("notes/projects"));
        assert_eq!(
            plan.required_property_keys,
            vec![
                "status".to_string(),
                "assignee".to_string(),
                "due".to_string(),
            ]
        );
        assert_eq!(plan.limit, 25);
        assert_eq!(plan.offset, 25);
        assert_eq!(plan.property_queries.len(), 3);
        assert_eq!(plan.property_queries[1].key, "assignee");
        assert_eq!(
            plan.property_queries[1].value_contains.as_deref(),
            Some("han")
        );
        assert_eq!(
            plan.property_queries[2].sort,
            PropertyQuerySortHint::ValueDesc
        );
    }

    #[test]
    fn table_query_planner_rejects_missing_view_and_invalid_pagination() {
        let document = parse_base_document(
            r#"
views:
  - name: Projects
    type: table
"#,
        )
        .expect("parse document");
        let registry = BaseViewRegistry::from_document(&document).expect("build registry");

        let missing_view = BaseTableQueryPlanner
            .compile(
                &registry,
                &TableQueryPlanRequest {
                    view_name: "missing".to_string(),
                    page: 1,
                    page_size: 10,
                },
            )
            .expect_err("missing view should fail");
        assert!(matches!(
            missing_view,
            BaseTableQueryPlanError::ViewNotFound { view_name } if view_name == "missing"
        ));

        let invalid_page = BaseTableQueryPlanner
            .compile(
                &registry,
                &TableQueryPlanRequest {
                    view_name: "Projects".to_string(),
                    page: 0,
                    page_size: 10,
                },
            )
            .expect_err("invalid page should fail");
        assert!(matches!(
            invalid_page,
            BaseTableQueryPlanError::InvalidPagination {
                page: 0,
                page_size: 10
            }
        ));
    }

    #[test]
    fn base_validation_reports_schema_parse_errors_with_diagnostic_code() {
        let diagnostics = validate_base_yaml("views: not-a-list");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code, "bases.parse.invalid_schema");
        assert_eq!(diagnostics[0].severity, BaseDiagnosticSeverity::Error);
        assert_eq!(diagnostics[0].field.as_deref(), Some("views"));
    }

    #[test]
    fn base_validation_reports_duplicate_view_names_and_columns() {
        let document = parse_base_document(
            r#"
views:
  - name: Projects
    type: table
    columns:
      - status
      - status
      - key: due
        hidden: true
  - name: projects
    type: table
    columns:
      - key: due
        hidden: true
"#,
        )
        .expect("parse document");

        let diagnostics = validate_base_document(&document);
        assert_eq!(diagnostics.len(), 3);
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "bases.view.duplicate_name"
                && diagnostic.severity == BaseDiagnosticSeverity::Error
        }));
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "bases.column.duplicate_key"
                && diagnostic.severity == BaseDiagnosticSeverity::Warning
        }));
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "bases.table.all_columns_hidden"
                && diagnostic.severity == BaseDiagnosticSeverity::Warning
        }));
    }

    #[test]
    fn base_validation_accepts_valid_config_json_without_diagnostics() {
        let document = parse_base_document(
            r#"
views:
  - name: Projects
    type: table
    columns:
      - status
      - due
"#,
        )
        .expect("parse document");
        let config_json = serde_json::to_string(&document).expect("serialize config");

        let diagnostics = validate_base_config_json(&config_json);
        assert!(diagnostics.is_empty());
    }
}
