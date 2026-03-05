use serde::{Deserialize, Serialize};
use serde_json::Map as JsonMap;

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
    #[serde(default)]
    pub filters: Vec<BaseFilterClause>,
    /// Sort clauses.
    #[serde(default)]
    pub sorts: Vec<BaseSortClause>,
    /// Column configuration.
    #[serde(default)]
    pub columns: Vec<BaseColumnConfig>,
    /// Group-by key list.
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Aggregate projection definitions.
    #[serde(default)]
    pub aggregates: Vec<BaseAggregateSpec>,
    /// Relation field definitions.
    #[serde(default)]
    pub relations: Vec<BaseRelationSpec>,
    /// Rollup field definitions.
    #[serde(default)]
    pub rollups: Vec<BaseRollupSpec>,
    /// Unknown keys preserved for forward compatibility.
    #[serde(default)]
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
    /// String ends with suffix.
    EndsWith,
}

/// One sort clause in a table view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseSortClause {
    /// Property key to sort by.
    pub key: String,
    /// Sort direction.
    pub direction: BaseSortDirection,
    /// Null ordering policy for this sort key.
    #[serde(default)]
    pub null_order: BaseNullOrder,
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

/// Null ordering policy for sort operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BaseNullOrder {
    /// Place null values first.
    #[default]
    First,
    /// Place null values last.
    Last,
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

/// Aggregate projection definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseAggregateSpec {
    /// Aggregate output field alias.
    pub alias: String,
    /// Aggregate operation.
    pub op: BaseAggregateOp,
    /// Optional source key (omitted for `count`).
    pub key: Option<String>,
}

/// Supported aggregate operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseAggregateOp {
    Count,
    Sum,
    Min,
    Max,
}

/// Relation field declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseRelationSpec {
    /// Property key containing relation links.
    pub key: String,
}

/// Rollup field declaration over a relation field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseRollupSpec {
    /// Output alias.
    pub alias: String,
    /// Relation field key.
    pub relation_key: String,
    /// Target property key in related note.
    pub target_key: String,
    /// Rollup operation.
    pub op: BaseRollupOp,
}

/// Supported rollup operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseRollupOp {
    Count,
    Sum,
    Min,
    Max,
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
    /// Group-by key list.
    pub group_by: Vec<String>,
    /// Aggregate definitions.
    pub aggregates: Vec<BaseAggregateSpec>,
    /// Relation definitions.
    pub relations: Vec<BaseRelationSpec>,
    /// Rollup definitions.
    pub rollups: Vec<BaseRollupSpec>,
    /// Query limit.
    pub limit: usize,
    /// Query offset.
    pub offset: usize,
    /// Per-key property query hints for executor layer wiring.
    pub property_queries: Vec<PropertyQueryPlanRequest>,
}
