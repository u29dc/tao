//! Base table execution, validation, persistence, and caching services.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use rayon::prelude::*;
use rusqlite::Connection;
use rusqlite::params_from_iter;
use rusqlite::types::Value as SqlValue;
use serde_json::Value as JsonValue;
use tao_sdk_bases::{
    BaseAggregateOp, BaseAggregateSpec, BaseCoercionMode, BaseColumnConfig, BaseDiagnostic,
    BaseDocument, BaseFieldType, BaseFilterClause, BaseRelationSpec, BaseRollupOp, BaseRollupSpec,
    BaseSortClause, BaseSortDirection, TableQueryPlan, coerce_json_value, compare_json_values,
    compare_optional_json_values, evaluate_filter, validate_base_config_json,
};
use tao_sdk_core::{note_extension_from_path, note_folder_from_path, note_title_from_path};
use tao_sdk_links::resolve_target;
use tao_sdk_storage::{BaseRecordInput, BasesRepository, FilesRepository};
use thiserror::Error;

/// One row returned from base table execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseTableRow {
    /// Stable file id.
    pub file_id: String,
    /// Normalized file path.
    pub file_path: String,
    /// Projected column values keyed by column key.
    pub values: serde_json::Map<String, JsonValue>,
}

/// Paged table result from executing one base query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseTableSummary {
    /// Column key.
    pub key: String,
    /// Non-null value count.
    pub count: u64,
    /// Minimum value across matching rows.
    pub min: Option<JsonValue>,
    /// Maximum value across matching rows.
    pub max: Option<JsonValue>,
    /// Average value for numeric cells only.
    pub avg: Option<JsonValue>,
}

/// Grouped output metadata for one base page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BaseGroupingMetadata {
    /// Grouping keys used to materialize grouped rows.
    pub group_by: Vec<String>,
    /// Aggregate aliases included in grouped rows.
    pub aggregate_aliases: Vec<String>,
}

/// Relation resolution diagnostic scoped to base execution.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BaseRelationDiagnostic {
    /// Source row file id.
    pub file_id: String,
    /// Source row file path.
    pub file_path: String,
    /// Relation field key.
    pub key: String,
    /// Target relation token.
    pub target: String,
    /// Stable reason code.
    pub reason: String,
}

/// Execution metadata for one base page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BaseExecutionMetadata {
    /// Planner adapter label.
    pub adapter: String,
    /// Physical path label.
    pub path: String,
}

/// Paged table result from executing one base query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseTablePage {
    /// Total rows that matched filters before pagination.
    pub total: u64,
    /// Summary rows for configured columns over the filtered result set.
    pub summaries: Vec<BaseTableSummary>,
    /// Optional grouping metadata when grouped mode is enabled.
    pub grouping: Option<BaseGroupingMetadata>,
    /// Relation diagnostics scoped to this base execution.
    pub relation_diagnostics: Vec<BaseRelationDiagnostic>,
    /// Execution metadata for planner-backed dispatch.
    pub execution: BaseExecutionMetadata,
    /// Rows in this page.
    pub rows: Vec<BaseTableRow>,
}

/// Executor service that runs compiled base table plans against SQLite metadata.
#[derive(Debug, Default, Clone, Copy)]
pub struct BaseTableExecutorService;

/// Execution options for base table query plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaseTableExecutionOptions {
    /// Compute summary rows across the filtered result set.
    pub include_summaries: bool,
    /// Coercion mode for typed field normalization.
    pub coercion_mode: BaseCoercionMode,
}

impl Default for BaseTableExecutionOptions {
    fn default() -> Self {
        Self {
            include_summaries: true,
            coercion_mode: BaseCoercionMode::Permissive,
        }
    }
}

impl BaseTableExecutorService {
    /// Execute one compiled table query plan and return a paged result.
    pub fn execute(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
    ) -> Result<BaseTablePage, BaseTableExecutorError> {
        self.execute_with_options(connection, plan, BaseTableExecutionOptions::default())
    }

    /// Execute one compiled table query plan with explicit execution options.
    pub fn execute_with_options(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
        options: BaseTableExecutionOptions,
    ) -> Result<BaseTablePage, BaseTableExecutorError> {
        const PARALLEL_CANDIDATE_THRESHOLD: usize = 1_024;

        if plan.limit == 0 {
            return Err(BaseTableExecutorError::InvalidPlan {
                reason: "limit must be greater than zero".to_string(),
            });
        }

        let mut candidates = load_table_candidates(connection, plan.source_prefix.as_deref())?;
        let candidate_indices = candidates
            .iter()
            .enumerate()
            .map(|(index, row)| (row.file_id.clone(), index))
            .collect::<HashMap<_, _>>();

        if !plan.required_property_keys.is_empty() {
            let key_placeholders = (1..=plan.required_property_keys.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let source_param = plan.required_property_keys.len() + 1;
            let like_param = source_param + 1;
            let query = format!(
                r#"
SELECT
  p.file_id,
  p.key,
  p.value_type,
  p.value_json
FROM properties p
INNER JOIN files f ON f.file_id = p.file_id
WHERE f.is_markdown = 1
  AND p.key IN ({key_placeholders})
  AND (
    ?{source_param} IS NULL
    OR f.normalized_path = ?{source_param}
    OR f.normalized_path LIKE ?{like_param}
  )
ORDER BY p.file_id ASC, p.key ASC
"#
            );
            let mut parameters = plan
                .required_property_keys
                .iter()
                .map(|key| SqlValue::Text(key.clone()))
                .collect::<Vec<_>>();
            if let Some(source_prefix) = plan.source_prefix.as_ref() {
                parameters.push(SqlValue::Text(source_prefix.clone()));
                parameters.push(SqlValue::Text(format!("{source_prefix}/%")));
            } else {
                parameters.push(SqlValue::Null);
                parameters.push(SqlValue::Null);
            }

            let mut statement =
                connection
                    .prepare(&query)
                    .map_err(|source| BaseTableExecutorError::Sql {
                        operation: "prepare_property_projection",
                        source,
                    })?;
            let rows = statement
                .query_map(params_from_iter(parameters), |row| {
                    Ok((
                        row.get::<_, String>("file_id")?,
                        row.get::<_, String>("key")?,
                        row.get::<_, String>("value_type")?,
                        row.get::<_, String>("value_json")?,
                    ))
                })
                .map_err(|source| BaseTableExecutorError::Sql {
                    operation: "query_property_projection",
                    source,
                })?;
            for row in rows {
                let (file_id, key, value_type, value_json) =
                    row.map_err(|source| BaseTableExecutorError::Sql {
                        operation: "map_property_projection_row",
                        source,
                    })?;
                let Some(candidate_index) = candidate_indices.get(&file_id).copied() else {
                    continue;
                };
                let value = serde_json::from_str::<JsonValue>(&value_json).map_err(|source| {
                    BaseTableExecutorError::ParsePropertyValue {
                        file_id: file_id.clone(),
                        key: key.clone(),
                        source,
                    }
                })?;
                let value =
                    coerce_json_value(&value, map_field_type(&value_type), options.coercion_mode)
                        .map_err(|source| BaseTableExecutorError::Coercion {
                        file_id: file_id.clone(),
                        key: key.clone(),
                        source: Box::new(source),
                    })?;
                candidates[candidate_index].properties.insert(key, value);
            }
        }

        let mut relation_diagnostics = Vec::new();
        if !plan.relations.is_empty() {
            let targets = load_relation_target_lookup(connection)?;
            resolve_relation_fields(
                &mut candidates,
                &plan.relations,
                &targets,
                &mut relation_diagnostics,
            );
        }
        if !plan.rollups.is_empty() {
            apply_rollups(connection, &mut candidates, &plan.rollups)?;
        }

        let mut candidates = if candidates.len() >= PARALLEL_CANDIDATE_THRESHOLD {
            candidates
                .into_par_iter()
                .filter(|row| row_matches_filters(row, &plan.filters))
                .collect::<Vec<_>>()
        } else {
            candidates
                .into_iter()
                .filter(|row| row_matches_filters(row, &plan.filters))
                .collect::<Vec<_>>()
        };

        if candidates.len() >= PARALLEL_CANDIDATE_THRESHOLD {
            candidates
                .par_sort_unstable_by(|left, right| compare_table_rows(left, right, &plan.sorts));
        } else {
            candidates.sort_by(|left, right| compare_table_rows(left, right, &plan.sorts));
        }

        let execution = BaseExecutionMetadata {
            adapter: "base_table".to_string(),
            path: "query-planner".to_string(),
        };
        let grouped_mode = !plan.group_by.is_empty() || !plan.aggregates.is_empty();
        let (total, summaries, grouping, rows) = if grouped_mode {
            let grouped_rows =
                materialize_grouped_rows(&candidates, &plan.group_by, &plan.aggregates);
            let total = grouped_rows.len() as u64;
            let rows = grouped_rows
                .into_iter()
                .skip(plan.offset)
                .take(plan.limit)
                .collect::<Vec<_>>();
            let grouping = Some(BaseGroupingMetadata {
                group_by: plan.group_by.clone(),
                aggregate_aliases: plan
                    .aggregates
                    .iter()
                    .map(|aggregate| aggregate.alias.clone())
                    .collect(),
            });
            (total, Vec::new(), grouping, rows)
        } else {
            let total = candidates.len() as u64;
            let summaries = if options.include_summaries {
                compute_table_summaries(&candidates, &plan.columns)
            } else {
                Vec::new()
            };
            let rows = candidates
                .into_iter()
                .skip(plan.offset)
                .take(plan.limit)
                .map(|row| project_table_row(row, &plan.columns))
                .collect::<Vec<_>>();
            (total, summaries, None, rows)
        };

        Ok(BaseTablePage {
            total,
            summaries,
            grouping,
            relation_diagnostics,
            execution,
            rows,
        })
    }
}

#[derive(Debug, Clone)]
struct TableRowCandidate {
    file_id: String,
    file_path: String,
    properties: HashMap<String, JsonValue>,
}

impl TableRowCandidate {
    fn lookup_value(&self, key: &str) -> Option<JsonValue> {
        if key.eq_ignore_ascii_case("path") || key.eq_ignore_ascii_case("file_path") {
            return Some(JsonValue::String(self.file_path.clone()));
        }
        if key.eq_ignore_ascii_case("folder") || key.eq_ignore_ascii_case("file_folder") {
            return Some(JsonValue::String(note_folder_from_path(&self.file_path)));
        }
        if key.eq_ignore_ascii_case("ext") || key.eq_ignore_ascii_case("file_ext") {
            return Some(JsonValue::String(note_extension_from_path(&self.file_path)));
        }
        if key.eq_ignore_ascii_case("title") {
            return Some(JsonValue::String(note_title_from_path(&self.file_path)));
        }

        self.properties.get(key).cloned()
    }
}

fn map_field_type(value_type: &str) -> BaseFieldType {
    match value_type.trim().to_ascii_lowercase().as_str() {
        "number" | "int" | "integer" | "float" | "double" => BaseFieldType::Number,
        "bool" | "boolean" | "checkbox" => BaseFieldType::Bool,
        "date" | "datetime" => BaseFieldType::Date,
        "json" | "object" | "array" => BaseFieldType::Json,
        _ => BaseFieldType::String,
    }
}

#[derive(Debug, Clone)]
struct RelationTarget {
    file_id: String,
    file_path: String,
}

#[derive(Debug, Clone)]
struct RelationTargetLookup {
    candidates: Vec<String>,
    by_path: HashMap<String, RelationTarget>,
}

fn load_relation_target_lookup(
    connection: &Connection,
) -> Result<RelationTargetLookup, BaseTableExecutorError> {
    let mut statement = connection
        .prepare(
            r#"
SELECT file_id, normalized_path
FROM files
WHERE is_markdown = 1
ORDER BY normalized_path ASC
"#,
        )
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "prepare_relation_lookup",
            source,
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>("file_id")?,
                row.get::<_, String>("normalized_path")?,
            ))
        })
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "query_relation_lookup",
            source,
        })?;

    let mut lookup = HashMap::new();
    let mut candidates = Vec::new();
    for row in rows {
        let (file_id, file_path) = row.map_err(|source| BaseTableExecutorError::Sql {
            operation: "map_relation_lookup_row",
            source,
        })?;
        let target = RelationTarget {
            file_id: file_id.clone(),
            file_path: file_path.clone(),
        };
        candidates.push(file_path.clone());
        lookup.insert(file_path.clone(), target.clone());
        lookup.insert(file_path.to_ascii_lowercase(), target);
    }

    candidates.sort();
    candidates.dedup();

    Ok(RelationTargetLookup {
        candidates,
        by_path: lookup,
    })
}

fn resolve_relation_fields(
    candidates: &mut [TableRowCandidate],
    relations: &[BaseRelationSpec],
    relation_targets: &RelationTargetLookup,
    diagnostics: &mut Vec<BaseRelationDiagnostic>,
) {
    for row in candidates {
        for relation in relations {
            let Some(raw_value) = row.properties.get(&relation.key).cloned() else {
                continue;
            };
            let tokens = extract_relation_tokens(&raw_value);
            if tokens.is_empty() {
                continue;
            }

            let mut resolved_values = Vec::new();
            for token in tokens {
                let Some(normalized_target) = normalize_relation_token(&token) else {
                    diagnostics.push(BaseRelationDiagnostic {
                        file_id: row.file_id.clone(),
                        file_path: row.file_path.clone(),
                        key: relation.key.clone(),
                        target: token.clone(),
                        reason: "invalid_relation_token".to_string(),
                    });
                    resolved_values.push(serde_json::json!({
                        "target": token,
                        "resolved": false,
                        "reason": "invalid_relation_token",
                    }));
                    continue;
                };

                let resolution = resolve_target(
                    &normalized_target,
                    Some(&row.file_path),
                    &relation_targets.candidates,
                );
                if let Some(resolved_path) = resolution.resolved_path {
                    let lookup_key = resolved_path.to_ascii_lowercase();
                    if let Some(target) = relation_targets
                        .by_path
                        .get(&resolved_path)
                        .or_else(|| relation_targets.by_path.get(&lookup_key))
                    {
                        resolved_values.push(serde_json::json!({
                            "file_id": target.file_id,
                            "path": target.file_path,
                            "resolved": true,
                        }));
                    } else {
                        diagnostics.push(BaseRelationDiagnostic {
                            file_id: row.file_id.clone(),
                            file_path: row.file_path.clone(),
                            key: relation.key.clone(),
                            target: normalized_target.clone(),
                            reason: "relation_target_not_found".to_string(),
                        });
                        resolved_values.push(serde_json::json!({
                            "target": normalized_target,
                            "resolved": false,
                            "reason": "relation_target_not_found",
                        }));
                    }
                } else {
                    diagnostics.push(BaseRelationDiagnostic {
                        file_id: row.file_id.clone(),
                        file_path: row.file_path.clone(),
                        key: relation.key.clone(),
                        target: normalized_target.clone(),
                        reason: "relation_target_not_found".to_string(),
                    });
                    resolved_values.push(serde_json::json!({
                        "target": normalized_target,
                        "resolved": false,
                        "reason": "relation_target_not_found",
                    }));
                }
            }

            row.properties
                .insert(relation.key.clone(), JsonValue::Array(resolved_values));
        }
    }
}

fn extract_relation_tokens(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::String(value) => vec![value.clone()],
        JsonValue::Array(values) => values
            .iter()
            .flat_map(extract_relation_tokens)
            .collect::<Vec<_>>(),
        JsonValue::Object(map) => map
            .get("path")
            .and_then(JsonValue::as_str)
            .map(|value| vec![value.to_string()])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn normalize_relation_token(raw: &str) -> Option<String> {
    let mut normalized = raw.trim();
    if normalized.is_empty() {
        return None;
    }
    if let Some(inner) = normalized
        .strip_prefix("[[")
        .and_then(|value| value.strip_suffix("]]"))
    {
        normalized = inner.trim();
    }
    if let Some((before_pipe, _)) = normalized.split_once('|') {
        normalized = before_pipe.trim();
    }
    if let Some((before_fragment, _)) = normalized.split_once('#') {
        normalized = before_fragment.trim();
    }
    normalized = normalized.trim_start_matches('/');
    if normalized.is_empty() {
        return None;
    }
    let normalized = normalized.replace('\\', "/");
    if normalized.to_ascii_lowercase().ends_with(".md") {
        Some(normalized)
    } else {
        Some(format!("{normalized}.md"))
    }
}

fn apply_rollups(
    connection: &Connection,
    candidates: &mut [TableRowCandidate],
    rollups: &[BaseRollupSpec],
) -> Result<(), BaseTableExecutorError> {
    let mut target_file_ids = HashSet::new();
    let mut target_keys = HashSet::new();
    for row in candidates.iter() {
        for rollup in rollups {
            target_keys.insert(rollup.target_key.clone());
            for target_file_id in relation_target_file_ids(row, &rollup.relation_key) {
                target_file_ids.insert(target_file_id);
            }
        }
    }

    let rollup_values = load_rollup_property_values(connection, &target_file_ids, &target_keys)?;

    for row in candidates.iter_mut() {
        for rollup in rollups {
            let target_file_ids = relation_target_file_ids(row, &rollup.relation_key);
            let value =
                match rollup.op {
                    BaseRollupOp::Count => {
                        JsonValue::Number(serde_json::Number::from(target_file_ids.len() as i64))
                    }
                    BaseRollupOp::Sum => {
                        let total = target_file_ids
                            .iter()
                            .filter_map(|file_id| {
                                rollup_values
                                    .get(&(file_id.clone(), rollup.target_key.clone()))
                                    .and_then(JsonValue::as_f64)
                            })
                            .sum::<f64>();
                        serde_json::Number::from_f64(total)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null)
                    }
                    BaseRollupOp::Min => {
                        let mut min: Option<JsonValue> = None;
                        for file_id in &target_file_ids {
                            let Some(candidate) = rollup_values
                                .get(&(file_id.clone(), rollup.target_key.clone()))
                                .cloned()
                            else {
                                continue;
                            };
                            if min.as_ref().is_none_or(|current| {
                                compare_json_values(&candidate, current).is_lt()
                            }) {
                                min = Some(candidate);
                            }
                        }
                        min.unwrap_or(JsonValue::Null)
                    }
                    BaseRollupOp::Max => {
                        let mut max: Option<JsonValue> = None;
                        for file_id in &target_file_ids {
                            let Some(candidate) = rollup_values
                                .get(&(file_id.clone(), rollup.target_key.clone()))
                                .cloned()
                            else {
                                continue;
                            };
                            if max.as_ref().is_none_or(|current| {
                                compare_json_values(&candidate, current).is_gt()
                            }) {
                                max = Some(candidate);
                            }
                        }
                        max.unwrap_or(JsonValue::Null)
                    }
                };
            row.properties.insert(rollup.alias.clone(), value);
        }
    }

    Ok(())
}

fn relation_target_file_ids(row: &TableRowCandidate, relation_key: &str) -> Vec<String> {
    row.properties
        .get(relation_key)
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|entry| {
                    if !entry
                        .get("resolved")
                        .and_then(JsonValue::as_bool)
                        .unwrap_or(false)
                    {
                        return None;
                    }
                    entry
                        .get("file_id")
                        .and_then(JsonValue::as_str)
                        .map(|value| value.to_string())
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn load_rollup_property_values(
    connection: &Connection,
    file_ids: &HashSet<String>,
    keys: &HashSet<String>,
) -> Result<HashMap<(String, String), JsonValue>, BaseTableExecutorError> {
    if file_ids.is_empty() || keys.is_empty() {
        return Ok(HashMap::new());
    }

    let file_ids = file_ids.iter().cloned().collect::<Vec<_>>();
    let keys = keys.iter().cloned().collect::<Vec<_>>();

    let file_placeholders = (1..=file_ids.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let key_placeholders = ((file_ids.len() + 1)..=(file_ids.len() + keys.len()))
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        r#"
SELECT file_id, key, value_type, value_json
FROM properties
WHERE file_id IN ({file_placeholders})
  AND key IN ({key_placeholders})
ORDER BY file_id ASC, key ASC
"#
    );

    let mut parameters = Vec::with_capacity(file_ids.len() + keys.len());
    parameters.extend(
        file_ids
            .iter()
            .map(|file_id| SqlValue::Text(file_id.clone())),
    );
    parameters.extend(keys.iter().map(|key| SqlValue::Text(key.clone())));

    let mut statement =
        connection
            .prepare(&query)
            .map_err(|source| BaseTableExecutorError::Sql {
                operation: "prepare_rollup_projection",
                source,
            })?;
    let rows = statement
        .query_map(params_from_iter(parameters), |row| {
            Ok((
                row.get::<_, String>("file_id")?,
                row.get::<_, String>("key")?,
                row.get::<_, String>("value_type")?,
                row.get::<_, String>("value_json")?,
            ))
        })
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "query_rollup_projection",
            source,
        })?;

    let mut values = HashMap::new();
    for row in rows {
        let (file_id, key, value_type, value_json) =
            row.map_err(|source| BaseTableExecutorError::Sql {
                operation: "map_rollup_projection_row",
                source,
            })?;
        let value = serde_json::from_str::<JsonValue>(&value_json).map_err(|source| {
            BaseTableExecutorError::ParsePropertyValue {
                file_id: file_id.clone(),
                key: key.clone(),
                source,
            }
        })?;
        let value = coerce_json_value(
            &value,
            map_field_type(&value_type),
            BaseCoercionMode::Permissive,
        )
        .map_err(|source| BaseTableExecutorError::Coercion {
            file_id: file_id.clone(),
            key: key.clone(),
            source: Box::new(source),
        })?;
        values.insert((file_id, key), value);
    }

    Ok(values)
}

fn materialize_grouped_rows(
    rows: &[TableRowCandidate],
    group_by: &[String],
    aggregates: &[BaseAggregateSpec],
) -> Vec<BaseTableRow> {
    let mut groups = std::collections::BTreeMap::<String, Vec<&TableRowCandidate>>::new();

    for row in rows {
        let mut group_values = serde_json::Map::new();
        for key in group_by {
            group_values.insert(
                key.clone(),
                row.lookup_value(key).unwrap_or(JsonValue::Null),
            );
        }
        let group_key = serde_json::to_string(&group_values).unwrap_or_default();
        groups.entry(group_key).or_default().push(row);
    }

    groups
        .into_values()
        .map(|members| {
            let anchor = members[0];
            let mut values = serde_json::Map::new();
            for key in group_by {
                values.insert(
                    key.clone(),
                    anchor.lookup_value(key).unwrap_or(JsonValue::Null),
                );
            }
            for aggregate in aggregates {
                values.insert(
                    aggregate.alias.clone(),
                    compute_aggregate_value(&members, aggregate),
                );
            }

            BaseTableRow {
                file_id: anchor.file_id.clone(),
                file_path: anchor.file_path.clone(),
                values,
            }
        })
        .collect()
}

fn compute_aggregate_value(
    rows: &[&TableRowCandidate],
    aggregate: &BaseAggregateSpec,
) -> JsonValue {
    match aggregate.op {
        BaseAggregateOp::Count => JsonValue::Number(serde_json::Number::from(rows.len() as i64)),
        BaseAggregateOp::Sum => {
            let total = aggregate
                .key
                .as_ref()
                .map(|key| {
                    rows.iter()
                        .filter_map(|row| row.lookup_value(key).and_then(|value| value.as_f64()))
                        .sum::<f64>()
                })
                .unwrap_or(0.0);
            serde_json::Number::from_f64(total)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        BaseAggregateOp::Min => aggregate
            .key
            .as_ref()
            .and_then(|key| {
                rows.iter()
                    .filter_map(|row| row.lookup_value(key))
                    .reduce(|left, right| {
                        if compare_json_values(&left, &right).is_le() {
                            left
                        } else {
                            right
                        }
                    })
            })
            .unwrap_or(JsonValue::Null),
        BaseAggregateOp::Max => aggregate
            .key
            .as_ref()
            .and_then(|key| {
                rows.iter()
                    .filter_map(|row| row.lookup_value(key))
                    .reduce(|left, right| {
                        if compare_json_values(&left, &right).is_ge() {
                            left
                        } else {
                            right
                        }
                    })
            })
            .unwrap_or(JsonValue::Null),
    }
}

fn load_table_candidates(
    connection: &Connection,
    source_prefix: Option<&str>,
) -> Result<Vec<TableRowCandidate>, BaseTableExecutorError> {
    let (query, params): (&str, Vec<SqlValue>) = if let Some(prefix) = source_prefix {
        (
            r#"
SELECT
  file_id,
  normalized_path
FROM files
WHERE is_markdown = 1
  AND (normalized_path = ?1 OR normalized_path LIKE ?2)
ORDER BY normalized_path ASC
"#,
            vec![
                SqlValue::Text(prefix.to_string()),
                SqlValue::Text(format!("{prefix}/%")),
            ],
        )
    } else {
        (
            r#"
SELECT
  file_id,
  normalized_path
FROM files
WHERE is_markdown = 1
ORDER BY normalized_path ASC
"#,
            Vec::new(),
        )
    };

    let mut statement =
        connection
            .prepare(query)
            .map_err(|source| BaseTableExecutorError::Sql {
                operation: "prepare_table_candidate_files",
                source,
            })?;
    let rows = statement
        .query_map(params_from_iter(params), |row| {
            Ok(TableRowCandidate {
                file_id: row.get("file_id")?,
                file_path: row.get("normalized_path")?,
                properties: HashMap::new(),
            })
        })
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "query_table_candidate_files",
            source,
        })?;

    rows.map(|row| {
        row.map_err(|source| BaseTableExecutorError::Sql {
            operation: "map_table_candidate_files_row",
            source,
        })
    })
    .collect()
}

fn row_matches_filters(row: &TableRowCandidate, filters: &[BaseFilterClause]) -> bool {
    filters.iter().all(|filter| row_matches_filter(row, filter))
}

fn row_matches_filter(row: &TableRowCandidate, filter: &BaseFilterClause) -> bool {
    evaluate_filter(
        row.lookup_value(&filter.key).as_ref(),
        filter.op,
        &filter.value,
    )
    .unwrap_or(false)
}

fn compare_table_rows(
    left: &TableRowCandidate,
    right: &TableRowCandidate,
    sorts: &[BaseSortClause],
) -> Ordering {
    for sort in sorts {
        let ordering = compare_optional_json_values(
            left.lookup_value(&sort.key).as_ref(),
            right.lookup_value(&sort.key).as_ref(),
            sort.null_order,
        );
        let ordering = match sort.direction {
            BaseSortDirection::Asc => ordering,
            BaseSortDirection::Desc => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    left.file_path
        .cmp(&right.file_path)
        .then_with(|| left.file_id.cmp(&right.file_id))
}

fn project_table_row(row: TableRowCandidate, columns: &[BaseColumnConfig]) -> BaseTableRow {
    let mut values = serde_json::Map::new();
    for column in columns {
        values.insert(
            column.key.clone(),
            row.lookup_value(&column.key).unwrap_or(JsonValue::Null),
        );
    }

    BaseTableRow {
        file_id: row.file_id,
        file_path: row.file_path,
        values,
    }
}

fn compute_table_summaries(
    rows: &[TableRowCandidate],
    columns: &[BaseColumnConfig],
) -> Vec<BaseTableSummary> {
    const PARALLEL_SUMMARY_ROW_THRESHOLD: usize = 1_024;
    const PARALLEL_SUMMARY_COLUMN_THRESHOLD: usize = 3;

    if rows.len() >= PARALLEL_SUMMARY_ROW_THRESHOLD
        && columns.len() >= PARALLEL_SUMMARY_COLUMN_THRESHOLD
    {
        columns
            .par_iter()
            .map(|column| compute_column_summary(rows, column))
            .collect()
    } else {
        columns
            .iter()
            .map(|column| compute_column_summary(rows, column))
            .collect()
    }
}

fn compute_column_summary(
    rows: &[TableRowCandidate],
    column: &BaseColumnConfig,
) -> BaseTableSummary {
    let mut count = 0_u64;
    let mut min: Option<JsonValue> = None;
    let mut max: Option<JsonValue> = None;
    let mut numeric_sum = 0_f64;
    let mut numeric_count = 0_u64;

    for row in rows {
        let Some(value) = row.lookup_value(&column.key) else {
            continue;
        };
        if value.is_null() {
            continue;
        }

        count += 1;
        if min
            .as_ref()
            .is_none_or(|current| compare_json_values(&value, current).is_lt())
        {
            min = Some(value.clone());
        }
        if max
            .as_ref()
            .is_none_or(|current| compare_json_values(&value, current).is_gt())
        {
            max = Some(value.clone());
        }
        if let Some(number) = value.as_f64() {
            numeric_sum += number;
            numeric_count += 1;
        }
    }

    let avg = if numeric_count > 0 {
        serde_json::Number::from_f64(numeric_sum / (numeric_count as f64)).map(JsonValue::Number)
    } else {
        None
    };

    BaseTableSummary {
        key: column.key.clone(),
        count,
        min,
        max,
        avg,
    }
}

/// Base table execution failures.
#[derive(Debug, Error)]
pub enum BaseTableExecutorError {
    /// Plan payload was invalid for execution.
    #[error("invalid base table plan: {reason}")]
    InvalidPlan {
        /// Validation message.
        reason: String,
    },
    /// Listing file rows failed.
    #[error("failed to list file metadata for base table execution: {source}")]
    FilesRepository {
        /// Repository error.
        #[source]
        source: tao_sdk_storage::FilesRepositoryError,
    },
    /// Listing property rows by key failed.
    #[error("failed to list property rows for key '{key}' during base table execution: {source}")]
    PropertiesRepository {
        /// Property key.
        key: String,
        /// Repository error.
        #[source]
        source: tao_sdk_storage::PropertiesRepositoryError,
    },
    /// SQL execution failed during property projection.
    #[error("base table property projection sql operation '{operation}' failed: {source}")]
    Sql {
        /// SQL operation label.
        operation: &'static str,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },
    /// Stored property JSON payload could not be decoded.
    #[error("failed to parse property json for file '{file_id}' key '{key}': {source}")]
    ParsePropertyValue {
        /// File id.
        file_id: String,
        /// Property key.
        key: String,
        /// JSON parse error.
        #[source]
        source: serde_json::Error,
    },
    /// Typed coercion failed for one property value.
    #[error("failed to coerce property value for file '{file_id}' key '{key}': {source}")]
    Coercion {
        /// File id.
        file_id: String,
        /// Property key.
        key: String,
        /// Coercion error payload.
        #[source]
        source: Box<tao_sdk_bases::BaseCoercionError>,
    },
}

mod cache;
mod persistence;
mod validation;

pub use cache::{BaseTableCacheError, BaseTableCachedQueryService};
pub use persistence::{
    BaseColumnConfigPersistError, BaseColumnConfigPersistResult, BaseColumnConfigPersistenceService,
};
pub use validation::{BaseValidationError, BaseValidationResult, BaseValidationService};
