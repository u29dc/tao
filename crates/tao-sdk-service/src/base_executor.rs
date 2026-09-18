//! Base table execution, validation, and persistence services.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use rayon::prelude::*;
use rusqlite::Connection;
use serde_json::Value as JsonValue;
use tao_sdk_bases::{
    BaseAggregateOp, BaseAggregateSpec, BaseCoercionMode, BaseColumnConfig, BaseDiagnostic,
    BaseFieldType, BaseFilterClause, BaseRelationSpec, BaseRollupOp, BaseRollupSpec,
    BaseSortClause, BaseSortDirection, TableQueryPlan, coerce_json_value, compare_json_values,
    compare_optional_json_values, evaluate_filter, validate_base_config_json,
};
use tao_sdk_core::{note_extension_from_path, note_folder_from_path, note_title_from_path};
use tao_sdk_links::{LinkCasePolicy, LinkResolutionIndex, LinkTarget, parse_link_target};
use tao_sdk_markdown::LinkSyntax;
use tao_sdk_storage::{BasesRepository, FilesRepository};
use tao_sdk_vault::CasePolicy;
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

/// Reusable preparation scoped to one unchanged database snapshot. Corpus
/// builders discard this context before their next index generation.
#[derive(Debug, Default)]
pub(crate) struct BaseTableExecutionContext {
    relation_targets: Option<(CasePolicy, RelationTargetLookup)>,
}

/// Execution options for base table query plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaseTableExecutionOptions {
    /// Compute summary rows across the filtered result set.
    pub include_summaries: bool,
    /// Coercion mode for typed field normalization.
    pub coercion_mode: BaseCoercionMode,
    /// Path case policy for relation target resolution.
    pub case_policy: CasePolicy,
}

impl Default for BaseTableExecutionOptions {
    fn default() -> Self {
        Self {
            include_summaries: true,
            coercion_mode: BaseCoercionMode::Permissive,
            case_policy: CasePolicy::Sensitive,
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

    /// Materialize a view once, without repeatedly evaluating it for each page.
    /// Used by corpus derivation, whose output needs every matching row.
    pub fn execute_all_with_options(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
        options: BaseTableExecutionOptions,
    ) -> Result<BaseTablePage, BaseTableExecutorError> {
        self.execute_all_with_context(
            connection,
            plan,
            options,
            &mut BaseTableExecutionContext::default(),
        )
    }

    pub(crate) fn execute_all_with_context(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
        options: BaseTableExecutionOptions,
        context: &mut BaseTableExecutionContext,
    ) -> Result<BaseTablePage, BaseTableExecutorError> {
        let mut complete_plan = plan.clone();
        complete_plan.offset = 0;
        complete_plan.limit = usize::MAX;
        self.execute_with_context(connection, &complete_plan, options, context)
    }

    /// Execute one compiled table query plan with explicit execution options.
    pub fn execute_with_options(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
        options: BaseTableExecutionOptions,
    ) -> Result<BaseTablePage, BaseTableExecutorError> {
        self.execute_with_context(
            connection,
            plan,
            options,
            &mut BaseTableExecutionContext::default(),
        )
    }

    fn execute_with_context(
        &self,
        connection: &Connection,
        plan: &TableQueryPlan,
        options: BaseTableExecutionOptions,
        context: &mut BaseTableExecutionContext,
    ) -> Result<BaseTablePage, BaseTableExecutorError> {
        const PARALLEL_CANDIDATE_THRESHOLD: usize = 1_024;
        // The measured rows and the hydrated rows must belong to the same snapshot.
        let _snapshot = if connection.is_autocommit() {
            Some(connection.unchecked_transaction().map_err(|source| {
                BaseTableExecutorError::Sql {
                    operation: "begin_base_read_snapshot",
                    source,
                }
            })?)
        } else {
            None
        };

        if plan.limit == 0 {
            return Err(BaseTableExecutorError::InvalidPlan {
                reason: "limit must be greater than zero".to_string(),
            });
        }

        for filter in &plan.filters {
            tao_sdk_bases::validate_filter_operand(filter.op, &filter.value).map_err(|error| {
                BaseTableExecutorError::InvalidPlan {
                    reason: format!("invalid filter '{}': {error:?}", filter.key),
                }
            })?;
        }
        if let Some(page) = execute_simple_sql_page(connection, plan, options)? {
            return Ok(page);
        }
        let mut candidates = load_table_candidates(
            connection,
            plan.source_prefix.as_deref(),
            options.case_policy,
        )?;
        budget::check_cells(
            if options.include_summaries || !plan.group_by.is_empty() || !plan.aggregates.is_empty()
            {
                candidates.len()
            } else {
                candidates.len().min(plan.limit)
            },
            plan.columns
                .len()
                .max(plan.group_by.len().saturating_add(plan.aggregates.len())),
        )?;
        let candidate_indices = candidates
            .iter()
            .enumerate()
            .map(|(index, row)| (row.file_id.clone(), index))
            .collect::<HashMap<_, _>>();

        load_candidate_properties(
            connection,
            &mut candidates,
            &candidate_indices,
            &plan.required_property_keys,
            options.coercion_mode,
        )?;

        // Relation-independent predicates can discard rows before expensive derived work.
        let derived_keys = plan
            .relations
            .iter()
            .map(|relation| relation.key.as_str())
            .chain(plan.rollups.iter().map(|rollup| rollup.alias.as_str()))
            .collect::<HashSet<_>>();
        let (derived_filters, independent_filters): (Vec<_>, Vec<_>) = plan
            .filters
            .iter()
            .cloned()
            .partition(|filter| derived_keys.contains(filter.key.as_str()));
        candidates = filter_candidates(candidates, &independent_filters)?;

        let mut relation_diagnostics = Vec::new();
        if !plan.relations.is_empty() {
            if context
                .relation_targets
                .as_ref()
                .is_none_or(|(policy, _)| *policy != options.case_policy)
            {
                context.relation_targets = Some((
                    options.case_policy,
                    load_relation_target_lookup(connection, link_case_policy(options.case_policy))?,
                ));
            }
            let targets = &context
                .relation_targets
                .as_ref()
                .expect("initialized relation context")
                .1;
            resolve_relation_fields(
                &mut candidates,
                &plan.relations,
                targets,
                &mut relation_diagnostics,
            );
        }
        if !plan.rollups.is_empty() {
            apply_rollups(connection, &mut candidates, &plan.rollups)?;
        }

        let mut candidates = filter_candidates(candidates, &derived_filters)?;
        let grouped_mode = !plan.group_by.is_empty() || !plan.aggregates.is_empty();
        if !grouped_mode {
            // Select only the requested sorted prefix when summaries do not require an order.
            let end = plan.offset.saturating_add(plan.limit).min(candidates.len());
            if end > 0 && end < candidates.len() {
                candidates.select_nth_unstable_by(end, |left, right| {
                    compare_table_rows(left, right, &plan.sorts)
                });
            }
            let ordered = &mut candidates[..end];
            if ordered.len() >= PARALLEL_CANDIDATE_THRESHOLD {
                ordered.par_sort_unstable_by(|left, right| {
                    compare_table_rows(left, right, &plan.sorts)
                });
            } else {
                ordered
                    .sort_unstable_by(|left, right| compare_table_rows(left, right, &plan.sorts));
            }
        }

        let execution = BaseExecutionMetadata {
            adapter: "base_table".to_string(),
            path: "query-planner".to_string(),
        };
        let (total, summaries, grouping, rows) = if grouped_mode {
            let mut grouped_rows =
                materialize_grouped_rows(&candidates, &plan.group_by, &plan.aggregates)?;
            grouped_rows.sort_by(|left, right| {
                for sort in &plan.sorts {
                    let order = compare_sorted_values(
                        left.values.get(&sort.key),
                        right.values.get(&sort.key),
                        sort,
                    );
                    if !order.is_eq() {
                        return order;
                    }
                }
                left.file_id.cmp(&right.file_id)
            });
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
    fn value(&self, key: &str) -> Option<Cow<'_, JsonValue>> {
        if let Some(key) = key.strip_prefix("note.") {
            return self.properties.get(key).map(Cow::Borrowed);
        }
        if key.eq_ignore_ascii_case("path") || key.eq_ignore_ascii_case("file_path") {
            return Some(Cow::Owned(JsonValue::String(self.file_path.clone())));
        }
        if key.eq_ignore_ascii_case("folder") || key.eq_ignore_ascii_case("file_folder") {
            return Some(Cow::Owned(JsonValue::String(note_folder_from_path(
                &self.file_path,
            ))));
        }
        if key.eq_ignore_ascii_case("ext") || key.eq_ignore_ascii_case("file_ext") {
            return Some(Cow::Owned(JsonValue::String(note_extension_from_path(
                &self.file_path,
            ))));
        }
        if key.eq_ignore_ascii_case("title") {
            return Some(Cow::Owned(JsonValue::String(note_title_from_path(
                &self.file_path,
            ))));
        }

        self.properties.get(key).map(Cow::Borrowed)
    }
    fn lookup_value(&self, key: &str) -> Option<JsonValue> {
        self.value(key).map(Cow::into_owned)
    }
}

fn link_case_policy(case_policy: CasePolicy) -> LinkCasePolicy {
    match case_policy {
        CasePolicy::Sensitive => LinkCasePolicy::Sensitive,
        CasePolicy::Insensitive => LinkCasePolicy::Insensitive,
    }
}

#[derive(Debug, Clone)]
struct RelationTarget {
    file_id: String,
    file_path: String,
}

#[derive(Debug, Clone)]
struct RelationTargetLookup {
    index: LinkResolutionIndex,
    by_path: HashMap<String, RelationTarget>,
}

fn resolve_relation_fields(
    candidates: &mut [TableRowCandidate],
    relations: &[BaseRelationSpec],
    relation_targets: &RelationTargetLookup,
    diagnostics: &mut Vec<BaseRelationDiagnostic>,
) {
    for row in candidates {
        for relation in relations {
            let Some(raw_value) = row.lookup_value(&relation.key) else {
                continue;
            };
            let tokens = extract_relation_tokens(&raw_value);
            if tokens.is_empty() {
                continue;
            }

            let mut resolved_values = Vec::new();
            for token in tokens {
                let Some(target) = parse_relation_target(&token) else {
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

                let resolution = relation_targets
                    .index
                    .resolve_link(&target, Some(&row.file_path));
                if resolution.is_ambiguous {
                    diagnostics.push(BaseRelationDiagnostic {
                        file_id: row.file_id.clone(),
                        file_path: row.file_path.clone(),
                        key: relation.key.clone(),
                        target: token.clone(),
                        reason: "ambiguous_relation_target".to_string(),
                    });
                }
                if let Some(resolved_path) = resolution.resolved_path {
                    if let Some(target) = relation_targets.by_path.get(&resolved_path) {
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
                            target: token.clone(),
                            reason: "relation_target_not_found".to_string(),
                        });
                        resolved_values.push(serde_json::json!({
                            "target": token,
                            "resolved": false,
                            "reason": "relation_target_not_found",
                        }));
                    }
                } else {
                    diagnostics.push(BaseRelationDiagnostic {
                        file_id: row.file_id.clone(),
                        file_path: row.file_path.clone(),
                        key: relation.key.clone(),
                        target: token.clone(),
                        reason: "relation_target_not_found".to_string(),
                    });
                    resolved_values.push(serde_json::json!({
                        "target": token,
                        "resolved": false,
                        "reason": "relation_target_not_found",
                    }));
                }
            }

            row.properties.insert(
                relation
                    .key
                    .strip_prefix("note.")
                    .unwrap_or(&relation.key)
                    .to_string(),
                JsonValue::Array(resolved_values),
            );
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

fn parse_relation_target(raw: &str) -> Option<LinkTarget> {
    let target = parse_link_target(raw, LinkSyntax::Wiki)?;
    if target.invalid_reason.is_some() || (target.path.is_empty() && target.fragment.is_none()) {
        None
    } else {
        Some(target)
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
                        sum_numeric_values(target_file_ids.iter().filter_map(|file_id| {
                            rollup_values
                                .get(&(file_id.clone(), rollup.target_key.clone()))
                                .cloned()
                        }))?
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
                            if candidate.is_null() {
                                continue;
                            }
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
                            if candidate.is_null() {
                                continue;
                            }
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
        .get(relation_key.strip_prefix("note.").unwrap_or(relation_key))
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

fn canonical_group_value(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Number(ref number) if number.as_i64().is_none() && number.as_u64().is_none() => {
            let candidate = number
                .as_f64()
                .and_then(|float| serde_json::Number::from_i128(float as i128))
                .map(JsonValue::Number);
            candidate
                .filter(|candidate| compare_json_values(candidate, &value).is_eq())
                .unwrap_or(value)
        }
        JsonValue::Array(values) => {
            JsonValue::Array(values.into_iter().map(canonical_group_value).collect())
        }
        JsonValue::Object(values) => JsonValue::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_group_value(value)))
                .collect(),
        ),
        _ => value,
    }
}

fn sum_numeric_values(
    values: impl Iterator<Item = JsonValue>,
) -> Result<JsonValue, BaseTableExecutorError> {
    let mut integer_sum = Some(0_i128);
    let mut float_sum = 0.0;
    for value in values.filter(JsonValue::is_number) {
        let integer = value
            .as_i64()
            .map(i128::from)
            .or_else(|| value.as_u64().map(i128::from));
        integer_sum = match (integer_sum, integer) {
            (Some(total), Some(number)) => Some(
                total
                    .checked_add(number)
                    .ok_or(BaseTableExecutorError::NumericOverflow)?,
            ),
            _ => None,
        };
        float_sum += value.as_f64().unwrap_or_default();
    }
    let number = match integer_sum {
        Some(total) => serde_json::Number::from_i128(total),
        None => serde_json::Number::from_f64(float_sum),
    }
    .ok_or(BaseTableExecutorError::NumericOverflow)?;
    Ok(JsonValue::Number(number))
}

fn materialize_grouped_rows(
    rows: &[TableRowCandidate],
    group_by: &[String],
    aggregates: &[BaseAggregateSpec],
) -> Result<Vec<BaseTableRow>, BaseTableExecutorError> {
    let mut groups = std::collections::BTreeMap::<String, Vec<&TableRowCandidate>>::new();

    for row in rows {
        let mut group_values = serde_json::Map::new();
        for key in group_by {
            group_values.insert(
                key.clone(),
                canonical_group_value(row.lookup_value(key).unwrap_or(JsonValue::Null)),
            );
        }
        let group_key = serde_json::to_string(&group_values).unwrap_or_default();
        groups.entry(group_key).or_default().push(row);
    }

    if groups.is_empty() && group_by.is_empty() {
        groups.insert("{}".to_string(), Vec::new());
    }
    groups
        .into_iter()
        .map(|(group_key, members)| {
            let mut values = serde_json::from_str::<serde_json::Map<String, JsonValue>>(&group_key)
                .expect("serialized group object");
            for aggregate in aggregates {
                values.insert(
                    aggregate.alias.clone(),
                    compute_aggregate_value(&members, aggregate)?,
                );
            }

            Ok(BaseTableRow {
                file_id: format!("group_{}", blake3::hash(group_key.as_bytes()).to_hex()),
                file_path: String::new(),
                values,
            })
        })
        .collect()
}

fn compute_aggregate_value(
    rows: &[&TableRowCandidate],
    aggregate: &BaseAggregateSpec,
) -> Result<JsonValue, BaseTableExecutorError> {
    Ok(match aggregate.op {
        BaseAggregateOp::Count => JsonValue::Number(serde_json::Number::from(rows.len() as i64)),
        BaseAggregateOp::Sum => sum_numeric_values(
            rows.iter()
                .filter_map(|row| aggregate.key.as_ref().and_then(|key| row.lookup_value(key))),
        )?,
        BaseAggregateOp::Min => aggregate
            .key
            .as_ref()
            .and_then(|key| {
                rows.iter()
                    .filter_map(|row| row.lookup_value(key))
                    .filter(|value| !value.is_null())
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
                    .filter(|value| !value.is_null())
                    .reduce(|left, right| {
                        if compare_json_values(&left, &right).is_ge() {
                            left
                        } else {
                            right
                        }
                    })
            })
            .unwrap_or(JsonValue::Null),
    })
}

fn filter_candidates(
    rows: Vec<TableRowCandidate>,
    filters: &[BaseFilterClause],
) -> Result<Vec<TableRowCandidate>, BaseTableExecutorError> {
    rows.into_iter()
        .filter_map(|row| {
            for filter in filters {
                match evaluate_filter(row.value(&filter.key).as_deref(), filter.op, &filter.value) {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(error) => {
                        return Some(Err(BaseTableExecutorError::InvalidPlan {
                            reason: format!(
                                "filter '{}' on '{}': {error:?}",
                                filter.key, row.file_path
                            ),
                        }));
                    }
                }
            }
            Some(Ok(row))
        })
        .collect()
}

fn compare_sorted_values(
    left: Option<&JsonValue>,
    right: Option<&JsonValue>,
    sort: &BaseSortClause,
) -> Ordering {
    let ordering = compare_optional_json_values(left, right, sort.null_order);
    // Null positioning is independent of ascending/descending value order.
    if left.is_some_and(|value| !value.is_null())
        && right.is_some_and(|value| !value.is_null())
        && matches!(sort.direction, BaseSortDirection::Desc)
    {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_table_rows(
    left: &TableRowCandidate,
    right: &TableRowCandidate,
    sorts: &[BaseSortClause],
) -> Ordering {
    for sort in sorts {
        let ordering = compare_sorted_values(
            left.value(&sort.key).as_deref(),
            right.value(&sort.key).as_deref(),
            sort,
        );
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
    /// The view is too large to materialize within the bounded execution policy.
    #[error(
        "base work budget exceeded: {resource} {observed} > {limit}; narrow the view source or projected properties and retry"
    )]
    WorkBudgetExceeded {
        /// Resource counted before hydration.
        resource: &'static str,
        /// Required amount.
        observed: u64,
        /// Supported maximum.
        limit: u64,
    },
    /// Numeric aggregation exceeded the supported finite JSON number range.
    #[error("base numeric aggregate is outside the supported JSON number range")]
    NumericOverflow,
    /// Plan payload was invalid for execution.
    #[error("invalid base table plan: {reason}")]
    InvalidPlan {
        /// Validation message.
        reason: String,
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

mod budget;
mod sql;
use sql::{
    execute_simple_sql_page, load_candidate_properties, load_relation_target_lookup,
    load_rollup_property_values, load_table_candidates,
};

mod validation;

pub use validation::{BaseValidationError, BaseValidationResult, BaseValidationService};
