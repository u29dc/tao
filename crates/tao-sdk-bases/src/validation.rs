use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::ast::{BaseAggregateOp, BaseDocument, BaseViewKind};
use crate::evaluator::validate_filter_operand;
use crate::parser::{BaseParseError, parse_base_document};

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

/// Persisted base config decode failures.
#[derive(Debug, Error)]
pub enum BaseConfigDecodeError {
    /// Persisted config JSON could not be decoded at all.
    #[error("failed to decode base config json: {source}")]
    DeserializeConfigJson {
        /// Underlying JSON parse error.
        #[source]
        source: serde_json::Error,
    },
    /// Persisted config JSON did not match a supported payload shape.
    #[error("base config json is not a supported document payload")]
    UnsupportedPayload,
    /// Raw wrapped yaml could not be parsed into a base document.
    #[error("parse base yaml failed: {source}")]
    ParseRawYaml {
        /// Underlying base YAML parse error.
        #[source]
        source: BaseParseError,
    },
}

/// Decode persisted base config JSON into one parsed base document.
pub fn decode_base_config_json(config_json: &str) -> Result<BaseDocument, BaseConfigDecodeError> {
    if let Ok(document) = serde_json::from_str::<BaseDocument>(config_json) {
        return Ok(document);
    }

    let raw_value = serde_json::from_str::<JsonValue>(config_json)
        .map_err(|source| BaseConfigDecodeError::DeserializeConfigJson { source })?;
    let Some(raw_yaml) = raw_value.get("raw").and_then(JsonValue::as_str) else {
        return Err(BaseConfigDecodeError::UnsupportedPayload);
    };

    parse_base_document(raw_yaml).map_err(|source| BaseConfigDecodeError::ParseRawYaml { source })
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
    match decode_base_config_json(config_json) {
        Ok(document) => validate_base_document(&document),
        Err(BaseConfigDecodeError::ParseRawYaml { source }) => vec![parse_error_diagnostic(source)],
        Err(source) => vec![BaseDiagnostic {
            code: "bases.parse.invalid_schema".to_string(),
            severity: BaseDiagnosticSeverity::Error,
            message: source.to_string(),
            field: None,
        }],
    }
}

/// Validate a parsed base document and return normalized diagnostics.
#[must_use]
pub fn validate_base_document(document: &BaseDocument) -> Vec<BaseDiagnostic> {
    let mut diagnostics = Vec::new();
    let mut seen_view_names = HashSet::new();
    if document.views.is_empty() {
        diagnostics.push(semantic_error("views", "at least one view is required"));
    }

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

        let field = format!("views[{view_index}]");
        for filter in &view.filters {
            if let Err(error) = validate_filter_operand(filter.op, &filter.value) {
                diagnostics.push(semantic_error(
                    &format!("{field}.filters"),
                    &format!("invalid operand for '{}': {error:?}", filter.key),
                ));
            }
        }
        for key in view
            .filters
            .iter()
            .map(|f| f.key.as_str())
            .chain(view.columns.iter().map(|c| c.key.as_str()))
            .chain(view.sorts.iter().map(|s| s.key.as_str()))
            .chain(view.group_by.iter().map(String::as_str))
            .chain(view.aggregates.iter().filter_map(|a| a.key.as_deref()))
            .chain(view.relations.iter().map(|r| r.key.as_str()))
            .chain(view.rollups.iter().map(|r| r.target_key.as_str()))
        {
            if key.trim().is_empty() || key.starts_with("formula.") || key.starts_with("file.") {
                diagnostics.push(semantic_error(
                    &field,
                    &format!("unsupported or empty field reference '{key}'"),
                ));
            }
        }
        for key in view.extras.keys() {
            if !matches!(
                key.as_str(),
                "sticky" | "rowHeight" | "columnWidth" | "width" | "height"
            ) {
                diagnostics.push(semantic_error(
                    &format!("{field}.{key}"),
                    &format!("unsupported view field '{key}'"),
                ));
            }
        }
        let mut group_keys = HashSet::new();
        for key in &view.group_by {
            if !group_keys.insert(key.as_str()) {
                diagnostics.push(semantic_error(
                    &format!("{field}.group_by"),
                    &format!("duplicate grouping key '{key}'"),
                ));
            }
        }
        let relation_keys = view
            .relations
            .iter()
            .map(|relation| relation.key.as_str())
            .collect::<HashSet<_>>();
        if relation_keys.len() != view.relations.len() {
            diagnostics.push(semantic_error(
                &format!("{field}.relations"),
                "duplicate relation field",
            ));
        }
        for relation in &view.relations {
            if crate::lexer::property_key(&relation.key).is_none() {
                diagnostics.push(semantic_error(
                    &format!("{field}.relations"),
                    "relations require a note property; qualify reserved names with note.",
                ));
            }
        }
        let mut aliases = HashSet::new();
        for alias in view
            .rollups
            .iter()
            .map(|r| r.alias.as_str())
            .chain(view.aggregates.iter().map(|a| a.alias.as_str()))
        {
            if alias.trim().is_empty()
                || ["note.", "file.", "formula."]
                    .iter()
                    .any(|prefix| alias.starts_with(prefix))
                || crate::lexer::is_file_field(alias)
                || !aliases.insert(alias)
                || group_keys.contains(alias)
                || relation_keys.contains(alias)
            {
                diagnostics.push(semantic_error(
                    &field,
                    &format!("empty, reserved, duplicate, or conflicting output alias '{alias}'"),
                ));
            }
        }
        for aggregate in &view.aggregates {
            if !matches!(aggregate.op, BaseAggregateOp::Count)
                && aggregate
                    .key
                    .as_deref()
                    .is_none_or(|key| key.trim().is_empty())
            {
                diagnostics.push(semantic_error(
                    &format!("{field}.aggregates"),
                    &format!("aggregate '{}' requires a source key", aggregate.alias),
                ));
            }
            if matches!(aggregate.op, BaseAggregateOp::Count) && aggregate.key.is_some() {
                diagnostics.push(semantic_error(
                    &format!("{field}.aggregates"),
                    "count counts rows and does not accept a source key",
                ));
            }
        }
        for rollup in &view.rollups {
            if crate::lexer::property_key(&rollup.target_key).is_none() {
                diagnostics.push(semantic_error(
                    &format!("{field}.rollups"),
                    "rollup targets require a note property; qualify reserved names with note.",
                ));
            }
            if !relation_keys.contains(rollup.relation_key.as_str()) {
                diagnostics.push(semantic_error(
                    &format!("{field}.rollups"),
                    &format!(
                        "rollup '{}' references undeclared relation '{}'",
                        rollup.alias, rollup.relation_key
                    ),
                ));
            }
        }
        if !view.group_by.is_empty() || !view.aggregates.is_empty() {
            let aggregate_aliases = view
                .aggregates
                .iter()
                .map(|aggregate| aggregate.alias.as_str())
                .collect::<HashSet<_>>();
            for sort in &view.sorts {
                if !group_keys.contains(sort.key.as_str())
                    && !aggregate_aliases.contains(sort.key.as_str())
                {
                    diagnostics.push(semantic_error(
                        &format!("{field}.sorts"),
                        &format!(
                            "grouped output cannot sort by unprojected field '{}'",
                            sort.key
                        ),
                    ));
                }
            }
            for filter in &view.filters {
                if aggregate_aliases.contains(filter.key.as_str()) {
                    diagnostics.push(semantic_error(
                        &format!("{field}.filters"),
                        "aggregate filters are unsupported; filters apply before grouping",
                    ));
                }
            }
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
                let column_key = column.key.clone();
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

fn semantic_error(field: &str, message: &str) -> BaseDiagnostic {
    BaseDiagnostic {
        code: "bases.validation.invalid_semantics".to_string(),
        severity: BaseDiagnosticSeverity::Error,
        message: message.to_string(),
        field: Some(field.to_string()),
    }
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
mod semantic_tests {
    use super::*;
    use crate::{BaseTableQueryPlanner, BaseViewRegistry, TableQueryPlanRequest};

    #[test]
    fn unsupported_filters_and_execution_fields_never_validate_cleanly() {
        for yaml in [
            "filters:\n  or: ['note.status == active']\nviews: [table]",
            "filters:\n  and: []\n  not: []\nviews: [table]",
            "formulas: {total: '1+2'}\nviews: [table]",
            "views:\n - name: Test\n   columns: [path]\n   limit: 2",
            "views:\n - name: Test\n   columns: [path]\n   aggregates: [{alias: total, op: sum}]",
            "views:\n - name: Test\n   columns: [path]\n   rollups: [{alias: total, relation: missing, target: amount, op: sum}]",
            "views:\n - name: Test\n   columns: [path]\n   filters: [{key: rank, op: in, value: 2}]",
            "views:\n - name: Test\n   columns: [path]\n   group_by: [team]\n   sorts: [{key: rank}]",
            "filters:\n  and: ['note.rank == 1 || note.rank == 2']\nviews: [table]",
            "filters:\n  and: ['note.rank == 1 + 2']\nviews: [table]",
            "filters:\n  and: ['note.a || note.b.isEmpty()']\nviews: [table]",
            "views:\n - name: Test\n   filters: [{key: status, op: eq, value: active, negate: true}]",
            "views:\n - name: Test\n   sorts: [{key: rank, direction: asc, null_order: last}]",
            "views:\n - name: Test\n   relations: [file.name]",
            "views:\n - name: Test\n   relations: [parent]\n   rollups: [{alias: total, relation: parent, target: file.name, op: min}]",
            "views:\n - name: Test\n   aggregates: [{alias: note.total, op: count}]",
            "views:\n - name: Test\n   aggregates: [{alias: total, op: sum, key: formula.total}]",
            "filters:\n  and: ['file.inFolder(\"Work\")']\nviews: [{name: Test, source: Life}]",
        ] {
            let diagnostics = validate_base_yaml(yaml);
            assert!(
                diagnostics
                    .iter()
                    .any(|d| matches!(d.severity, BaseDiagnosticSeverity::Error)),
                "{yaml}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn root_folder_filters_intersect_view_scopes() {
        let document = parse_base_document("filters:\n  and: ['file.inFolder(\"Work\")', 'file.inFolder(\"Work/Projects\")']\nviews: [{name: Test, source: Work}]").unwrap();
        assert_eq!(document.views[0].source.as_deref(), Some("Work/Projects"));
    }

    #[test]
    fn inferred_columns_preserve_namespaces_and_exclude_target_hydration() {
        let document = parse_base_document("views:\n - name: Test\n   filters: [{key: note.title, op: eq, value: Metadata}]\n   relations: [parent]\n   rollups: [{alias: related_total, relation: parent, target: amount, op: sum}]").unwrap();
        let registry = BaseViewRegistry::from_document(&document).unwrap();
        let plan = BaseTableQueryPlanner
            .compile(
                &registry,
                &TableQueryPlanRequest {
                    view_name: "Test".into(),
                    page: 1,
                    page_size: 10,
                },
            )
            .unwrap();
        assert_eq!(plan.required_property_keys, ["title", "parent"]);
        assert_eq!(
            plan.columns
                .iter()
                .map(|column| column.key.as_str())
                .collect::<Vec<_>>(),
            ["note.title", "parent", "related_total"]
        );
    }

    #[test]
    fn reserved_note_names_and_exact_case_keys_survive_planning() {
        let document = parse_base_document(
            "views:\n - name: Test\n   columns: [file.name, note.title, Status, status]",
        )
        .unwrap();
        let registry = BaseViewRegistry::from_document(&document).unwrap();
        let plan = BaseTableQueryPlanner
            .compile(
                &registry,
                &TableQueryPlanRequest {
                    view_name: "Test".into(),
                    page: 1,
                    page_size: 10,
                },
            )
            .unwrap();
        assert_eq!(plan.required_property_keys, ["title", "Status", "status"]);
        assert_eq!(plan.columns[1].key, "note.title");
    }
}
