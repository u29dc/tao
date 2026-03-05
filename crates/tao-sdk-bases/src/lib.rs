//! `.base` parsing and typed document models.

pub mod ast;
pub mod evaluator;
pub mod lexer;
pub mod parser;
pub mod planner;
pub mod types;
pub mod validation;

pub use ast::{
    BaseAggregateOp, BaseAggregateSpec, BaseColumnConfig, BaseDocument, BaseFilterClause,
    BaseFilterOp, BaseNullOrder, BaseRelationSpec, BaseRollupOp, BaseRollupSpec, BaseSortClause,
    BaseSortDirection, BaseViewDefinition, BaseViewKind, BaseViewRegistryEntry,
    PropertyQueryPlanRequest, PropertyQuerySortHint, TableQueryPlan, TableQueryPlanRequest,
};
pub use evaluator::{
    BaseEvalError, compare_json_values, compare_optional_json_values, evaluate_filter,
    json_scalar_to_string,
};
pub use parser::{BaseParseError, parse_base_document};
pub use planner::{
    BaseTableQueryPlanError, BaseTableQueryPlanner, BaseViewRegistry, BaseViewRegistryError,
};
pub use types::{BaseCoercionError, BaseCoercionMode, BaseFieldType, coerce_json_value};
pub use validation::{
    BaseDiagnostic, BaseDiagnosticSeverity, validate_base_config_json, validate_base_document,
    validate_base_yaml,
};

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        BaseAggregateOp, BaseAggregateSpec, BaseColumnConfig, BaseDiagnosticSeverity,
        BaseFilterClause, BaseFilterOp, BaseNullOrder, BaseParseError, BaseRelationSpec,
        BaseRollupOp, BaseRollupSpec, BaseSortClause, BaseSortDirection, BaseTableQueryPlanError,
        BaseTableQueryPlanner, BaseViewKind, BaseViewRegistry, BaseViewRegistryError,
        PropertyQuerySortHint, TableQueryPlanRequest, parse_base_document,
        validate_base_config_json, validate_base_document, validate_base_yaml,
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
                null_order: BaseNullOrder::First,
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
                    null_order: BaseNullOrder::First,
                },
                BaseSortClause {
                    key: "title".to_string(),
                    direction: BaseSortDirection::Asc,
                    null_order: BaseNullOrder::First,
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
    fn parse_base_document_parses_grouping_relation_rollup_fields() {
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
        .expect("parse document");

        let table = &document.views[0];
        assert_eq!(table.group_by, vec!["status".to_string()]);
        assert_eq!(
            table.aggregates,
            vec![
                BaseAggregateSpec {
                    alias: "total".to_string(),
                    op: BaseAggregateOp::Count,
                    key: None,
                },
                BaseAggregateSpec {
                    alias: "priority_sum".to_string(),
                    op: BaseAggregateOp::Sum,
                    key: Some("priority".to_string()),
                },
            ]
        );
        assert_eq!(
            table.relations,
            vec![BaseRelationSpec {
                key: "meetings".to_string(),
            }]
        );
        assert_eq!(
            table.rollups,
            vec![BaseRollupSpec {
                alias: "meeting_count".to_string(),
                relation_key: "meetings".to_string(),
                target_key: "priority".to_string(),
                op: BaseRollupOp::Count,
            }]
        );
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
