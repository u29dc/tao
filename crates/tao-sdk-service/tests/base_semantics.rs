//! End-to-end Bases regressions use real Markdown ingestion, not hand-built JSON rows.
use std::fs;
use std::path::Path;

use rusqlite::Connection;
use serde_json::{Value, json};
use tao_sdk_bases::{
    BaseCoercionMode, BaseTableQueryPlanner, BaseViewRegistry, TableQueryPlan,
    TableQueryPlanRequest, parse_base_document,
};
use tao_sdk_service::{
    BaseTableExecutionOptions, BaseTableExecutorError, BaseTableExecutorService, BaseTablePage,
    FullIndexService, PropertyQueryRequest, PropertyQueryService, PropertyQuerySort,
};
use tao_sdk_storage::run_migrations;
use tao_sdk_vault::CasePolicy;

fn ingest(notes: &[(&str, &str)]) -> (tempfile::TempDir, Connection) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.benchmarks");
    fs::create_dir_all(&root).unwrap();
    let temp = tempfile::Builder::new()
        .prefix("base-semantics-")
        .tempdir_in(root)
        .unwrap();
    for (path, content) in notes {
        let path = temp.path().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }
    let mut connection = Connection::open_in_memory().unwrap();
    run_migrations(&mut connection).unwrap();
    FullIndexService::default()
        .rebuild(temp.path(), &mut connection, CasePolicy::Sensitive)
        .unwrap();
    (temp, connection)
}

fn plan(yaml: &str) -> TableQueryPlan {
    let document = parse_base_document(yaml).unwrap();
    let registry = BaseViewRegistry::from_document(&document).unwrap();
    BaseTableQueryPlanner
        .compile(
            &registry,
            &TableQueryPlanRequest {
                view_name: "Test".into(),
                page: 1,
                page_size: 50,
            },
        )
        .unwrap()
}

fn options() -> BaseTableExecutionOptions {
    BaseTableExecutionOptions {
        include_summaries: false,
        coercion_mode: BaseCoercionMode::Permissive,
        case_policy: CasePolicy::Sensitive,
    }
}

fn execute(connection: &Connection, yaml: &str) -> BaseTablePage {
    BaseTableExecutorService
        .execute_with_options(connection, &plan(yaml), options())
        .unwrap()
}

#[test]
fn numeric_filters_namespaces_and_exact_property_case_after_ingestion() {
    let (_temp, connection) = ingest(&[(
        "one.md",
        "---\nrank: 1\nlarge: 9007199254740993\ntitle: Metadata title\nStatus: upper\nstatus: lower\n---\n# One\n",
    )]);
    for (op, value) in [("eq", "1.0"), ("in", "[1.0]")] {
        let page = execute(
            &connection,
            &format!(
                "views:\n - name: Test\n   columns: [file.name, note.title, Status, status, large]\n   filters: [{{key: rank, op: {op}, value: {value}}}]"
            ),
        );
        assert_eq!(page.total, 1);
        assert_eq!(page.rows[0].values["title"], "one");
        assert_eq!(page.rows[0].values["note.title"], "Metadata title");
        assert_eq!(page.rows[0].values["Status"], "upper");
        assert_eq!(page.rows[0].values["status"], "lower");
        assert_eq!(
            page.rows[0].values["large"],
            json!(9_007_199_254_740_993_u64)
        );
    }
    let page = execute(
        &connection,
        "views:\n - name: Test\n   columns: [large]\n   filters: [{key: large, op: eq, value: 9007199254740992.0}]",
    );
    assert_eq!(page.total, 0);
}

#[test]
fn literal_source_scope_and_sql_paging_equal_fallback() {
    let (_temp, connection) = ingest(&[
        ("A_B%/a.md", "---\nstatus: active\n---\n"),
        ("A_B%/b.md", "---\nstatus: active\n---\n"),
        ("AXBextra/c.md", "---\nstatus: other\n---\n"),
    ]);
    let mut query = plan("views:\n - name: Test\n   source: A_B%\n   columns: [path, status]");
    query.limit = 1;
    query.offset = 1;
    let paged = BaseTableExecutorService
        .execute_with_options(&connection, &query, options())
        .unwrap();
    assert_eq!(paged.total, 2);
    assert_eq!(paged.rows.len(), 1);
    assert_eq!(paged.rows[0].file_path, "A_B%/b.md");
    assert_eq!(paged.execution.path, "sql-page");
    let fallback = BaseTableExecutorService
        .execute(&connection, &query)
        .unwrap();
    assert_eq!(paged.rows, fallback.rows);
    query.source_prefix = Some("a_b%".into());
    let insensitive = BaseTableExecutorService
        .execute_with_options(
            &connection,
            &query,
            BaseTableExecutionOptions {
                case_policy: CasePolicy::Insensitive,
                ..options()
            },
        )
        .unwrap();
    assert_eq!(insensitive.rows, paged.rows);
}

#[test]
fn null_order_is_independent_of_sort_direction_and_empty_is_not_missing() {
    let (_temp, connection) = ingest(&[
        ("a.md", "---\nrank: 1\nstatus: ''\n---\n"),
        ("b.md", "---\nrank: 2\nstatus: active\n---\n"),
        ("missing.md", "# Missing\n"),
        ("null.md", "---\nrank: null\nstatus: null\n---\n"),
    ]);
    for direction in ["asc", "desc"] {
        for nulls in ["first", "last"] {
            let page = execute(
                &connection,
                &format!(
                    "views:\n - name: Test\n   columns: [rank]\n   sorts: [{{key: rank, direction: {direction}, nulls: {nulls}}}]"
                ),
            );
            let values = page
                .rows
                .iter()
                .map(|row| row.values["rank"].clone())
                .collect::<Vec<_>>();
            let numbers = if direction == "asc" {
                vec![json!(1), json!(2)]
            } else {
                vec![json!(2), json!(1)]
            };
            let expected = if nulls == "first" {
                [vec![Value::Null, Value::Null], numbers].concat()
            } else {
                [numbers, vec![Value::Null, Value::Null]].concat()
            };
            assert_eq!(values, expected, "{direction} {nulls}");
        }
    }
    let empty = execute(
        &connection,
        "filters:\n and: ['note.status.isEmpty()']\nviews:\n - name: Test\n   columns: [status]",
    );
    assert_eq!(empty.total, 3);
    let less = execute(
        &connection,
        "views:\n - name: Test\n   columns: [rank]\n   filters: [{key: rank, op: lt, value: 2}]",
    );
    assert_eq!(less.total, 1);
    let wrong_type = plan(
        "views:\n - name: Test\n   columns: [status]\n   filters: [{key: status, op: gt, value: 1}]",
    );
    assert!(
        BaseTableExecutorService
            .execute(&connection, &wrong_type)
            .is_err()
    );
}

#[test]
fn groups_sort_after_aggregation_have_stable_identity_and_materialize_once() {
    let (_temp, connection) = ingest(&[
        ("a.md", "---\nteam: A\n---\n"),
        ("b.md", "---\nteam: Z\n---\n"),
        ("c.md", "---\nteam: Z\n---\n"),
    ]);
    let mut query = plan(
        "views:\n - name: Test\n   group_by: [team]\n   aggregates: [{alias: total, op: count}]\n   columns: [team, total]\n   sorts: [{key: total, direction: desc}]",
    );
    query.limit = 1;
    let first = BaseTableExecutorService
        .execute_with_options(&connection, &query, options())
        .unwrap();
    assert_eq!(first.total, 2);
    assert_eq!(first.rows[0].values["team"], "Z");
    assert!(first.rows[0].file_id.starts_with("group_"));
    assert!(first.rows[0].file_path.is_empty());
    query.offset = 1;
    let all = BaseTableExecutorService
        .execute_all_with_options(&connection, &query, options())
        .unwrap();
    assert_eq!(all.rows.len(), 2);
    assert_eq!(all.rows[0], first.rows[0]);
    assert_eq!(all.rows[1].values["team"], "A");
}

#[test]
fn relation_ambiguity_is_reported_and_independent_filters_precede_resolution() {
    let (_temp, connection) = ingest(&[
        (
            "sources/a.md",
            "---\nstatus: active\nparent: '[[target]]'\n---\n",
        ),
        (
            "sources/b.md",
            "---\nstatus: archived\nparent: '[[missing]]'\n---\n",
        ),
        ("left/target.md", "---\namount: 2\n---\n"),
        ("right/target.md", "---\namount: 3\n---\n"),
    ]);
    let page = execute(
        &connection,
        "views:\n - name: Test\n   source: sources\n   columns: [parent, total]\n   filters: [{key: status, op: eq, value: active}]\n   relations: [parent]\n   rollups: [{alias: total, relation: parent, target: amount, op: sum}]",
    );
    assert_eq!(page.total, 1);
    assert_eq!(page.relation_diagnostics.len(), 1);
    assert_eq!(
        page.relation_diagnostics[0].reason,
        "ambiguous_relation_target"
    );
    assert_eq!(page.relation_diagnostics[0].file_path, "sources/a.md");
    assert!(
        page.rows[0].values["parent"][0]["resolved"]
            .as_bool()
            .unwrap()
    );
}

#[test]
fn property_paging_preserves_unicode_filter_semantics_and_total() {
    let (_temp, connection) = ingest(&[
        ("a.md", "---\nlabel: ÄBC\n---\n"),
        ("b.md", "---\nlabel: äbd\n---\n"),
        ("c.md", "---\nlabel: other\n---\n"),
    ]);
    let mut request = PropertyQueryRequest {
        key: "label".into(),
        value_contains: Some("äb".into()),
        limit: Some(1),
        offset: 1,
        sort: PropertyQuerySort::FilePathAsc,
    };
    let filtered = PropertyQueryService.query(&connection, &request).unwrap();
    assert_eq!(filtered.total, 2);
    assert_eq!(filtered.rows[0].file_path, "b.md");
    request.value_contains = None;
    let plain = PropertyQueryService.query(&connection, &request).unwrap();
    assert_eq!(plain.total, 3);
    assert_eq!(plain.rows[0].file_path, "b.md");
}

#[test]
fn numeric_groups_and_integer_sums_preserve_value_meaning() {
    let (_temp, connection) = ingest(&[
        ("a.md", "---\nteam: 1\namount: 9007199254740993\n---\n"),
        ("b.md", "---\nteam: 1.0\namount: 1\n---\n"),
    ]);
    let page = execute(
        &connection,
        "views:\n - name: Test\n   group_by: [team]\n   aggregates: [{alias: total, op: sum, key: amount}]\n   columns: [team, total]",
    );
    assert_eq!(page.total, 1);
    assert_eq!(
        page.rows[0].values["total"],
        json!(9_007_199_254_740_994_u64)
    );
    let empty = execute(
        &connection,
        "views:\n - name: Test\n   source: missing\n   aggregates: [{alias: total, op: count}]\n   columns: [total]",
    );
    assert_eq!(empty.rows[0].values["total"], json!(0));
}

#[test]
fn scalar_predicate_sql_pushdown_matches_full_evaluation() {
    let (_temp, connection) = ingest(&[
        ("a.md", "---\nstatus: active\nready: true\n---\n"),
        ("b.md", "---\nstatus: null\nready: false\n---\n"),
        ("c.md", "# no metadata\n"),
    ]);
    for predicate in [
        "{key: status, op: eq, value: active}",
        "{key: status, op: neq, value: active}",
        "{key: status, op: eq, value: null}",
        "{key: status, op: exists, value: false}",
        "{key: ready, op: eq, value: true}",
    ] {
        let mut query = plan(&format!(
            "views:\n - name: Test\n   columns: [path, status]\n   filters: [{predicate}]"
        ));
        query.limit = 1;
        let pushed = BaseTableExecutorService
            .execute_with_options(&connection, &query, options())
            .unwrap();
        let fallback = BaseTableExecutorService
            .execute(&connection, &query)
            .unwrap();
        assert_eq!(pushed.execution.path, "sql-page");
        assert_eq!(pushed.total, fallback.total, "{predicate}");
        assert_eq!(pushed.rows, fallback.rows, "{predicate}");
    }
}

#[test]
fn rollups_read_namespaced_target_properties() {
    let (_temp, connection) = ingest(&[
        ("source.md", "---\nparent: '[[target]]'\n---\n"),
        ("target.md", "---\ntitle: Metadata title\n---\n"),
    ]);
    let page = execute(
        &connection,
        "views:\n - name: Test\n   source: source.md\n   columns: [target_title]\n   relations: [parent]\n   rollups: [{alias: target_title, relation: parent, target: note.title, op: min}]",
    );
    assert_eq!(page.total, 1);
    assert_eq!(page.rows[0].values["target_title"], "Metadata title");
}

#[test]
fn large_property_projections_batch_both_file_ids_and_keys() {
    let mut notes = (0..1005)
        .map(|index| {
            (
                format!("note-{index:04}.md"),
                format!("---\nrank: {index}\n---\n"),
            )
        })
        .collect::<Vec<_>>();
    let many_properties = (0..1005)
        .map(|index| format!("p{index}: {index}\n"))
        .collect::<String>();
    notes[0].1 = format!("---\nrank: 0\n{many_properties}---\n");
    let refs = notes
        .iter()
        .map(|(path, content)| (path.as_str(), content.as_str()))
        .collect::<Vec<_>>();
    let (_temp, connection) = ingest(&refs);
    let columns = (0..1005)
        .map(|index| format!("p{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut query = plan(&format!(
        "views:\n - name: Test\n   columns: [{columns}]\n   sorts: [{{key: rank, direction: asc}}]"
    ));
    query.limit = 3;
    let page = BaseTableExecutorService
        .execute_with_options(&connection, &query, options())
        .unwrap();
    assert_eq!(page.total, 1005);
    assert_eq!(page.rows.len(), 3);
    assert_eq!(page.rows[0].values["p1004"], 1004);
    assert!(page.rows[1].values["p1004"].is_null());
}

#[test]
fn relation_rollups_share_root_alias_and_same_note_fragment_resolution() {
    let (_temp, connection) = ingest(&[
        (
            "target.md",
            "---\namount: 1\naliases: ['Project Alpha', 'Complete, Alias']\n---\n# Section\n",
        ),
        ("sources/target.md", "---\namount: 2\n---\n# Decoy\n"),
        ("sources/a.md", "---\nparent: '[[/target]]'\n---\n"),
        ("sources/b.md", "---\nparent: '[[Project Alpha]]'\n---\n"),
        (
            "sources/c.md",
            "---\nparent: '[[/target#Section|Display label]]'\n---\n",
        ),
        (
            "sources/d.md",
            "---\nparent: '[[#Section]]'\namount: 3\n---\n# Section\n",
        ),
        (
            "sources/e.md",
            "---\nparent: '[[Complete, Alias#Section]]'\n---\n",
        ),
    ]);
    let page = execute(
        &connection,
        "views:\n - name: Test\n   source: sources\n   columns: [parent, total]\n   relations: [parent]\n   rollups: [{alias: total, relation: parent, target: amount, op: sum}]\n",
    );
    assert!(page.relation_diagnostics.is_empty());
    for source in ["a", "b", "c", "e"] {
        let row = page
            .rows
            .iter()
            .find(|row| row.file_path == format!("sources/{source}.md"))
            .unwrap();
        assert_eq!(row.values["total"], json!(1), "{source}");
        assert_eq!(row.values["parent"][0]["path"], "target.md", "{source}");
    }
    let same = page
        .rows
        .iter()
        .find(|row| row.file_path == "sources/d.md")
        .unwrap();
    assert_eq!(same.values["total"], json!(3));
    assert_eq!(same.values["parent"][0]["path"], "sources/d.md");
}

#[test]
fn relation_alias_ambiguity_is_reported_and_rooted_missing_target_stays_missing() {
    let (_temp, connection) = ingest(&[
        ("left/target.md", "---\namount: 1\naliases: [Shared]\n---\n"),
        ("right/target.md", "---\namount: 2\nalias: Shared\n---\n"),
        ("sources/a.md", "---\nparent: '[[Shared]]'\n---\n"),
        ("sources/b.md", "---\nparent: '[[/target]]'\n---\n"),
    ]);
    let page = execute(
        &connection,
        "views:\n - name: Test\n   source: sources\n   columns: [parent, total]\n   relations: [parent]\n   rollups: [{alias: total, relation: parent, target: amount, op: sum}]\n",
    );
    assert!(
        page.relation_diagnostics
            .iter()
            .any(|diagnostic| diagnostic.file_path == "sources/a.md"
                && diagnostic.reason == "ambiguous_relation_target")
    );
    let missing = page
        .rows
        .iter()
        .find(|row| row.file_path == "sources/b.md")
        .unwrap();
    assert_eq!(missing.values["total"], json!(0));
    assert_eq!(missing.values["parent"][0]["resolved"], false);
    assert!(
        page.relation_diagnostics
            .iter()
            .any(|diagnostic| diagnostic.file_path == "sources/b.md"
                && diagnostic.target == "[[/target]]"
                && diagnostic.reason == "relation_target_not_found")
    );
}

#[test]
fn base_property_budget_is_checked_before_json_hydration_on_both_execution_paths() {
    let (_temp, connection) = ingest(&[("healthy.md", "# Canonical healthy note\n")]);
    // Deliberately invalid oversized JSON proves the size guard runs before parsing/allocation.
    connection.execute(
        "INSERT INTO properties(property_id,file_id,key,value_type,value_json) SELECT 'oversized',file_id,'payload','json',zeroblob(16777217) FROM files WHERE normalized_path='healthy.md'", []
    ).unwrap();
    let query = plan("views:\n - name: Test\n   columns: [payload]");
    for include_summaries in [false, true] {
        let error = BaseTableExecutorService
            .execute_with_options(
                &connection,
                &query,
                BaseTableExecutionOptions {
                    include_summaries,
                    ..options()
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            BaseTableExecutorError::WorkBudgetExceeded {
                resource: "serialized property bytes",
                ..
            }
        ));
        assert!(error.to_string().contains("narrow the view source"));
    }
    // Unrequested payloads do not make a small, supported projection unusable.
    let page = execute(&connection, "views:\n - name: Test\n   columns: [path]");
    assert_eq!(page.total, 1);
    assert_eq!(page.rows[0].file_path, "healthy.md");
}

#[test]
fn over_budget_base_is_quarantined_while_canonical_index_stays_healthy() {
    let columns = (0..1001)
        .map(|index| format!("property{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut notes = (0..501)
        .map(|index| {
            (
                format!("note-{index}.md"),
                format!("# Healthy evidence {index}\n"),
            )
        })
        .collect::<Vec<_>>();
    notes.push((
        "over-budget.base".to_string(),
        format!("views:\n - name: Test\n   columns: [{columns}]\n"),
    ));
    let refs = notes
        .iter()
        .map(|(path, content)| (path.as_str(), content.as_str()))
        .collect::<Vec<_>>();
    let (_temp, connection) = ingest(&refs);
    let message: String = connection.query_row(
        "SELECT message FROM file_diagnostics WHERE path='over-budget.base' AND kind='base_view_failed'", [], |row| row.get(0)
    ).unwrap();
    assert!(
        message.contains("materialized cells 501501 > 500000"),
        "{message}"
    );
    let canonical: u64 = connection
        .query_row("SELECT COUNT(*) FROM canonical_documents", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(canonical, 501);
    let searchable: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM search_segments WHERE surface='docs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(searchable, 501);
    assert_eq!(
        execute(&connection, "views:\n - name: Test\n   columns: [path]").total,
        501
    );
}
