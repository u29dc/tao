use std::cmp::Ordering;

use rusqlite::{Connection, params};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tao_sdk_core::note_title_from_path;
use tao_sdk_core::{compare_json_values, json_values_equal};

use crate::{
    SearchQueryError, SearchQueryProjectedItem, SearchQueryProjectedPage, SearchQueryProjection,
    SearchQueryRequest, parser,
    parser::{CompareOp, LiteralValue, NullOrder, SortDirection, SortKey, WhereExpr},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryEvalError {
    pub message: String,
}

impl std::fmt::Display for QueryEvalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for QueryEvalError {}

pub fn execute_projected_query(
    connection: &Connection,
    request: SearchQueryRequest,
    projection: SearchQueryProjection,
) -> Result<SearchQueryProjectedPage, SearchQueryError> {
    let query = request.query.trim();
    if query.is_empty() {
        return Err(SearchQueryError::EmptyQuery);
    }
    if request.limit == 0 || request.limit > 1_000 {
        return Err(SearchQueryError::InvalidLimit {
            value: request.limit,
        });
    }
    let limit_i64 = i64::try_from(request.limit).map_err(|_| SearchQueryError::InvalidLimit {
        value: request.limit,
    })?;
    let offset_i64 =
        i64::try_from(request.offset).map_err(|_| SearchQueryError::InvalidOffset {
            value: request.offset,
        })?;

    let needle = query.to_lowercase();
    let fts_query = parser::build_fts_query(query);
    let mut count_statement = connection
        .prepare_cached(
            r#"
WITH matches AS (
  SELECT 1
  FROM search_segments s
  JOIN search_segments_fts ON search_segments_fts.rowid = s.rowid
  WHERE s.surface = 'docs' AND s.extension IN ('md', 'markdown')
    AND search_segments_fts MATCH ?1
)
SELECT COUNT(*) FROM matches
"#,
        )
        .map_err(|source| SearchQueryError::PrepareQuery { source })?;
    let total = count_statement
        .query_row(params![fts_query], |row| row.get::<_, u64>(0))
        .map_err(|source| SearchQueryError::RunQuery { source })?;

    let mut statement = connection
        .prepare_cached(
            r#"
WITH matches AS (
  SELECT
    s.file_id,
    s.normalized_path,
    s.label AS title,
    s.updated_at AS indexed_at,
    LOWER(s.title_text) AS title_lc,
    s.normalized_path_lc,
    LOWER(s.body_text) AS content_lc
  FROM search_segments s
  JOIN search_segments_fts ON search_segments_fts.rowid = s.rowid
  WHERE s.surface = 'docs' AND s.extension IN ('md', 'markdown')
    AND search_segments_fts MATCH ?1
),
scored AS (
  SELECT
    file_id,
    normalized_path,
    title,
    indexed_at,
    CASE WHEN instr(title_lc, ?2) > 0 THEN 1 ELSE 0 END AS title_match,
    CASE WHEN instr(normalized_path_lc, ?2) > 0 THEN 1 ELSE 0 END AS path_match,
    CASE WHEN instr(content_lc, ?2) > 0 THEN 1 ELSE 0 END AS content_match
  FROM matches
)
SELECT
  file_id,
  normalized_path,
  title,
  indexed_at,
  title_match,
  path_match,
  content_match,
  (
    CASE WHEN title_match > 0 THEN 3 ELSE 0 END
    + CASE WHEN path_match > 0 THEN 2 ELSE 0 END
    + CASE WHEN content_match > 0 THEN 1 ELSE 0 END
  ) AS score
FROM scored
ORDER BY score DESC, normalized_path ASC
LIMIT ?3
OFFSET ?4
"#,
        )
        .map_err(|source| SearchQueryError::PrepareQuery { source })?;

    let rows = statement
        .query_map(params![fts_query, needle, limit_i64, offset_i64], |row| {
            let path: String = row.get("normalized_path")?;
            let title_match: i64 = if projection.include_matched_in {
                row.get("title_match")?
            } else {
                0
            };
            let path_match: i64 = if projection.include_matched_in {
                row.get("path_match")?
            } else {
                0
            };
            let content_match: i64 = if projection.include_matched_in {
                row.get("content_match")?
            } else {
                0
            };
            let matched_in = if projection.include_matched_in {
                let mut matched_in = Vec::new();
                if title_match != 0 {
                    matched_in.push("title".to_string());
                }
                if path_match != 0 {
                    matched_in.push("path".to_string());
                }
                if content_match != 0 {
                    matched_in.push("content".to_string());
                }
                if matched_in.is_empty() {
                    matched_in.push("fts".to_string());
                }
                Some(matched_in)
            } else {
                None
            };
            Ok(SearchQueryProjectedItem {
                file_id: if projection.include_file_id {
                    Some(row.get("file_id")?)
                } else {
                    None
                },
                title: if projection.include_title {
                    Some(row.get("title")?)
                } else {
                    None
                },
                path: if projection.include_path {
                    Some(path)
                } else {
                    None
                },
                indexed_at: row.get("indexed_at")?,
                matched_in,
            })
        })
        .map_err(|source| SearchQueryError::RunQuery { source })?;
    let items = rows
        .map(|row| row.map_err(|source| SearchQueryError::MapQueryRow { source }))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(SearchQueryProjectedPage {
        query: query.to_string(),
        limit: request.limit,
        offset: request.offset,
        total,
        items,
    })
}

#[must_use]
pub fn title_from_path(path: &str) -> String {
    note_title_from_path(path)
}

pub fn apply_where_filter(
    rows: Vec<JsonMap<String, JsonValue>>,
    where_expr: Option<&WhereExpr>,
) -> Result<Vec<JsonMap<String, JsonValue>>, QueryEvalError> {
    let Some(where_expr) = where_expr else {
        return Ok(rows);
    };

    let mut filtered = Vec::with_capacity(rows.len());
    for row in rows {
        if evaluate_expr(where_expr, &row)? {
            filtered.push(row);
        }
    }
    Ok(filtered)
}

pub fn apply_sort(rows: &mut [JsonMap<String, JsonValue>], sort_keys: &[SortKey]) {
    if sort_keys.is_empty() {
        return;
    }

    rows.sort_by(|left, right| compare_row_maps(left, right, sort_keys));
}

fn compare_row_maps(
    left: &JsonMap<String, JsonValue>,
    right: &JsonMap<String, JsonValue>,
    sort_keys: &[SortKey],
) -> Ordering {
    for key in sort_keys {
        let left_value = left.get(&key.field);
        let right_value = right.get(&key.field);
        let mut ordering = compare_nullable_values(left_value, right_value, key.null_order);
        if key.direction == SortDirection::Desc
            && left_value.is_some_and(|value| !value.is_null())
            && right_value.is_some_and(|value| !value.is_null())
        {
            ordering = ordering.reverse();
        }
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    compare_nullable_values(left.get("path"), right.get("path"), NullOrder::First).then_with(|| {
        compare_nullable_values(left.get("file_id"), right.get("file_id"), NullOrder::First)
    })
}

fn compare_nullable_values(
    left: Option<&JsonValue>,
    right: Option<&JsonValue>,
    null_order: NullOrder,
) -> Ordering {
    let left = left.filter(|value| !value.is_null());
    let right = right.filter(|value| !value.is_null());
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => match null_order {
            NullOrder::First => Ordering::Less,
            NullOrder::Last => Ordering::Greater,
        },
        (Some(_), None) => match null_order {
            NullOrder::First => Ordering::Greater,
            NullOrder::Last => Ordering::Less,
        },
        (Some(left), Some(right)) => compare_json_values(left, right),
    }
}

fn evaluate_expr(
    expr: &WhereExpr,
    row: &JsonMap<String, JsonValue>,
) -> Result<bool, QueryEvalError> {
    match expr {
        WhereExpr::Compare { field, op, value } => {
            let left = row.get(field);
            evaluate_comparison(field, left, *op, value)
        }
        WhereExpr::Not(inner) => evaluate_expr(inner, row).map(|value| !value),
        WhereExpr::And(left, right) => {
            let left = evaluate_expr(left, row)?;
            if !left {
                return Ok(false);
            }
            evaluate_expr(right, row)
        }
        WhereExpr::Or(left, right) => {
            let left = evaluate_expr(left, row)?;
            if left {
                return Ok(true);
            }
            evaluate_expr(right, row)
        }
    }
}

fn evaluate_comparison(
    field: &str,
    left: Option<&JsonValue>,
    op: CompareOp,
    right: &LiteralValue,
) -> Result<bool, QueryEvalError> {
    let right = right.to_json_value();
    match op {
        CompareOp::Eq => Ok(left.is_some_and(|value| json_values_equal(value, &right))),
        CompareOp::Neq => Ok(left.is_none_or(|value| !json_values_equal(value, &right))),
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            let Some(left) = left else {
                return Ok(false);
            };
            if left.is_null() || right.is_null() {
                return Ok(false);
            }
            let ordered = compare_ordered(field, left, &right)?;
            Ok(match op {
                CompareOp::Gt => ordered.is_gt(),
                CompareOp::Gte => ordered.is_ge(),
                CompareOp::Lt => ordered.is_lt(),
                CompareOp::Lte => ordered.is_le(),
                _ => unreachable!(),
            })
        }
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => {
            let Some(left) = left else {
                return Ok(false);
            };
            let right = scalar_to_string(field, &right)?;
            Ok(match op {
                CompareOp::Contains => value_contains_text(left, &right),
                CompareOp::StartsWith => scalar_to_string(field, left)?.starts_with(&right),
                CompareOp::EndsWith => scalar_to_string(field, left)?.ends_with(&right),
                _ => unreachable!(),
            })
        }
    }
}

fn compare_ordered(
    field: &str,
    left: &JsonValue,
    right: &JsonValue,
) -> Result<Ordering, QueryEvalError> {
    match (left, right) {
        (JsonValue::Number(_), JsonValue::Number(_)) => Ok(compare_json_values(left, right)),
        (JsonValue::String(left), JsonValue::String(right)) => Ok(left.cmp(right)),
        _ => Err(QueryEvalError {
            message: format!(
                "type mismatch for ordered comparison on field '{}': left={} right={}",
                field,
                json_type_name(left),
                json_type_name(right)
            ),
        }),
    }
}

fn scalar_to_string(field: &str, value: &JsonValue) -> Result<String, QueryEvalError> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        JsonValue::Number(value) => Ok(value.to_string()),
        JsonValue::Bool(value) => Ok(value.to_string()),
        _ => Err(QueryEvalError {
            message: format!(
                "type mismatch for string comparison on field '{}': got {}",
                field,
                json_type_name(value)
            ),
        }),
    }
}

fn value_contains_text(value: &JsonValue, needle: &str) -> bool {
    let lowered_needle = needle.to_lowercase();
    match value {
        JsonValue::Array(values) => values
            .iter()
            .any(|entry| value_contains_text(entry, &lowered_needle)),
        JsonValue::Object(values) => values
            .values()
            .any(|entry| value_contains_text(entry, &lowered_needle)),
        JsonValue::String(value) => value.to_lowercase().contains(&lowered_needle),
        JsonValue::Number(value) => value.to_string().to_lowercase().contains(&lowered_needle),
        JsonValue::Bool(value) => value.to_string().to_lowercase().contains(&lowered_needle),
        JsonValue::Null => false,
    }
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map as JsonMap, Value as JsonValue, json};

    use crate::parser::{parse_sort_keys, parse_where_expression};

    use super::{apply_sort, apply_where_filter};

    #[test]
    fn exact_numbers_and_optional_numeric_predicates() {
        let rows = vec![
            JsonMap::from_iter([("n".into(), json!(9_007_199_254_740_993_u64))]),
            JsonMap::from_iter([("n".into(), json!(9_007_199_254_740_992_u64))]),
            JsonMap::from_iter([("n".into(), JsonValue::Null)]),
            JsonMap::new(),
        ];
        let predicate = parse_where_expression("n == 9007199254740993").unwrap();
        let selected = apply_where_filter(rows.clone(), Some(&predicate)).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0]["n"], json!(9_007_199_254_740_993_u64));
        let predicate = parse_where_expression("n > 9007199254740992").unwrap();
        assert_eq!(apply_where_filter(rows, Some(&predicate)).unwrap().len(), 1);
        let rows = vec![JsonMap::from_iter([("n".into(), json!(1.0))])];
        let predicate = parse_where_expression("n == 1").unwrap();
        assert_eq!(apply_where_filter(rows, Some(&predicate)).unwrap().len(), 1);
    }

    #[test]
    fn null_placement_is_independent_of_sort_direction() {
        for direction in ["asc", "desc"] {
            for nulls in ["nulls_first", "nulls_last"] {
                let mut rows = vec![
                    JsonMap::from_iter([("n".into(), json!(2))]),
                    JsonMap::from_iter([("n".into(), JsonValue::Null)]),
                    JsonMap::from_iter([("n".into(), json!(1))]),
                ];
                let sort = parse_sort_keys(Some(&format!("n:{direction}:{nulls}"))).unwrap();
                apply_sort(&mut rows, &sort);
                assert!(rows[if nulls == "nulls_first" { 0 } else { 2 }]["n"].is_null());
                let numbers = rows
                    .iter()
                    .filter_map(|row| row["n"].as_i64())
                    .collect::<Vec<_>>();
                assert_eq!(
                    numbers,
                    if direction == "asc" {
                        vec![1, 2]
                    } else {
                        vec![2, 1]
                    }
                );
            }
        }
    }

    #[test]
    fn where_filter_reports_type_mismatch() {
        let rows = vec![JsonMap::from_iter([("score".to_string(), json!(2))])];
        let where_expr = parse_where_expression("score > 'high'").expect("parse where");
        let error = apply_where_filter(rows, Some(&where_expr)).expect_err("type mismatch");
        assert!(error.message.contains("type mismatch"));
    }

    #[test]
    fn sort_is_deterministic_with_null_ordering() {
        let mut rows = vec![
            JsonMap::from_iter([
                ("path".to_string(), json!("notes/b.md")),
                ("priority".to_string(), JsonValue::Null),
            ]),
            JsonMap::from_iter([
                ("path".to_string(), json!("notes/a.md")),
                ("priority".to_string(), json!(3)),
            ]),
        ];
        let sort_keys =
            parse_sort_keys(Some("priority:asc:nulls_last,path:asc")).expect("sort keys");
        apply_sort(&mut rows, &sort_keys);
        assert_eq!(
            rows[0].get("path").and_then(JsonValue::as_str),
            Some("notes/a.md")
        );
        assert_eq!(
            rows[1].get("path").and_then(JsonValue::as_str),
            Some("notes/b.md")
        );
    }

    #[test]
    fn where_contains_matches_array_values() {
        let rows = vec![JsonMap::from_iter([
            ("path".to_string(), json!("notes/a.md")),
            (
                "related".to_string(),
                json!(["[[notes/x.md]]", "[[notes/y.md]]"]),
            ),
        ])];
        let where_expr = parse_where_expression("related contains 'y'").expect("parse where");
        let filtered = apply_where_filter(rows, Some(&where_expr)).expect("apply where");
        assert_eq!(filtered.len(), 1);
    }
}
