use std::cmp::Ordering;

use rusqlite::{Connection, params};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tao_sdk_core::note_title_from_path;

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

    let needle = query.to_ascii_lowercase();
    let fts_query = parser::build_fts_query(query);
    let mut statement = connection
        .prepare_cached(
            r#"
WITH matches AS (
  SELECT
    si.file_id,
    COALESCE(si.normalized_path, si.normalized_path_lc) AS normalized_path,
    si.updated_at AS indexed_at,
    si.title_lc,
    si.normalized_path_lc,
    si.content_lc
  FROM search_index si
  JOIN search_index_fts ON search_index_fts.rowid = si.rowid
  WHERE search_index_fts MATCH ?1
),
scored AS (
  SELECT
    file_id,
    normalized_path,
    indexed_at,
    CASE WHEN instr(title_lc, ?2) > 0 THEN 1 ELSE 0 END AS title_match,
    CASE WHEN instr(normalized_path_lc, ?2) > 0 THEN 1 ELSE 0 END AS path_match,
    CASE WHEN instr(content_lc, ?2) > 0 THEN 1 ELSE 0 END AS content_match
  FROM matches
)
SELECT
  file_id,
  normalized_path,
  indexed_at,
  title_match,
  path_match,
  content_match,
  (
    CASE WHEN title_match > 0 THEN 3 ELSE 0 END
    + CASE WHEN path_match > 0 THEN 2 ELSE 0 END
    + CASE WHEN content_match > 0 THEN 1 ELSE 0 END
  ) AS score,
  COUNT(*) OVER() AS total_count
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
            let total: u64 = row.get("total_count")?;
            let matched_in = if projection.include_matched_in {
                let mut matched_in = Vec::new();
                if title_match != 0 {
                    matched_in.push("title".to_string());
                }
                if path_match != 0 {
                    matched_in.push("path".to_string());
                }
                if matched_in.is_empty() && content_match != 0 {
                    matched_in.push("content".to_string());
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
                    Some(title_from_path(&path))
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
            .map(|item| (item, total))
        })
        .map_err(|source| SearchQueryError::RunQuery { source })?;
    let mut total = 0_u64;
    let items = rows
        .map(|row| row.map_err(|source| SearchQueryError::MapQueryRow { source }))
        .map(|row| {
            row.map(|(item, row_total)| {
                total = row_total;
                item
            })
        })
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
        if key.direction == SortDirection::Desc {
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

fn compare_json_values(left: &JsonValue, right: &JsonValue) -> Ordering {
    let left_rank = json_type_rank(left);
    let right_rank = json_type_rank(right);
    if left_rank != right_rank {
        return left_rank.cmp(&right_rank);
    }

    match (left, right) {
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            let left = left.as_f64().unwrap_or(0.0);
            let right = right.as_f64().unwrap_or(0.0);
            left.partial_cmp(&right).unwrap_or(Ordering::Equal)
        }
        (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
        _ => left.to_string().cmp(&right.to_string()),
    }
}

fn json_type_rank(value: &JsonValue) -> u8 {
    match value {
        JsonValue::Null => 0,
        JsonValue::Bool(_) => 1,
        JsonValue::Number(_) => 2,
        JsonValue::String(_) => 3,
        JsonValue::Array(_) => 4,
        JsonValue::Object(_) => 5,
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
        CompareOp::Eq => Ok(left.is_some_and(|value| value == &right)),
        CompareOp::Neq => Ok(left.is_none_or(|value| value != &right)),
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            let Some(left) = left else {
                return Ok(false);
            };
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
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            let left = left.as_f64().unwrap_or(0.0);
            let right = right.as_f64().unwrap_or(0.0);
            Ok(left.partial_cmp(&right).unwrap_or(Ordering::Equal))
        }
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
    match value {
        JsonValue::Array(values) => values
            .iter()
            .any(|entry| value_contains_text(entry, needle)),
        JsonValue::Object(values) => values
            .values()
            .any(|entry| value_contains_text(entry, needle)),
        JsonValue::String(value) => value
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase()),
        JsonValue::Number(value) => value
            .to_string()
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase()),
        JsonValue::Bool(value) => value
            .to_string()
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase()),
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
