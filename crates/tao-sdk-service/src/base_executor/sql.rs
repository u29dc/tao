use super::*;
use rusqlite::params_from_iter;
use rusqlite::types::Value as SqlValue;

pub(super) fn execute_simple_sql_page(
    connection: &Connection,
    plan: &TableQueryPlan,
    options: BaseTableExecutionOptions,
) -> Result<Option<BaseTablePage>, BaseTableExecutorError> {
    if options.include_summaries
        || !matches!(options.case_policy, CasePolicy::Sensitive)
        || !plan.relations.is_empty()
        || !plan.rollups.is_empty()
        || !plan.group_by.is_empty()
        || !plan.aggregates.is_empty()
        || plan
            .sorts
            .iter()
            .any(|sort| !matches!(sort.key.as_str(), "path" | "file_path"))
    {
        return Ok(None);
    }
    let mut values = Vec::<SqlValue>::new();
    let mut predicate = "is_markdown = 1".to_string();
    if let Some(prefix) = &plan.source_prefix {
        predicate.push_str(
            " AND (normalized_path = ?1 OR (normalized_path >= ?2 AND normalized_path < ?3))",
        );
        values.extend([
            SqlValue::Text(prefix.clone()),
            SqlValue::Text(format!("{prefix}/")),
            SqlValue::Text(format!("{prefix}0")),
        ]);
    }
    for filter in &plan.filters {
        let Some(sql) = scalar_filter_sql(filter, &mut values) else {
            return Ok(None);
        };
        predicate.push_str(" AND ");
        predicate.push_str(&sql);
        if values.len() > 800 {
            return Ok(None);
        }
    }
    let total = connection
        .query_row(
            &format!("SELECT COUNT(*) FROM files WHERE {predicate}"),
            params_from_iter(&values),
            |row| row.get::<_, u64>(0),
        )
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "count_base_page",
            source,
        })?;
    let direction = if plan
        .sorts
        .first()
        .is_some_and(|sort| matches!(sort.direction, BaseSortDirection::Desc))
    {
        "DESC"
    } else {
        "ASC"
    };
    let query = format!(
        "SELECT file_id, normalized_path FROM files WHERE {predicate} ORDER BY normalized_path {direction}, file_id ASC LIMIT ?{} OFFSET ?{}",
        values.len() + 1,
        values.len() + 2
    );
    values.push(SqlValue::Integer(i64::try_from(plan.limit).unwrap_or(-1)));
    values.push(SqlValue::Integer(
        i64::try_from(plan.offset).unwrap_or(i64::MAX),
    ));
    let materialized_rows = budget::check_candidates(connection, &query, &values)?;
    budget::check_cells(materialized_rows as usize, plan.columns.len())?;
    let mut statement =
        connection
            .prepare(&query)
            .map_err(|source| BaseTableExecutorError::Sql {
                operation: "prepare_base_page",
                source,
            })?;
    let mut rows = statement
        .query_map(params_from_iter(values), |row| {
            Ok(TableRowCandidate {
                file_id: row.get(0)?,
                file_path: row.get(1)?,
                properties: HashMap::new(),
            })
        })
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "query_base_page",
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "read_base_page",
            source,
        })?;
    let indices = rows
        .iter()
        .enumerate()
        .map(|(index, row)| (row.file_id.clone(), index))
        .collect();
    load_candidate_properties(
        connection,
        &mut rows,
        &indices,
        &plan.required_property_keys,
        options.coercion_mode,
    )?;
    Ok(Some(BaseTablePage {
        total,
        summaries: Vec::new(),
        grouping: None,
        relation_diagnostics: Vec::new(),
        execution: BaseExecutionMetadata {
            adapter: "base_table".to_string(),
            path: "sql-page".to_string(),
        },
        rows: rows
            .into_iter()
            .map(|row| project_table_row(row, &plan.columns))
            .collect(),
    }))
}

/// Safe predicate pushdown for exact strings, booleans, null, and presence.
/// Numeric predicates use the shared lossless evaluator rather than SQLite's
/// JSON real conversion for unsigned integers outside its signed range.
fn scalar_filter_sql(filter: &BaseFilterClause, values: &mut Vec<SqlValue>) -> Option<String> {
    use tao_sdk_bases::BaseFilterOp;
    let key = tao_sdk_bases::lexer::property_key(&filter.key)?;
    if !matches!(
        filter.op,
        BaseFilterOp::Eq | BaseFilterOp::NotEq | BaseFilterOp::Exists
    ) {
        return None;
    }
    if !matches!(filter.op, BaseFilterOp::Exists)
        && !matches!(
            filter.value,
            JsonValue::String(_) | JsonValue::Bool(_) | JsonValue::Null
        )
    {
        return None;
    }
    values.push(SqlValue::Text(key.to_string()));
    let mut predicate = format!("p.file_id = files.file_id AND p.key = ?{}", values.len());
    if !matches!(filter.op, BaseFilterOp::Exists) {
        predicate.push_str(" AND ");
        predicate.push_str(&match &filter.value {
            JsonValue::String(value) => {
                values.push(SqlValue::Text(value.clone()));
                format!(
                    "json_type(p.value_json) = 'text' AND json_extract(p.value_json, '$') = ?{}",
                    values.len()
                )
            }
            JsonValue::Bool(true) => "json_type(p.value_json) = 'true'".to_string(),
            JsonValue::Bool(false) => "json_type(p.value_json) = 'false'".to_string(),
            JsonValue::Null => "json_type(p.value_json) = 'null'".to_string(),
            _ => unreachable!("validated pushdown operand"),
        });
    }
    let negate = matches!(filter.op, BaseFilterOp::NotEq)
        || (matches!(filter.op, BaseFilterOp::Exists) && filter.value == JsonValue::Bool(false));
    Some(format!(
        "{}EXISTS (SELECT 1 FROM properties p WHERE {predicate})",
        if negate { "NOT " } else { "" }
    ))
}

pub(super) fn load_candidate_properties(
    connection: &Connection,
    candidates: &mut [TableRowCandidate],
    indices: &HashMap<String, usize>,
    keys: &[String],
    mode: BaseCoercionMode,
) -> Result<(), BaseTableExecutorError> {
    let file_ids = candidates
        .iter()
        .map(|row| row.file_id.clone())
        .collect::<Vec<_>>();
    visit_property_values(connection, &file_ids, keys, mode, |file_id, key, value| {
        if let Some(index) = indices.get(&file_id) {
            candidates[*index].properties.insert(key, value);
        }
    })
}

// Keep each statement below even SQLite's older 999-variable limit. The rows
// are visited directly rather than buffering a second full projection.
fn visit_property_values(
    connection: &Connection,
    file_ids: &[String],
    keys: &[String],
    mode: BaseCoercionMode,
    mut visit: impl FnMut(String, String, JsonValue),
) -> Result<(), BaseTableExecutorError> {
    // Complete this preflight before parsing even the first stored JSON value.
    let mut property_rows = 0u64;
    let mut property_bytes = 0u64;
    for ids in file_ids.chunks(400) {
        for keys in keys.chunks(400) {
            let query = format!(
                "SELECT COUNT(*),COALESCE(SUM(length(CAST(value_json AS BLOB))+length(CAST(key AS BLOB))),0) FROM properties WHERE file_id IN ({}) AND key IN ({})",
                vec!["?"; ids.len()].join(","),
                vec!["?"; keys.len()].join(",")
            );
            let (rows, bytes): (u64, u64) = connection
                .query_row(&query, params_from_iter(ids.iter().chain(keys)), |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .map_err(|source| BaseTableExecutorError::Sql {
                    operation: "measure_base_properties",
                    source,
                })?;
            property_rows = property_rows.saturating_add(rows);
            property_bytes = property_bytes.saturating_add(bytes);
            budget::check_limit(
                "property rows",
                property_rows,
                budget::MAX_BASE_PROPERTY_ROWS,
            )?;
            budget::check_limit(
                "serialized property bytes",
                property_bytes,
                budget::MAX_BASE_BYTES,
            )?;
        }
    }
    for ids in file_ids.chunks(400) {
        for keys in keys.chunks(400) {
            let placeholders = |count: usize| vec!["?"; count].join(",");
            let query = format!(
                "SELECT file_id, key, value_type, value_json FROM properties WHERE file_id IN ({}) AND key IN ({}) ORDER BY file_id, key",
                placeholders(ids.len()),
                placeholders(keys.len())
            );
            let mut statement =
                connection
                    .prepare(&query)
                    .map_err(|source| BaseTableExecutorError::Sql {
                        operation: "prepare_property_projection",
                        source,
                    })?;
            let rows = statement
                .query_map(params_from_iter(ids.iter().chain(keys)), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(|source| BaseTableExecutorError::Sql {
                    operation: "query_property_projection",
                    source,
                })?;
            for row in rows {
                let (file_id, key, value_type, value_json) =
                    row.map_err(|source| BaseTableExecutorError::Sql {
                        operation: "map_property_projection",
                        source,
                    })?;
                let value = serde_json::from_str(&value_json).map_err(|source| {
                    BaseTableExecutorError::ParsePropertyValue {
                        file_id: file_id.clone(),
                        key: key.clone(),
                        source,
                    }
                })?;
                let value = coerce_json_value(&value, map_field_type(&value_type), mode).map_err(
                    |source| BaseTableExecutorError::Coercion {
                        file_id: file_id.clone(),
                        key: key.clone(),
                        source: Box::new(source),
                    },
                )?;
                visit(file_id, key, value);
            }
        }
    }
    Ok(())
}

fn map_field_type(value_type: &str) -> BaseFieldType {
    match value_type.trim().to_ascii_lowercase().as_str() {
        "number" | "int" | "integer" | "float" | "double" => BaseFieldType::Number,
        "bool" | "boolean" | "checkbox" => BaseFieldType::Bool,
        "date" | "datetime" => BaseFieldType::Date,
        "json" | "object" | "array" | "list" | "null" => BaseFieldType::Json,
        _ => BaseFieldType::String,
    }
}

pub(super) fn load_relation_target_lookup(
    connection: &Connection,
    case_policy: LinkCasePolicy,
) -> Result<RelationTargetLookup, BaseTableExecutorError> {
    budget::check_candidates(
        connection,
        "SELECT file_id,normalized_path FROM files WHERE is_markdown=1",
        &[],
    )?;
    let (alias_rows, alias_bytes): (u64, u64) = connection.query_row(
        "SELECT COUNT(*),COALESCE(SUM(length(CAST(j.value AS BLOB))+length(CAST(f.normalized_path AS BLOB))),0) FROM properties p JOIN files f USING(file_id) JOIN json_each(p.value_json) j WHERE f.is_markdown=1 AND lower(p.key) IN ('alias','aliases') AND json_type(p.value_json) IN ('text','array') AND j.type='text'", [],
        |row| Ok((row.get(0)?, row.get(1)?))
    ).map_err(|source| BaseTableExecutorError::Sql { operation: "measure_base_aliases", source })?;
    budget::check_limit(
        "relation aliases",
        alias_rows,
        budget::MAX_BASE_PROPERTY_ROWS,
    )?;
    budget::check_limit("relation alias bytes", alias_bytes, budget::MAX_BASE_BYTES)?;
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
        lookup.insert(file_path, target);
    }

    candidates.sort();
    candidates.dedup();

    // Use the canonical alias values exactly as the graph does: scalar/list
    // strings are whole aliases, including whitespace and literal commas.
    let mut statement=connection.prepare(
        "SELECT f.normalized_path,j.value FROM properties p JOIN files f USING(file_id) JOIN json_each(p.value_json) j WHERE f.is_markdown=1 AND lower(p.key) IN ('alias','aliases') AND json_type(p.value_json) IN ('text','array') AND j.type='text' ORDER BY f.normalized_path,p.key,j.key"
    ).map_err(|source|BaseTableExecutorError::Sql{operation:"prepare_relation_aliases",source})?;
    let aliases = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(0)?))
        })
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "query_relation_aliases",
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| BaseTableExecutorError::Sql {
            operation: "map_relation_aliases",
            source,
        })?;
    Ok(RelationTargetLookup {
        index: LinkResolutionIndex::with_case_policy(&candidates, case_policy)
            .with_aliases(&aliases),
        by_path: lookup,
    })
}

pub(super) fn load_rollup_property_values(
    connection: &Connection,
    file_ids: &HashSet<String>,
    keys: &HashSet<String>,
) -> Result<HashMap<(String, String), JsonValue>, BaseTableExecutorError> {
    if file_ids.is_empty() || keys.is_empty() {
        return Ok(HashMap::new());
    }

    let mut file_ids = file_ids.iter().cloned().collect::<Vec<_>>();
    let mut logical_keys = HashMap::<&str, Vec<&String>>::new();
    for key in keys {
        if let Some(storage_key) = tao_sdk_bases::lexer::property_key(key) {
            logical_keys.entry(storage_key).or_default().push(key);
        }
    }
    let mut keys = logical_keys
        .keys()
        .map(|key| (*key).to_string())
        .collect::<Vec<_>>();
    file_ids.sort();
    keys.sort();
    let mut values = HashMap::new();
    visit_property_values(
        connection,
        &file_ids,
        &keys,
        BaseCoercionMode::Permissive,
        |file_id, key, value| {
            if let Some(aliases) = logical_keys.get(key.as_str()) {
                for alias in aliases {
                    values.insert((file_id.clone(), (*alias).clone()), value.clone());
                }
            }
        },
    )?;

    Ok(values)
}

pub(super) fn load_table_candidates(
    connection: &Connection,
    source_prefix: Option<&str>,
    case_policy: CasePolicy,
) -> Result<Vec<TableRowCandidate>, BaseTableExecutorError> {
    let (query, params): (&str, Vec<SqlValue>) = if let Some(prefix) =
        source_prefix.filter(|_| matches!(case_policy, CasePolicy::Sensitive))
    {
        (
            r#"
SELECT
  file_id,
  normalized_path
FROM files
WHERE is_markdown = 1
  AND (normalized_path = ?1 OR (normalized_path >= ?2 AND normalized_path < ?3))
ORDER BY normalized_path ASC
"#,
            vec![
                SqlValue::Text(prefix.to_string()),
                SqlValue::Text(format!("{prefix}/")),
                SqlValue::Text(format!("{prefix}0")),
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

    budget::check_candidates(connection, query, &params)?;

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

    let mut candidates = rows
        .map(|row| {
            row.map_err(|source| BaseTableExecutorError::Sql {
                operation: "map_table_candidate_files_row",
                source,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(prefix) = source_prefix.filter(|_| matches!(case_policy, CasePolicy::Insensitive)) {
        let prefix = prefix.to_lowercase();
        let folder = format!("{prefix}/");
        candidates.retain(|row| {
            let path = row.file_path.to_lowercase();
            path == prefix || path.starts_with(&folder)
        });
    }
    Ok(candidates)
}
