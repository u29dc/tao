use super::*;

pub(crate) fn paginate_json_items(
    items: Vec<JsonValue>,
    limit: u32,
    offset: u32,
) -> Vec<JsonValue> {
    items
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .collect()
}

pub(crate) fn link_edge_to_json(edge: tao_sdk_service::LinkGraphEdge) -> JsonValue {
    serde_json::json!({
        "link_id": edge.link_id,
        "source_file_id": edge.source_file_id,
        "source_path": edge.source_path,
        "raw_target": edge.raw_target,
        "resolved_file_id": edge.resolved_file_id,
        "resolved_path": edge.resolved_path,
        "heading_slug": edge.heading_slug,
        "block_id": edge.block_id,
        "is_unresolved": edge.is_unresolved,
        "unresolved_reason": edge.unresolved_reason,
        "source_field": edge.source_field,
    })
}

pub(crate) fn handle_meta_token_aggregate(
    args: GraphWindowArgs,
    property_key: &str,
    command: &str,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    let resolved = args.resolve()?;
    let rows = with_connection(runtime, &resolved, |connection| {
        Ok(PropertiesRepository::list_by_key_with_paths(
            connection,
            property_key,
        )?)
    })
    .map_err(|source| anyhow!("query property key '{}' failed: {source}", property_key))?;
    let mut counts = HashMap::<String, usize>::new();
    for row in rows {
        for token in extract_property_tokens(&row.value_json) {
            *counts.entry(token).or_insert(0) += 1;
        }
    }
    let mut items = counts
        .into_iter()
        .map(|(token, total)| serde_json::json!({ "token": token, "total": total }))
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        right["total"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&left["total"].as_u64().unwrap_or(0))
            .then_with(|| {
                left["token"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["token"].as_str().unwrap_or_default())
            })
    });
    let total = items.len();
    let items = paginate_json_items(items, args.limit, args.offset);
    Ok(CommandResult {
        command: command.to_string(),
        summary: format!("{command} completed"),
        args: serde_json::json!({
            "total": total,
            "limit": args.limit,
            "offset": args.offset,
            "items": items,
        }),
    })
}

pub(crate) fn extract_property_tokens(value_json: &str) -> Vec<String> {
    let parsed = serde_json::from_str::<JsonValue>(value_json)
        .unwrap_or_else(|_| JsonValue::String(value_json.to_string()));
    let mut tokens = Vec::new();
    collect_json_string_tokens(&parsed, &mut tokens);
    let mut deduped = Vec::new();
    let mut seen = HashSet::<String>::new();
    for token in tokens {
        let key = token.to_ascii_lowercase();
        if seen.insert(key) {
            deduped.push(token);
        }
    }
    deduped
}

pub(crate) fn collect_json_string_tokens(value: &JsonValue, out: &mut Vec<String>) {
    match value {
        JsonValue::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return;
            }
            for token in trimmed
                .split([',', ' '])
                .map(str::trim)
                .filter(|token| !token.is_empty())
            {
                out.push(token.trim_start_matches('#').to_string());
            }
        }
        JsonValue::Array(values) => {
            for item in values {
                collect_json_string_tokens(item, out);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::Object(_) => {}
    }
}

pub(crate) fn normalize_relative_note_path_arg(path: &str, flag: &str) -> Result<String> {
    let normalized = path.trim().trim_matches('/').replace('\\', "/");
    validate_relative_vault_path(&normalized)
        .map_err(|source| anyhow!("invalid {flag} '{}': {source}", path))?;
    Ok(normalized)
}
