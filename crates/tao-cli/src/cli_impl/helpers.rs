use super::*;

pub(crate) fn link_edge_to_json(edge: tao_sdk_service::LinkGraphEdge) -> JsonValue {
    let fragment_status = edge
        .evidence
        .as_ref()
        .and_then(|value| value.get("fragment_status"))
        .and_then(JsonValue::as_str);
    let fragment_issue = matches!(
        fragment_status,
        Some("bad_anchor" | "bad_block" | "bad_page" | "pending")
    );
    let issue_scope = if edge.is_unresolved {
        Some("document")
    } else if fragment_issue {
        Some("fragment")
    } else {
        None
    };
    let issue_status = if !edge.is_unresolved && fragment_status == Some("pending") {
        Some("pending")
    } else if issue_scope.is_some() {
        Some("broken")
    } else {
        None
    };
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
        "evidence": edge.evidence,
        "issue_scope": issue_scope,
        "issue_status": issue_status,
    })
}

pub(crate) fn handle_meta_token_aggregate(
    args: GraphWindowArgs,
    property_key: &str,
    command: &str,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    use tao_sdk_service::{
        MetadataAggregationKind, MetadataAggregationRequest, MetadataAggregationService,
    };
    let kind = match property_key {
        "tags" => MetadataAggregationKind::Tags,
        "aliases" => MetadataAggregationKind::Aliases,
        _ => return Err(anyhow!("unsupported metadata token key '{property_key}'")),
    };
    let request = MetadataAggregationRequest::new(kind, args.limit, args.offset)?;
    let resolved = args.resolve()?;
    let page = with_connection(runtime, &resolved, |connection| {
        Ok(MetadataAggregationService.aggregate(connection, request)?)
    })?;
    let items = page
        .items
        .into_iter()
        .map(|item| serde_json::json!({ "token": item.value, "total": item.total }))
        .collect::<Vec<_>>();
    Ok(CommandResult {
        command: command.to_string(),
        summary: format!("{command} completed"),
        args: serde_json::json!({
            "total": page.total,
            "limit": page.limit,
            "offset": page.offset,
            "items": items,
        }),
    })
}

pub(crate) fn normalize_relative_note_path_arg(path: &str, flag: &str) -> Result<String> {
    let normalized = path.trim().trim_matches('/').replace('\\', "/");
    validate_relative_vault_path(&normalized)
        .map_err(|source| anyhow!("invalid {flag} '{}': {source}", path))?;
    Ok(normalized)
}
