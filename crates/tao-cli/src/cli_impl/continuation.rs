use super::*;

/// One publication observed inside the same SQLite snapshot as the returned rows.
pub(crate) struct PageContinuation {
    pub(crate) token: String,
    pub(crate) generation: String,
}

pub(crate) fn supports_continuation(command: &Commands) -> bool {
    matches!(
        command,
        Commands::Doc {
            command: DocCommands::List(_)
        } | Commands::Base {
            command: BaseCommands::View(_)
        } | Commands::Graph {
            command: GraphCommands::Links(_) | GraphCommands::Audit(_)
        } | Commands::Meta { .. }
            | Commands::Task { .. }
    ) || matches!(command, Commands::Query(args) if !args.explain || args.execute)
}

pub(crate) fn validate_continuation_usage(command: &Commands, token: Option<&str>) -> Result<()> {
    if let Some(token) = token {
        if !supports_continuation(command) {
            return Err(CliContractError::invalid_argument(
                "--continuation requires a paged read; doc read uses --revision",
            )
            .into());
        }
        if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(CliContractError::invalid_argument(
                "invalid continuation token; use meta.continuation.token from the first page",
            )
            .into());
        }
    }
    Ok(())
}

pub(crate) fn published_generation(
    runtime: &mut RuntimeMode,
    resolved: &ResolvedVaultPathArgs,
) -> Result<String> {
    with_connection(runtime, resolved, |connection| {
        let state = tao_sdk_storage::IndexGenerationRepository::get(connection)?;
        Ok(format!(
            "{}:{}:{}:{}:{}:{}:{}:{}:{}",
            state.canonical_generation,
            state.search_generation,
            state.derived_generation,
            state.published_derived_generation,
            state.files_total,
            state.segments_total,
            state.aliases_total,
            state.published_segments,
            state.published_aliases
        ))
    })
}

fn normalize_page_identity(value: &mut JsonValue) {
    match value {
        JsonValue::Object(object) => {
            // These are transport/window arguments, not selection or disclosure policy.
            for field in [
                "vault_root",
                "db_path",
                "offset",
                "limit",
                "page",
                "page_size",
            ] {
                object.remove(field);
            }
            for value in object.values_mut() {
                normalize_page_identity(value);
            }
        }
        JsonValue::Array(values) => {
            for value in values {
                normalize_page_identity(value);
            }
        }
        _ => {}
    }
}

pub(crate) fn prepare_continuation(
    runtime: &mut RuntimeMode,
    resolved: Option<&ResolvedVaultPathArgs>,
    command: &Commands,
    supplied: Option<&str>,
    toon: bool,
    json_stream: bool,
) -> Result<Option<PageContinuation>> {
    validate_continuation_usage(command, supplied)?;
    if !supports_continuation(command) {
        return Ok(None);
    }
    let resolved = resolved.ok_or_else(|| {
        CliContractError::invalid_argument("paged read requires a resolved vault")
    })?;
    let generation = published_generation(runtime, resolved)?;
    let mut operation: JsonValue = serde_json::from_str(&daemon_cache_key(command)?)?;
    normalize_page_identity(&mut operation);
    let identity = serde_json::json!({"runtime":runtime_cache_key(resolved),"operation":operation,
        "generation":generation,"toon":toon,"json_stream":json_stream,"build":env!("TAO_BUILD_ID")});
    let token = blake3::hash(&serde_json::to_vec(&identity)?)
        .to_hex()
        .to_string();
    if supplied.is_some_and(|supplied| supplied != token) {
        return Err(CliContractError::blocked(
            "continuation_mismatch",
            "the index, query, scope, or output policy changed since the previous page",
            Some(
                "restart from the first page without --continuation and use its new token"
                    .to_string(),
            ),
            Some(serde_json::json!({"restart_required":true})),
        )
        .into());
    }
    Ok(Some(PageContinuation { token, generation }))
}

pub(crate) fn decorate_continuation(
    output: String,
    continuation: Option<&PageContinuation>,
) -> Result<String> {
    let Some(continuation) = continuation else {
        return Ok(output);
    };
    let mut envelope: JsonValue = serde_json::from_str(&output)?;
    envelope["meta"]["continuation"] = serde_json::json!({"token":continuation.token,
        "consistency":"generation_bound", "generation":continuation.generation});
    Ok(serde_json::to_string(&envelope)?)
}
