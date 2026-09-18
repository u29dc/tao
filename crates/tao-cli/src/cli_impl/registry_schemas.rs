//! Inline, executable discovery contracts. Parser defaults and enums come from clap.
use clap::CommandFactory;
use serde_json::{Map, Value, json};

use super::ToolDefinition;

pub(crate) fn tool_schemas(tool: &ToolDefinition) -> Value {
    let mut root = super::super::Cli::command();
    root.build();
    let mut command = &root;
    for part in tool.command.split_whitespace().skip(1) {
        if let Some(child) = command.find_subcommand(part) {
            command = child;
        } else {
            break;
        }
    }
    let mut properties = Map::new();
    let mut required = Vec::new();
    for parameter in tool.parameters {
        let mut definition = if parameter.type_name == "string[]" {
            json!({"type":"array","items":{"type":"string"},"description":parameter.description})
        } else {
            json!({"type":parameter.type_name,"description":parameter.description})
        };
        if parameter.required {
            required.push(parameter.name);
        }
        if let Some(arg) = command.get_arguments().find(|arg| {
            arg.get_id().as_str() == parameter.name || arg.get_long() == Some(parameter.name)
        }) {
            if let Some(default) = arg
                .get_default_values()
                .first()
                .and_then(|value| value.to_str())
            {
                definition["default"] = match parameter.type_name {
                    "integer" | "number" | "boolean" => {
                        serde_json::from_str(default).unwrap_or_else(|_| json!(default))
                    }
                    _ => json!(default),
                };
            }
            if let Some(values) = arg.get_value_parser().possible_values() {
                definition["enum"] = json!(
                    values
                        .filter(|value| !value.is_hide_set())
                        .map(|value| value.get_name().to_string())
                        .collect::<Vec<_>>()
                );
            }
        }
        if parameter.name == "offset" {
            definition["minimum"] = json!(0);
            if matches!(tool.name, "doc.read" | "doc.list") {
                definition["maximum"] = json!(i64::MAX);
            }
        }
        if matches!(parameter.name, "page" | "page_size" | "max_nodes") {
            definition["minimum"] = json!(1);
        }
        if parameter.name == "wait_content_ms" {
            definition["minimum"] = json!(0);
            definition["maximum"] = json!(600_000);
        }
        if parameter.name == "limit"
            && matches!(
                tool.name,
                "doc.list"
                    | "doc.read"
                    | "query.run"
                    | "meta.properties"
                    | "meta.tags"
                    | "meta.aliases"
            )
        {
            definition["minimum"] = json!(1);
            definition["maximum"] = json!(1000);
        }
        properties.insert(parameter.name.to_string(), definition);
    }
    properties.insert("toon".into(), json!({"type":"boolean","default":false}));
    properties.insert(
        "execution_mode".into(),
        json!({"type":"string","enum":["auto","direct","required-daemon"],"default":"auto"}),
    );
    properties.insert(
        "timeout_ms".into(),
        json!({"type":"integer","minimum":1,"maximum":3600000,"default":120000}),
    );
    if matches!(
        tool.name,
        "doc.list"
            | "base.view"
            | "graph.links"
            | "graph.audit"
            | "meta.properties"
            | "meta.tags"
            | "meta.aliases"
            | "task.list"
            | "query.run"
    ) {
        properties.insert("continuation".into(), json!({"type":"string","pattern":"^[a-f0-9]{64}$","description":"Use meta.continuation.token from a prior paged read; rejected if the query, scope, output policy or publication changed. Query explain without execute is not a paged read."}));
    }
    let data_properties: Map<String, Value> = tool
        .output_fields
        .iter()
        .map(|name| ((*name).to_string(), output_field(tool.name, name)))
        .collect();
    let mut input = json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":format!("tao://schemas/cli/{}/input/v1",tool.name),"type":"object","properties":properties,"required":required,"additionalProperties":false});
    if tool.name == "doc.read" {
        input["if"] = json!({"required":["offset"],"properties":{"offset":{"minimum":1}}});
        input["then"] = json!({"required":["revision"],"properties":{"revision":{"minLength":1}}});
    }
    if tool.name == "vault.reindex" {
        input["if"] = json!({"required":["dry_run"],"properties":{"dry_run":{"const":true}}});
        input["then"] = json!({"properties":{"wait_content_ms":{"const":0}}});
    }
    let output = json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":format!("tao://schemas/cli/{}/output/v1",tool.name),"type":"object","required":["ok","meta"],"properties":{
        "ok":{"type":"boolean"},
        "data":{"type":"object","properties":data_properties,"additionalProperties":true},
        "error":{"type":"object","required":["code","message","hint"],"properties":{"code":{"type":"string"},"message":{"type":"string"},"hint":{"type":["string","null"]},"details":{}},"additionalProperties":false},
        "meta":{"type":"object","required":["tool","elapsed"],"properties":{"tool":{"type":"string"},"elapsed":{"type":"integer","minimum":0},"count":{"type":"integer","minimum":0},"total":{"type":"integer","minimum":0},"hasMore":{"type":"boolean"},"runtime":{"type":"object"},"continuation":{"type":"object","required":["token","consistency","generation"],"properties":{"token":{"type":"string"},"consistency":{"const":"generation_bound"},"generation":{"type":"string"}}}},"additionalProperties":true}
    },"oneOf":[{"properties":{"ok":{"const":true}},"required":["data"],"not":{"required":["error"]}},{"properties":{"ok":{"const":false}},"required":["error"],"not":{"required":["data"]}}],"additionalProperties":false});
    json!({"input":input,"output":output,"capabilities":capabilities(tool.name)})
}

fn output_field(tool: &str, name: &str) -> Value {
    match (tool, name) {
        ("graph.path", "path") | ("graph.audit", "exclude_prefixes") => {
            return json!({"type":"array","items":{"type":"string"}});
        }
        ("graph.walk", "total") | ("vault.daemon.start", "pid") => {
            return json!({"type":["integer","null"],"minimum":0});
        }
        ("base.list", "invalid") => return json!({"type":"array"}),
        ("vault.daemon.stop_all", "stopped") => return json!({"type":"integer","minimum":0}),
        ("doc.read", "original") => {
            return json!({"type":"object","required":["path","current_revision_matches_served","retained_original_revision"],"properties":{"path":{"type":"string"},"current_revision_matches_served":{"type":["boolean","null"]},"retained_original_revision":{"type":"boolean"}}});
        }
        ("doc.read", "segments") => {
            return json!({"type":"array","items":{"type":"object","required":["ordinal","locator","text","method","coverage"],"properties":{"ordinal":{"type":"integer","minimum":1},"locator":{"type":"object","required":["kind","start","end"],"properties":{"kind":{"type":"string"},"start":{"type":"integer","minimum":1},"end":{"type":"integer","minimum":1}}},"text":{"type":"string"},"method":{"type":"string"},"coverage":{"type":"string"}}}});
        }
        _ => {}
    }
    match name {
        "docs" => {
            json!({"type":"array","items":{"type":"object","properties":{"path":{"type":"string"},"revision":{"type":["string","null"]},"coverage":{"type":["string","null"]},"stale":{"type":"boolean"},"locator":{"type":["object","null"],"properties":{"kind":{"type":"string"},"start":{"type":"integer","minimum":1},"end":{"type":"integer","minimum":1},"ordinal":{"type":"integer","minimum":1}}}}}})
        }
        "items"
        | "rows"
        | "segments"
        | "diagnostics"
        | "checks"
        | "outgoing"
        | "backlinks"
        | "columns"
        | "sorts"
        | "views"
        | "relation_diagnostics"
        | "candidates"
        | "files"
        | "properties"
        | "tasks"
        | "graph"
        | "precedence"
        | "tools"
        | "globalFlags"
        | "outputFormats"
        | "failed" => {
            json!({"type":"array"})
        }
        "stale"
        | "complete"
        | "found"
        | "has_more"
        | "recursive"
        | "include_markdown"
        | "include_non_md"
        | "include_folders"
        | "include_members"
        | "database_exists"
        | "migrations_table_exists"
        | "db_ready"
        | "running"
        | "started"
        | "already_running"
        | "foreground"
        | "stopped"
        | "all_indexed_text_returned"
        | "content_complete"
        | "index_complete"
        | "content_truncated"
        | "dry_run"
        | "would_write"
        | "search_index_stale"
        | "search_segments_rebuilt"
        | "would_rebuild_search_index" => json!({"type":"boolean"}),
        "path"
        | "title"
        | "body"
        | "format"
        | "file_group"
        | "coverage"
        | "availability"
        | "desired_revision"
        | "extractor_identity"
        | "desired_extractor_identity"
        | "from"
        | "to"
        | "vault_root"
        | "db_path"
        | "data_dir"
        | "case_policy"
        | "socket"
        | "socket_dir"
        | "base_id"
        | "file_path"
        | "view_name"
        | "query"
        | "mode"
        | "kind"
        | "status"
        | "state"
        | "scope"
        | "direction"
        | "representation"
        | "domain"
        | "version"
        | "defaultOutputFormat"
        | "scan_mode"
        | "search_corpus_refresh" => json!({"type":"string"}),
        "served_revision" | "continuation_revision" | "truncation_reason" | "reason" => {
            json!({"type":["string","null"]})
        }
        "next_offset" => json!({"type":["integer","null"],"minimum":0}),
        "total"
        | "limit"
        | "offset"
        | "headings_total"
        | "total_segments"
        | "page"
        | "page_size"
        | "valid_total"
        | "invalid_total"
        | "depth"
        | "sample_size"
        | "max_depth"
        | "max_nodes"
        | "explored_nodes"
        | "examined_edges"
        | "edge_count"
        | "returned"
        | "discovered_nodes"
        | "total_files"
        | "linked_files"
        | "unlinked_files"
        | "total_floating"
        | "notes_count"
        | "attachments_count"
        | "outgoing_total"
        | "backlinks_total"
        | "files_checked"
        | "valid"
        | "invalid"
        | "unsupported"
        | "uptime_ms"
        | "cached_connections"
        | "cached_kernels"
        | "cached_results"
        | "discovered_sockets"
        | "running_before_stop"
        | "pruned_stale"
        | "migrations_applied"
        | "known_migrations"
        | "applied_migrations"
        | "pending_migrations"
        | "indexed_files"
        | "markdown_files"
        | "links_total"
        | "unresolved_links"
        | "properties_total"
        | "bases_total"
        | "search_segments_total"
        | "search_aliases_total"
        | "drift_paths"
        | "batches_applied"
        | "upserted_files"
        | "removed_files" => json!({"type":"integer","minimum":0}),
        "grouping" => json!({"type":["object","null"]}),
        "metadata" | "original" | "content" | "content_coverage" | "runtime" | "execution"
        | "stats" | "logical_plan" | "physical_plan" | "explain" | "context" | "sources"
        | "inputs" | "tool" | "schemas" => {
            json!({"type":"object"})
        }
        _ => json!({}),
    }
}

fn capabilities(name: &str) -> Value {
    match name {
        "query.run" => json!({"scopes":{
            "docs":{"formats":["md","markdown"],"select":true,"where":true,"sort":true,"query":true},
            "base:<path-or-id>":{"view_required":true,"where":true,"sort":true,"query":true},
            "graph":{"path":true,"where":false,"sort":false,"query":false},
            "task":{"query":true,"where":false,"sort":false},
            "meta:tags|aliases|properties":{"where":false,"sort":false,"query":false}},"unsupported_options":"rejected before refresh","explain_without_execute":"observational"}),
        "doc.read" => {
            json!({"content":"indexed revision-bound segments","continuation_requires":"continuation_revision","locators":{"markdown":"line","txt":"line","pdf":"physical one-based page"},"binary_assets":"inventory and original reference"})
        }
        "vault.reindex" => {
            json!({"writes":"internal index/cache only","source_files":"read-only","content_completion":"separate from inventory completion","wait_content_ms_default":0})
        }
        "search.run" => {
            json!({"content_formats":["md","markdown","txt","pdf"],"no_pii":"structured property projection; free text is not a sanitizer"})
        }
        _ => json!({"source_files":"read-only"}),
    }
}

#[cfg(test)]
pub(crate) fn assert_schema_types(schema: &Value, value: &Value, path: &str) {
    let matches_type = |kind: &str| match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "integer" => value.is_u64() || value.is_i64(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        other => panic!("unknown schema type {other} at {path}"),
    };
    if let Some(kind) = schema.get("type") {
        let valid = match kind {
            Value::String(kind) => matches_type(kind),
            Value::Array(kinds) => kinds
                .iter()
                .any(|kind| matches_type(kind.as_str().unwrap())),
            _ => panic!("invalid schema type at {path}"),
        };
        assert!(valid, "schema type {kind} does not match {path}: {value}");
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for key in required {
                assert!(
                    object.contains_key(key.as_str().unwrap()),
                    "missing {path}.{key}"
                );
            }
        }
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (key, value) in object {
                if let Some(property) = properties.get(key) {
                    assert_schema_types(property, value, &format!("{path}.{key}"));
                }
            }
        }
    }
    if let Some(array) = value.as_array()
        && let Some(items) = schema.get("items")
    {
        for (index, item) in array.iter().enumerate() {
            assert_schema_types(items, item, &format!("{path}[{index}]"));
        }
    }
    if let Some(number) = value.as_f64() {
        if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64) {
            assert!(number >= minimum, "{path} is below its declared minimum");
        }
        if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64) {
            assert!(number <= maximum, "{path} exceeds its declared maximum");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_parameters_match_clap_arguments_and_defaults() {
        let mut root = super::super::super::Cli::command();
        root.build();
        for tool in super::super::TOOLS {
            let mut command = &root;
            for part in tool.command.split_whitespace().skip(1) {
                if let Some(child) = command.find_subcommand(part) {
                    command = child;
                } else {
                    break;
                }
            }
            let schemas = tool_schemas(tool);
            for field in tool.output_fields {
                assert!(
                    schemas["output"]["properties"]["data"]["properties"][field]
                        .get("type")
                        .is_some(),
                    "{}.{} has no declared output type",
                    tool.name,
                    field
                );
            }
            let mut names = std::collections::HashSet::new();
            for parameter in tool.parameters {
                assert!(
                    names.insert(parameter.name),
                    "duplicate {}.{}",
                    tool.name,
                    parameter.name
                );
                let arg = command.get_arguments().find(|arg| {
                    arg.get_id().as_str() == parameter.name
                        || arg.get_long() == Some(parameter.name)
                });
                assert!(
                    arg.is_some(),
                    "{}.{} is not a clap argument",
                    tool.name,
                    parameter.name
                );
                let schema = &schemas["input"]["properties"][parameter.name];
                if let Some(default) = schema.get("default") {
                    assert_schema_types(
                        schema,
                        default,
                        &format!("{}.{} default", tool.name, parameter.name),
                    );
                }
            }
        }
    }

    #[test]
    fn removed_privacy_flag_is_rejected_and_not_advertised() {
        use clap::Parser;
        assert!(
            super::super::super::Cli::try_parse_from(["tao", "search", "example", "--include-pii"])
                .is_err()
        );
        let tool = super::super::tool_detail("search.run").unwrap();
        assert!(
            tool.parameters
                .iter()
                .all(|parameter| parameter.name != "include_pii")
        );
        assert!(
            tool_schemas(&tool)["input"]["properties"]
                .get("include_pii")
                .is_none()
        );
    }
}
