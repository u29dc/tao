use std::env;
use std::fs;
use std::io::Cursor;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use super::*;
use clap::{CommandFactory, Parser, error::ErrorKind as ClapErrorKind};
use rusqlite::Connection;
use serde_json::Value as JsonValue;
use tao_sdk_properties::MAX_FRONT_MATTER_BYTES;
use tao_sdk_storage::{
    FilesRepository, IndexStateRecordInput, IndexStateRepository, LinkRecordInput, LinksRepository,
};

mod base;
mod contracts;
mod graph;
mod index;
mod query;
mod revisions;
mod runtime;
mod search;
mod validation;

fn assert_json_contract(value: &JsonValue, expected_command: &str) {
    let envelope = value.as_object().expect("envelope must be object");
    assert_eq!(envelope.len(), 3);
    assert!(envelope.contains_key("ok"));
    assert!(envelope.contains_key("data"));
    assert!(envelope.contains_key("meta"));
    assert_eq!(
        envelope.get("ok").and_then(JsonValue::as_bool),
        Some(true),
        "expected ok=true for command {expected_command}",
    );
    assert!(!envelope.contains_key("error"));

    let payload = envelope
        .get("data")
        .and_then(JsonValue::as_object)
        .expect("data payload must be object");
    assert!(!payload.is_empty());
    assert_eq!(
        envelope
            .get("meta")
            .and_then(|meta| meta.get("tool"))
            .and_then(JsonValue::as_str),
        Some(expected_command)
    );
    assert!(
        envelope
            .get("meta")
            .and_then(|meta| meta.get("elapsed"))
            .and_then(JsonValue::as_u64)
            .is_some()
    );
    assert_registry_output_fields_match_payload(expected_command, payload);
}

fn assert_usage_error(result: RunResult, expected_kind: ClapErrorKind) {
    assert_eq!(result.exit_kind, ExitKind::Usage);
    assert!(result.stdout.is_none());
    assert!(result.stderr.is_none());
    match result.clap_output {
        Some(ClapOutput::Error(error)) => {
            assert_eq!(error.kind(), expected_kind);
        }
        other => panic!("expected native clap error output, got {other:?}"),
    }
}

fn validate_output(vault_root: &Path, path: &str, recursive: bool) -> JsonValue {
    let mut args = vec![
        "tao".to_string(),
        "validate".to_string(),
        path.to_string(),
        "--vault-root".to_string(),
        vault_root.to_string_lossy().to_string(),
    ];
    if recursive {
        args.push("--recursive".to_string());
    }
    let cli = Cli::parse_from(args);
    let result = dispatch(cli.command).expect("dispatch validate");
    let output = render_output(cli.json, &result).expect("render validate");
    serde_json::from_str(&output).expect("parse validate output")
}

fn assert_registry_output_fields_match_payload(
    expected_command: &str,
    payload: &serde_json::Map<String, JsonValue>,
) {
    let Some(tool) = registry::tool_detail(expected_command) else {
        return;
    };
    registry::assert_schema_types(
        &registry::tool_schemas(&tool)["output"]["properties"]["data"],
        &JsonValue::Object(payload.clone()),
        expected_command,
    );
    for key in payload.keys() {
        assert!(
            tool.output_fields.contains(&key.as_str()),
            "registry for {expected_command} does not advertise runtime output field `{key}`"
        );
    }
    for field in tool.output_fields {
        if conditional_output_field(expected_command, field) {
            continue;
        }
        assert!(
            payload.contains_key(*field),
            "registry for {expected_command} advertises missing output field `{field}`"
        );
    }
}

fn conditional_output_field(tool: &str, field: &str) -> bool {
    match tool {
        "vault.reindex" => matches!(field, "dry_run" | "would_write"),
        "graph.audit" => !matches!(field, "kind" | "items" | "total" | "limit" | "offset"),
        "query.run" => true, // Plans, docs, bases, graph panels and adapters have distinct payloads.
        "tools" => matches!(field, "tools" | "tool" | "schemas"),
        "vault.daemon.start" => field != "socket",
        "vault.daemon.status" => matches!(
            field,
            "uptime_ms" | "cached_connections" | "cached_kernels" | "cached_results"
        ),
        "vault.daemon.stop" => field == "running",
        _ => false,
    }
}

fn with_temp_cwd<T>(operation: impl FnOnce() -> T) -> T {
    static CWD_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let lock = CWD_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let original_dir = env::current_dir().expect("get original cwd");
    let sandbox = tempfile::tempdir().expect("create cwd sandbox");
    env::set_current_dir(sandbox.path()).expect("set temp cwd");
    struct RestoreCwd(std::path::PathBuf);
    impl Drop for RestoreCwd {
        fn drop(&mut self) {
            let _ = env::set_current_dir(&self.0);
        }
    }
    let _restore = RestoreCwd(original_dir);
    operation()
}

fn seed_search_fixture(vault_root: &Path) {
    let contacts_dir = vault_root.join("WORK/013-RELATIONS/013-CON-contacts");
    let companies_dir = vault_root.join("WORK/013-RELATIONS/013-COM-companies");
    let meetings_dir = vault_root.join("WORK/013-RELATIONS/013-MTG-meetings");
    let finance_dir = vault_root.join("WORK/012-FINANCE/invoices");
    let views_dir = vault_root.join("views");
    fs::create_dir_all(&contacts_dir).expect("create contacts");
    fs::create_dir_all(&companies_dir).expect("create companies");
    fs::create_dir_all(&meetings_dir).expect("create meetings");
    fs::create_dir_all(&finance_dir).expect("create finance");
    fs::create_dir_all(&views_dir).expect("create views");

    fs::write(
        contacts_dir.join("jordan_hart.md"),
        r#"---
entity: Jordan Hart
kind: contact
company: "[[WORK/013-RELATIONS/013-COM-companies/northstar_transit_lab.md]]"
invoice_date: 2026-02-15
---
# Jordan Hart

Fictional contact note for graph-aware search tests.

Company: [[WORK/013-RELATIONS/013-COM-companies/northstar_transit_lab.md]]
Meeting: [[WORK/013-RELATIONS/013-MTG-meetings/2026-02-14-jordan-hart-intro.md]]
Invoice: [[WORK/012-FINANCE/invoices/2026-02-15-invoice-jordan-hart.pdf]]

- [ ] Send invoice context packet
"#,
    )
    .expect("write contact");
    fs::write(
        companies_dir.join("northstar_transit_lab.md"),
        r#"---
entity: Northstar Transit Lab
kind: company
---
# Northstar Transit Lab

Fictional company record linked from [[WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md]].
"#,
    )
    .expect("write company");
    fs::write(
        meetings_dir.join("2026-02-14-jordan-hart-intro.md"),
        r#"---
date: 2026-02-14
attendees:
  - "[[WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md]]"
---
# Jordan Hart Intro

Meeting note linking back to [[WORK/013-RELATIONS/013-CON-contacts/jordan_hart.md]].
"#,
    )
    .expect("write meeting");
    fs::write(
        finance_dir.join("2026-02-15-invoice-jordan-hart.pdf"),
        "fictional invoice pdf fixture",
    )
    .expect("write invoice");
    fs::write(
        views_dir.join("contacts.base"),
        r#"views:
  - name: Contacts
    type: table
    source: WORK/013-RELATIONS/013-CON-contacts
    columns:
      - title
      - entity
      - kind
      - company
      - invoice_date
"#,
    )
    .expect("write contacts base");
}

fn open_and_reindex_fixture(vault_root: &Path) {
    let open = Cli::parse_from([
        "tao",
        "vault",
        "open",
        "--vault-root",
        vault_root.to_string_lossy().as_ref(),
    ]);
    dispatch(open.command).expect("open vault");
    let reindex = Cli::parse_from([
        "tao",
        "vault",
        "reindex",
        "--vault-root",
        vault_root.to_string_lossy().as_ref(),
    ]);
    dispatch(reindex.command).expect("reindex vault");
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

fn json_item_paths(payload: &JsonValue) -> Vec<&str> {
    payload
        .get("data")
        .and_then(|data| data.get("items"))
        .and_then(JsonValue::as_array)
        .expect("items")
        .iter()
        .filter_map(|item| item.get("path").and_then(JsonValue::as_str))
        .collect::<Vec<_>>()
}
