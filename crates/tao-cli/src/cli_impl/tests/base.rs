use super::*;

#[test]
fn base_view_supports_obsidian_file_ext_root_filter() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/contents")).expect("create contents");
        fs::create_dir_all(vault_root.join("views")).expect("create views");

        fs::write(
            vault_root.join("views/contents.base"),
            r#"
filters:
  and:
    - file.inFolder("notes/contents")
    - file.ext == "md"
    - '!file.name.startsWith("index_")'
views:
  - type: table
    name: Table
    columns:
      - file.name
"#,
        )
        .expect("write base");
        fs::write(vault_root.join("notes/contents/alpha.md"), "# Alpha\n").expect("write alpha");
        fs::write(vault_root.join("notes/contents/index_home.md"), "# Index\n")
            .expect("write index");

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

        let cli = Cli::parse_from([
            "tao",
            "base",
            "view",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
            "--path-or-id",
            "views/contents.base",
            "--view-name",
            "Table",
        ]);
        let result = dispatch(cli.command).expect("dispatch base view");
        let output = render_output(cli.json, &result).expect("render base view");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope.get("data").expect("data");
        assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(1));
        let rows = args
            .get("rows")
            .and_then(JsonValue::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .get("values")
                .and_then(|values| values.get("title"))
                .and_then(JsonValue::as_str),
            Some("alpha")
        );
    });
}

#[test]
fn base_list_reports_invalid_entries_without_failing() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::create_dir_all(vault_root.join("views")).expect("create views");

        fs::write(
            vault_root.join("views/a-valid.base"),
            r#"
views:
  - name: Valid
    type: table
    columns:
      - title
"#,
        )
        .expect("write valid base");
        fs::write(
            vault_root.join("views/z-invalid.base"),
            r#"
filters:
  and:
    - file.name.endsWith("hub_")
views:
  - name: Invalid
    type: table
    columns:
      - title
"#,
        )
        .expect("write invalid base");
        fs::write(vault_root.join("notes/alpha.md"), "# Alpha\n").expect("write note");

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

        let cli = Cli::parse_from([
            "tao",
            "base",
            "list",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let result = dispatch(cli.command).expect("dispatch base list");
        let output = render_output(cli.json, &result).expect("render base list");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope.get("data").expect("data");

        assert_eq!(args.get("total").and_then(JsonValue::as_u64), Some(2));
        assert_eq!(args.get("valid_total").and_then(JsonValue::as_u64), Some(1));
        assert_eq!(
            args.get("invalid_total").and_then(JsonValue::as_u64),
            Some(1)
        );

        let items = args
            .get("items")
            .and_then(JsonValue::as_array)
            .expect("valid items");
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].get("file_path").and_then(JsonValue::as_str),
            Some("views/a-valid.base")
        );

        let invalid = args
            .get("invalid")
            .and_then(JsonValue::as_array)
            .expect("invalid items");
        assert_eq!(invalid.len(), 1);
        assert_eq!(
            invalid[0].get("file_path").and_then(JsonValue::as_str),
            Some("views/z-invalid.base")
        );
        let diagnostics = invalid[0]
            .get("diagnostics")
            .and_then(JsonValue::as_array)
            .expect("diagnostics");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].get("code").and_then(JsonValue::as_str),
            Some("bases.parse.invalid_schema")
        );
        assert!(
            diagnostics[0]
                .get("message")
                .and_then(JsonValue::as_str)
                .is_some_and(|message| message.contains("unsupported root filter expression"))
        );
    });
}

#[test]
fn base_list_and_schema_reject_semantically_invalid_definitions() {
    with_temp_cwd(|| {
        let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.benchmarks");
        fs::create_dir_all(&fixture_root).expect("create fixture root");
        let temp = tempfile::Builder::new()
            .prefix("base-cli-")
            .tempdir_in(fixture_root)
            .expect("create fixture");
        let vault_root = temp.path();
        fs::write(
            vault_root.join("valid.base"),
            "views: [{name: Valid, columns: [path]}]",
        )
        .expect("write valid base");
        fs::write(
            vault_root.join("invalid.base"),
            "views: [{name: Invalid, aggregates: [{alias: title, op: count}]}]",
        )
        .expect("write invalid base");
        for operation in ["open", "reindex"] {
            let cli = Cli::parse_from([
                "tao",
                "vault",
                operation,
                "--vault-root",
                vault_root.to_str().unwrap(),
            ]);
            dispatch(cli.command).expect("index fixture");
        }
        let cli = Cli::parse_from([
            "tao",
            "base",
            "list",
            "--vault-root",
            vault_root.to_str().unwrap(),
        ]);
        let result = dispatch(cli.command).expect("list definitions");
        assert_eq!(result.args["total"], 2);
        assert_eq!(result.args["valid_total"], 1);
        assert_eq!(result.args["invalid_total"], 1);
        assert_eq!(
            result.args["invalid"][0]["diagnostics"][0]["severity"],
            "error"
        );
        let cli = Cli::parse_from([
            "tao",
            "base",
            "schema",
            "--path-or-id",
            "invalid.base",
            "--vault-root",
            vault_root.to_str().unwrap(),
        ]);
        let error = dispatch(cli.command)
            .expect_err("schema must not describe an invalid view as executable");
        assert!(error.to_string().contains("reserved"), "{error}");
    });
}
