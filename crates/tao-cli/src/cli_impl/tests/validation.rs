use super::*;

#[test]
fn validate_cli_parses_file_folder_and_recursive_forms() {
    let markdown = Cli::parse_from(["tao", "validate", "notes/today.md"]);
    match markdown.command {
        Commands::Validate(args) => {
            assert_eq!(args.path, "notes/today.md");
            assert!(!args.recursive);
        }
        other => panic!("expected validate command, got {other:?}"),
    }

    let base = Cli::parse_from(["tao", "validate", "views/projects.base"]);
    match base.command {
        Commands::Validate(args) => {
            assert_eq!(args.path, "views/projects.base");
            assert!(!args.recursive);
        }
        other => panic!("expected validate command, got {other:?}"),
    }

    let folder = Cli::parse_from(["tao", "validate", "notes", "--recursive"]);
    match folder.command {
        Commands::Validate(args) => {
            assert_eq!(args.path, "notes");
            assert!(args.recursive);
        }
        other => panic!("expected validate command, got {other:?}"),
    }
}

#[test]
fn validate_missing_path_uses_native_clap_usage_error() {
    let result = run_from_args(
        ["tao", "validate"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    );

    assert_usage_error(result, ClapErrorKind::MissingRequiredArgument);
}

#[test]
fn validate_markdown_frontmatter_cases() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::write(
            vault_root.join("notes/dated.md"),
            "---\ntitle: Dated\ndate: 2026-05-25\n---\n# Dated\n",
        )
        .expect("write dated note");
        fs::write(
            vault_root.join("notes/missing.md"),
            "# Missing frontmatter\n",
        )
        .expect("write missing note");
        fs::write(
            vault_root.join("notes/malformed.md"),
            "---\ntitle: [\n---\n# Broken\n",
        )
        .expect("write malformed note");
        fs::write(
            vault_root.join("notes/unclosed.md"),
            "---\ntitle: Still open\n# Body\n",
        )
        .expect("write unclosed note");
        let oversized_value = "a".repeat(MAX_FRONT_MATTER_BYTES + 1);
        fs::write(
            vault_root.join("notes/oversized.md"),
            format!("---\ntitle: {oversized_value}\n---\n# Oversized\n"),
        )
        .expect("write oversized note");

        for path in ["notes/dated.md", "notes/missing.md"] {
            let output = validate_output(&vault_root, path, false);
            let data = output.get("data").expect("data");
            assert_eq!(
                data.get("mode").and_then(JsonValue::as_str),
                Some("markdown")
            );
            assert_eq!(
                data.get("files_checked").and_then(JsonValue::as_u64),
                Some(1)
            );
            assert_eq!(data.get("valid").and_then(JsonValue::as_u64), Some(1));
            assert_eq!(data.get("invalid").and_then(JsonValue::as_u64), Some(0));
            assert_eq!(
                data.get("diagnostics")
                    .and_then(JsonValue::as_array)
                    .map(Vec::len),
                Some(0)
            );
        }

        let malformed = validate_output(&vault_root, "notes/malformed.md", false);
        let malformed_data = malformed.get("data").expect("data");
        assert_eq!(
            malformed_data.get("invalid").and_then(JsonValue::as_u64),
            Some(1)
        );
        let diagnostics = malformed_data
            .get("diagnostics")
            .and_then(JsonValue::as_array)
            .expect("diagnostics");
        assert_eq!(
            diagnostics[0].get("code").and_then(JsonValue::as_str),
            Some("frontmatter.yaml_parse_failed")
        );
        assert_eq!(
            diagnostics[0].get("path").and_then(JsonValue::as_str),
            Some("notes/malformed.md")
        );
        assert!(
            diagnostics[0]
                .get("line")
                .and_then(JsonValue::as_u64)
                .is_some()
        );

        let unclosed = validate_output(&vault_root, "notes/unclosed.md", false);
        let unclosed_message = unclosed
            .get("data")
            .and_then(|data| data.get("diagnostics"))
            .and_then(JsonValue::as_array)
            .and_then(|diagnostics| diagnostics.first())
            .and_then(|diagnostic| diagnostic.get("message"))
            .and_then(JsonValue::as_str)
            .expect("unclosed diagnostic message");
        assert!(unclosed_message.contains("front matter fence is not closed"));

        let oversized = validate_output(&vault_root, "notes/oversized.md", false);
        let oversized_diagnostic = oversized
            .get("data")
            .and_then(|data| data.get("diagnostics"))
            .and_then(JsonValue::as_array)
            .and_then(|diagnostics| diagnostics.first())
            .expect("oversized diagnostic");
        assert_eq!(
            oversized_diagnostic.get("code").and_then(JsonValue::as_str),
            Some("frontmatter.too_large")
        );
        assert!(
            oversized_diagnostic
                .get("message")
                .and_then(JsonValue::as_str)
                .is_some_and(|message| message.contains("front matter exceeds"))
        );
    });
}

#[test]
fn validate_base_file_cases() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("views")).expect("create views");
        fs::write(
            vault_root.join("views/projects.base"),
            "views:\n  - name: Projects\n    type: table\n    columns:\n      - title\n",
        )
        .expect("write valid base");
        fs::write(
            vault_root.join("views/invalid.base"),
            "columns:\n  - title\n",
        )
        .expect("write invalid base");

        let valid = validate_output(&vault_root, "views/projects.base", false);
        let valid_data = valid.get("data").expect("data");
        assert_eq!(
            valid_data.get("mode").and_then(JsonValue::as_str),
            Some("base")
        );
        assert_eq!(valid_data.get("valid").and_then(JsonValue::as_u64), Some(1));
        assert_eq!(
            valid_data.get("invalid").and_then(JsonValue::as_u64),
            Some(0)
        );

        let invalid = validate_output(&vault_root, "views/invalid.base", false);
        let invalid_data = invalid.get("data").expect("data");
        assert_eq!(
            invalid_data.get("invalid").and_then(JsonValue::as_u64),
            Some(1)
        );
        let diagnostic = invalid_data
            .get("diagnostics")
            .and_then(JsonValue::as_array)
            .and_then(|diagnostics| diagnostics.first())
            .expect("invalid base diagnostic");
        assert_eq!(
            diagnostic.get("kind").and_then(JsonValue::as_str),
            Some("base")
        );
        assert_eq!(
            diagnostic.get("code").and_then(JsonValue::as_str),
            Some("bases.parse.invalid_schema")
        );
    });
}

#[test]
fn validate_folder_respects_recursion_and_taoignore() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes/deep")).expect("create nested notes");
        fs::create_dir_all(vault_root.join("ignored")).expect("create ignored");
        fs::write(vault_root.join(".taoignore"), "ignored/\n").expect("write taoignore");
        fs::write(vault_root.join("notes/root.md"), "# Root\n").expect("write root note");
        fs::write(
            vault_root.join("notes/deep/bad.md"),
            "---\ntitle: [\n---\n# Bad\n",
        )
        .expect("write bad note");
        fs::write(vault_root.join("notes/file.pdf"), "%PDF\n").expect("write unsupported");
        fs::write(
            vault_root.join("ignored/bad.md"),
            "---\ntitle: [\n---\n# Ignored\n",
        )
        .expect("write ignored bad note");

        let shallow = validate_output(&vault_root, "notes", false);
        let shallow_data = shallow.get("data").expect("data");
        assert_eq!(
            shallow_data.get("mode").and_then(JsonValue::as_str),
            Some("folder")
        );
        assert_eq!(
            shallow_data.get("recursive").and_then(JsonValue::as_bool),
            Some(false)
        );
        assert_eq!(
            shallow_data
                .get("files_checked")
                .and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            shallow_data.get("valid").and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            shallow_data.get("unsupported").and_then(JsonValue::as_u64),
            Some(1)
        );

        let recursive = validate_output(&vault_root, "notes", true);
        let recursive_data = recursive.get("data").expect("data");
        assert_eq!(
            recursive_data.get("recursive").and_then(JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            recursive_data
                .get("files_checked")
                .and_then(JsonValue::as_u64),
            Some(2)
        );
        assert_eq!(
            recursive_data.get("invalid").and_then(JsonValue::as_u64),
            Some(1)
        );
        let diagnostic_paths = recursive_data
            .get("diagnostics")
            .and_then(JsonValue::as_array)
            .expect("diagnostics")
            .iter()
            .filter_map(|diagnostic| diagnostic.get("path").and_then(JsonValue::as_str))
            .collect::<Vec<_>>();
        assert_eq!(diagnostic_paths, vec!["notes/deep/bad.md"]);
    });
}

#[test]
fn validate_returns_diagnostics_for_invalid_base() {
    with_temp_cwd(|| {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        fs::create_dir_all(vault_root.join("notes")).expect("create notes");
        fs::create_dir_all(vault_root.join("views")).expect("create views");

        fs::write(
            vault_root.join("views/invalid.base"),
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

        let cli = Cli::parse_from([
            "tao",
            "validate",
            "views/invalid.base",
            "--vault-root",
            vault_root.to_string_lossy().as_ref(),
        ]);
        let result = dispatch(cli.command).expect("dispatch validate");
        let output = render_output(cli.json, &result).expect("render validate");
        let envelope: JsonValue = serde_json::from_str(&output).expect("parse output");
        let args = envelope.get("data").expect("data");

        assert_eq!(
            args.get("path").and_then(JsonValue::as_str),
            Some("views/invalid.base")
        );
        assert_eq!(args.get("mode").and_then(JsonValue::as_str), Some("base"));
        assert_eq!(args.get("valid").and_then(JsonValue::as_u64), Some(0));
        assert_eq!(args.get("invalid").and_then(JsonValue::as_u64), Some(1));
        let diagnostics = args
            .get("diagnostics")
            .and_then(JsonValue::as_array)
            .expect("diagnostics");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].get("field").and_then(JsonValue::as_str),
            Some("filters.and")
        );
    });
}
