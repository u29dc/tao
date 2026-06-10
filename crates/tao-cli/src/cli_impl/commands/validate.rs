use super::super::*;

#[derive(Debug, Default)]
struct ValidationTotals {
    files_checked: u64,
    valid: u64,
    invalid: u64,
    unsupported: u64,
    diagnostics: Vec<JsonValue>,
}

pub(crate) fn handle(args: ValidateArgs, _runtime: &mut RuntimeMode) -> Result<CommandResult> {
    let resolved = args.resolve()?;
    let canonicalizer =
        PathCanonicalizationService::new(&resolved.vault_root, resolved.case_policy)
            .map_err(|source| anyhow!("resolve validate path failed: {source}"))?;
    let target = canonicalizer
        .canonicalize(&args.path)
        .map_err(|source| anyhow!("resolve validate path failed: {source}"))?;
    let metadata = fs::metadata(&target.absolute)
        .with_context(|| format!("read validate path metadata for '{}'", target.normalized))?;
    let display_path = display_normalized_path(&target.normalized);

    let (mode, totals) = if metadata.is_dir() {
        (
            "folder",
            validate_folder(&canonicalizer, &target.normalized, args.recursive)?,
        )
    } else if metadata.is_file() {
        let mut totals = ValidationTotals::default();
        validate_supported_file(&target.absolute, &target.normalized, &mut totals);
        (file_mode(&target.normalized), totals)
    } else {
        return Err(anyhow!(
            "validate path '{}' is neither a file nor a folder",
            args.path
        ));
    };

    Ok(CommandResult {
        command: "validate".to_string(),
        summary: "validate completed".to_string(),
        args: serde_json::json!({
            "path": display_path,
            "mode": mode,
            "recursive": args.recursive,
            "files_checked": totals.files_checked,
            "valid": totals.valid,
            "invalid": totals.invalid,
            "unsupported": totals.unsupported,
            "diagnostics": totals.diagnostics,
        }),
    })
}

fn validate_folder(
    canonicalizer: &PathCanonicalizationService,
    folder: &str,
    recursive: bool,
) -> Result<ValidationTotals> {
    let manifest = VaultScanService::new(canonicalizer.clone())
        .scan()
        .map_err(|source| anyhow!("scan vault for validation failed: {source}"))?;
    let mut totals = ValidationTotals::default();
    for entry in manifest
        .entries
        .iter()
        .filter(|entry| folder_contains_path(folder, &entry.normalized, recursive))
    {
        validate_supported_file(&entry.absolute, &entry.normalized, &mut totals);
    }
    Ok(totals)
}

fn validate_supported_file(absolute: &Path, normalized: &str, totals: &mut ValidationTotals) {
    match supported_kind(normalized) {
        Some("markdown") => validate_markdown_file(absolute, normalized, totals),
        Some("base") => validate_base_file(absolute, normalized, totals),
        Some(_) | None => totals.unsupported = totals.unsupported.saturating_add(1),
    }
}

fn validate_markdown_file(absolute: &Path, normalized: &str, totals: &mut ValidationTotals) {
    totals.files_checked = totals.files_checked.saturating_add(1);
    let markdown = match fs::read_to_string(absolute) {
        Ok(markdown) => markdown,
        Err(source) => {
            totals.invalid = totals.invalid.saturating_add(1);
            totals.diagnostics.push(diagnostic(
                normalized,
                "markdown",
                "error",
                "file.read_failed",
                source.to_string(),
                None,
                None,
            ));
            return;
        }
    };

    match extract_front_matter(&markdown).status {
        FrontMatterStatus::Missing | FrontMatterStatus::Parsed { .. } => {
            totals.valid = totals.valid.saturating_add(1);
        }
        FrontMatterStatus::Malformed { code, error } => {
            totals.invalid = totals.invalid.saturating_add(1);
            let location = markdown_diagnostic_location(&markdown, &error);
            totals.diagnostics.push(diagnostic(
                normalized,
                "markdown",
                "error",
                code.as_validation_code(),
                error,
                location,
                None,
            ));
        }
    }
}

fn validate_base_file(absolute: &Path, normalized: &str, totals: &mut ValidationTotals) {
    totals.files_checked = totals.files_checked.saturating_add(1);
    let raw = match fs::read_to_string(absolute) {
        Ok(raw) => raw,
        Err(source) => {
            totals.invalid = totals.invalid.saturating_add(1);
            totals.diagnostics.push(diagnostic(
                normalized,
                "base",
                "error",
                "file.read_failed",
                source.to_string(),
                None,
                None,
            ));
            return;
        }
    };

    let base_diagnostics = validate_base_yaml(&raw);
    let has_error = base_diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == BaseDiagnosticSeverity::Error);
    if has_error {
        totals.invalid = totals.invalid.saturating_add(1);
    } else {
        totals.valid = totals.valid.saturating_add(1);
    }

    for base_diagnostic in base_diagnostics {
        let severity = match base_diagnostic.severity {
            BaseDiagnosticSeverity::Error => "error",
            BaseDiagnosticSeverity::Warning => "warning",
        };
        totals.diagnostics.push(diagnostic(
            normalized,
            "base",
            severity,
            base_diagnostic.code,
            base_diagnostic.message,
            None,
            base_diagnostic.field,
        ));
    }
}

fn supported_kind(path: &str) -> Option<&'static str> {
    match Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
    {
        Some(extension) if extension.eq_ignore_ascii_case("md") => Some("markdown"),
        Some(extension) if extension.eq_ignore_ascii_case("base") => Some("base"),
        _ => None,
    }
}

fn file_mode(path: &str) -> &'static str {
    supported_kind(path).unwrap_or("unsupported")
}

fn folder_contains_path(folder: &str, path: &str, recursive: bool) -> bool {
    if folder.is_empty() {
        return recursive || !path.contains('/');
    }

    let Some(rest) = path.strip_prefix(folder) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix('/') else {
        return false;
    };

    recursive || !rest.contains('/')
}

fn display_normalized_path(path: &str) -> String {
    if path.is_empty() {
        ".".to_string()
    } else {
        path.to_string()
    }
}

fn markdown_diagnostic_location(markdown: &str, error: &str) -> Option<(u64, u64)> {
    if error.contains("front matter fence is not closed") {
        return Some((markdown.lines().count().max(1) as u64, 1));
    }

    let (line, column) = parse_yaml_location(error)?;
    Some((line.saturating_add(1), column))
}

fn parse_yaml_location(error: &str) -> Option<(u64, u64)> {
    let line_marker = " at line ";
    let column_marker = " column ";
    let after_line = error.rsplit_once(line_marker)?.1;
    let (line, after_line) = parse_u64_prefix(after_line)?;
    let after_column = after_line.strip_prefix(column_marker)?;
    let (column, _) = parse_u64_prefix(after_column)?;
    Some((line, column))
}

fn parse_u64_prefix(input: &str) -> Option<(u64, &str)> {
    let digits = input
        .char_indices()
        .take_while(|(_, character)| character.is_ascii_digit())
        .map(|(index, character)| (index, character.len_utf8()))
        .last()
        .map(|(index, width)| index + width)?;
    let value = input[..digits].parse().ok()?;
    Some((value, &input[digits..]))
}

fn diagnostic(
    path: impl Into<String>,
    kind: impl Into<String>,
    severity: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
    location: Option<(u64, u64)>,
    field: Option<String>,
) -> JsonValue {
    let mut object = serde_json::Map::new();
    object.insert("path".to_string(), JsonValue::String(path.into()));
    object.insert("kind".to_string(), JsonValue::String(kind.into()));
    object.insert("severity".to_string(), JsonValue::String(severity.into()));
    object.insert("code".to_string(), JsonValue::String(code.into()));
    object.insert("message".to_string(), JsonValue::String(message.into()));
    if let Some((line, column)) = location {
        object.insert("line".to_string(), JsonValue::from(line));
        object.insert("column".to_string(), JsonValue::from(column));
    }
    if let Some(field) = field {
        object.insert("field".to_string(), JsonValue::String(field));
    }
    JsonValue::Object(object)
}

pub(in crate::cli_impl) fn dispatch(
    args: ValidateArgs,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(args, runtime)
}
