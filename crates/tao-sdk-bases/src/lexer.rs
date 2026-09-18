/// Parse one quoted function argument in expressions like `file.inFolder("path")`.
pub(crate) fn parse_function_argument(
    expression: &str,
    prefix: &str,
    suffix: char,
) -> Option<String> {
    let body = expression.strip_prefix(prefix)?;
    let body = body.strip_suffix(suffix)?;
    let body = body.trim();

    if body.len() >= 2 && body.starts_with('"') && body.ends_with('"') {
        return serde_yaml::from_str::<String>(body).ok();
    }
    if body.len() >= 2 && body.starts_with('\'') && body.ends_with('\'') {
        return serde_yaml::from_str::<String>(body).ok();
    }

    None
}

/// Normalize Obsidian-ish field aliases to Tao canonical keys.
pub(crate) fn normalize_obsidian_field_key(raw: &str) -> String {
    let normalized = raw.trim();
    if normalized.eq_ignore_ascii_case("file.name") {
        return "title".to_string();
    }
    if normalized.eq_ignore_ascii_case("file.path") {
        return "path".to_string();
    }
    if normalized.eq_ignore_ascii_case("file.folder") {
        return "file_folder".to_string();
    }
    if normalized.eq_ignore_ascii_case("file.ext") {
        return "file_ext".to_string();
    }
    if let Some(rest) = normalized.strip_prefix("note.") {
        // Keep the namespace whenever a note property would shadow file metadata.
        if is_file_field(rest)
            || ["file.", "formula.", "note."]
                .iter()
                .any(|prefix| rest.starts_with(prefix))
        {
            return normalized.to_string();
        }
        return rest.to_string();
    }

    normalized.to_string()
}

/// Whether a canonical key refers to built-in file metadata.
pub fn is_file_field(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "title" | "path" | "file_path" | "folder" | "file_folder" | "ext" | "file_ext"
    )
}

/// Storage key for a note-property reference; built-in file fields have no property row.
pub fn property_key(key: &str) -> Option<&str> {
    if let Some(key) = key.strip_prefix("note.") {
        Some(key)
    } else if is_file_field(key) {
        None
    } else {
        Some(key)
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_obsidian_field_key, parse_function_argument};

    #[test]
    fn function_argument_parser_handles_quotes() {
        assert_eq!(
            parse_function_argument("file.inFolder(\"WORK\")", "file.inFolder(", ')'),
            Some("WORK".to_string())
        );
        assert_eq!(
            parse_function_argument("file.inFolder('WORK')", "file.inFolder(", ')'),
            Some("WORK".to_string())
        );
        assert_eq!(
            parse_function_argument("file.inFolder(WORK)", "file.inFolder(", ')'),
            None
        );
    }

    #[test]
    fn field_normalizer_maps_known_obsidian_aliases() {
        assert_eq!(normalize_obsidian_field_key("file.name"), "title");
        assert_eq!(normalize_obsidian_field_key("file.path"), "path");
        assert_eq!(normalize_obsidian_field_key("file.folder"), "file_folder");
        assert_eq!(normalize_obsidian_field_key("file.ext"), "file_ext");
        assert_eq!(normalize_obsidian_field_key("note.status"), "status");
    }
}
