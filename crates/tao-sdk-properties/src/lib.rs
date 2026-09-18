//! Front matter extraction and typed property projection utilities.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use thiserror::Error;

/// Maximum accepted front matter YAML payload size.
pub const MAX_FRONT_MATTER_BYTES: usize = 128 * 1024;
/// Maximum accepted nested YAML depth for front matter.
pub const MAX_FRONT_MATTER_DEPTH: usize = 64;

/// Front matter extraction result from markdown content.
#[derive(Debug, Clone, PartialEq)]
pub struct FrontMatterExtraction {
    /// Optional raw YAML block text without fence markers.
    pub raw: Option<String>,
    /// Markdown body without front matter fences when extraction succeeded.
    pub body: String,
    /// Parse status for extracted front matter.
    pub status: FrontMatterStatus,
}

/// Front matter parse status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FrontMatterStatus {
    /// Front matter block not present.
    Missing,
    /// Front matter parsed successfully.
    Parsed { value: Value },
    /// Front matter existed but could not be parsed.
    Malformed {
        /// Stable machine-readable error code.
        code: FrontMatterErrorCode,
        /// Human-readable diagnostic message.
        error: String,
    },
}

/// Stable front matter parse error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrontMatterErrorCode {
    /// Opening front matter fence was not closed.
    UnclosedFence,
    /// YAML payload exceeded the configured byte limit.
    TooLarge,
    /// YAML payload exceeded the configured depth limit.
    TooDeep,
    /// YAML parser reported malformed content.
    YamlParseFailed,
    /// The YAML root is neither a mapping nor empty.
    ExpectedMappingRoot,
    /// JSON cannot represent a non-finite numeric value.
    NonFiniteNumber,
}

impl FrontMatterErrorCode {
    /// Return the stable validation diagnostic code for this error.
    #[must_use]
    pub const fn as_validation_code(self) -> &'static str {
        match self {
            Self::UnclosedFence | Self::YamlParseFailed | Self::TooDeep => {
                "frontmatter.yaml_parse_failed"
            }
            Self::TooLarge => "frontmatter.too_large",
            Self::ExpectedMappingRoot => "frontmatter.expected_mapping",
            Self::NonFiniteNumber => "frontmatter.non_finite_number",
        }
    }
}

struct MarkdownLine<'a> {
    content: &'a str,
    next_start: usize,
}

fn next_markdown_line(markdown: &str, start: usize) -> Option<MarkdownLine<'_>> {
    if start >= markdown.len() {
        return None;
    }

    let remaining = &markdown[start..];
    if let Some(newline_offset) = remaining.find('\n') {
        let line_end = start + newline_offset;
        let content_end = if line_end > start && markdown.as_bytes()[line_end - 1] == b'\r' {
            line_end - 1
        } else {
            line_end
        };
        Some(MarkdownLine {
            content: &markdown[start..content_end],
            next_start: line_end + 1,
        })
    } else {
        Some(MarkdownLine {
            content: remaining,
            next_start: markdown.len(),
        })
    }
}

/// Extract front matter from markdown and capture parse failures without panicking.
#[must_use]
pub fn extract_front_matter(markdown: &str) -> FrontMatterExtraction {
    let content_start = if markdown.starts_with('\u{feff}') {
        '\u{feff}'.len_utf8()
    } else {
        0
    };
    let Some(opening_fence) = next_markdown_line(markdown, content_start) else {
        return FrontMatterExtraction {
            raw: None,
            body: markdown[content_start..].to_string(),
            status: FrontMatterStatus::Missing,
        };
    };

    if opening_fence.content != "---" {
        return FrontMatterExtraction {
            raw: None,
            body: markdown[content_start..].to_string(),
            status: FrontMatterStatus::Missing,
        };
    }

    let mut raw_lines = Vec::new();
    let mut raw_bytes = 0usize;
    let mut cursor = opening_fence.next_start;
    while let Some(line) = next_markdown_line(markdown, cursor) {
        if line.content == "---" {
            let raw = raw_lines.join("\n");
            let body = markdown[line.next_start..].to_string();

            return parse_front_matter_yaml(raw, body);
        }

        let separator_bytes = usize::from(!raw_lines.is_empty());
        let next_raw_bytes = raw_bytes
            .saturating_add(separator_bytes)
            .saturating_add(line.content.len());
        if next_raw_bytes > MAX_FRONT_MATTER_BYTES {
            return FrontMatterExtraction {
                raw: None,
                body: markdown.to_string(),
                status: FrontMatterStatus::Malformed {
                    code: FrontMatterErrorCode::TooLarge,
                    error: FrontMatterError::TooLarge {
                        bytes: next_raw_bytes,
                        max_bytes: MAX_FRONT_MATTER_BYTES,
                    }
                    .to_string(),
                },
            };
        }
        raw_bytes = next_raw_bytes;
        raw_lines.push(line.content);
        cursor = line.next_start;
    }

    FrontMatterExtraction {
        raw: Some(raw_lines.join("\n")),
        body: markdown.to_string(),
        status: FrontMatterStatus::Malformed {
            code: FrontMatterErrorCode::UnclosedFence,
            error: FrontMatterError::UnclosedFence.to_string(),
        },
    }
}

fn parse_front_matter_yaml(raw: String, body: String) -> FrontMatterExtraction {
    match serde_yaml::from_str::<Value>(&raw) {
        Ok(value) if yaml_exceeds_max_depth(&value, MAX_FRONT_MATTER_DEPTH) => {
            FrontMatterExtraction {
                raw: Some(raw),
                body,
                status: FrontMatterStatus::Malformed {
                    code: FrontMatterErrorCode::TooDeep,
                    error: FrontMatterError::TooDeep {
                        max_depth: MAX_FRONT_MATTER_DEPTH,
                    }
                    .to_string(),
                },
            }
        }
        Ok(Value::Null) => FrontMatterExtraction {
            raw: Some(raw),
            body,
            status: FrontMatterStatus::Parsed {
                value: Value::Mapping(Default::default()),
            },
        },
        Ok(value) if !matches!(value, Value::Mapping(_)) => FrontMatterExtraction {
            raw: Some(raw),
            body,
            status: FrontMatterStatus::Malformed {
                code: FrontMatterErrorCode::ExpectedMappingRoot,
                error: PropertyProjectionError::ExpectedMappingRoot.to_string(),
            },
        },
        Ok(value) if yaml_has_non_finite_number(&value) => FrontMatterExtraction {
            raw: Some(raw),
            body,
            status: FrontMatterStatus::Malformed {
                code: FrontMatterErrorCode::NonFiniteNumber,
                error: PropertyProjectionError::NonFiniteNumber.to_string(),
            },
        },
        Ok(value) => FrontMatterExtraction {
            raw: Some(raw),
            body,
            status: FrontMatterStatus::Parsed { value },
        },
        Err(source) => FrontMatterExtraction {
            raw: Some(raw),
            body,
            status: FrontMatterStatus::Malformed {
                code: FrontMatterErrorCode::YamlParseFailed,
                error: FrontMatterError::YamlParse { source }.to_string(),
            },
        },
    }
}

fn yaml_has_non_finite_number(root: &Value) -> bool {
    let mut stack = vec![root];
    while let Some(value) = stack.pop() {
        match value {
            Value::Number(number) if number.as_f64().is_some_and(|v| !v.is_finite()) => {
                return true;
            }
            Value::Sequence(values) => stack.extend(values),
            Value::Mapping(values) => {
                for (key, value) in values {
                    stack.push(key);
                    stack.push(value);
                }
            }
            Value::Tagged(tagged) => stack.push(&tagged.value),
            _ => {}
        }
    }
    false
}

fn yaml_exceeds_max_depth(root: &Value, max_depth: usize) -> bool {
    let mut stack = vec![(root, 0usize)];
    while let Some((value, depth)) = stack.pop() {
        if depth > max_depth {
            return true;
        }
        let next_depth = depth.saturating_add(1);
        match value {
            Value::Sequence(items) => {
                stack.extend(items.iter().map(|item| (item, next_depth)));
            }
            Value::Mapping(mapping) => {
                for (key, nested) in mapping {
                    stack.push((key, next_depth));
                    stack.push((nested, next_depth));
                }
            }
            Value::Tagged(tagged) => stack.push((&tagged.value, next_depth)),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    false
}

/// Normalized typed property pair.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedProperty {
    /// Property key.
    pub key: String,
    /// Normalized property value.
    pub value: TypedPropertyValue,
}

/// Normalized property value representation.
#[derive(Debug, Clone, PartialEq)]
pub enum TypedPropertyValue {
    /// Boolean value.
    Bool(bool),
    /// Lossless signed integer value.
    Integer(i64),
    /// Lossless unsigned integer value outside the signed range.
    UnsignedInteger(u64),
    /// Finite floating-point value.
    Number(f64),
    /// ISO-like date string.
    Date(String),
    /// Plain string value.
    String(String),
    /// List of normalized values.
    List(Vec<TypedPropertyValue>),
    /// Explicit null.
    Null,
}

/// Project parsed YAML mapping into normalized typed properties.
pub fn project_typed_properties(
    front_matter: &Value,
) -> Result<Vec<TypedProperty>, PropertyProjectionError> {
    if front_matter.is_null() {
        return Ok(Vec::new());
    }
    if yaml_has_non_finite_number(front_matter) {
        return Err(PropertyProjectionError::NonFiniteNumber);
    }
    let Value::Mapping(mapping) = front_matter else {
        return Err(PropertyProjectionError::ExpectedMappingRoot);
    };

    let mut projected_by_key = BTreeMap::new();
    for (key, value) in mapping {
        let Value::String(key) = key else {
            continue;
        };

        let canonical_key = canonical_property_key(key);
        let normalized_value =
            normalize_default_property_value(&canonical_key, normalize_yaml_value(value));

        if let Some(kind) = default_list_kind(&canonical_key) {
            let merged_value = if let Some(existing) = projected_by_key.remove(&canonical_key) {
                merge_default_list_values(kind, existing, normalized_value)
            } else {
                normalized_value
            };
            projected_by_key.insert(canonical_key, merged_value);
        } else {
            projected_by_key.insert(canonical_key, normalized_value);
        }
    }

    let projected = projected_by_key
        .into_iter()
        .map(|(key, value)| TypedProperty { key, value })
        .collect();

    Ok(projected)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DefaultListKind {
    Tags,
    Aliases,
    CssClasses,
}

fn canonical_property_key(key: &str) -> String {
    match key.to_ascii_lowercase().as_str() {
        "tag" | "tags" => "tags".to_string(),
        "alias" | "aliases" => "aliases".to_string(),
        "cssclass" | "cssclasses" => "cssclasses".to_string(),
        _ => key.to_string(),
    }
}

fn default_list_kind(key: &str) -> Option<DefaultListKind> {
    match key {
        "tags" => Some(DefaultListKind::Tags),
        "aliases" => Some(DefaultListKind::Aliases),
        "cssclasses" => Some(DefaultListKind::CssClasses),
        _ => None,
    }
}

fn normalize_default_property_value(key: &str, value: TypedPropertyValue) -> TypedPropertyValue {
    let Some(kind) = default_list_kind(key) else {
        return value;
    };

    let tokens = collect_default_tokens(kind, &value);
    TypedPropertyValue::List(tokens.into_iter().map(TypedPropertyValue::String).collect())
}

fn merge_default_list_values(
    kind: DefaultListKind,
    left: TypedPropertyValue,
    right: TypedPropertyValue,
) -> TypedPropertyValue {
    let mut merged = collect_default_tokens(kind, &left);
    merged.extend(collect_default_tokens(kind, &right));
    TypedPropertyValue::List(
        dedupe_tokens(merged)
            .into_iter()
            .map(TypedPropertyValue::String)
            .collect(),
    )
}

fn collect_default_tokens(kind: DefaultListKind, value: &TypedPropertyValue) -> Vec<String> {
    match value {
        TypedPropertyValue::List(values) => {
            let mut tokens = Vec::new();
            for item in values {
                if kind == DefaultListKind::Aliases
                    && let TypedPropertyValue::String(value) | TypedPropertyValue::Date(value) =
                        item
                {
                    let value = value.trim();
                    if !value.is_empty() {
                        tokens.push(value.to_owned());
                    }
                    continue;
                }
                tokens.extend(collect_default_tokens(kind, item));
            }
            dedupe_tokens(tokens)
        }
        TypedPropertyValue::String(value) | TypedPropertyValue::Date(value) => {
            dedupe_tokens(split_default_string_tokens(kind, value))
        }
        TypedPropertyValue::Bool(value) => vec![value.to_string()],
        TypedPropertyValue::Integer(value) => vec![value.to_string()],
        TypedPropertyValue::UnsignedInteger(value) => vec![value.to_string()],
        TypedPropertyValue::Number(value) => vec![value.to_string()],
        TypedPropertyValue::Null => Vec::new(),
    }
}

fn split_default_string_tokens(kind: DefaultListKind, value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    match kind {
        DefaultListKind::Tags => trimmed
            .replace(',', " ")
            .split_whitespace()
            .map(|token| token.trim_start_matches('#').trim().to_string())
            .filter(|token| !token.is_empty())
            .collect(),
        DefaultListKind::Aliases => {
            if trimmed.contains(',') {
                trimmed
                    .split(',')
                    .map(|token| token.trim().to_string())
                    .filter(|token| !token.is_empty())
                    .collect()
            } else {
                vec![trimmed.to_string()]
            }
        }
        DefaultListKind::CssClasses => trimmed
            .replace(',', " ")
            .split_whitespace()
            .map(|token| token.trim().to_string())
            .filter(|token| !token.is_empty())
            .collect(),
    }
}

fn dedupe_tokens(tokens: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for token in tokens {
        if seen.insert(token.to_lowercase()) {
            deduped.push(token);
        }
    }
    deduped
}

fn normalize_yaml_value(value: &Value) -> TypedPropertyValue {
    match value {
        Value::Bool(value) => TypedPropertyValue::Bool(*value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                TypedPropertyValue::Integer(value)
            } else if let Some(value) = value.as_u64() {
                TypedPropertyValue::UnsignedInteger(value)
            } else {
                TypedPropertyValue::Number(value.as_f64().expect("validated YAML number"))
            }
        }
        Value::String(value) => {
            if is_iso_date(value) {
                TypedPropertyValue::Date(value.clone())
            } else {
                TypedPropertyValue::String(value.clone())
            }
        }
        Value::Sequence(values) => {
            let normalized = values.iter().map(normalize_yaml_value).collect();
            TypedPropertyValue::List(normalized)
        }
        Value::Null => TypedPropertyValue::Null,
        Value::Tagged(tagged) => normalize_yaml_value(&tagged.value),
        Value::Mapping(_) => TypedPropertyValue::String(yaml_to_compact_string(value)),
    }
}

fn is_iso_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 10 {
        return false;
    }

    let shape = bytes[0].is_ascii_digit()
        && bytes[1].is_ascii_digit()
        && bytes[2].is_ascii_digit()
        && bytes[3].is_ascii_digit()
        && bytes[4] == b'-'
        && bytes[5].is_ascii_digit()
        && bytes[6].is_ascii_digit()
        && bytes[7] == b'-'
        && bytes[8].is_ascii_digit()
        && bytes[9].is_ascii_digit();
    if !shape {
        return false;
    }
    let year = u32::from(bytes[0] - b'0') * 1000
        + u32::from(bytes[1] - b'0') * 100
        + u32::from(bytes[2] - b'0') * 10
        + u32::from(bytes[3] - b'0');
    let month = (bytes[5] - b'0') * 10 + bytes[6] - b'0';
    let day = (bytes[8] - b'0') * 10 + bytes[9] - b'0';
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        _ => return false,
    };
    day >= 1 && day <= days
}

fn yaml_to_compact_string(value: &Value) -> String {
    serde_yaml::to_string(value)
        .unwrap_or_else(|_| "<invalid-yaml-mapping>".to_string())
        .replace('\n', " ")
        .trim()
        .to_string()
}

/// Typed projection errors.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PropertyProjectionError {
    /// Front matter root must be a mapping.
    #[error("front matter root must be a mapping")]
    ExpectedMappingRoot,
    /// YAML non-finite numbers have no lossless JSON numeric representation.
    #[error("front matter contains a non-finite number")]
    NonFiniteNumber,
}

/// Front matter parse errors.
#[derive(Debug, Error)]
pub enum FrontMatterError {
    /// Opening front matter fence was not closed.
    #[error("front matter fence is not closed")]
    UnclosedFence,
    /// Front matter YAML exceeded the configured byte limit.
    #[error("front matter exceeds {max_bytes} byte limit ({bytes} bytes)")]
    TooLarge {
        /// Observed YAML byte count at rejection time.
        bytes: usize,
        /// Maximum allowed YAML byte count.
        max_bytes: usize,
    },
    /// Front matter YAML exceeded the configured nesting depth limit.
    #[error("front matter exceeds {max_depth} level depth limit")]
    TooDeep {
        /// Maximum allowed YAML depth.
        max_depth: usize,
    },
    /// YAML parser reported malformed content.
    #[error("yaml parse failed: {source}")]
    YamlParse {
        /// Underlying YAML parser error.
        #[source]
        source: serde_yaml::Error,
    },
}

#[cfg(test)]
mod tests {
    use serde_yaml::Value;

    use super::{
        FrontMatterErrorCode, FrontMatterStatus, MAX_FRONT_MATTER_BYTES, MAX_FRONT_MATTER_DEPTH,
        PropertyProjectionError, TypedPropertyValue, extract_front_matter,
        project_typed_properties,
    };

    #[test]
    fn extract_parses_valid_front_matter() {
        let markdown = "---\ntitle: Today\ncount: 2\n---\n# Body";
        let extraction = extract_front_matter(markdown);

        match extraction.status {
            FrontMatterStatus::Parsed { value } => {
                assert_eq!(value["title"], Value::String("Today".to_string()));
                assert_eq!(value["count"], Value::Number(2.into()));
            }
            other => panic!("expected parsed status, got {other:?}"),
        }

        assert_eq!(extraction.body, "# Body");
    }

    #[test]
    fn extract_preserves_body_line_endings_after_front_matter() {
        let lf_markdown = "---\ntitle: Today\n---\n# Body\n";
        let lf_extraction = extract_front_matter(lf_markdown);

        assert!(matches!(
            lf_extraction.status,
            FrontMatterStatus::Parsed { .. }
        ));
        assert_eq!(lf_extraction.body, "# Body\n");

        let crlf_markdown = "---\r\ntitle: Today\r\n---\r\n# Body\r\n";
        let crlf_extraction = extract_front_matter(crlf_markdown);

        assert!(matches!(
            crlf_extraction.status,
            FrontMatterStatus::Parsed { .. }
        ));
        assert_eq!(crlf_extraction.body, "# Body\r\n");
    }

    #[test]
    fn extract_captures_yaml_parse_errors() {
        let markdown = "---\ntitle: [unclosed\n---\n# Body";
        let extraction = extract_front_matter(markdown);

        match extraction.status {
            FrontMatterStatus::Malformed { error, .. } => {
                assert!(error.contains("yaml parse failed"));
            }
            other => panic!("expected malformed status, got {other:?}"),
        }

        assert_eq!(extraction.body, "# Body");
    }

    #[test]
    fn extract_rejects_oversized_front_matter_without_raw_allocation() {
        let oversized_value = "a".repeat(MAX_FRONT_MATTER_BYTES + 1);
        let markdown = format!("---\ntitle: {oversized_value}\n---\n# Body");
        let extraction = extract_front_matter(&markdown);

        match extraction.status {
            FrontMatterStatus::Malformed { code, error } => {
                assert_eq!(code, FrontMatterErrorCode::TooLarge);
                assert!(error.contains("front matter exceeds"));
            }
            other => panic!("expected oversized malformed status, got {other:?}"),
        }
        assert_eq!(extraction.raw, None);
        assert_eq!(extraction.body, markdown);
    }

    #[test]
    fn extract_accepts_front_matter_at_size_limit() {
        let prefix = "title: ";
        let value = "a".repeat(MAX_FRONT_MATTER_BYTES - prefix.len());
        let markdown = format!("---\n{prefix}{value}\n---\n# Body");
        let extraction = extract_front_matter(&markdown);

        assert!(matches!(
            extraction.status,
            FrontMatterStatus::Parsed { .. }
        ));
    }

    #[test]
    fn extract_rejects_deeply_nested_front_matter() {
        let mut yaml = "value: ".to_string();
        for _ in 0..=MAX_FRONT_MATTER_DEPTH + 1 {
            yaml.push('[');
        }
        yaml.push_str("leaf");
        for _ in 0..=MAX_FRONT_MATTER_DEPTH + 1 {
            yaml.push(']');
        }
        let markdown = format!("---\n{yaml}\n---\n# Body");
        let extraction = extract_front_matter(&markdown);

        match extraction.status {
            FrontMatterStatus::Malformed { code, error } => {
                assert_eq!(code, FrontMatterErrorCode::TooDeep);
                assert!(error.contains("depth limit"));
            }
            other => panic!("expected depth malformed status, got {other:?}"),
        }
    }

    #[test]
    fn extract_marks_missing_when_no_front_matter_exists() {
        let markdown = "# Heading\nBody";
        let extraction = extract_front_matter(markdown);

        assert!(matches!(extraction.status, FrontMatterStatus::Missing));
        assert_eq!(extraction.raw, None);
        assert_eq!(extraction.body, markdown);
    }

    #[test]
    fn project_normalizes_bool_number_date_and_list_types() {
        let value: Value = serde_yaml::from_str(
            r#"
published: true
priority: 2
date: "2026-03-03"
tags:
  - alpha
  - beta
"#,
        )
        .expect("parse yaml");

        let properties = project_typed_properties(&value).expect("project properties");

        assert_eq!(
            properties[0].value,
            TypedPropertyValue::Date("2026-03-03".to_string())
        );
        assert_eq!(properties[1].value, TypedPropertyValue::Integer(2));
        assert_eq!(properties[2].value, TypedPropertyValue::Bool(true));
        assert_eq!(
            properties[3].value,
            TypedPropertyValue::List(vec![
                TypedPropertyValue::String("alpha".to_string()),
                TypedPropertyValue::String("beta".to_string())
            ])
        );
    }

    #[test]
    fn project_rejects_non_mapping_roots() {
        let value: Value = serde_yaml::from_str("- one\n- two").expect("parse yaml list");
        let error = project_typed_properties(&value).expect_err("non-mapping should fail");
        assert_eq!(error, PropertyProjectionError::ExpectedMappingRoot);
    }

    #[test]
    fn empty_frontmatter_is_empty_metadata_and_invalid_roots_preserve_body() {
        let empty = extract_front_matter("---\n---\n# Keep");
        let FrontMatterStatus::Parsed { value } = empty.status else {
            panic!("empty mapping")
        };
        assert!(project_typed_properties(&value).unwrap().is_empty());
        assert_eq!(empty.body, "# Keep");
        for yaml in ["42", "- one", "number: .nan", "number: .inf"] {
            let result = extract_front_matter(&format!("---\n{yaml}\n---\n# Keep"));
            assert!(matches!(result.status, FrontMatterStatus::Malformed { .. }));
            assert_eq!(result.body, "# Keep");
        }
    }

    #[test]
    fn preserves_integer_precision_alias_items_and_real_dates() {
        let value: Value = serde_yaml::from_str("large: 9007199254740993\nunsigned: 18446744073709551615\naliases: ['Smith, John', 'New York']\ninvalid_date: '2026-02-29'\nvalid_date: '2024-02-29'\n").unwrap();
        let values = project_typed_properties(&value)
            .unwrap()
            .into_iter()
            .map(|p| (p.key, p.value))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            values["large"],
            TypedPropertyValue::Integer(9_007_199_254_740_993)
        );
        assert_eq!(
            values["unsigned"],
            TypedPropertyValue::UnsignedInteger(u64::MAX)
        );
        assert_eq!(
            values["aliases"],
            TypedPropertyValue::List(vec![
                TypedPropertyValue::String("Smith, John".into()),
                TypedPropertyValue::String("New York".into())
            ])
        );
        assert_eq!(
            values["invalid_date"],
            TypedPropertyValue::String("2026-02-29".into())
        );
        assert_eq!(
            values["valid_date"],
            TypedPropertyValue::Date("2024-02-29".into())
        );
    }

    #[test]
    fn project_applies_default_property_key_mappings() {
        let value: Value = serde_yaml::from_str(
            r##"
tag: "#alpha #beta"
aliases: "Project Alpha"
alias:
  - Project Beta
cssclass: "wide compact"
cssclasses:
  - card
  - wide
"##,
        )
        .expect("parse yaml");

        let properties = project_typed_properties(&value).expect("project properties");
        assert_eq!(properties.len(), 3);
        assert_eq!(properties[0].key, "aliases");
        assert_eq!(properties[1].key, "cssclasses");
        assert_eq!(properties[2].key, "tags");

        assert_eq!(
            properties[0].value,
            TypedPropertyValue::List(vec![
                TypedPropertyValue::String("Project Alpha".to_string()),
                TypedPropertyValue::String("Project Beta".to_string())
            ])
        );
        assert_eq!(
            properties[1].value,
            TypedPropertyValue::List(vec![
                TypedPropertyValue::String("wide".to_string()),
                TypedPropertyValue::String("compact".to_string()),
                TypedPropertyValue::String("card".to_string())
            ])
        );
        assert_eq!(
            properties[2].value,
            TypedPropertyValue::List(vec![
                TypedPropertyValue::String("alpha".to_string()),
                TypedPropertyValue::String("beta".to_string())
            ])
        );
    }
    #[test]
    fn utf8_bom_preserves_frontmatter_and_body() {
        let source = "\u{feff}---\r\ntitle: Café\r\n---\r\n# Heading\r\n[[target]]\r\n";
        let extraction = extract_front_matter(source);
        assert!(matches!(
            extraction.status,
            FrontMatterStatus::Parsed { .. }
        ));
        assert_eq!(extraction.body, "# Heading\r\n[[target]]\r\n");
        assert_eq!(
            extract_front_matter("\u{feff}# Heading\n").body,
            "# Heading\n"
        );
    }
}
