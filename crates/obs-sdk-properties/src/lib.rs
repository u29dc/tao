//! Front matter extraction and typed property projection utilities.

use serde_yaml::Value;
use thiserror::Error;

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
#[derive(Debug, Clone, PartialEq)]
pub enum FrontMatterStatus {
    /// Front matter block not present.
    Missing,
    /// Front matter parsed successfully.
    Parsed { value: Value },
    /// Front matter existed but could not be parsed.
    Malformed { error: String },
}

/// Extract front matter from markdown and capture parse failures without panicking.
#[must_use]
pub fn extract_front_matter(markdown: &str) -> FrontMatterExtraction {
    let lines: Vec<&str> = markdown.lines().collect();
    if lines.first() != Some(&"---") {
        return FrontMatterExtraction {
            raw: None,
            body: markdown.to_string(),
            status: FrontMatterStatus::Missing,
        };
    }

    let Some(closing_index) = lines
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(index, line)| (*line == "---").then_some(index))
    else {
        return FrontMatterExtraction {
            raw: Some(lines[1..].join("\n")),
            body: markdown.to_string(),
            status: FrontMatterStatus::Malformed {
                error: FrontMatterError::UnclosedFence.to_string(),
            },
        };
    };

    let raw = lines[1..closing_index].join("\n");
    let body = if closing_index + 1 < lines.len() {
        lines[(closing_index + 1)..].join("\n")
    } else {
        String::new()
    };

    match serde_yaml::from_str::<Value>(&raw) {
        Ok(value) => FrontMatterExtraction {
            raw: Some(raw),
            body,
            status: FrontMatterStatus::Parsed { value },
        },
        Err(source) => FrontMatterExtraction {
            raw: Some(raw),
            body,
            status: FrontMatterStatus::Malformed {
                error: FrontMatterError::YamlParse { source }.to_string(),
            },
        },
    }
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
    /// Numeric value coerced to f64.
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
    let Value::Mapping(mapping) = front_matter else {
        return Err(PropertyProjectionError::ExpectedMappingRoot);
    };

    let mut projected = Vec::new();
    for (key, value) in mapping {
        let Value::String(key) = key else {
            continue;
        };

        projected.push(TypedProperty {
            key: key.clone(),
            value: normalize_yaml_value(value),
        });
    }

    projected.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(projected)
}

fn normalize_yaml_value(value: &Value) -> TypedPropertyValue {
    match value {
        Value::Bool(value) => TypedPropertyValue::Bool(*value),
        Value::Number(value) => TypedPropertyValue::Number(value.as_f64().unwrap_or(0.0)),
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
    if bytes.len() < 10 {
        return false;
    }

    bytes[0].is_ascii_digit()
        && bytes[1].is_ascii_digit()
        && bytes[2].is_ascii_digit()
        && bytes[3].is_ascii_digit()
        && bytes[4] == b'-'
        && bytes[5].is_ascii_digit()
        && bytes[6].is_ascii_digit()
        && bytes[7] == b'-'
        && bytes[8].is_ascii_digit()
        && bytes[9].is_ascii_digit()
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
}

/// Front matter parse errors.
#[derive(Debug, Error)]
pub enum FrontMatterError {
    /// Opening front matter fence was not closed.
    #[error("front matter fence is not closed")]
    UnclosedFence,
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
        FrontMatterStatus, PropertyProjectionError, TypedPropertyValue, extract_front_matter,
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
    fn extract_captures_yaml_parse_errors() {
        let markdown = "---\ntitle: [unclosed\n---\n# Body";
        let extraction = extract_front_matter(markdown);

        match extraction.status {
            FrontMatterStatus::Malformed { error } => {
                assert!(error.contains("yaml parse failed"));
            }
            other => panic!("expected malformed status, got {other:?}"),
        }

        assert_eq!(extraction.body, "# Body");
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
        assert_eq!(properties[1].value, TypedPropertyValue::Number(2.0));
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
}
