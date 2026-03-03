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

    use super::{FrontMatterStatus, extract_front_matter};

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
}
