use std::path::Path;

/// Content interpretation selected from a case-insensitive file extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    /// Markdown text, metadata and graph structure.
    Markdown,
    /// A structured Base definition.
    Base,
    /// Plain text without Markdown semantics.
    PlainText,
    /// PDF content is processed by the extraction worker.
    Pdf,
    /// An inventory-only asset without a text interpretation.
    Other,
}

impl FileKind {
    /// Whether ordinary fingerprinting verifies the complete text revision.
    #[must_use]
    pub fn is_text(self) -> bool {
        matches!(self, Self::Markdown | Self::Base | Self::PlainText)
    }
}

/// Return a lowercase extension without a leading dot, when present.
#[must_use]
pub fn normalized_extension(path: &Path) -> Option<String> {
    path.extension()
        .and_then(|extension| extension.to_str())
        .filter(|extension| !extension.is_empty())
        .map(str::to_lowercase)
}

/// Classify file content using the shared extension policy.
#[must_use]
pub fn file_kind(path: &Path) -> FileKind {
    match normalized_extension(path).as_deref() {
        Some("md") => FileKind::Markdown,
        Some("base") => FileKind::Base,
        Some("txt") => FileKind::PlainText,
        Some("pdf") => FileKind::Pdf,
        _ => FileKind::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::{FileKind, file_kind, normalized_extension};
    use std::path::Path;

    #[test]
    fn extension_case_has_one_interpretation() {
        for (name, expected) in [
            ("note.MD", FileKind::Markdown),
            ("view.BaSe", FileKind::Base),
            ("text.TXT", FileKind::PlainText),
            ("document.PdF", FileKind::Pdf),
            ("image.PNG", FileKind::Other),
            ("README", FileKind::Other),
        ] {
            assert_eq!(file_kind(Path::new(name)), expected);
        }
        assert_eq!(
            normalized_extension(Path::new("image.PNG")),
            Some("png".into())
        );
        assert_eq!(normalized_extension(Path::new("trailing.")), None);
    }
}
