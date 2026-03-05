use std::cmp::Ordering;
use std::path::Path;

/// Derive a stable note title from a normalized vault-relative path.
#[must_use]
pub fn note_title_from_path(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map_or_else(|| path.to_string(), |stem| stem.to_string())
}

/// Derive a stable folder segment from a normalized vault-relative path.
#[must_use]
pub fn note_folder_from_path(path: &str) -> String {
    Path::new(path)
        .parent()
        .and_then(|parent| parent.to_str())
        .map_or_else(String::new, |parent| parent.to_string())
}

/// Normalize one path-like token for deterministic comparisons.
#[must_use]
pub fn normalize_path_like(value: &str) -> String {
    value.trim().trim_matches('/').replace('\\', "/")
}

/// Deterministic lexical compare for normalized path-like values.
#[must_use]
pub fn cmp_normalized_paths(left: &str, right: &str) -> Ordering {
    normalize_path_like(left)
        .to_ascii_lowercase()
        .cmp(&normalize_path_like(right).to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::{
        cmp_normalized_paths, normalize_path_like, note_folder_from_path, note_title_from_path,
    };

    #[test]
    fn derives_title_and_folder_from_path() {
        assert_eq!(note_title_from_path("notes/projects/alpha.md"), "alpha");
        assert_eq!(
            note_folder_from_path("notes/projects/alpha.md"),
            "notes/projects"
        );
    }

    #[test]
    fn normalizes_and_compares_path_like_tokens() {
        assert_eq!(normalize_path_like("/Notes\\Alpha.md/"), "Notes/Alpha.md");
        assert_eq!(
            cmp_normalized_paths("Notes/Alpha.md", "notes/alpha.md"),
            std::cmp::Ordering::Equal
        );
    }
}
