## Crate

`tao-sdk-markdown`

## Purpose

Parse markdown notes into structured title/frontmatter/body representations with render-cache support.

## Public API

- Markdown parser and parse result structures
- Render-cache interfaces in `render_cache`

## Internal Design

- Frontmatter-aware parser with validation for malformed headers.
- Optional render-cache keyed by content hash.

## Data Flow

Raw markdown -> parse -> structured note fields -> optional render cache lookup/update.

## Dependencies

- External: `blake3`, `thiserror`

## Testing

- `cargo test -p tao-sdk-markdown --release`
- Tests cover frontmatter extraction, title fallback, and cache behavior.

## Limits

- Does not persist parsed artifacts; persistence is service/storage responsibility.
