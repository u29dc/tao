## Crate

`tao-sdk-links`

## Purpose

Parse wikilinks and resolve note/heading/block targets deterministically.

## Public API

- `parse_wikilink`, `extract_wikilinks`
- `resolve_target`, `resolve_heading_target`, `resolve_block_target`
- `slugify_heading`, `extract_block_ids`

## Internal Design

- Tokenization/parser for wikilink syntax.
- Resolver routines for canonical path and fragment matching.

## Data Flow

Markdown/frontmatter strings -> wikilink tokens -> resolver -> canonical link resolution metadata.

## Dependencies

- External: `thiserror`

## Testing

- `cargo test -p tao-sdk-links --release`
- Tests cover parsing, ambiguity handling, heading and block resolution.

## Limits

- Storage and indexing of links happen in service/storage crates.
