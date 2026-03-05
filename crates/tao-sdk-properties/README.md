## Crate

`tao-sdk-properties`

## Purpose

Extract frontmatter and project typed property values for indexing and query workflows.

## Public API

- `extract_front_matter`
- `project_typed_properties`
- Typed property status/value enums

## Internal Design

- YAML extraction layer with explicit malformed/absent states.
- Typed projection for bool/number/date/list/string values.

## Data Flow

Markdown text -> frontmatter extraction -> typed property projection -> service/storage consumers.

## Dependencies

- External: `serde`, `serde_yaml`, `thiserror`

## Testing

- `cargo test -p tao-sdk-properties --release`
- Tests cover malformed YAML, type projection, and default key mappings.

## Limits

- Query-level aggregation is outside this crate.
