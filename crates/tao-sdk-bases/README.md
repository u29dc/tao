## Crate

`tao-sdk-bases`

## Purpose

Parse, validate, and plan `.base` view definitions for table-oriented note queries.

## Public API

- `parse_base_document`
- `validate_base_yaml`
- `BaseViewRegistry`
- `BaseTableQueryPlanner`

## Internal Design

- YAML parsing into typed view structs.
- Validation pass for schema and diagnostic reporting.
- Planner that compiles view config into executable query plan requests.

## Data Flow

Base YAML -> parser -> typed document -> validation -> registry/planner -> service execution.

## Dependencies

- External: `serde`, `serde_json`, `serde_yaml`, `thiserror`

## Testing

- `cargo test -p tao-sdk-bases --release`
- Unit tests cover parsing variants, validation diagnostics, and planner compilation.

## Limits

- Execution is performed by service/storage layers, not by this crate alone.

## Query semantics

- Numeric equality and membership compare values, so `1` and `1.0` match. Integer comparisons do not round through floating point, including values above `2^53`.
- Ordered predicates do not match missing or explicit-null values. Incompatible non-null operand types are errors. Sorting has a separate deterministic type order; descending direction does not reverse explicit null placement.
- `exists` distinguishes an absent property from an explicit null. `is_empty` matches absence, null, an empty string, or an empty collection. Whitespace, `0`, and `false` are not empty.
- Property keys are case-sensitive. File aliases such as `file.name` retain their existing canonical column names; use `note.title`, `note.path`, etc. to read frontmatter fields that collide with file metadata.
- Root Obsidian filters currently support an `and` sequence of supported expressions. Unsupported logical structures, formulas, execution-affecting fields, invalid operands, and invalid aggregate/rollup references are reported rather than ignored.
- Filters run before grouping. Grouped sorts refer to grouping keys or aggregate aliases; aggregate filters are not supported. Count counts rows and accepts no source key. Integer-only sums remain integers and fail explicitly when outside the supported JSON number range.

## Execution

The service applies literal folder scopes, filters independent of derived fields before relation work, then groups, sorts output, and pages. Grouped rows have a stable `group_` identity and no arbitrary member file path. Ambiguous relation resolution is included in diagnostics.

Simple path-ordered queries without summaries use SQL count and pagination, including exact string/bool/null/presence predicates. Complex queries use a typed in-memory fallback and select the required sorted prefix. Property hydration and rollups use bounded SQL parameter batches. Corpus generation materializes each view once and shares a lazy relation index across views in its unchanged database snapshot.
