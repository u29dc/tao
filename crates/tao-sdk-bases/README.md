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
