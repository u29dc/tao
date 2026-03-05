## Crate

`tao-sdk-config`

## Purpose

Load, normalize, and bootstrap Tao configuration from defaults and TOML files.

## Public API

- `TaoConfig` and typed nested config structs
- `parse_toml`, `load_from_path`, `load_or_bootstrap`
- `default_template`, `config_path`

## Internal Design

- Typed config schema with strict normalization.
- Bootstrap helper to create default config file when missing.

## Data Flow

Path/defaults -> TOML parse -> typed config -> normalization -> consumer crates.

## Dependencies

- External: `serde`, `toml`, `thiserror`

## Testing

- `cargo test -p tao-sdk-config --release`
- Tests cover defaults, merge semantics, bootstrap behavior, and invalid path handling.

## Limits

- Does not manage runtime overrides directly; callers compose final precedence.
