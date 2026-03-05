## Crate

`tao-sdk-core`

## Purpose

Host core cross-cutting primitives shared across SDK crates.

## Public API

- Event bus primitives in `event_bus`
- Core types exported from `lib.rs`

## Internal Design

- Keep only stable foundational utilities.
- Avoid domain-specific policy in this crate.

## Data Flow

Producer crate publishes event -> subscribers consume typed payloads.

## Dependencies

- No external runtime dependencies.

## Testing

- `cargo test -p tao-sdk-core --release`
- Event fan-out and unsubscribe behavior covered by unit tests.

## Limits

- Not a general utility dumping ground.
