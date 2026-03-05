## Crate

`tao-tui`

## Purpose

Provide a minimal terminal shell entrypoint for Tao operations.

## Public API

- Binary: `tao-tui`
- Placeholder route shell and command palette scaffolding

## Internal Design

- Keeps terminal route handling isolated from SDK service logic.
- Intended as lightweight adapter, not primary product surface.

## Data Flow

TUI input -> route command handling -> SDK/bridge calls -> terminal rendering.

## Dependencies

- External: `clap`

## Testing

- `cargo test -p tao-tui --release`

## Limits

- Feature scope is intentionally limited compared with CLI and macOS app.
