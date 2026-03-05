## CLI-002 Standardize structured error codes across CLI

Status: todo

### Objective
Ensure every command returns stable error codes and contextual messages.

### Scope
Error mapping in command dispatch and SDK error translation.

### Concrete Steps
1. Define CLI error code registry in one module.
2. Map known SDK/storage/parser errors to registry codes.
3. Ensure JSON envelope includes code, message, and optional hint.
4. Add contract tests for representative error paths.

### Required Files and Locations
- crates/tao-cli/src/main.rs
- crates/tao-sdk-bridge/src/lib.rs
- crates/tao-cli/tests/json_contracts.rs

### Implementation Notes
Keep messages concise and automation-friendly.

### Dependencies
- CLI-001

### Acceptance Criteria
- [ ] All failing command tests assert stable error codes.
- [ ] Envelope shape is consistent across command families.
- [ ] No internal stack traces leak in user-facing errors.
