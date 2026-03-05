## BASE-008 Add base schema introspection command

Status: done

### Objective
Expose base/view schema and derived field metadata for tooling and agents.

### Scope
CLI command, SDK API, and JSON contract tests.

### Concrete Steps
1. Add SDK API returning parsed schema summary for one base file.
2. Expose command `base schema --path <base-file>`.
3. Return fields, types, filters, sorts, groups, and rollups metadata.
4. Add contract tests covering valid and invalid schema files.

### Required Files and Locations
- crates/tao-sdk-service/src/lib.rs
- crates/tao-cli/src/main.rs
- crates/tao-cli/tests/json_contracts.rs

### Implementation Notes
This command is read-only and should not require write gates.

### Dependencies
- BASE-001

### Acceptance Criteria
- [x] base schema returns structured metadata for valid base files.
- [x] Invalid base files return structured parse errors.
- [x] Command appears in CLI help and JSON contract tests.
