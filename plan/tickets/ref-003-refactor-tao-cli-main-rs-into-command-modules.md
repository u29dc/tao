## REF-003 Refactor tao-cli main.rs into command modules

Status: done

### Objective
Eliminate CLI monolith by splitting command implementation into per-domain modules.

### Scope
CLI crate source layout.

### Concrete Steps
1. Create src/commands directory with one module per command family.
2. Move argument structs and handlers into their command module.
3. Create shared JSON envelope and error mapping modules.
4. Keep main.rs under 300 lines by limiting it to startup and dispatch.

### Required Files and Locations
- crates/tao-cli/src/main.rs
- crates/tao-cli/src/commands/
- crates/tao-cli/src/error.rs

### Implementation Notes
Maintain exact CLI behavior and flags while restructuring.

### Dependencies
- CLI-003

### Acceptance Criteria
- [ ] main.rs is significantly reduced and readable.
- [ ] Command modules compile and tests pass.
- [ ] CLI help and JSON contract outputs are unchanged except intentional changes from other tickets.
