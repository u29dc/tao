## CLI-004 Harden daemon socket lifecycle and stale recovery

Status: done

### Objective
Ensure daemon mode recovers cleanly from stale sockets and orphaned processes.

### Scope
Daemon start/status/stop logic and tests.

### Concrete Steps
1. Detect stale socket files before daemon start and recover safely.
2. Add liveness checks and clear error messages for dead sockets.
3. Add daemon integration tests for stale/restart flows.
4. Update bench scripts to tolerate stale sockets via explicit stop/start.

### Required Files and Locations
- crates/tao-sdk-service/src/lib.rs
- crates/tao-cli/src/main.rs
- scripts/bench.sh

### Implementation Notes
Use deterministic socket paths under fixture-local .tao directory.

### Dependencies
- CLI-003

### Acceptance Criteria
- [ ] Daemon start succeeds after stale socket cleanup.
- [ ] status command reports stale/dead states accurately.
- [ ] Integration tests cover stale socket and restart scenarios.
