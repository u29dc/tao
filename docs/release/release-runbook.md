# Release Checklist and Runbook

## Scope

Use this runbook for `rc` and stable releases of `obs` workspace artifacts.

## Preflight Checklist

1. Confirm branch is `main` and clean.
2. Confirm release ticket dependencies are `done` in `plan/tickets.csv`.
3. Run `bun run util:check`.
4. Run `./scripts/check-perf-budgets.sh`.
5. Run `swift test --package-path apps/obs-macos`.
6. Validate migration preflight path:
   - `cargo run -p obs-cli -- --json vault preflight --vault-root <vault> --db-path <db>`
7. Verify release docs are present:
   - `docs/release/versioning-policy.md`
   - `docs/release/release-runbook.md`

## Build and Package

1. CLI/TUI package:
   - `./scripts/release-package-cli.sh`
2. macOS package (ad-hoc signed artifact):
   - CI workflow `swift-release-artifact` produces `.app.zip`.

## Version and Tag

1. Choose next tag:
   - RC: `vX.Y.Z-rc.N`
   - Stable: `vX.Y.Z`
2. Update release report file:
   - `docs/release/vX.Y.Z-rc.N.md` or `docs/release/vX.Y.Z.md`
3. Commit release report and version updates.
4. Create signed tag:
   - `git tag -s vX.Y.Z-rc.N -m "release vX.Y.Z-rc.N"`
5. Push branch and tags:
   - `git push origin main --follow-tags`

## Validation After Publish

1. Confirm CI status green for:
   - `rust-ci`
   - `swift-ci`
   - `swift-release-artifact`
2. Confirm artifacts uploaded in Actions:
   - CLI/TUI release bundle
   - macOS signed app bundle
3. Smoke-check CLI install:
   - `obs --help`
   - `obs --json vault open --vault-root <vault> --db-path <db>`

## Rollback Plan

1. Stop release announcements and mark release as blocked.
2. Identify bad commit/tag scope.
3. Revert release commits on `main`:
   - `git revert <sha-range>`
4. Publish rollback report in `docs/release/rollback-<date>.md` with:
   - symptom
   - impact
   - root cause
   - mitigation
5. Retag with next patch/rc once fixed.

## Failure Classifications

- Build failure: compile/test/package breaks.
- Contract failure: JSON/bridge schema regression.
- Migration failure: preflight mismatch or migration apply failure.
- Perf failure: budget gate regression.
- Runtime failure: smoke test fails after artifact build.
