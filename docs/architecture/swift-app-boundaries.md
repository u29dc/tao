# Swift App Module Boundaries

## Goal

Define module boundaries so Swift remains a native UI adapter over Rust SDK.

## Proposed Swift Targets

- `ObsApp`: app lifecycle, window scene, startup routing.
- `ObsFeatureNavigation`: vault picker, file tree/list, command palette.
- `ObsFeatureNote`: note reading/editing screen orchestration.
- `ObsFeatureProperties`: properties panel and editing controls.
- `ObsFeatureBases`: bases table view and interactions.
- `ObsBridgeClient`: Swift-facing wrapper over Rust bridge bindings.
- `ObsDesignSystem`: typography, spacing, color tokens, motion policy.
- `ObsTestingSupport`: fixture loading and test doubles for UI tests.

## Dependency Rules

- Feature modules depend on `ObsBridgeClient` interfaces, not raw FFI types.
- `ObsBridgeClient` depends on generated bindings and DTO mappers.
- `ObsDesignSystem` has no dependency on feature modules.
- No module may perform vault parsing, link resolution, or DB access directly.

## Folder Layout (initial)

```text
apps/obs-macos/
  README.md
  ObsApp/
  ObsFeatureNavigation/
  ObsFeatureNote/
  ObsFeatureProperties/
  ObsFeatureBases/
  ObsBridgeClient/
  ObsDesignSystem/
  ObsTestingSupport/
```

## Build Contract

- Swift build must succeed headless in CI.
- Bridge API mismatches fail compile in `ObsBridgeClient`.
- UI smoke tests must launch app and open sample vault.
