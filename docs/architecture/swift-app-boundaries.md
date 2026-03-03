# Swift App Module Boundaries

## Goal

Define module boundaries so Swift remains a native UI adapter over Rust SDK.

## Proposed Swift Targets

- `TaoApp`: app lifecycle, window scene, startup routing.
- `TaoFeatureNavigation`: vault picker, file tree/list, command palette.
- `TaoFeatureNote`: note reading/editing screen orchestration.
- `TaoFeatureProperties`: properties panel and editing controls.
- `TaoFeatureBases`: bases table view and interactions.
- `TaoBridgeClient`: Swift-facing wrapper over Rust bridge bindings.
- `TaoDesignSystem`: typography, spacing, color tokens, motion policy.
- `TaoTestingSupport`: fixture loading and test doubles for UI tests.

## Dependency Rules

- Feature modules depend on `TaoBridgeClient` interfaces, not raw FFI types.
- `TaoBridgeClient` depends on generated bindings and DTO mappers.
- `TaoDesignSystem` has no dependency on feature modules.
- No module may perform vault parsing, link resolution, or DB access directly.

## Folder Layout (initial)

```text
apps/tao-macos/
  README.md
  TaoApp/
  TaoFeatureNavigation/
  TaoFeatureNote/
  TaoFeatureProperties/
  TaoFeatureBases/
  TaoBridgeClient/
  TaoDesignSystem/
  TaoTestingSupport/
```

## Build Contract

- Swift build must succeed headless in CI.
- Bridge API mismatches fail compile in `TaoBridgeClient`.
- UI smoke tests must launch app and open sample vault.
