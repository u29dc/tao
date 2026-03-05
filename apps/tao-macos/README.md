# tao-macos

Native macOS Swift application shell for `tao`.

This app must remain a UI adapter over Rust SDK services and must not contain domain logic.

## Active Targets

- TaoApp
- TaoFeatureNavigation
- TaoFeatureNote
- TaoFeatureProperties
- TaoFeatureBases
- TaoBridgeClient
- TaoDesignSystem
- TaoTestingSupport

## Status

Production app shell is implemented.

- SwiftUI split view renders indexed file tree, note detail, and settings-driven vault selection.
- TaoBridgeClient is the active boundary over Rust bridge/runtime APIs.
- Package and scaffold tests cover launch, note read/write, links, bases, and event polling.
