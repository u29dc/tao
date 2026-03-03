# Bridge DTO Contracts

## Purpose

Define versioned DTO contracts crossing the Swift<->Rust bridge.

## Versioning

- Contract version starts at `v1`.
- Additive fields are allowed in minor updates.
- Field removals/renames require major contract bump.
- Every DTO includes `schema_version` when serialized through bridge boundary.

## DTO Conventions

- Use explicit optional fields for nullable values.
- Use string enums at boundary for forward compatibility.
- Use RFC3339 UTC timestamps.
- Use stable IDs, never array index positions, as references.

## Core DTOs

### BridgeEnvelope

```text
BridgeEnvelope<T> {
  schema_version: String,
  ok: bool,
  value: Option<T>,
  error: Option<BridgeError>
}
```

### BridgeError

```text
BridgeError {
  code: String,
  message: String,
  hint: Option<String>,
  context: Map<String, JsonValue>
}
```

### BridgeNoteSummary

```text
BridgeNoteSummary {
  id: String,
  path: String,
  title: String,
  updated_at: Option<String>
}
```

### BridgePropertyValue

```text
BridgePropertyValue {
  key: String,
  kind: String,
  text: Option<String>,
  number: Option<f64>,
  bool: Option<bool>,
  date: Option<String>,
  json: Option<String>
}
```

### BridgeLinkRef

```text
BridgeLinkRef {
  source_path: String,
  target_path: Option<String>,
  heading: Option<String>,
  block_id: Option<String>,
  display_text: Option<String>,
  kind: String,
  resolved: bool
}
```

## Backward Compatibility Rules

- Rust bridge crate exposes adapters for prior minor DTO versions for one release window.
- Swift `ObsBridgeClient` must map unknown enum values to `unknown` case.
- Unknown fields must be ignored, not rejected.

## Validation

- Add contract tests in bridge crate to serialize/deserialize canonical fixtures.
- Add Swift decoding tests for mixed-version payload fixtures.
