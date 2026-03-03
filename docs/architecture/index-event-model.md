# Index Event Model

## Purpose

Define typed events emitted by SDK indexing and watch pipelines.

## Event Envelope

```text
IndexEvent {
  event_id: String,
  occurred_at: String,
  generation: u64,
  kind: String,
  payload: JsonValue
}
```

## Event Kinds

- `index.scan.started`
- `index.scan.progress`
- `index.scan.completed`
- `index.scan.failed`
- `index.file.changed`
- `index.file.removed`
- `index.file.reindexed`
- `index.links.updated`
- `index.properties.updated`
- `index.bases.updated`
- `index.reconcile.started`
- `index.reconcile.completed`

## Payload Contracts

### `index.scan.progress`

```text
{
  files_seen: u64,
  files_total_estimate: u64,
  changed_candidates: u64
}
```

### `index.file.reindexed`

```text
{
  path: String,
  file_id: String,
  changed_sections: Vec<String>
}
```

### `index.reconcile.completed`

```text
{
  drift_detected: bool,
  repaired_files: u64,
  orphan_records_removed: u64
}
```

## Delivery Rules

- Events are ordered per generation.
- Consumers must tolerate duplicate delivery.
- Consumers must ignore unknown event kinds.
- UI should debounce high-frequency progress events.

## Error Events

- `index.scan.failed` payload includes `code`, `message`, `hint`, and `retryable`.
