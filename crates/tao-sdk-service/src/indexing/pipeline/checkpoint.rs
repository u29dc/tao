use super::*;

/// Result payload for checkpointed incremental indexing workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointedIndexResult {
    /// Whether this run resumed from a previously persisted checkpoint.
    pub started_from_checkpoint: bool,
    /// Total unique paths tracked by the active checkpoint.
    pub total_paths: u64,
    /// Number of unique paths processed in this invocation.
    pub processed_paths: u64,
    /// Number of unique paths still pending in checkpoint state.
    pub remaining_paths: u64,
    /// Number of incremental batches applied in this invocation.
    pub batches_applied: u64,
    /// Number of file rows upserted in this invocation.
    pub upserted_files: u64,
    /// Number of file rows removed in this invocation.
    pub removed_files: u64,
    /// Number of link rows rebuilt in this invocation.
    pub links_reindexed: u64,
    /// Number of property rows rebuilt in this invocation.
    pub properties_reindexed: u64,
    /// Number of base rows rebuilt in this invocation.
    pub bases_reindexed: u64,
    /// Whether checkpoint state was fully consumed and removed.
    pub checkpoint_completed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncrementalCheckpointState {
    pending_paths: Vec<String>,
    next_offset: usize,
    max_batch_size: usize,
    case_policy: String,
    created_unix_ms: u128,
    updated_unix_ms: u128,
}

/// Service that persists incremental indexing checkpoints and resumes after restart.
#[derive(Debug, Default, Clone, Copy)]
pub struct CheckpointedIndexService {
    incremental: IncrementalIndexService,
}

impl CheckpointedIndexService {
    /// Apply incremental indexing with persisted checkpoints.
    ///
    /// When `changed_paths` is empty, this resumes from checkpoint state if present.
    /// When `changed_paths` is non-empty, this starts a new checkpoint run.
    pub fn apply_checkpointed(
        &self,
        vault_root: &Path,
        connection: &mut Connection,
        changed_paths: &[PathBuf],
        max_batch_size: usize,
        max_batches_per_run: Option<usize>,
        case_policy: CasePolicy,
    ) -> Result<CheckpointedIndexResult, CheckpointedIndexError> {
        if max_batch_size == 0 {
            return Err(CheckpointedIndexError::InvalidBatchSize { value: 0 });
        }
        if matches!(max_batches_per_run, Some(0)) {
            return Err(CheckpointedIndexError::InvalidBatchLimit { value: 0 });
        }

        let (mut checkpoint, started_from_checkpoint, effective_case_policy) =
            if changed_paths.is_empty() {
                let Some(state) = load_checkpoint_state(connection)? else {
                    return Ok(CheckpointedIndexResult {
                        started_from_checkpoint: true,
                        total_paths: 0,
                        processed_paths: 0,
                        remaining_paths: 0,
                        batches_applied: 0,
                        upserted_files: 0,
                        removed_files: 0,
                        links_reindexed: 0,
                        properties_reindexed: 0,
                        bases_reindexed: 0,
                        checkpoint_completed: true,
                    });
                };
                let policy = parse_checkpoint_case_policy(&state.case_policy)?;
                (state, true, policy)
            } else {
                let mut seen = std::collections::BTreeSet::new();
                let mut pending_paths = Vec::new();
                for path in changed_paths {
                    let normalized = normalize_changed_path(path).map_err(|source| {
                        CheckpointedIndexError::NormalizeChangedPath {
                            source: Box::new(source),
                        }
                    })?;
                    if seen.insert(normalized.clone()) {
                        pending_paths.push(normalized);
                    }
                }

                let now_unix_ms =
                    current_unix_ms_raw().map_err(|source| CheckpointedIndexError::Clock {
                        source: Box::new(source),
                    })?;
                let state = IncrementalCheckpointState {
                    pending_paths,
                    next_offset: 0,
                    max_batch_size,
                    case_policy: checkpoint_case_policy_label(case_policy).to_string(),
                    created_unix_ms: now_unix_ms,
                    updated_unix_ms: now_unix_ms,
                };
                save_checkpoint_state(connection, &state)?;
                (state, false, case_policy)
            };

        let mut processed_paths = 0_u64;
        let mut batches_applied = 0_u64;
        let mut upserted_files = 0_u64;
        let mut removed_files = 0_u64;
        let mut links_reindexed = 0_u64;
        let mut properties_reindexed = 0_u64;
        let mut bases_reindexed = 0_u64;

        while checkpoint.next_offset < checkpoint.pending_paths.len() {
            if max_batches_per_run
                .is_some_and(|limit| batches_applied >= u64::try_from(limit).unwrap_or(u64::MAX))
            {
                break;
            }

            let batch_end = std::cmp::min(
                checkpoint.next_offset + checkpoint.max_batch_size,
                checkpoint.pending_paths.len(),
            );
            let batch_paths = checkpoint.pending_paths[checkpoint.next_offset..batch_end]
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<_>>();

            let batch_result = self
                .incremental
                .apply_changes(vault_root, connection, &batch_paths, effective_case_policy)
                .map_err(|source| CheckpointedIndexError::ApplyIncremental {
                    source: Box::new(source),
                })?;

            checkpoint.next_offset = batch_end;
            checkpoint.updated_unix_ms =
                current_unix_ms_raw().map_err(|source| CheckpointedIndexError::Clock {
                    source: Box::new(source),
                })?;
            save_checkpoint_state(connection, &checkpoint)?;

            processed_paths += batch_result.processed_paths;
            batches_applied += 1;
            upserted_files += batch_result.upserted_files;
            removed_files += batch_result.removed_files;
            links_reindexed += batch_result.links_reindexed;
            properties_reindexed += batch_result.properties_reindexed;
            bases_reindexed += batch_result.bases_reindexed;
        }

        let total_paths = checkpoint.pending_paths.len() as u64;
        let remaining_paths = total_paths - checkpoint.next_offset as u64;
        let checkpoint_completed = remaining_paths == 0;

        if checkpoint_completed {
            IndexStateRepository::delete_by_key(connection, CHECKPOINT_STATE_KEY).map_err(
                |source| CheckpointedIndexError::DeleteCheckpointState {
                    source: Box::new(source),
                },
            )?;
        }

        let completed_unix_ms =
            current_unix_ms_raw().map_err(|source| CheckpointedIndexError::Clock {
                source: Box::new(source),
            })?;
        let summary_json = serde_json::to_string(&json!({
            "mode": "checkpointed_incremental",
            "started_from_checkpoint": started_from_checkpoint,
            "total_paths": total_paths,
            "processed_paths": processed_paths,
            "remaining_paths": remaining_paths,
            "batches_applied": batches_applied,
            "upserted_files": upserted_files,
            "removed_files": removed_files,
            "links_reindexed": links_reindexed,
            "properties_reindexed": properties_reindexed,
            "bases_reindexed": bases_reindexed,
            "checkpoint_completed": checkpoint_completed,
            "completed_unix_ms": completed_unix_ms,
        }))
        .map_err(|source| CheckpointedIndexError::SerializeSummary {
            source: Box::new(source),
        })?;
        IndexStateRepository::upsert(
            connection,
            &IndexStateRecordInput {
                key: CHECKPOINT_SUMMARY_KEY.to_string(),
                value_json: summary_json,
            },
        )
        .map_err(|source| CheckpointedIndexError::UpsertIndexState {
            source: Box::new(source),
        })?;

        Ok(CheckpointedIndexResult {
            started_from_checkpoint,
            total_paths,
            processed_paths,
            remaining_paths,
            batches_applied,
            upserted_files,
            removed_files,
            links_reindexed,
            properties_reindexed,
            bases_reindexed,
            checkpoint_completed,
        })
    }
}

fn load_checkpoint_state(
    connection: &Connection,
) -> Result<Option<IncrementalCheckpointState>, CheckpointedIndexError> {
    let stored =
        IndexStateRepository::get_by_key(connection, CHECKPOINT_STATE_KEY).map_err(|source| {
            CheckpointedIndexError::GetCheckpointState {
                source: Box::new(source),
            }
        })?;
    let Some(record) = stored else {
        return Ok(None);
    };
    let checkpoint = serde_json::from_str::<IncrementalCheckpointState>(&record.value_json)
        .map_err(|source| CheckpointedIndexError::DeserializeCheckpoint {
            source: Box::new(source),
        })?;
    Ok(Some(checkpoint))
}

fn save_checkpoint_state(
    connection: &Connection,
    checkpoint: &IncrementalCheckpointState,
) -> Result<(), CheckpointedIndexError> {
    let value_json = serde_json::to_string(checkpoint).map_err(|source| {
        CheckpointedIndexError::SerializeCheckpoint {
            source: Box::new(source),
        }
    })?;
    IndexStateRepository::upsert(
        connection,
        &IndexStateRecordInput {
            key: CHECKPOINT_STATE_KEY.to_string(),
            value_json,
        },
    )
    .map_err(|source| CheckpointedIndexError::UpsertCheckpointState {
        source: Box::new(source),
    })
}

fn checkpoint_case_policy_label(case_policy: CasePolicy) -> &'static str {
    match case_policy {
        CasePolicy::Sensitive => "sensitive",
        CasePolicy::Insensitive => "insensitive",
    }
}

fn parse_checkpoint_case_policy(label: &str) -> Result<CasePolicy, CheckpointedIndexError> {
    match label {
        "sensitive" => Ok(CasePolicy::Sensitive),
        "insensitive" => Ok(CasePolicy::Insensitive),
        _ => Err(CheckpointedIndexError::InvalidCheckpointCasePolicy {
            value: label.to_string(),
        }),
    }
}
