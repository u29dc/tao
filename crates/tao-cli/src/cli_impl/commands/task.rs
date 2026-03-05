use anyhow::Result;

use super::super::{CommandResult, RuntimeMode, TaskCommands, handle_task};

pub(in crate::cli_impl) fn dispatch(
    command: TaskCommands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle_task(command, allow_writes, runtime)
}
