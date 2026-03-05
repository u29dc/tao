use anyhow::Result;

use super::super::{CommandResult, DocCommands, RuntimeMode, handle_doc};

pub(in crate::cli_impl) fn dispatch(
    command: DocCommands,
    allow_writes: bool,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle_doc(command, allow_writes, runtime)
}
