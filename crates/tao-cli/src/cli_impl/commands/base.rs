use anyhow::Result;

use super::super::{BaseCommands, CommandResult, RuntimeMode, handle_base};

pub(in crate::cli_impl) fn dispatch(
    command: BaseCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle_base(command, runtime)
}
