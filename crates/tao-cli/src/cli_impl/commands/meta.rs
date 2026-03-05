use anyhow::Result;

use super::super::{CommandResult, MetaCommands, RuntimeMode, handle_meta};

pub(in crate::cli_impl) fn dispatch(
    command: MetaCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle_meta(command, runtime)
}
