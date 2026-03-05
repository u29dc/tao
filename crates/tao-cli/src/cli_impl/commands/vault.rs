use anyhow::Result;

use super::super::{CommandResult, RuntimeMode, VaultCommands, handle_vault};

pub(in crate::cli_impl) fn dispatch(
    command: VaultCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle_vault(command, runtime)
}
