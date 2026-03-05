use anyhow::Result;

use super::super::{CommandResult, GraphCommands, RuntimeMode, handle_graph};

pub(in crate::cli_impl) fn dispatch(
    command: GraphCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle_graph(command, runtime)
}
