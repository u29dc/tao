use anyhow::Result;

use super::super::{CommandResult, QueryArgs, RuntimeMode, handle_query};

pub(in crate::cli_impl) fn dispatch(
    args: QueryArgs,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle_query(args, runtime)
}
