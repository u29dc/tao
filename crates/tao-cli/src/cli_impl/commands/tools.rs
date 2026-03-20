use super::super::*;

pub(crate) fn handle(args: ToolsArgs) -> Result<CommandResult> {
    let version = env!("CARGO_PKG_VERSION");
    let args = if let Some(name) = args.name.as_deref() {
        let tool = registry::tool_detail(name).ok_or_else(|| {
            anyhow!(
                "unknown tool '{}'; run `tao tools` to inspect supported tools",
                name
            )
        })?;
        serde_json::json!({
            "version": version,
            "tool": tool,
            "globalFlags": registry::global_flags(),
        })
    } else {
        serde_json::json!({
            "version": version,
            "tools": registry::tools_catalog(),
            "globalFlags": registry::global_flags(),
        })
    };

    Ok(CommandResult {
        command: "tools".to_string(),
        summary: "tools completed".to_string(),
        args,
    })
}

pub(in crate::cli_impl) fn dispatch(args: ToolsArgs) -> Result<CommandResult> {
    handle(args)
}
