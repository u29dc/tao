use super::super::*;

pub(crate) fn handle(command: MetaCommands, runtime: &mut RuntimeMode) -> Result<CommandResult> {
    match command {
        MetaCommands::Properties(args) => {
            use tao_sdk_service::{
                MetadataAggregationKind, MetadataAggregationRequest, MetadataAggregationService,
            };
            let request = MetadataAggregationRequest::new(
                MetadataAggregationKind::Properties,
                args.limit,
                args.offset,
            )?;
            let resolved = args.resolve()?;
            let page = with_connection(runtime, &resolved, |connection| {
                Ok(MetadataAggregationService.aggregate(connection, request)?)
            })?;
            let items = page
                .items
                .into_iter()
                .map(|item| serde_json::json!({ "key": item.value, "total": item.total }))
                .collect::<Vec<_>>();
            Ok(CommandResult {
                command: "meta.properties".to_string(),
                summary: "meta properties completed".to_string(),
                args: serde_json::json!({
                    "total": page.total,
                    "limit": page.limit,
                    "offset": page.offset,
                    "items": items,
                }),
            })
        }
        MetaCommands::Tags(args) => handle_meta_token_aggregate(args, "tags", "meta.tags", runtime),
        MetaCommands::Aliases(args) => {
            handle_meta_token_aggregate(args, "aliases", "meta.aliases", runtime)
        }
    }
}

pub(in crate::cli_impl) fn dispatch(
    command: MetaCommands,
    runtime: &mut RuntimeMode,
) -> Result<CommandResult> {
    handle(command, runtime)
}
