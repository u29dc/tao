use super::super::*;

pub(crate) fn handle(command: ConfigCommands) -> Result<CommandResult> {
    match command {
        ConfigCommands::Show(args) => {
            let inspection = SdkConfigInspectionService::inspect(SdkConfigOverrides {
                vault_root: args.vault_root.as_deref().map(PathBuf::from),
                db_path: args.db_path.as_deref().map(PathBuf::from),
                ..SdkConfigOverrides::default()
            })
            .map_err(|source| anyhow!("resolve sdk config failed: {source}"))?;
            let config = inspection.config;
            let case_policy = match config.case_policy {
                CasePolicy::Sensitive => "sensitive",
                CasePolicy::Insensitive => "insensitive",
            };
            let config_files = inspection
                .config_files
                .into_iter()
                .map(|file| {
                    serde_json::json!({
                        "scope": file.scope,
                        "path": file.path,
                        "exists": file.exists,
                    })
                })
                .collect::<Vec<_>>();

            Ok(CommandResult {
                command: "config.show".to_string(),
                summary: "config show completed".to_string(),
                args: serde_json::json!({
                    "vault_root": config.vault_root,
                    "data_dir": config.data_dir,
                    "db_path": config.db_path,
                    "case_policy": case_policy,
                    "tracing_enabled": config.tracing_enabled,
                    "feature_flags": config.feature_flags,
                    "read_only": config.read_only,
                    "sources": {
                        "vault_root": inspection.sources.vault_root,
                        "data_dir": inspection.sources.data_dir,
                        "db_path": inspection.sources.db_path,
                        "case_policy": inspection.sources.case_policy,
                        "tracing_enabled": inspection.sources.tracing_enabled,
                        "feature_flags": inspection.sources.feature_flags,
                        "read_only": inspection.sources.read_only,
                    },
                    "inputs": {
                        "vault_root_override": args.vault_root,
                        "db_path_override": args.db_path,
                        "env": {
                            "TAO_VAULT_ROOT": std::env::var_os("TAO_VAULT_ROOT").is_some(),
                            "TAO_CONFIG_PATH": std::env::var_os("TAO_CONFIG_PATH").is_some(),
                            "TAO_DATA_DIR": std::env::var_os("TAO_DATA_DIR").is_some(),
                            "TAO_DB_PATH": std::env::var_os("TAO_DB_PATH").is_some(),
                            "TAO_CASE_POLICY": std::env::var_os("TAO_CASE_POLICY").is_some(),
                            "TAO_TRACING_ENABLED": std::env::var_os("TAO_TRACING_ENABLED").is_some(),
                            "TAO_FEATURE_FLAGS": std::env::var_os("TAO_FEATURE_FLAGS").is_some(),
                            "TAO_READ_ONLY": std::env::var_os("TAO_READ_ONLY").is_some(),
                        },
                        "config_files": config_files,
                    },
                    "precedence": [
                        "explicit CLI overrides",
                        "TAO_* environment variables",
                        "vault config.toml",
                        "repo/root config.toml",
                        "global config.toml",
                        "built-in defaults",
                    ],
                }),
            })
        }
    }
}

pub(in crate::cli_impl) fn dispatch(command: ConfigCommands) -> Result<CommandResult> {
    handle(command)
}
