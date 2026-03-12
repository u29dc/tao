use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GlobalFlag {
    pub(crate) name: &'static str,
    pub(crate) description: &'static str,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolParameter {
    pub(crate) name: &'static str,
    #[serde(rename = "type")]
    pub(crate) type_name: &'static str,
    pub(crate) required: bool,
    pub(crate) description: &'static str,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolDefinition {
    pub(crate) name: &'static str,
    pub(crate) command: &'static str,
    pub(crate) category: &'static str,
    pub(crate) description: &'static str,
    pub(crate) parameters: &'static [ToolParameter],
    pub(crate) output_fields: &'static [&'static str],
    pub(crate) output_schema: Option<&'static str>,
    pub(crate) input_schema: Option<&'static str>,
    pub(crate) idempotent: bool,
    pub(crate) rate_limit: Option<&'static str>,
    pub(crate) example: &'static str,
}

const PARAM_TOOL_NAME: ToolParameter = ToolParameter {
    name: "name",
    type_name: "string",
    required: false,
    description: "Dotted tool name to inspect, for example `doc.read`.",
};
const PARAM_VAULT_ROOT: ToolParameter = ToolParameter {
    name: "vault_root",
    type_name: "string",
    required: false,
    description: "Absolute vault root path. Falls back to config or env defaults.",
};
const PARAM_DB_PATH: ToolParameter = ToolParameter {
    name: "db_path",
    type_name: "string",
    required: false,
    description: "Absolute sqlite database override path.",
};
const PARAM_PATH: ToolParameter = ToolParameter {
    name: "path",
    type_name: "string",
    required: true,
    description: "Vault-relative normalized note path.",
};
const PARAM_QUERY_PATH: ToolParameter = ToolParameter {
    name: "path",
    type_name: "string",
    required: false,
    description: "Optional vault-relative note path for graph query scopes.",
};
const PARAM_CONTENT: ToolParameter = ToolParameter {
    name: "content",
    type_name: "string",
    required: true,
    description: "Full markdown payload for one note write.",
};
const PARAM_PATH_OR_ID: ToolParameter = ToolParameter {
    name: "path_or_id",
    type_name: "string",
    required: true,
    description: "Base id or normalized base file path.",
};
const PARAM_VIEW_NAME: ToolParameter = ToolParameter {
    name: "view_name",
    type_name: "string",
    required: true,
    description: "Base table view name.",
};
const PARAM_QUERY_VIEW_NAME: ToolParameter = ToolParameter {
    name: "view_name",
    type_name: "string",
    required: false,
    description: "Optional base table view name when `--from base:<id-or-path>` is used.",
};
const PARAM_PAGE: ToolParameter = ToolParameter {
    name: "page",
    type_name: "integer",
    required: false,
    description: "One-based page number.",
};
const PARAM_PAGE_SIZE: ToolParameter = ToolParameter {
    name: "page_size",
    type_name: "integer",
    required: false,
    description: "Page size for paged base views.",
};
const PARAM_LIMIT: ToolParameter = ToolParameter {
    name: "limit",
    type_name: "integer",
    required: false,
    description: "Maximum number of rows to return.",
};
const PARAM_OFFSET: ToolParameter = ToolParameter {
    name: "offset",
    type_name: "integer",
    required: false,
    description: "Zero-based row offset.",
};
const PARAM_SCOPE: ToolParameter = ToolParameter {
    name: "scope",
    type_name: "string",
    required: true,
    description: "Vault-relative folder or file prefix to audit.",
};
const PARAM_INCLUDE_MARKDOWN: ToolParameter = ToolParameter {
    name: "include_markdown",
    type_name: "boolean",
    required: false,
    description: "Include markdown files in a scoped inbound audit.",
};
const PARAM_INCLUDE_NON_MD: ToolParameter = ToolParameter {
    name: "include_non_md",
    type_name: "boolean",
    required: false,
    description: "Include non-markdown files in a scoped inbound audit.",
};
const PARAM_EXCLUDE_PREFIX: ToolParameter = ToolParameter {
    name: "exclude_prefix",
    type_name: "string[]",
    required: false,
    description: "Repeatable path prefixes to exclude from a scoped audit.",
};
const PARAM_DEPTH: ToolParameter = ToolParameter {
    name: "depth",
    type_name: "integer",
    required: false,
    description: "Maximum traversal depth.",
};
const PARAM_INCLUDE_UNRESOLVED: ToolParameter = ToolParameter {
    name: "include_unresolved",
    type_name: "boolean",
    required: false,
    description: "Include unresolved links in graph traversal output.",
};
const PARAM_INCLUDE_FOLDERS: ToolParameter = ToolParameter {
    name: "include_folders",
    type_name: "boolean",
    required: false,
    description: "Include folder overlay edges in graph traversal output.",
};
const PARAM_SAMPLE_SIZE: ToolParameter = ToolParameter {
    name: "sample_size",
    type_name: "integer",
    required: false,
    description: "Number of members to sample when full members are not requested.",
};
const PARAM_COMPONENT_MODE: ToolParameter = ToolParameter {
    name: "mode",
    type_name: "string",
    required: false,
    description: "Graph component mode: `weak` or `strong`.",
};
const PARAM_INCLUDE_MEMBERS: ToolParameter = ToolParameter {
    name: "include_members",
    type_name: "boolean",
    required: false,
    description: "Return the full member list for each component.",
};
const PARAM_DIRECTION: ToolParameter = ToolParameter {
    name: "direction",
    type_name: "string",
    required: false,
    description: "Graph neighbor direction: `all`, `outgoing`, or `incoming`.",
};
const PARAM_FROM: ToolParameter = ToolParameter {
    name: "from",
    type_name: "string",
    required: true,
    description: "Start note path or query scope selector, depending on the command.",
};
const PARAM_TO: ToolParameter = ToolParameter {
    name: "to",
    type_name: "string",
    required: true,
    description: "End note path.",
};
const PARAM_MAX_DEPTH: ToolParameter = ToolParameter {
    name: "max_depth",
    type_name: "integer",
    required: false,
    description: "Maximum shortest-path traversal depth.",
};
const PARAM_MAX_NODES: ToolParameter = ToolParameter {
    name: "max_nodes",
    type_name: "integer",
    required: false,
    description: "Maximum explored nodes before aborting path search.",
};
const PARAM_STATE: ToolParameter = ToolParameter {
    name: "state",
    type_name: "string",
    required: false,
    description: "Task state filter or target state, depending on the command.",
};
const PARAM_QUERY: ToolParameter = ToolParameter {
    name: "query",
    type_name: "string",
    required: false,
    description: "Free-text filter or search query.",
};
const PARAM_LINE: ToolParameter = ToolParameter {
    name: "line",
    type_name: "integer",
    required: true,
    description: "One-based line number for the target markdown task.",
};
const PARAM_SELECT: ToolParameter = ToolParameter {
    name: "select",
    type_name: "string",
    required: false,
    description: "Comma-separated docs projection columns.",
};
const PARAM_WHERE: ToolParameter = ToolParameter {
    name: "where",
    type_name: "string",
    required: false,
    description: "Typed filter expression.",
};
const PARAM_SORT: ToolParameter = ToolParameter {
    name: "sort",
    type_name: "string",
    required: false,
    description: "Comma-separated multi-key sort expression.",
};
const PARAM_EXPLAIN: ToolParameter = ToolParameter {
    name: "explain",
    type_name: "boolean",
    required: false,
    description: "Return logical and physical plan metadata.",
};
const PARAM_EXECUTE: ToolParameter = ToolParameter {
    name: "execute",
    type_name: "boolean",
    required: false,
    description: "Execute rows when used with `--explain`.",
};
const PARAM_SOCKET: ToolParameter = ToolParameter {
    name: "socket",
    type_name: "string",
    required: false,
    description: "Explicit unix domain socket path for the warm daemon.",
};
const PARAM_FOREGROUND: ToolParameter = ToolParameter {
    name: "foreground",
    type_name: "boolean",
    required: false,
    description: "Run the daemon in the foreground.",
};
const PARAM_STARTUP_TIMEOUT_MS: ToolParameter = ToolParameter {
    name: "startup_timeout_ms",
    type_name: "integer",
    required: false,
    description: "Maximum wait time for background daemon startup.",
};
const PARAM_SOCKET_DIR: ToolParameter = ToolParameter {
    name: "socket_dir",
    type_name: "string",
    required: false,
    description: "Directory used to discover managed daemon sockets.",
};

const GLOBAL_FLAGS: &[GlobalFlag] = &[
    GlobalFlag {
        name: "--text",
        description: "Emit plain-text summaries instead of JSON envelopes.",
    },
    GlobalFlag {
        name: "--json-stream",
        description: "Use the streaming JSON fast path for supported docs queries.",
    },
    GlobalFlag {
        name: "--allow-writes",
        description: "Enable vault content mutations for write commands.",
    },
    GlobalFlag {
        name: "--daemon-socket <path>",
        description: "Route command execution through a warm daemon socket.",
    },
];

const TOOLS: &[ToolDefinition] = &[
    ToolDefinition {
        name: "base.list",
        command: "tao base list",
        category: "base",
        description: "List indexed base definitions.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao base list --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "base.schema",
        command: "tao base schema --path-or-id <value>",
        category: "base",
        description: "Return one base schema contract.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_PATH_OR_ID],
        output_fields: &["path", "id", "views", "schema"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao base schema --vault-root /abs/vault --path-or-id views/projects.base",
    },
    ToolDefinition {
        name: "base.view",
        command: "tao base view --path-or-id <value> --view-name <value>",
        category: "base",
        description: "Query one base table view.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_PATH_OR_ID,
            PARAM_VIEW_NAME,
            PARAM_PAGE,
            PARAM_PAGE_SIZE,
        ],
        output_fields: &["path", "view", "rows", "page", "page_size", "total"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao base view --vault-root /abs/vault --path-or-id views/projects.base --view-name ActiveProjects",
    },
    ToolDefinition {
        name: "doc.list",
        command: "tao doc list",
        category: "doc",
        description: "List markdown note windows.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao doc list --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "doc.read",
        command: "tao doc read --path <value>",
        category: "doc",
        description: "Return one note by normalized path.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_PATH],
        output_fields: &["path", "title", "front_matter", "body"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao doc read --vault-root /abs/vault --path notes/today.md",
    },
    ToolDefinition {
        name: "doc.write",
        command: "tao --allow-writes doc write --path <value> --content <value>",
        category: "doc",
        description: "Create or update one note.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_PATH, PARAM_CONTENT],
        output_fields: &["path", "action", "bytes"],
        output_schema: None,
        input_schema: None,
        idempotent: false,
        rate_limit: None,
        example: "tao --allow-writes doc write --vault-root /abs/vault --path notes/new.md --content '# New'",
    },
    ToolDefinition {
        name: "graph.backlinks",
        command: "tao graph backlinks --path <value>",
        category: "graph",
        description: "Return backlinks for one note.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_PATH],
        output_fields: &["path", "items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph backlinks --vault-root /abs/vault --path notes/project.md",
    },
    ToolDefinition {
        name: "graph.components",
        command: "tao graph components",
        category: "graph",
        description: "Return connected graph components across resolved edges.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_LIMIT,
            PARAM_OFFSET,
            PARAM_INCLUDE_MEMBERS,
            PARAM_SAMPLE_SIZE,
            PARAM_COMPONENT_MODE,
        ],
        output_fields: &["items", "total", "limit", "offset", "mode"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph components --vault-root /abs/vault --mode weak",
    },
    ToolDefinition {
        name: "graph.deadends",
        command: "tao graph deadends",
        category: "graph",
        description: "Return notes with no outgoing resolved edges.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_LIMIT, PARAM_OFFSET],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph deadends --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "graph.floating",
        command: "tao graph floating",
        category: "graph",
        description: "Return strict floating files with built-in graph-view filtering.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_LIMIT, PARAM_OFFSET],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph floating --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "graph.inbound-scope",
        command: "tao graph inbound-scope --scope <value>",
        category: "graph",
        description: "Return scoped inbound-link counts for file audits.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_SCOPE,
            PARAM_INCLUDE_MARKDOWN,
            PARAM_INCLUDE_NON_MD,
            PARAM_EXCLUDE_PREFIX,
            PARAM_LIMIT,
            PARAM_OFFSET,
        ],
        output_fields: &["scope", "items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph inbound-scope --vault-root /abs/vault --scope notes --include-markdown",
    },
    ToolDefinition {
        name: "graph.neighbors",
        command: "tao graph neighbors --path <value>",
        category: "graph",
        description: "Return one-hop neighbors for one note.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_PATH,
            PARAM_DIRECTION,
            PARAM_LIMIT,
            PARAM_OFFSET,
        ],
        output_fields: &["path", "direction", "items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph neighbors --vault-root /abs/vault --path notes/root.md",
    },
    ToolDefinition {
        name: "graph.orphans",
        command: "tao graph orphans",
        category: "graph",
        description: "Return isolated notes with no incoming or outgoing resolved edges.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_LIMIT, PARAM_OFFSET],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph orphans --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "graph.outgoing",
        command: "tao graph outgoing --path <value>",
        category: "graph",
        description: "Return outgoing links for one note.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_PATH],
        output_fields: &["path", "items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph outgoing --vault-root /abs/vault --path notes/root.md",
    },
    ToolDefinition {
        name: "graph.path",
        command: "tao graph path --from <value> --to <value>",
        category: "graph",
        description: "Return the shortest path between two notes.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_FROM,
            PARAM_TO,
            PARAM_MAX_DEPTH,
            PARAM_MAX_NODES,
        ],
        output_fields: &["from", "to", "items", "explored_nodes", "found"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph path --vault-root /abs/vault --from notes/a.md --to notes/b.md",
    },
    ToolDefinition {
        name: "graph.unresolved",
        command: "tao graph unresolved",
        category: "graph",
        description: "Return unresolved graph links.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_LIMIT, PARAM_OFFSET],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph unresolved --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "graph.walk",
        command: "tao graph walk --path <value>",
        category: "graph",
        description: "Walk graph neighbors from one root note.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_PATH,
            PARAM_DEPTH,
            PARAM_LIMIT,
            PARAM_INCLUDE_UNRESOLVED,
            PARAM_INCLUDE_FOLDERS,
        ],
        output_fields: &["path", "items", "depth", "limit"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao graph walk --vault-root /abs/vault --path notes/root.md --depth 2",
    },
    ToolDefinition {
        name: "health",
        command: "tao health",
        category: "system",
        description: "Return machine-oriented runtime readiness with actionable checks.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &["status", "checks", "stats", "vault_root", "db_path"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao health --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "meta.aliases",
        command: "tao meta aliases",
        category: "meta",
        description: "Aggregate aliases across a vault window.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_LIMIT, PARAM_OFFSET],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao meta aliases --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "meta.properties",
        command: "tao meta properties",
        category: "meta",
        description: "Aggregate property keys across a vault window.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_LIMIT, PARAM_OFFSET],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao meta properties --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "meta.tags",
        command: "tao meta tags",
        category: "meta",
        description: "Aggregate tags across a vault window.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH, PARAM_LIMIT, PARAM_OFFSET],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao meta tags --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "meta.tasks",
        command: "tao meta tasks",
        category: "meta",
        description: "Aggregate task counts across a vault window.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_STATE,
            PARAM_QUERY,
            PARAM_LIMIT,
            PARAM_OFFSET,
        ],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao meta tasks --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "query.run",
        command: "tao query --from <scope>",
        category: "query",
        description: "Run the unified read query entrypoint across docs, graph, task, meta, or base scopes.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_FROM,
            PARAM_QUERY,
            PARAM_QUERY_PATH,
            PARAM_QUERY_VIEW_NAME,
            PARAM_SELECT,
            PARAM_WHERE,
            PARAM_SORT,
            PARAM_EXPLAIN,
            PARAM_EXECUTE,
            PARAM_LIMIT,
            PARAM_OFFSET,
        ],
        output_fields: &["from", "items", "rows", "total", "limit", "offset", "plan"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao query --vault-root /abs/vault --from docs --query project --limit 20",
    },
    ToolDefinition {
        name: "task.list",
        command: "tao task list",
        category: "task",
        description: "List extracted markdown tasks.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_STATE,
            PARAM_QUERY,
            PARAM_LIMIT,
            PARAM_OFFSET,
        ],
        output_fields: &["items", "total", "limit", "offset"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao task list --vault-root /abs/vault --state open",
    },
    ToolDefinition {
        name: "task.set-state",
        command: "tao --allow-writes task set-state --path <value> --line <n> --state <value>",
        category: "task",
        description: "Update checkbox state on one markdown task line.",
        parameters: &[
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_PATH,
            PARAM_LINE,
            PARAM_STATE,
        ],
        output_fields: &["path", "line", "state", "updated"],
        output_schema: None,
        input_schema: None,
        idempotent: false,
        rate_limit: None,
        example: "tao --allow-writes task set-state --vault-root /abs/vault --path notes/tasks.md --line 4 --state done",
    },
    ToolDefinition {
        name: "tools",
        command: "tao tools [name]",
        category: "system",
        description: "Return the public tool registry catalog or one tool definition.",
        parameters: &[PARAM_TOOL_NAME],
        output_fields: &["version", "tools", "tool", "globalFlags"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao tools graph.walk",
    },
    ToolDefinition {
        name: "vault.daemon.start",
        command: "tao vault daemon start",
        category: "vault",
        description: "Start the warm runtime daemon in the background or foreground.",
        parameters: &[
            PARAM_SOCKET,
            PARAM_VAULT_ROOT,
            PARAM_DB_PATH,
            PARAM_FOREGROUND,
            PARAM_STARTUP_TIMEOUT_MS,
        ],
        output_fields: &["socket", "started", "already_running", "pid"],
        output_schema: None,
        input_schema: None,
        idempotent: false,
        rate_limit: None,
        example: "tao vault daemon start --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "vault.daemon.status",
        command: "tao vault daemon status",
        category: "vault",
        description: "Return warm daemon runtime status.",
        parameters: &[PARAM_SOCKET, PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &["socket", "running", "state", "uptime_ms"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao vault daemon status --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "vault.daemon.stop",
        command: "tao vault daemon stop",
        category: "vault",
        description: "Stop the warm runtime daemon for one socket.",
        parameters: &[PARAM_SOCKET, PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &["socket", "stopped"],
        output_schema: None,
        input_schema: None,
        idempotent: false,
        rate_limit: None,
        example: "tao vault daemon stop --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "vault.daemon.stop_all",
        command: "tao vault daemon stop-all",
        category: "vault",
        description: "Stop all managed daemons and prune stale socket files.",
        parameters: &[PARAM_SOCKET_DIR],
        output_fields: &[
            "socket_dir",
            "discovered_sockets",
            "running_before_stop",
            "stopped",
            "pruned_stale",
            "failed",
        ],
        output_schema: None,
        input_schema: None,
        idempotent: false,
        rate_limit: None,
        example: "tao vault daemon stop-all",
    },
    ToolDefinition {
        name: "vault.open",
        command: "tao vault open",
        category: "vault",
        description: "Open one vault path and initialize sqlite state.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &["vault_root", "db_path", "db_ready", "migrations_applied"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao vault open --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "vault.preflight",
        command: "tao vault preflight",
        category: "vault",
        description: "Validate migration state and checksums before startup migration apply.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &["vault_root", "db_path", "pending_migrations", "status"],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao vault preflight --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "vault.reconcile",
        command: "tao vault reconcile",
        category: "vault",
        description: "Apply one incremental reconcile pass.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &["vault_root", "db_path", "drift_paths", "duration_ms"],
        output_schema: None,
        input_schema: None,
        idempotent: false,
        rate_limit: None,
        example: "tao vault reconcile --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "vault.reindex",
        command: "tao vault reindex",
        category: "vault",
        description: "Run smart reindex and refresh index totals.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &["vault_root", "db_path", "indexed_files", "links_total"],
        output_schema: None,
        input_schema: None,
        idempotent: false,
        rate_limit: None,
        example: "tao vault reindex --vault-root /abs/vault",
    },
    ToolDefinition {
        name: "vault.stats",
        command: "tao vault stats",
        category: "vault",
        description: "Return the current vault health snapshot.",
        parameters: &[PARAM_VAULT_ROOT, PARAM_DB_PATH],
        output_fields: &[
            "vault_root",
            "files_total",
            "markdown_files",
            "db_healthy",
            "db_migrations",
            "index_lag",
            "watcher_status",
            "last_index_updated_at",
        ],
        output_schema: None,
        input_schema: None,
        idempotent: true,
        rate_limit: None,
        example: "tao vault stats --vault-root /abs/vault",
    },
];

pub(crate) fn global_flags() -> &'static [GlobalFlag] {
    GLOBAL_FLAGS
}

pub(crate) fn tools_catalog() -> Vec<ToolDefinition> {
    let mut tools = TOOLS.to_vec();
    tools.sort_by(|left, right| {
        left.category
            .cmp(right.category)
            .then_with(|| left.name.cmp(right.name))
    });
    tools
}

pub(crate) fn tool_detail(name: &str) -> Option<ToolDefinition> {
    TOOLS.iter().copied().find(|tool| tool.name == name)
}
