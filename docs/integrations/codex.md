# Codex integration

Codex supports project-scoped MCP configuration in a trusted repository's
`.codex/config.toml`. Keep credentials in the parent environment and forward
only their names:

```toml
[mcp_servers.braincore]
enabled = true
required = false
command = "bun"
args = ["/absolute/path/to/BrainCore/dist/mcp.js"]
env_vars = ["BRAINCORE_URL", "BRAINCORE_API_KEY"]
startup_timeout_sec = 20.0
tool_timeout_sec = 60.0
enabled_tools = [
  "braincore_remember",
  "braincore_ingest",
  "braincore_search",
  "braincore_get",
  "braincore_explain",
  "braincore_revise",
  "braincore_lifecycle",
  "braincore_operation",
]
```

Build the source checkout first using the README and replace the placeholder
absolute path. This configuration uses the source build; no published 2.0.0 npm
package is required.

Before starting Codex:

```bash
export BRAINCORE_URL=http://127.0.0.1:8787
export BRAINCORE_API_KEY='your-tenant-bound-key'
```

Restart/newly launch Codex after changing MCP configuration, then inspect MCP
status and ask it to remember or search within an explicit scope. Use a `read`
key for retrieval-only agents and a `write` key for memory mutation; reserve
`admin` for operators.

The configuration uses the stdio MCP contract: command/args launch the server, `env_vars` forwards parent variables,
and `enabled_tools` constrains exposure. Project configuration loads only for a
trusted repository.
