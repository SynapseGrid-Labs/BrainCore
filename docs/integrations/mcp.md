# MCP integration

BrainCore builds a stdio MCP server from source. It calls the supported
HTTP API and cannot bypass tenant, scope, evidence, idempotency, or lifecycle
rules.

Set credentials in the environment rather than in a committed configuration:

```bash
export BRAINCORE_URL=http://127.0.0.1:8787
export BRAINCORE_API_KEY='the-key-shown-by-braincore-up'
bun /absolute/path/to/BrainCore/dist/mcp.js
```

Available tools are `braincore_remember`, `braincore_ingest`,
`braincore_search`, `braincore_get`, `braincore_explain`,
`braincore_revise`, `braincore_lifecycle`, and `braincore_operation`.
Writes require an explicit project, user, agent, or run scope. Removal requests
map to audited suppression rather than deletion.

Search results include an opaque id. Pass that value unchanged as memory_id to
braincore_get or braincore_explain; clients should not parse or construct
memory IDs.

Build first with the source installation steps in the README. The absolute
path above is a placeholder for your checkout. Version 2.0.0 registry packages
are not included in this source publication.
