# Claude Code integration

Claude Code supports local stdio MCP servers and project-level `.mcp.json`
files. Add BrainCore without putting a key in the command or repository:

```bash
export BRAINCORE_URL=http://127.0.0.1:8787
export BRAINCORE_API_KEY='your-tenant-bound-key'
claude mcp add braincore --scope local -- \
  bun /absolute/path/to/BrainCore/dist/mcp.js
claude mcp get braincore
```

Build the source checkout using the README first and replace the placeholder
absolute path. This source candidate does not include a published 2.0.0 npm package.

Use `--scope project` only when the resulting `.mcp.json` contains no secret.
Claude Code asks for approval before using a project-scoped server. Team members
still provide `BRAINCORE_URL` and `BRAINCORE_API_KEY` in their own environment.

In Claude Code, run `/mcp` to inspect the connection. Ask Claude to use an
explicit project/user/agent/run scope. Retrieval-only agents should receive a
`read` key; memory-mutating agents need `write`; `admin` is for operators.

This command form follows Anthropic's documented stdio MCP syntax, where `--`
separates Claude options from the server command.
