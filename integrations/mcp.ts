#!/usr/bin/env node
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from "@modelcontextprotocol/sdk/types.js";

import { BrainCoreClient, type Scope } from "../packages/typescript/src/index.js";

const scopeProperties = {
  project_id: { type: "string", minLength: 1, maxLength: 200 },
  user_id: { type: "string", minLength: 1, maxLength: 200 },
  agent_id: { type: "string", minLength: 1, maxLength: 200 },
  run_id: { type: "string", minLength: 1, maxLength: 200 },
} as const;

const tools = [
  {
    name: "braincore_remember",
    description: "Store a direct, evidence-backed memory in an explicit scope.",
    inputSchema: {
      type: "object" as const,
      properties: {
        content: { type: "string", minLength: 1, maxLength: 100_000 },
        ...scopeProperties,
        source_uri: { type: "string", format: "uri" },
      },
      required: ["content"],
      additionalProperties: false,
    },
  },
  {
    name: "braincore_ingest",
    description: "Ingest text or a conversation for asynchronous memory extraction.",
    inputSchema: {
      type: "object" as const,
      properties: {
        content: { type: "string", minLength: 1, maxLength: 500_000 },
        ...scopeProperties,
      },
      required: ["content"],
      additionalProperties: false,
    },
  },
  {
    name: "braincore_search",
    description: "Search durable memory and return evidence-aware stream health.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: { type: "string", minLength: 1, maxLength: 10_000 },
        ...scopeProperties,
        limit: { type: "integer", minimum: 1, maximum: 100, default: 10 },
      },
      required: ["query"],
      additionalProperties: false,
    },
  },
  ...["get", "explain"].map((action) => ({
    name: `braincore_${action}`,
    description:
      action === "get"
        ? "Get one memory by its opaque ID."
        : "Explain a memory using its evidence, revisions, and lifecycle history.",
    inputSchema: {
      type: "object" as const,
      properties: { memory_id: { type: "string", format: "uuid" } },
      required: ["memory_id"],
      additionalProperties: false,
    },
  })),
  {
    name: "braincore_revise",
    description: "Correct a memory non-destructively and preserve its revision history.",
    inputSchema: {
      type: "object" as const,
      properties: {
        memory_id: { type: "string", format: "uuid" },
        content: { type: "string", minLength: 1, maxLength: 100_000 },
        reason: { type: "string", minLength: 1, maxLength: 1000 },
      },
      required: ["memory_id", "content", "reason"],
      additionalProperties: false,
    },
  },
  {
    name: "braincore_lifecycle",
    description: "Suppress, retire, restore, or promote a memory with an audit reason.",
    inputSchema: {
      type: "object" as const,
      properties: {
        memory_id: { type: "string", format: "uuid" },
        action: { type: "string", enum: ["suppress", "retire", "restore", "promote"] },
        reason: { type: "string", minLength: 1, maxLength: 1000 },
      },
      required: ["memory_id", "action", "reason"],
      additionalProperties: false,
    },
  },
  {
    name: "braincore_operation",
    description: "Inspect an asynchronous operation and all per-item outcomes.",
    inputSchema: {
      type: "object" as const,
      properties: { operation_id: { type: "string", format: "uuid" } },
      required: ["operation_id"],
      additionalProperties: false,
    },
  },
];

function textResult(value: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(value, null, 2) }] };
}

function requiredString(args: Record<string, unknown>, key: string): string {
  const value = args[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new McpError(ErrorCode.InvalidParams, `${key} must be a non-empty string`);
  }
  return value;
}

function scopeFrom(args: Record<string, unknown>): Scope {
  const scope: Scope = {};
  for (const key of ["project_id", "user_id", "agent_id", "run_id"] as const) {
    if (typeof args[key] === "string" && args[key].length > 0) scope[key] = args[key];
  }
  if (Object.keys(scope).length === 0) {
    throw new McpError(ErrorCode.InvalidParams, "at least one scope field is required");
  }
  return scope;
}

export function createBrainCoreMcpServer(client: BrainCoreClient): Server {
  const server = new Server(
    { name: "braincore", version: "2.0.0" },
    {
      capabilities: { tools: {} },
      instructions:
        "Use explicit scopes. Corrections use revise; removals use audited suppression.",
    },
  );
  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools }));
  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const args = (request.params.arguments ?? {}) as Record<string, unknown>;
    try {
      switch (request.params.name) {
        case "braincore_remember":
          return textResult(
            await client.remember(requiredString(args, "content"), scopeFrom(args), {
              evidence: {
                source_type: "mcp",
                source_uri: typeof args.source_uri === "string" ? args.source_uri : undefined,
              },
            }),
          );
        case "braincore_ingest":
          return textResult(
            await client.ingest(requiredString(args, "content"), scopeFrom(args), {
              source_type: "mcp",
            }),
          );
        case "braincore_search":
          return textResult(
            await client.search(requiredString(args, "query"), scopeFrom(args), {
              limit: typeof args.limit === "number" ? args.limit : 10,
            }),
          );
        case "braincore_get":
          return textResult(await client.get(requiredString(args, "memory_id")));
        case "braincore_explain":
          return textResult(await client.explain(requiredString(args, "memory_id")));
        case "braincore_revise":
          return textResult(
            await client.revise(
              requiredString(args, "memory_id"),
              requiredString(args, "content"),
              requiredString(args, "reason"),
            ),
          );
        case "braincore_lifecycle": {
          const action = requiredString(args, "action");
          if (!["suppress", "retire", "restore", "promote"].includes(action)) {
            throw new McpError(ErrorCode.InvalidParams, "unsupported lifecycle action");
          }
          return textResult(
            await client.lifecycle(
              requiredString(args, "memory_id"),
              action as "suppress" | "retire" | "restore" | "promote",
              requiredString(args, "reason"),
            ),
          );
        }
        case "braincore_operation":
          return textResult(await client.operation(requiredString(args, "operation_id")));
        default:
          throw new McpError(ErrorCode.MethodNotFound, "unknown BrainCore tool");
      }
    } catch (error) {
      if (error instanceof McpError) throw error;
      return {
        isError: true,
        content: [
          {
            type: "text" as const,
            text: error instanceof Error ? error.message : "BrainCore integration failed",
          },
        ],
      };
    }
  });
  return server;
}

async function main(): Promise<void> {
  const apiKey = process.env.BRAINCORE_API_KEY;
  if (!apiKey) throw new Error("BRAINCORE_API_KEY is required");
  const client = new BrainCoreClient({ baseUrl: process.env.BRAINCORE_URL, apiKey });
  await createBrainCoreMcpServer(client).connect(new StdioServerTransport());
}

if (import.meta.main) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : "BrainCore MCP server failed");
    process.exitCode = 1;
  });
}
