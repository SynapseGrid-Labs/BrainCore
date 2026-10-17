import { afterEach, describe, expect, test } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";

import { createBrainCoreMcpServer } from "../integrations/mcp.js";
import { BrainCoreClient } from "../packages/typescript/src/index.js";

const closeables: Array<{ close(): Promise<void> }> = [];

function readTextResult(result: unknown): string {
  if (typeof result !== "object" || result === null || !("content" in result)) {
    throw new Error("expected MCP call result");
  }
  const content = (result as { content: unknown }).content;
  if (!Array.isArray(content)) throw new Error("expected MCP content array");
  const first = content[0] as Record<string, unknown> | undefined;
  if (first?.type !== "text" || typeof first.text !== "string") {
    throw new Error("expected MCP text result");
  }
  return first.text;
}

afterEach(async () => {
  await Promise.all(closeables.splice(0).map((item) => item.close()));
});

describe("BrainCore MCP server", () => {
  test("lists tools and routes remember through the HTTP SDK", async () => {
    let observedBody: Record<string, unknown> | undefined;
    const sdk = new BrainCoreClient({
      apiKey: "bck_test",
      fetch: async (_input: string | URL | Request, init?: RequestInit) => {
        observedBody = JSON.parse(String(init?.body)) as Record<string, unknown>;
        return new Response(JSON.stringify({ operation_id: crypto.randomUUID(), state: "queued" }), {
          status: 202,
          headers: { "Content-Type": "application/json" },
        });
      },
    });
    const server = createBrainCoreMcpServer(sdk);
    const client = new Client({ name: "test-client", version: "2.0.0" });
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    closeables.push(client, server);
    await server.connect(serverTransport);
    await client.connect(clientTransport);

    const listed = await client.listTools();
    expect(listed.tools.map((tool) => tool.name)).toContain("braincore_remember");
    const called = await client.callTool({
      name: "braincore_remember",
      arguments: { content: "Alice likes tea.", user_id: "alice" },
    });
    expect(called.isError).not.toBe(true);
    expect(observedBody).toMatchObject({
      content: "Alice likes tea.",
      scope: { user_id: "alice" },
      evidence: { source_type: "mcp" },
    });
  });

  test("rejects writes without a scope before calling the API", async () => {
    let calls = 0;
    const sdk = new BrainCoreClient({
      apiKey: "bck_test",
      fetch: async () => {
        calls += 1;
        return new Response("{}");
      },
    });
    const server = createBrainCoreMcpServer(sdk);
    const client = new Client({ name: "test-client", version: "2.0.0" });
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    closeables.push(client, server);
    await server.connect(serverTransport);
    await client.connect(clientTransport);

    await expect(
      client.callTool({ name: "braincore_remember", arguments: { content: "No scope" } }),
    ).rejects.toThrow("at least one scope field is required");
    expect(calls).toBe(0);
  });

  test("passes a search result ID directly into explain", async () => {
    const memoryId = crypto.randomUUID();
    let explainedMemoryId: string | undefined;
    const sdk = new BrainCoreClient({
      apiKey: "bck_test",
      fetch: async (input: string | URL | Request) => {
        const url = String(input);
        if (url.endsWith("/v1/search")) {
          return Response.json({
            results: [{ id: memoryId, content: "Alice likes tea." }],
            streams: { full_text: { state: "ok" }, vector: { state: "failed" } },
            retrieval_status: "degraded",
            warnings: ["Vector search was skipped."],
          });
        }
        const explainSuffix = "/explain";
        if (url.endsWith(explainSuffix)) {
          explainedMemoryId = decodeURIComponent(
            url.slice(0, -explainSuffix.length).split("/").at(-1) ?? "",
          );
          return Response.json({ memory_id: explainedMemoryId, evidence: [] });
        }
        return new Response("{}", { status: 404 });
      },
    });
    const server = createBrainCoreMcpServer(sdk);
    const client = new Client({ name: "test-client", version: "2.0.0" });
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    closeables.push(client, server);
    await server.connect(serverTransport);
    await client.connect(clientTransport);

    const searchResult = await client.callTool({
      name: "braincore_search",
      arguments: { query: "Alice", project_id: "project-a" },
    });
    const searchPayload = JSON.parse(readTextResult(searchResult)) as {
      results: Array<{ id: string }>;
    };
    expect(searchPayload).toMatchObject({
      retrieval_status: "degraded",
      streams: { vector: { state: "failed" } },
      warnings: ["Vector search was skipped."],
    });
    const resultId = searchPayload.results[0]?.id;
    expect(resultId).toBe(memoryId);

    const explainResult = await client.callTool({
      name: "braincore_explain",
      arguments: { memory_id: resultId },
    });
    expect(explainResult.isError).not.toBe(true);
    expect(explainedMemoryId).toBe(memoryId);
    expect(JSON.parse(readTextResult(explainResult))).toMatchObject({ memory_id: memoryId });
  });
});
