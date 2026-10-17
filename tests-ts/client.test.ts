import { describe, expect, test } from "bun:test";

import { BrainCoreClient, BrainCoreError } from "../packages/typescript/src/index.js";

describe("BrainCoreClient", () => {
  test("remember supplies authentication and idempotency", async () => {
    let captured: Request | undefined;
    const fetcher = async (input: string | URL | Request, init?: RequestInit) => {
      captured = new Request(input, init);
      return Response.json({ operation_id: "op-1", state: "queued" }, { status: 202 });
    };
    const client = new BrainCoreClient({ baseUrl: "http://example.test", apiKey: "test-key", fetch: fetcher });
    const result = await client.remember("Alice likes tea", { user_id: "alice" }, { idempotencyKey: "idem-1" });
    expect(result.operation_id).toBe("op-1");
    expect(captured?.headers.get("Authorization")).toBe("Bearer test-key");
    expect(captured?.headers.get("Idempotency-Key")).toBe("idem-1");
    expect(await captured?.json()).toEqual({ content: "Alice likes tea", scope: { user_id: "alice" } });
  });

  test("stable server errors become BrainCoreError", async () => {
    const fetcher = async () =>
      Response.json(
        { error: { code: "scope_required", message: "Scope required", details: { field: "scope" } } },
        { status: 422 },
      );
    const client = new BrainCoreClient({ fetch: fetcher });
    await expect(client.search("query")).rejects.toBeInstanceOf(BrainCoreError);
    try {
      await client.search("query");
    } catch (error) {
      expect((error as BrainCoreError).code).toBe("scope_required");
      expect((error as BrainCoreError).statusCode).toBe(422);
    }
  });

  test("waitForOperation stops at a partial terminal state", async () => {
    let calls = 0;
    const fetcher = async () => {
      calls += 1;
      return Response.json({ id: "op-1", state: calls === 1 ? "running" : "partial", items: [] });
    };
    const client = new BrainCoreClient({ fetch: fetcher });
    const operation = await client.waitForOperation("op-1", { intervalMs: 1, timeoutMs: 100 });
    expect(operation.state).toBe("partial");
    expect(calls).toBe(2);
  });
});
