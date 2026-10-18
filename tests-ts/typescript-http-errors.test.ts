import { describe, expect, test } from "bun:test";

import { BrainCoreClient, BrainCoreError } from "../packages/typescript/src/index.js";

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error("Expected the request to reject");
}

const invalidBodies = [
  ["HTML", "<html><body>Proxy error: synthetic upstream</body></html>"],
  ["empty", ""],
  ["whitespace", " \n\t"],
  ["malformed JSON", '{"error":'],
  ["JSON null", "null"],
  ["JSON boolean", "false"],
  ["JSON number", "42"],
  ["JSON string", '"upstream error"'],
  ["JSON array", '[{"error":{"code":"not_an_envelope"}}]'],
  ["missing error", '{"message":"not an error envelope"}'],
] as const;

describe("BrainCoreClient HTTP error handling", () => {
  for (const status of [400, 401, 403, 429, 502, 503]) {
    for (const [label, body] of invalidBodies) {
      test(`${status}: ${label} preserves HTTP status without returning raw content`, async () => {
        const client = new BrainCoreClient({
          fetch: async () => new Response(body, { status, statusText: "Request failed" }),
        });
        const error = await rejection(client.get("synthetic-memory"));
        expect(error).toBeInstanceOf(BrainCoreError);
        expect(error).toMatchObject({
          name: "BrainCoreError", statusCode: status, code: "http_error",
          message: "Request failed", details: {},
        });
        expect(String(error)).not.toContain("synthetic upstream");
      });
    }
  }

  for (const errorValue of [null, false, 42, "bad envelope", [], ["bad envelope"]]) {
    test(`non-object error member ${JSON.stringify(errorValue)} uses the fallback`, async () => {
      const client = new BrainCoreClient({
        fetch: async () => Response.json({ error: errorValue }, { status: 502, statusText: "Bad Gateway" }),
      });
      expect(await rejection(client.operation("synthetic-operation"))).toMatchObject({
        statusCode: 502, code: "http_error", message: "Bad Gateway", details: {},
      });
    });
  }

  for (const details of [null, false, 42, "invalid details", [], ["invalid details"]]) {
    test(`non-object details ${JSON.stringify(details)} cannot violate the error contract`, async () => {
      const client = new BrainCoreClient({
        fetch: async () => Response.json({ error: { code: "access_denied", message: "Access denied", details } }, { status: 403 }),
      });
      expect(await rejection(client.listApiKeys())).toMatchObject({
        statusCode: 403, code: "access_denied", message: "Access denied", details: {},
      });
    });
  }

  for (const status of [401, 403, 409, 422, 429, 503]) {
    test(`${status}: valid error envelopes retain server code, message and nested details`, async () => {
      let calls = 0;
      const details = { field: "scope", retry_after: 2, nested: { keys: ["synthetic"] } };
      const client = new BrainCoreClient({ fetch: async () => {
        calls += 1;
        return Response.json({ error: { code: "server_error", message: "Server explanation", details } }, { status });
      } });
      const error = await rejection(client.retryOperation("synthetic-operation", "synthetic-retry"));
      expect(error).toBeInstanceOf(BrainCoreError);
      expect(error).toMatchObject({ statusCode: status, code: "server_error", message: "Server explanation", details });
      expect(calls).toBe(1);
    });
  }

  test("an error with no status text still has a useful fallback message", async () => {
    const client = new BrainCoreClient({ fetch: async () => new Response(null, { status: 503 }) });
    expect(await rejection(client.get("synthetic-memory"))).toMatchObject({
      statusCode: 503, code: "http_error", message: "HTTP 503", details: {},
    });
  });

  test("malformed code and message fields use stable fallback values", async () => {
    const client = new BrainCoreClient({
      fetch: async () => Response.json({ error: { code: { invalid: true }, message: ["invalid"] } }, { status: 502, statusText: "Bad Gateway" }),
    });
    expect(await rejection(client.get("synthetic-memory"))).toMatchObject({
      statusCode: 502, code: "http_error", message: "Bad Gateway", details: {},
    });
  });

  test("JSON error parsing does not depend on the content-type header", async () => {
    const client = new BrainCoreClient({ fetch: async () => new Response(
      JSON.stringify({ error: { code: "rate_limited", message: "Try again", details: { retry_after: 3 } } }),
      { status: 429, headers: { "Content-Type": "text/plain" } },
    ) });
    expect(await rejection(client.get("synthetic-memory"))).toMatchObject({
      statusCode: 429, code: "rate_limited", message: "Try again", details: { retry_after: 3 },
    });
  });

  test("failed writes preserve the exact URL, headers, body and idempotency key", async () => {
    let captured: { input: string | URL | Request; init?: RequestInit } | undefined;
    const client = new BrainCoreClient({ baseUrl: "https://example.test/", apiKey: "synthetic-api-key", fetch: async (input, init) => {
      captured = { input, init };
      return new Response("<html>Unavailable</html>", { status: 503 });
    } });
    const error = await rejection(client.remember("Synthetic content", { project_id: "example" }, { idempotencyKey: "synthetic-write", metadata: { kind: "example" } }));
    expect(error).toBeInstanceOf(BrainCoreError);
    expect(captured).toEqual({
      input: "https://example.test/v1/memories",
      init: {
        method: "POST",
        headers: { Accept: "application/json", "Content-Type": "application/json", Authorization: "Bearer synthetic-api-key", "Idempotency-Key": "synthetic-write" },
        body: JSON.stringify({ content: "Synthetic content", scope: { project_id: "example" }, metadata: { kind: "example" } }),
      },
    });
  });

  test("bodyless requests omit content-type and preserve encoded paths", async () => {
    let captured: RequestInit | undefined;
    let url: string | URL | Request | undefined;
    const client = new BrainCoreClient({ baseUrl: "https://example.test", fetch: async (input, init) => {
      url = input; captured = init;
      return Response.json({ id: "synthetic/id", state: "succeeded", items: [] });
    } });
    expect(await client.operation("synthetic/id")).toEqual({ id: "synthetic/id", state: "succeeded", items: [] });
    expect(url).toBe("https://example.test/v1/operations/synthetic%2Fid");
    expect(captured).toEqual({ method: "GET", headers: { Accept: "application/json" }, body: undefined });
  });

  test("204 retains the existing empty object result without parsing JSON", async () => {
    const client = new BrainCoreClient({ fetch: async () => new Response(null, { status: 204 }) });
    expect(await client.revokeApiKey("synthetic-key", "synthetic-revoke")).toEqual({});
  });

  for (const payload of [{ operation_id: "synthetic-operation", state: "queued" }, ["synthetic"], null]) {
    test(`successful JSON ${JSON.stringify(payload)} passes through unchanged`, async () => {
      const client = new BrainCoreClient({ fetch: async () => Response.json(payload, { status: 202 }) });
      const actual: unknown = await client.get("synthetic-memory");
      expect(actual).toEqual(payload);
    });
  }

  for (const body of ["<html>Unexpected success</html>", "", '{"broken":']) {
    test(`malformed successful JSON ${JSON.stringify(body)} still rejects`, async () => {
      const client = new BrainCoreClient({ fetch: async () => new Response(body, { status: 200 }) });
      expect(await rejection(client.get("synthetic-memory"))).toBeInstanceOf(SyntaxError);
    });
  }

  test("transport failures preserve the original rejection", async () => {
    const original = new TypeError("Synthetic network failure");
    const client = new BrainCoreClient({ fetch: async () => { throw original; } });
    expect(await rejection(client.get("synthetic-memory"))).toBe(original);
  });
});
