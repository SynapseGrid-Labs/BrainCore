import { afterEach, describe, expect, spyOn, test } from "bun:test";
import type { Sql } from "postgres";

import { embeddingsFor, runOnce, validateEmbeddingBatch, type Job } from "../worker/src/index.js";

const vector = () => [1, ...Array<number>(383).fill(0)];
const body = (count = 1) => ({
  dimensions: 384,
  model: "feature-hash-v1",
  preprocessing_revision: "token-v1",
  embeddings: Array.from({ length: count }, vector),
});
const spies: Array<{ mockRestore(): void }> = [];
afterEach(() => { for (const spy of spies.splice(0)) spy.mockRestore(); });

function mockProvider(handler: (init: RequestInit) => Promise<Response> | Response): void {
  const spy = spyOn(globalThis, "fetch").mockImplementation(((_, init) => Promise.resolve(handler(init!))) as typeof fetch);
  spies.push(spy);
}

function job(id: string, overrides: Partial<Job> = {}): Job {
  return {
    id, tenant_id: "tenant", operation_id: "operation", operation_item_id: `item-${id}`,
    kind: "embed_memory", payload: { memory_id: `memory-${id}` }, attempts: 0, max_attempts: 5,
    content: `Durable memory ${id}.`, content_hash: `hash-${id}`, dedupe_key: `dedupe-${id}`, memory_version: "1",
    ...overrides,
  };
}

type Query = { text: string; values: unknown[] };
function database(jobs: Job[], expired: Job[] = []) {
  const state = { depth: 0, ownsLease: true, contentCurrent: true, queries: [] as Query[] };
  const query = async (parts: TemplateStringsArray, ...values: unknown[]) => {
    const text = parts.join("?").replace(/\s+/g, " ").trim();
    state.queries.push({ text, values });
    if (text.includes("FROM braincore.jobs WHERE state='running'")) return expired.splice(0);
    if (text.includes("FROM braincore.jobs job")) return jobs.splice(0);
    if (text.startsWith("SELECT id FROM braincore.jobs")) return state.ownsLease ? [{ id: "job" }] : [];
    if (text.startsWith("SELECT id FROM braincore.operations")) return [{ id: "operation" }];
    if (text.startsWith("SELECT content FROM braincore.artifacts")) return [{ content: "Alice likes tea. Bob likes coffee." }];
    if (text.startsWith("UPDATE braincore.memories") && text.includes("RETURNING id")) {
      return state.contentCurrent ? [{ id: "memory" }] : [];
    }
    if (text.startsWith("INSERT INTO braincore.memories")) return [{ id: "memory" }];
    return [];
  };
  const sql = Object.assign(query, {
    json: (value: unknown) => value,
    begin: async (callback: (transaction: unknown) => Promise<unknown>) => {
      state.depth += 1;
      try { return await callback(sql); } finally { state.depth -= 1; }
    },
  });
  return { sql: sql as unknown as Sql, state };
}

describe("bounded embedding batches", () => {
  test("splits requests and preserves result ordering", async () => {
    const sizes: number[] = [];
    mockProvider((init) => {
      const request = JSON.parse(String(init.body)) as { inputs: string[] };
      sizes.push(request.inputs.length);
      return Response.json({ ...body(request.inputs.length), embeddings: request.inputs.map((text) => [Number(text) + 1, ...Array<number>(383).fill(0)]) });
    });
    const vectors = await embeddingsFor(Array.from({ length: 35 }, (_, index) => String(index)));
    expect(sizes).toEqual([16, 16, 3]);
    expect(vectors.map((value) => value[0])).toEqual(Array.from({ length: 35 }, (_, index) => index + 1));
  });

  test("rejects a malformed later batch without returning partial vectors", async () => {
    let calls = 0;
    mockProvider((init) => {
      calls += 1;
      const count = JSON.parse(String(init.body)).inputs.length as number;
      return Response.json(calls === 1 ? body(count) : { ...body(count), embeddings: Array.from({ length: count }, () => Array(384).fill(0)) });
    });
    await expect(embeddingsFor(Array(20).fill("query"))).rejects.toThrow("invalid ingestion vectors");
    expect(calls).toBe(2);
  });

  test("rejects non-finite, zero, nonnumeric, and incompatible vectors", () => {
    for (const value of [NaN, Infinity, true, "1", 1e40, 1e-50]) {
      expect(() => validateEmbeddingBatch({ ...body(), embeddings: [[value, ...Array(383).fill(0)]] }, 1)).toThrow();
    }
    for (const overrides of [
      { model: "different-model" }, { preprocessing_revision: "token-v2" },
      { embeddings: [Array(384).fill(0)] }, { embeddings: [vector(), vector()] },
    ]) {
      expect(() => validateEmbeddingBatch({ ...body(), ...overrides }, 1)).toThrow();
    }
  });

  test("cancelled lease budget prevents a provider request", async () => {
    let calls = 0;
    mockProvider(() => { calls += 1; return Response.json(body()); });
    await expect(embeddingsFor(["query"], undefined, AbortSignal.abort())).rejects.toThrow();
    expect(calls).toBe(0);
  });
});

describe("worker lease and content fences", () => {
  test("embeds same-tenant jobs together outside write transactions", async () => {
    const { sql, state } = database([job("one"), job("two")]);
    let calls = 0;
    mockProvider((init) => {
      expect(state.depth).toBe(0);
      const inputs = JSON.parse(String(init.body)).inputs as string[];
      expect(inputs).toEqual(["Durable memory one.", "Durable memory two."]);
      calls += 1;
      return Response.json(body(2));
    });
    expect(await runOnce(sql)).toBe(true);
    expect(calls).toBe(1);
    const writes = state.queries.filter((query) => query.text.startsWith("UPDATE braincore.memories"));
    expect(writes).toHaveLength(2);
    expect(writes[0].text).toContain("encode(sha256(convert_to(content,'UTF8')),'hex')");
    expect(writes[0].values).toContain("hash-one");
    expect(writes[0].values).toContain("dedupe-one");
    expect(writes[0].text).toContain("xmin::text=?");
    expect(state.queries.some((query) => query.text.includes("FOR UPDATE OF job SKIP LOCKED LIMIT"))).toBe(true);
  });

  test("validates all vectors before writing any member of a batch", async () => {
    const { sql, state } = database([job("one"), job("two")]);
    mockProvider(() => Response.json({ ...body(2), embeddings: [vector(), Array(384).fill(0)] }));
    await runOnce(sql);
    expect(state.queries.filter((query) => query.text.startsWith("UPDATE braincore.memories"))).toHaveLength(0);
    expect(state.queries.filter((query) => query.text.startsWith("UPDATE braincore.jobs SET state=?") && query.values[0] === "queued")).toHaveLength(2);
  });

  test("an invalid vector cannot poison otherwise valid jobs in the batch", async () => {
    const { sql, state } = database([job("good"), job("bad")]);
    const sizes: number[] = [];
    mockProvider((init) => {
      const inputs = JSON.parse(String(init.body)).inputs as string[];
      sizes.push(inputs.length);
      return Response.json({
        ...body(inputs.length),
        embeddings: inputs.map((input) => input.includes("bad") ? Array(384).fill(0) : vector()),
      });
    });
    await runOnce(sql);
    expect(sizes).toEqual([2, 1, 1]);
    const writes = state.queries.filter((query) => query.text.startsWith("UPDATE braincore.memories"));
    expect(writes).toHaveLength(1);
    expect(writes[0].values).toContain("memory-good");
    const retries = state.queries.filter((query) => query.text.startsWith("UPDATE braincore.jobs SET state=?") && query.values[0] === "queued");
    expect(retries).toHaveLength(1);
    expect(retries[0].values).toContain("bad");
  });

  test("lost leases cannot write vectors or complete jobs", async () => {
    const { sql, state } = database([job("one")]);
    mockProvider(() => { state.ownsLease = false; return Response.json(body()); });
    await runOnce(sql);
    expect(state.queries.filter((query) => query.text.startsWith("UPDATE braincore.memories"))).toHaveLength(0);
    expect(state.queries.some((query) => query.text.includes("SET state='succeeded'"))).toBe(false);
    const fence = state.queries.find((query) => query.text.startsWith("SELECT id FROM braincore.jobs"));
    expect(fence?.text).toContain("leased_by=? AND attempts=? AND leased_until > clock_timestamp()");
  });

  test("a corrected memory causes a retry instead of success", async () => {
    const { sql, state } = database([job("one")]);
    mockProvider(() => { state.contentCurrent = false; return Response.json(body()); });
    await runOnce(sql);
    expect(state.queries.some((query) => query.text.includes("SET state='succeeded'"))).toBe(false);
    expect(state.queries.some((query) => query.text.startsWith("UPDATE braincore.jobs SET state=?") && query.values[0] === "queued")).toBe(true);
  });

  test("terminal failures can only mark the matching pending content failed", async () => {
    const { sql, state } = database([job("one", { attempts: 4 })]);
    mockProvider(() => Response.json({}, { status: 503 }));
    await runOnce(sql);
    const failure = state.queries.find((query) => query.text.startsWith("UPDATE braincore.memories"));
    expect(failure?.text).toContain("embedding_state='pending'");
    expect(failure?.text).toContain("encode(dedupe_key,'hex')=?");
    expect(failure?.text).toContain("xmin::text=?");
    expect(failure?.values).toContain("hash-one");
    expect(state.queries.some((query) => query.text.startsWith("UPDATE braincore.operation_items SET state=?") && query.values[0] === "partial")).toBe(true);
  });

  test("expired final attempts produce terminal outcomes rather than infinite retries", async () => {
    const { sql, state } = database([], [job("expired", { attempts: 5 })]);
    expect(await runOnce(sql)).toBe(false);
    expect(state.queries.some((query) => query.text.startsWith("UPDATE braincore.jobs SET state=?") && query.values[0] === "dead_letter")).toBe(true);
    expect(state.queries.some((query) => query.text.startsWith("UPDATE braincore.operation_items SET state=?") && query.values[0] === "partial")).toBe(true);
  });

  test("ingestion provider work completes before opening its write transaction", async () => {
    const { sql, state } = database([job("ingest", { kind: "extract_ingestion", payload: { artifact_id: "artifact", scope: { user_id: "alice" } } })]);
    mockProvider((init) => {
      expect(state.depth).toBe(0);
      return Response.json(body(JSON.parse(String(init.body)).inputs.length));
    });
    await runOnce(sql);
    expect(state.queries.filter((query) => query.text.startsWith("INSERT INTO braincore.memories"))).toHaveLength(2);
    expect(state.queries.some((query) => query.text.includes("SET state='succeeded'"))).toBe(true);
  });
});
