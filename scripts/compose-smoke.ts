import { readFile } from "node:fs/promises";

function parseEnv(content: string): Record<string, string> {
  return Object.fromEntries(
    content
      .split(/\r?\n/)
      .filter((line) => line && !line.startsWith("#"))
      .map((line) => {
        const index = line.indexOf("=");
        return [line.slice(0, index), line.slice(index + 1)];
      }),
  );
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

const envPath = process.argv[2];
if (!envPath) throw new Error("environment path argument is required");
const config = parseEnv(await readFile(envPath, "utf8"));
const baseUrl = config.BRAINCORE_URL;
const authorization = `Bearer ${config.BRAINCORE_BOOTSTRAP_ADMIN_KEY}`;
const headers = { Authorization: authorization, Accept: "application/json" };

const readiness = await fetch(`${baseUrl}/v1/readiness`, { headers });
assert(readiness.ok, `readiness returned ${readiness.status}`);

const dashboard = await fetch(baseUrl);
assert(dashboard.ok, `dashboard returned ${dashboard.status}`);
assert((await dashboard.text()).includes("BrainCore"), "dashboard did not contain the product name");
assert(dashboard.headers.get("content-security-policy")?.includes("default-src 'self'"), "CSP missing");

const remembered = await fetch(`${baseUrl}/v1/memories`, {
  method: "POST",
  headers: {
    ...headers,
    "Content-Type": "application/json",
    "Idempotency-Key": crypto.randomUUID(),
  },
  body: JSON.stringify({
    content: "The clean Compose smoke test prefers evidence-backed memory.",
    scope: { project_id: "compose-smoke" },
  }),
});
assert(remembered.status === 202, `remember returned ${remembered.status}`);
const accepted = (await remembered.json()) as { operation_id: string };

let operation: Record<string, unknown> = {};
for (let attempt = 0; attempt < 100; attempt += 1) {
  const response = await fetch(`${baseUrl}/v1/operations/${accepted.operation_id}`, { headers });
  assert(response.ok, `operation returned ${response.status}`);
  operation = (await response.json()) as Record<string, unknown>;
  if (["succeeded", "partial", "failed", "dead_letter"].includes(String(operation.state))) break;
  await Bun.sleep(100);
}
assert(operation.state === "succeeded", `operation ended as ${String(operation.state)}`);
const items = operation.items as Array<Record<string, unknown>>;
const memoryId = String(items[0].memory_id);

const search = await fetch(`${baseUrl}/v1/search`, {
  method: "POST",
  headers: { ...headers, "Content-Type": "application/json" },
  body: JSON.stringify({ query: "evidence memory", scope: { project_id: "compose-smoke" } }),
});
assert(search.ok, `search returned ${search.status}`);
const searchBody = (await search.json()) as {
  results: Array<{ id: string }>;
  streams: Record<string, { state: string }>;
};
assert(searchBody.results.some((item) => item.id === memoryId), "remembered memory was not found");
assert(searchBody.streams.vector.state === "ok", "vector retrieval stream was not healthy");

const explanation = await fetch(`${baseUrl}/v1/memories/${memoryId}/explain`, { headers });
assert(explanation.ok, `explain returned ${explanation.status}`);
const explanationBody = (await explanation.json()) as { evidence: unknown[] };
assert(explanationBody.evidence.length > 0, "explanation contained no evidence");

console.log("Compose smoke test passed: dashboard, remember, worker, search, and explain.");
