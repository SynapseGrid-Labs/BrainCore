export type Scope = {
  project_id?: string;
  user_id?: string;
  agent_id?: string;
  run_id?: string;
};

export type { components, operations, paths } from "./schema.js";

export type OperationState =
  | "queued"
  | "running"
  | "succeeded"
  | "partial"
  | "failed"
  | "dead_letter";

export type Operation = {
  id: string;
  state: OperationState;
  items: Array<Record<string, unknown>>;
  [key: string]: unknown;
};

export class BrainCoreError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string,
    public readonly details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = "BrainCoreError";
  }
}

export type ClientOptions = {
  baseUrl?: string;
  apiKey?: string;
  fetch?: FetchLike;
};

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export class BrainCoreClient {
  private readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly fetcher: FetchLike;

  constructor(options: ClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "http://127.0.0.1:8787").replace(/\/$/, "");
    this.apiKey = options.apiKey;
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
    idempotencyKey?: string,
  ): Promise<T> {
    const response = await this.fetcher(`${this.baseUrl}${path}`, {
      method,
      headers: {
        Accept: "application/json",
        ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        ...(this.apiKey ? { Authorization: `Bearer ${this.apiKey}` } : {}),
        ...(idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      // Proxies can return HTML, empty bodies or malformed JSON for HTTP errors.
      // The response status remains useful even when no API envelope is available.
      let payload: unknown;
      try {
        payload = await response.json();
      } catch {
        payload = undefined;
      }
      const isRecord = (value: unknown): value is Record<string, unknown> =>
        typeof value === "object" && value !== null && !Array.isArray(value);
      const error = isRecord(payload) && isRecord(payload.error) ? payload.error : {};
      throw new BrainCoreError(
        response.status,
        typeof error.code === "string" ? error.code : "http_error",
        typeof error.message === "string" ? error.message : response.statusText || `HTTP ${response.status}`,
        isRecord(error.details) ? error.details : {},
      );
    }
    return (response.status === 204 ? {} : await response.json()) as T;
  }

  remember(content: string, scope: Scope, options: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
    const idempotencyKey = String(options.idempotencyKey ?? crypto.randomUUID());
    const { idempotencyKey: _, ...extra } = options;
    return this.request("POST", "/v1/memories", { content, scope, ...extra }, idempotencyKey);
  }

  ingest(content: string, scope: Scope, options: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
    const idempotencyKey = String(options.idempotencyKey ?? crypto.randomUUID());
    const { idempotencyKey: _, ...extra } = options;
    return this.request("POST", "/v1/ingestions", { content, scope, ...extra }, idempotencyKey);
  }

  rememberBatch(items: Array<Record<string, unknown>>, idempotencyKey: string = crypto.randomUUID()): Promise<Record<string, unknown>> {
    return this.request("POST", "/v1/memories:batch", { items }, idempotencyKey);
  }

  ingestBatch(items: Array<Record<string, unknown>>, idempotencyKey: string = crypto.randomUUID()): Promise<Record<string, unknown>> {
    return this.request("POST", "/v1/ingestions:batch", { items }, idempotencyKey);
  }

  search(query: string | null, scope?: Scope, options: { limit?: number; offset?: number; includeInactive?: boolean; referenceNamespacePrefix?: string[]; metadataFilter?: Record<string, unknown> } = {}): Promise<Record<string, unknown>> {
    return this.request("POST", "/v1/search", {
      query,
      scope,
      limit: options.limit ?? 10,
      offset: options.offset ?? 0,
      include_inactive: options.includeInactive ?? false,
      reference_namespace_prefix: options.referenceNamespacePrefix,
      metadata_filter: options.metadataFilter,
    });
  }

  get(memoryId: string): Promise<Record<string, unknown>> {
    return this.request("GET", `/v1/memories/${encodeURIComponent(memoryId)}`);
  }

  resolve(namespace: string[], key: string, scope?: Scope): Promise<Record<string, unknown>> {
    return this.request("POST", "/v1/memories:resolve", { namespace, key, scope });
  }

  explain(memoryId: string): Promise<Record<string, unknown>> {
    return this.request("GET", `/v1/memories/${encodeURIComponent(memoryId)}/explain`);
  }

  revise(memoryId: string, content: string, reason: string, idempotencyKey: string = crypto.randomUUID()): Promise<Record<string, unknown>> {
    return this.request("POST", `/v1/memories/${encodeURIComponent(memoryId)}/revisions`, { content, reason }, idempotencyKey);
  }

  feedback(memoryId: string, rating: -1 | 0 | 1, comment?: string, idempotencyKey: string = crypto.randomUUID()): Promise<Record<string, unknown>> {
    return this.request("POST", `/v1/memories/${encodeURIComponent(memoryId)}/feedback`, { rating, comment }, idempotencyKey);
  }

  lifecycle(memoryId: string, action: "suppress" | "retire" | "restore" | "promote", reason: string, idempotencyKey: string = crypto.randomUUID()): Promise<Record<string, unknown>> {
    return this.request("PATCH", `/v1/memories/${encodeURIComponent(memoryId)}/lifecycle`, { action, reason }, idempotencyKey);
  }

  suppress(memoryId: string, reason: string, idempotencyKey?: string): Promise<Record<string, unknown>> {
    return this.lifecycle(memoryId, "suppress", reason, idempotencyKey);
  }

  retire(memoryId: string, reason: string, idempotencyKey?: string): Promise<Record<string, unknown>> {
    return this.lifecycle(memoryId, "retire", reason, idempotencyKey);
  }

  restore(memoryId: string, reason: string, idempotencyKey?: string): Promise<Record<string, unknown>> {
    return this.lifecycle(memoryId, "restore", reason, idempotencyKey);
  }

  promote(memoryId: string, reason: string, idempotencyKey?: string): Promise<Record<string, unknown>> {
    return this.lifecycle(memoryId, "promote", reason, idempotencyKey);
  }

  operation(operationId: string): Promise<Operation> {
    return this.request("GET", `/v1/operations/${encodeURIComponent(operationId)}`);
  }

  retryOperation(operationId: string, idempotencyKey: string = crypto.randomUUID()): Promise<Record<string, unknown>> {
    return this.request("POST", `/v1/operations/${encodeURIComponent(operationId)}/retry`, undefined, idempotencyKey);
  }

  createApiKey(
    name: string,
    roles: Array<"read" | "write" | "admin">,
    scope?: Scope,
    expiresAt?: string,
    idempotencyKey: string = crypto.randomUUID(),
  ): Promise<Record<string, unknown>> {
    return this.request(
      "POST",
      "/v1/admin/api-keys",
      { name, roles, scope, expires_at: expiresAt },
      idempotencyKey,
    );
  }

  listApiKeys(): Promise<{ api_keys: Array<Record<string, unknown>> }> {
    return this.request("GET", "/v1/admin/api-keys");
  }

  revokeApiKey(keyId: string, idempotencyKey: string = crypto.randomUUID()): Promise<Record<string, unknown>> {
    return this.request("POST", `/v1/admin/api-keys/${encodeURIComponent(keyId)}/revoke`, undefined, idempotencyKey);
  }

  rotateApiKey(keyId: string, idempotencyKey: string = crypto.randomUUID()): Promise<Record<string, unknown>> {
    return this.request("POST", `/v1/admin/api-keys/${encodeURIComponent(keyId)}/rotate`, undefined, idempotencyKey);
  }

  async waitForOperation(operationId: string, options: { timeoutMs?: number; intervalMs?: number } = {}): Promise<Operation> {
    const deadline = Date.now() + (options.timeoutMs ?? 60_000);
    for (;;) {
      const operation = await this.operation(operationId);
      if (["succeeded", "partial", "failed", "dead_letter"].includes(operation.state)) return operation;
      if (Date.now() >= deadline) throw new Error(`operation ${operationId} timed out`);
      await new Promise((resolve) => setTimeout(resolve, options.intervalMs ?? 200));
    }
  }
}
