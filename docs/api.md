# HTTP API

The OpenAPI document is available at `/openapi.json`; its landing page is at
`/docs`. Both require an authorization header or an authenticated dashboard
session. Every product endpoint is versioned under `/v1`.
Send `Authorization: Bearer <api-key>`. Tenant identity always comes from that
key and is never accepted from a request body or header.

## Scopes

A scope can contain `project_id`, `user_id`, `agent_id`, and `run_id`.
Non-admin keys are bound to at least one of those fields when created. Every
write, search, object lookup, reference lookup, operation read, revision,
feedback action, and lifecycle action enforces the credential boundary.
Requests may narrow a key's scope but cannot widen it. Admin keys are explicitly
tenant-wide. Scope values are combined, so a project-and-user query returns only
records matching both.

Stable memory references are unique inside their exact scope. Scoped keys may
omit `scope` when resolving a reference because their credential supplies it;
tenant-wide admin keys must provide `scope` to avoid ambiguous or cross-scope
reference lookup.

## Operations

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/memories` | Store a direct evidence-backed memory |
| `POST` | `/v1/ingestions` | Queue text/conversation extraction |
| `POST` | `/v1/memories:batch` | Store direct memories as one operation |
| `POST` | `/v1/ingestions:batch` | Queue multiple extraction items |
| `POST` | `/v1/search` | Hybrid retrieval with stream health |
| `GET` | `/v1/memories/{id}` | Read a memory |
| `GET` | `/v1/memories/{id}/explain` | Read evidence, revisions, and lifecycle history |
| `POST` | `/v1/memories/{id}/revisions` | Correct without overwriting history |
| `PATCH` | `/v1/memories/{id}/lifecycle` | Suppress, retire, restore, or promote |
| `POST` | `/v1/memories/{id}/feedback` | Attach retrieval feedback |
| `GET` | `/v1/operations/{id}` | Inspect operation and per-item outcomes |
| `POST` | `/v1/operations/{id}/retry` | Retry dead-letter items as an administrator |
| `POST` | `/v1/admin/embeddings:refresh` | Queue a bounded tenant-scoped embedding repair |

The full contract, including administrator key and dashboard-session endpoints,
is generated in [`openapi.json`](../openapi.json).

## Idempotency and partial failure

Every write requires `Idempotency-Key`. SDKs generate a UUID automatically.
Reusing a key with the same body returns the original response; reusing it with
a different body returns `409`. Keys are namespaced by credential and expire
after seven days, after which the same value starts a new request.

API-key creation and rotation are the exception because their secret is shown
once and is never stored reversibly. Replaying either request returns
`409 api_key_secret_not_replayable`. If the first response was lost, list keys
to confirm creation, then create a replacement or rotate again with a new
idempotency key.

Dashboard-session creation follows the same one-time-secret rule. Replaying its
idempotency key returns `409 session_secret_not_replayable`; submit a new login
request with a new key if the first response was lost.

Complete validation failure returns `422`. A mixed batch is accepted and each
item reaches a terminal state. Its operation may finish as `partial` without
losing successful items. Write-side embedding failure preserves evidence,
marks the item partial, and queues retries. Search exposes each retrieval stream
as `ok`, `disabled`, or `failed`, including visible fail-open degradation.

## Errors

Non-success responses use one envelope:

```json
{
  "error": {
    "code": "stable_machine_code",
    "message": "human-readable explanation",
    "details": {}
  }
}
```

Clients should branch on HTTP status and `error.code`, not message text.
Payloads are bounded and validation rejects unknown or malformed content.

## Search diagnostics and embedding repair

Search includes an additive `retrieval_status` (`complete` or `degraded`) alongside
`streams` and `warnings`. The same fields pass through the SDK and MCP wrappers.
Degraded results must be evaluated separately from complete retrieval. When
`BRAINCORE_SEARCH_DIAGNOSTICS=true`, an optional `diagnostics` object reports
stage durations in milliseconds and an embedding-cache hit indicator. Durations
overlap and do not include the full transport/serialization cost; measure HTTP
or MCP round trips separately.

Administrators can POST `{"scope":{"project_id":"example"},"limit":100}` to
`/v1/admin/embeddings:refresh` with an `Idempotency-Key`. Omit scope for the
credential's entire tenant. The job limit is 1–1000. A separate `scan_limit` (default 1000, maximum 10000)
bounds each tenant page before filtering. The endpoint queues missing,
failed, or incompatible embeddings, skips already queued/running work, and
returns the usual operation ID plus `scanned_items` and `next_cursor`. Supply a
non-null cursor as `after_id` on the next request, with a fresh idempotency key,
until the cursor is null. A page can enqueue zero jobs while still having more
tenant rows to inspect. Repeat a full pass when concurrently locked or newly
inserted rows remain pending. `rescan_recommended=true` flags candidates skipped
because they were locked or changed during selection; finish the page walk,
then begin a fresh pass. A null cursor alone is not proof of complete coverage.
It preserves source content, evidence, and
correction history and records an audit event. Reusing the same key replays the
same batch; each subsequent batch needs a new key. A zero-item batch succeeds
immediately. Retry terminal failures through the existing operation retry API.

See [performance rollout](performance.md) before refreshing an existing corpus
or changing the embedding model.
