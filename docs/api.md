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

For a stable-reference write, an explicit non-null `valid_from` sets the memory's
validity start on creation or update, including updates that keep the same
content. Omitting `valid_from` or sending `null` preserves an existing reference's
start time, including an unknown null start; a new memory defaults to the
server's current UTC time. These two
forms have the same temporal effect; batch idempotency still fingerprints the
raw item fields. Reference updates leave `valid_until` unchanged.

Search normally includes a memory from `valid_from` (inclusive) until
`valid_until` (exclusive, or no end when null). `as_of` selects the time to check.
An unknown null start imposes no lower time bound.
`include_inactive=true` without `as_of` bypasses that temporal filtering.

MCP `braincore_remember`, `braincore_ingest`, and `braincore_search` require at
least one explicit `project_id`, `user_id`, `agent_id`, or `run_id`, including
when using an administrator key. Their published tool schemas enforce the same
requirement as their handlers. Each supplied value must be a nonempty string
of at most 200 characters; additional scope fields narrow the request together.

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

## Request budgets

The API limits encoded `POST`, `PUT`, and `PATCH` bodies to **8 MiB
(8,388,608 bytes)** by default. Both `Content-Length` and the bytes actually
received are checked; an absent or understated length cannot bypass the cap.
An oversized body returns `413 request_too_large` before the handler accepts work.
Operators can set `BRAINCORE_MAX_BODY_BYTES` from 1,024 to 33,554,432 bytes.

Each ingestion accepts up to 500,000 Unicode characters; each direct memory
accepts up to 100,000. The default byte budget accommodates one maximum-length
ingestion even when supplementary Unicode characters use JSON surrogate-pair
escapes (12 bytes per character), plus the bounded scope/source fields. Metadata,
JSON structure, escaping, and whitespace also consume the byte budget. Arbitrary
metadata can therefore exceed it, and a lower configured cap can reject otherwise
valid content.

Native direct memories and revisions must contain a non-whitespace character.
Blank content returns `422` before an operation or embedding job is accepted;
blank items in mixed memory batches fail individually. Valid content is stored
verbatim, including surrounding whitespace. Raw ingestion transcripts retain
their existing content contract.

Text fields, metadata keys, and nested string values reject NUL (`U+0000`) and
unpaired Unicode surrogates, which cannot be stored as PostgreSQL text/JSONB.
Other valid Unicode and supported control characters are preserved. Invalid
single requests return `422`; invalid items in a mixed batch receive failed
outcomes while valid siblings proceed. Validation details include only error
type, location, and message, without echoing raw inputs or validation context.
JSON numbers must remain finite after parsing, including in nested metadata;
`NaN`, infinities, and values such as `1e400` that overflow the parser's numeric
range receive the same validation treatment.

Batch endpoints accept 1–100 items, each subject to its own content limits, and
share the same total HTTP byte budget. Split large batches into smaller requests
with new idempotency keys; the item-count limit does not promise that 100
maximum-length items fit in one body.

## Idempotency and partial failure

Every write requires `Idempotency-Key`. SDKs generate a UUID automatically.
Reusing a key with the same body returns the original response; reusing it with
a different body returns `409`. Keys are namespaced by credential and expire
after seven days, after which the same value starts a new request.

API-key creation and rotation are the exception because their secret is shown
once and is never stored reversibly. Replaying either request returns
`409 api_key_secret_not_replayable`. If the first response was lost, list keys
to confirm creation, then create a replacement or rotate again with a new
idempotency key using another administrator credential.

An administrator cannot rotate the key authenticating the current request:
`POST /v1/admin/api-keys/{key_id}/rotate` returns
`409 cannot_rotate_current_key` for that key. This also applies to a dashboard
session created from it. Use a separate administrator credential to rotate the
target, protect and verify its one-time replacement, then separately retire the
authorizing credential if needed. Rotation invalidates the target's old secret
and dashboard sessions. See [key administration](operations.md#key-administration)
for bootstrap-key configuration.

Dashboard-session creation follows the same one-time-secret rule. Replaying its
idempotency key returns `409 session_secret_not_replayable`; submit a new login
request with a new key if the first response was lost.

Complete validation failure returns `422`. A mixed batch is accepted and each
item has its own outcome. Each valid item's database writes run in a savepoint:
an expected reference/content conflict or a database data/constraint failure
rolls back that item while allowing other items to proceed. Item failures expose
stable codes, including `batch_item_failed` for database failures, without raw
database diagnostics. Authorization failures, connection failures, and unexpected
errors still abort the request.

An accepted operation may finish as `partial` without losing successful items.
If every item fails before work is queued, the `202` response can already report
`state="failed"`. Replaying the same request with the same idempotency key returns
the same operation and outcomes. Inspect `/v1/operations/{id}` before retrying.
Correct non-retryable failed items and submit only those items as a new request
with a new key; replaying the original key cannot change its outcome. The
administrator retry endpoint retries failed worker jobs, and returns
`409 operation_not_retryable` when there are no eligible failed jobs.

Write-side embedding failure preserves evidence, marks the item partial, and
queues retries. Search exposes each retrieval stream as `ok`, `disabled`, or
`failed`, including visible fail-open degradation.

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

Vector execution can succeed while coverage is incomplete. Eligible memories
with pending, failed, missing, incompatible, or unidentified embeddings produce
a warning and `retrieval_status="degraded"`; compatible vector results and
available lexical results still return. Coverage uses the same tenant, scope,
namespace, object-type, metadata, lifecycle, and validity filters as retrieval.
Explicit `embedding_state="not_applicable"` records with empty source text do
not count as missing applicable embeddings. Use the bounded administrator refresh
below and inspect outcomes before treating the corpus as fully covered.

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
