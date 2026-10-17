# Retrieval performance and rollout

This implementation targets the BrainCore 2.x `braincore.memories` schema. The
older 1.x `mcp/memory_search.py` and backfill scripts are not present here.
Evidence is already served by the explain endpoint, so retrieval now ranks UUID
candidates and fetches content/metadata only for the requested result page.
PostgreSQL remains canonical. No new search service or hot-only corpus is added.

These are implemented mechanisms and benchmark tools, **not measured production
speedups**. Keep hardware, dataset, vectors, filters and arrival trace fixed
when comparing candidates with the baseline.

## Retrieval controls

| Environment setting | Default | Purpose |
| --- | --- | --- |
| `BRAINCORE_SEARCH_FULL_TEXT_MODE` | `projection` | Complete transactional lexical projection; `canonical` is an explicit reader rollback |
| `BRAINCORE_SEARCH_VECTOR_MODE` | `iterative_hnsw` | Indexed ANN selection; `planner` is a deprecated alias, `exact` is explicit exhaustive ordering |
| `BRAINCORE_SEARCH_HNSW_EF_SEARCH` | `1000` | Candidate effort for HNSW |
| `BRAINCORE_SEARCH_HNSW_MAX_SCAN_TUPLES` | `20000` | Iterative scan work budget |
| `BRAINCORE_SEARCH_HNSW_SCAN_MEM_MULTIPLIER` | `1` | Iterative scan memory multiplier |
| `BRAINCORE_SEARCH_DIAGNOSTICS` | `false` | Per-stage timings and sanitized diagnostic logging |
| `BRAINCORE_EMBEDDING_TIMEOUT_SECONDS` | `5` | End-to-end embedding deadline, including query-client queue wait |
| `BRAINCORE_EMBEDDING_MAX_CONCURRENCY` | `8` | Per-process embedding concurrency ceiling |
| `BRAINCORE_EMBEDDING_CACHE_MAX_ENTRIES` | `256` | Per-process successful-query cache bound; `0` disables |
| `BRAINCORE_EMBEDDING_CACHE_TTL_SECONDS` | `60` | Query-vector lifetime; `0` disables |
| `BRAINCORE_WORKER_BATCH_SIZE` | `8` | Maximum jobs claimed in one iteration |
| `BRAINCORE_EMBEDDING_BATCH_SIZE` | `16` | Maximum embedding inputs per provider call |

The exact mode materializes eligible vectors before distance ordering. Iterative
HNSW requires pgvector 0.8 or later, uses `strict_order`, and sets all search
controls transaction-locally. The deprecated `planner` spelling selects the same
indexed query and controls. Unsupported pgvector versions fail API readiness;
ANN query or control failures disclose a failed vector stream without implicitly
retrying an exhaustive scan. Exact search remains an explicit deployment choice.
The default does not guess a selectivity crossover or issue per-query `COUNT(*)`
calls. Calibrate exact versus ANN before selecting a deployment mode. ANN can
underfill at its scan budget; use filtered recall and fill-rate gates.

The default search effort favors candidate recall. Explicit environment settings
still override it, including existing installations with a smaller configured
value. Larger effort can increase CPU, memory and latency. Records below the
returned vector page can contribute to reciprocal-rank fusion, so validate the
complete candidate streams as well as filtered recall, result fill and useful
retrieval before selecting a smaller budget. `strict_order` orders the candidates
found; it does not make HNSW exhaustive or guarantee a recall threshold.

Eligibility includes authenticated tenant, credential/request scope, reference
prefix, object type, metadata, lifecycle and validity before candidate limits
and again during hydration. Search supports historical `as_of` validity checks.
Without `as_of`, active search checks current validity; `include_inactive`
without `as_of` permits inactive records regardless of validity. Query embeddings
are overlapped with lexical SQL. Independent SQL streams remain sequential so one search does
not multiply database connection demand. Connection waits and embedding waits
are measured separately.

Cache keys include tenant, exact input hash, immutable model identity, and
preprocessing revision. No response, authorization decision, evidence or failure
is cached. Corrections clear old vectors atomically with the content change.
A cached query still runs current eligibility checks in PostgreSQL.

## Runtime storage

Choose active database storage according to search and write latency, sustained
I/O, durability and recovery requirements. Backup archive placement is a separate
decision and does not establish suitable active database performance. Record the
actual storage and resource limits when interpreting a benchmark.

The full-text GIN index identifies matching records, but exact `ts_rank_cd`
ordering still fetches and ranks every eligible match before applying the result
limit. A small returned page therefore does not bound heap or tsvector reads.
Measure this stage on the intended runtime storage before changing the query or
schema. Storage changes alone do not establish a speedup or guarantee latency.

## Lexical projection migration

Migration `021_lexical_projection.sql` creates a compact full-text relation with
all eligibility fields, the canonical tsvector, and timestamp/ID tie fields.
Content, embeddings and subtype payloads stay canonical. Matching and exact
`ts_rank_cd` ordering apply to the entire eligible match set before LIMIT;
canonical hydration rechecks eligibility. No candidate truncation is introduced.

Canonical insert/update/delete transactions synchronize this relation through
an AFTER trigger. Updates with identical mirrored values skip its rewrite;
changes to timestamps or any filter field are synchronized even when embeddings
are the only other changed data. The projection is derived, not writer authority.

Run schema migration first, then `braincore-lexical-backfill --batch-size 1000`.
The separate backfill locks canonical rows in bounded UUID batches, commits its
cursor with each batch, resumes after interruption, and checks missing rows
before marking the projection ready. It is not a bulk schema-migration transaction.
The default API readiness blocks until completion, and requests cannot silently
serve a partial healthy lexical stream. Compose runs the backfill before the app.
Installations with restricted database roles must grant readers SELECT on
`braincore.memories_lexical` and `braincore.lexical_projection_state`. The fixed-path
trigger synchronizes writes without granting clients direct projection mutation.

Preserve backups and accepted writes during deployment. For reader rollback,
set `BRAINCORE_SEARCH_FULL_TEXT_MODE=canonical` against the current canonical
rows; retain the projection and synchronization until the rollout is resolved.
Backfill and indexes add storage/WAL and lock costs. Measure them and retrieval
on the actual isolated corpus before promotion; this design does not guarantee
a latency or representative recall threshold by itself.

## Embedding identity migration

The API, worker and provider must agree on `BRAINCORE_EMBEDDING_MODEL` and
`BRAINCORE_EMBEDDING_PREPROCESSING_REVISION`. Defaults are `feature-hash-v1`
and `token-v1`. The provider receives both in `/embed` requests and must return
both (the legacy `algorithm` field can identify the model), `dimensions: 384`
and an embedding for each input in the same order. Vectors must be numeric,
finite, representable as float32 and nonzero after float32 conversion. Do not
silently repoint one revision label to different model weights or preprocessing.
Custom providers need to implement this contract before rollout.

Migration `002_embedding_identity.sql` adds nullable identity columns and a
partial B-tree over tenant/model/preprocessing for ready vectors. The latter
supports bounded range probes for incompatible coverage in the healthy case.
Migration `003_embedding_refresh_scan.sql` adds a tenant/UUID paging index and
an index of active embedding jobs for bounded administrative repair. These
indexes add disk, WAL and write-maintenance cost and share the same planned
migration/build requirements. Unknown legacy vectors are deliberately not
assigned an assumed model. Search
excludes incompatible vectors and discloses degraded vector coverage; eligible
lexical records and their explanations remain available.

1. Back up and verify restoration. Stop API traffic and old workers before the
   migration. Keep application/worker/provider revisions pinned together.
2. Run migrations in a maintenance window. The index is built transactionally,
   can block writers, and needs disk/WAL headroom. Measure build and recovery
   time on a restored corpus before a large deployment. This change does not
   claim an online, zero-downtime index migration.
3. Start the compatible embedder, API for administrative access, and workers.
   Read `/v1/admin/status`; `embeddings_incompatible` counts ready vectors whose
   identity differs from the configured model/revision or is unknown.
4. For each tenant, use its administrator credential to POST bounded batches to
   `/v1/admin/embeddings:refresh`, optionally narrowed to a scope. Track operation
   outcomes, retries, pending/failed/incompatible counts and ingestion lag. A
   response returns `scanned_items` and `next_cursor`; pass the latter as
   `after_id` with a new key until null. `scan_limit` bounds the tenant page
   before scope/repair filtering, and `limit` bounds enqueued jobs. A zero-item
   page can still have a next cursor or queued work; it does not establish
   completion. `rescan_recommended=true` flags locked or concurrently changed
   candidates; repeat a full pass and inspect status before declaring repair
   complete, including after new insertions during the scan.
5. Require compatible embedding coverage, controlled pending/failed work, and
   filtered correctness/latency gates before routing ordinary search traffic.
   Repeating a refresh with a new idempotency key is safe: it skips ready current
   vectors and active jobs. The endpoint never changes tenant identity or
   canonical evidence to repair an embedding.

Workers bound claims and embedding batches, make provider calls outside write
transactions, and condition writes on the current lease/attempt and source
snapshot. Changed source content or reclaimed jobs cannot accept stale results.
Crashes leave durable jobs; retries finish with explicit item outcomes. Terminal
embedding failure preserves canonical content and evidence. Large ingestion
batches share a lease budget and fail visibly if the provider cannot finish in
that budget; tune size/deadline using lag and retry measurements.

## Experiments and acceptance

Use the [benchmark harness](../benchmarks/README.md) in disposable databases.
Keep complete and degraded requests separate. Measure search-only SQL,
embedding-inclusive HTTP and MCP independently; synthetic SQL results do not
represent application ranking or embedding cost. Use exact eligible truth for
vector recall and judged relevance for hybrid ranking. A tiny smoke run checks
the harness, not tail performance.

Before enabling a more complex route, use at least 10,000 observations per
important bucket across repeated randomized A/B trials, confidence intervals,
100K/1M/10M corpora as capacity allows, selective scope/metadata filters,
concurrency 1/8/32/128, controlled warm/cold cache and sustained writes. Include
old-but-valid, suppressed, expired, duplicate-heavy and corrected records.
Cold-cache resets, index maintenance and system resource collection must happen
only in the isolated environment; never label an unverified reset as cold.

Provisional gates are zero isolation/visibility violations, 100% exact recall,
no measured ANN recall reduction and at least 99% Recall@10/@20 per material
bucket. Target 20% p95/p99 improvement before accepting significant complexity;
no untargeted bucket, achieved throughput or write throughput should regress
more than 5%. Instrumentation should add less than 1% median overhead. Pending
work and worker memory must remain bounded; insertion-lag p99 should be no
worse than baseline and at most 60 seconds until a production SLO is specified.
Do not sum stage percentiles. Log hardware/build/dataset identities without
credentials, machine names, private endpoints or operational history.

## Backlog controlled by measurements

- At about **100K** memories, establish baselines for lean hydration, query
  embedding repetition/outages, and explicit exact versus indexed ANN search.
  Verify provenance, scope, corrections and worker recovery before tuning for speed.
- At about **1M**, measure the exact/ANN crossover and correlated filter plans.
  Prototype selectivity statistics or additional scope/metadata indexes only
  where plans justify their read benefit and write cost. No dynamic router is
  enabled without calibration.
- At about **10M**, hold one tenant's eligible set fixed while adding unrelated
  tenants. Prototype a bounded partition layout only if that workload degrades,
  or residency, maintenance or recovery budgets are exceeded.
- At about **100M**, require a capacity/recovery test. Compare partitioned or
  tiered PostgreSQL and alternative indexes before considering a separate
  derived search tier. One 384-dimensional float32 vector uses roughly 143 GiB
  across 100M rows before tuples, indexes, WAL, evidence and replicas; this is
  arithmetic, not a measured capacity result.

The 2.x memory table provides a unified scoped canonical surface. The lexical
projection separates full-text locality from canonical payloads without changing
visibility or rank rules. A pgvector replacement, archival policy or external
service remains conditional on measured limits. Age alone does not make a
memory ineligible. A future projection must preserve every eligible object,
provenance and audit state, use durable versioned updates, track freshness and
support canonical fallback for missing as well as stale candidates.

## Rollback

Rollback to a reviewed compatible earlier application image and configuration
against the current canonical database. Verify tenant/visibility checks,
failed-stream disclosure, identity checks and stale-write protection before
routing traffic. The deprecated `planner` setting is an alias for indexed ANN;
it does not restore an earlier query implementation. Exact mode is an explicit
exhaustive search choice, not a performance rollback mechanism. The query cache
can be disabled with a zero entry limit, and diagnostics can be disabled
independently.

An application rollback can retain additive columns and indexes, but do not mix
incompatible old and new workers or providers. Writers that do not stamp model
identity require renewed coverage checks and bounded refresh before a later
upgrade. Preserve all accepted writes and canonical evidence through rollback.
Do not restore an older database backup as an application rollback; a separate
database recovery decision must account for every subsequently committed write.
