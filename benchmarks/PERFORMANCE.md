# Reproducible performance experiments

The quality adapters in [README.md](README.md) remain separate. This harness
measures a synthetic PostgreSQL/pgvector workload and, optionally, text search
through a disposable BrainCore HTTP API. It ships no performance claims or
extrapolated results.

## Disposable database

Use Python dependencies from the repository's locked environment. PostgreSQL
with pgvector is required; iterative scans require pgvector 0.8 or later.
The supplied compose file pins the same PostgreSQL/pgvector image as the
repository's integration tests.

Choose an **empty directory on storage sized for your experiment** and set
BRAINCORE_BENCHMARK_DATA to it. The harness will not discover or clear existing
databases, choose a production target, evict host caches, or delete data.
Do not use a live BrainCore database.

    docker compose -f benchmarks/compose.performance.yaml up -d --wait
    export BRAINCORE_BENCHMARK_DSN='postgresql://braincore_test:test-only-password@127.0.0.1:55433/braincore_benchmark'

The connection string is for the disposable container only. The CLI reads it
from an environment variable and never includes it in artifacts. All SQL
commands require an explicit acknowledgement and a database named
braincore_benchmark or braincore_benchmark_<suffix>. Fixture creation refuses an
existing braincore_perf schema. Use a fresh database for another scale.

    uv run python -m benchmarks.performance fixture \
      --acknowledge-disposable-database --scale 100k \
      --dimensions 384 --tenants 100 --hot-tenant-fraction 0.5 \
      --seed 20261002 --output artifacts/perf-fixture.json

Supported scales are 100k, 1m, 10m and 100m. Use --rows 2000 --dimensions 32
for a quick functional smoke test; results at that scale do not support
larger-scale claims. Generation streams rows through COPY with bounded client
memory. Index construction, PostgreSQL storage, and WAL still scale with the
corpus. A 100M request additionally requires --allow-100m,
--storage-budget-gib and --memory-budget-gib. The declared storage budget must
cover at least three times raw vector bytes; this is a preliminary floor, **not
a capacity guarantee**. Row overhead, the graph, WAL and build temporary space
can require substantially more. Operator budgets are recorded as unverified.

## Ground truth and query matrix

    uv run python -m benchmarks.performance ground-truth \
      --acknowledge-disposable-database --queries 20 --top-k 10 \
      --selectivities 1 0.1 0.01 0.001 0 \
      --output artifacts/perf-ground-truth.json

Create ground truth separately from timed runs so exact scans cannot secretly
warm a claimed cold start. Each query alternates between the hot tenant and a
tail tenant and filters a uniform scope bucket. Selectivity is **conditional
within the selected tenant**, not a fraction of the entire corpus. Requested
selectivity can differ from realized selectivity; eligible counts are recorded
per query. The zero-selectivity cases are deliberately empty. Increase
--queries for more distinct query vectors; increasing timed requests otherwise
repeats this same query matrix.

The fixture has independent seeded random vectors, a configurable hot tenant,
uniform tail tenants, and independent uniform scope buckets. It is not a
representative language embedding corpus. Every row is active with a
precomputed vector. The SQL schema intentionally isolates vector retrieval; it
does not reproduce BrainCore authentication, embedding readiness/model checks,
hydration, full-text fusion, or asynchronous ingestion.

Exact reference queries disable index and bitmap scans and use the same
tenant/scope/state predicates as ANN. They sort by cosine distance with ID as a
deterministic tie-breaker. ANN preserves the index-compatible distance-only
ORDER BY. Duplicate-distance ties can lower ID-based recall despite equivalent
distances; random independent vectors make exact ties uncommon.

Recall denominator is min(k, eligible), not k. Fill is unique returned IDs
divided by the same denominator, capped at one; duplicate or excess returns
are separately invalid. Empty expected sets have null recall/fill and an
explicit empty-result correctness check. A mismatched corpus fingerprint or
incomplete reference set is rejected.

## Timed SQL comparison

    uv run python -m benchmarks.performance run \
      --acknowledge-disposable-database \
      --ground-truth artifacts/perf-ground-truth.json \
      --modes exact hnsw iterative --ef-search 100 --max-scan-tuples 20000 \
      --cache warm --warmups 1 --requests 100000 --concurrency 8 \
      --rate 50 --queue-size 200 --timeout-ms 5000 \
      --output-dir artifacts/perf-run

The three modes are forced exact, HNSW without iterative scans, and HNSW
strict-order iterative scans. The benchmark checks EXPLAIN plans for every
query and records whether the expected index path was selected. These forced
paths compare algorithms; they are not a claim about the application planner.
Each worker owns a persistent connection configured before timing. The service
measurement includes the query round trip, fetching IDs, and local score
calculation. Connection establishment and workload warmup are excluded.

With --rate, arrivals follow a deterministic open-loop schedule. A bounded
queue drops overload instead of slowing down the offered load; every drop
remains in the error rate. Response latency starts at the scheduled arrival
and includes scheduler and queue delay. Server statement timeouts are errors.
Without --rate the run uses closed-loop workers with at most --concurrency
outstanding requests. Healthy, degraded and failed response latency are
separate. Offered, actual offered, completed, and healthy throughput have
distinct fields. Percentiles use the nearest-rank definition.

Run each requested scale, concurrency and filter mix explicitly. Repeat runs,
change mode order between replicates, and preserve all results. A single run
is not a statistical confidence interval. All artifacts contain actual
measurements; the harness does not manufacture projections for unrun scales.

### Cache conditions

- warm requires at least one full query-matrix warmup per SQL connection.
- uncontrolled requires --warmups 0 and makes no cache claim.
- cold_start requires --warmups 0, one SQL mode, a separately generated
  ground-truth file and --cold-evidence-sha256 with the SHA-256 of an external
  reset record. Stop/reset the **disposable** database and relevant OS caches
  under your own controlled procedure before starting the command.

A cold start is operator-attested, not verified by the harness. PostgreSQL
restart alone does not clear OS filesystem caches. DISCARD ALL is not cache
eviction. Metadata reads and EXPLAIN occur before timing; no retrieval warmup
or exact ground-truth scan occurs in a cold-start run. Only the workload's
initial state is attested: later queries naturally warm caches. Never evict
caches on a shared production host for this benchmark.

## HTTP mode: embedding plus retrieval

Prepare a disposable BrainCore service with synthetic memories using the
normal supported ingest API and wait for terminal ingestion outcomes. API
workload JSON is a nonempty array of search bodies, for example:

    [{"query":"synthetic fact","scope":{"project_id":"performance"},"limit":10}]

Set BRAINCORE_BENCHMARK_API_URL to its loopback URL and
BRAINCORE_BENCHMARK_API_KEY to the test key. Then run:

    uv run python -m benchmarks.performance api \
      --acknowledge-disposable-api --workload artifacts/api-workload.json \
      --embedding-profile declared-test-model \
      --requests 10000 --concurrency 8 --rate 20 \
      --output-dir artifacts/perf-api-run

This mode includes query embedding, API authentication, retrieval, fusion and
serialization. Disabled/failed/missing vector streams and warnings count as
degraded; a faster degraded response cannot improve healthy percentiles.
Raw content, endpoint addresses, keys, response warnings and exception text are
excluded from exported artifacts. The embedding profile is a public operator
label, not a verified provider attestation.

The public API does not accept precomputed query vectors. Precomputed timing
therefore belongs to the SQL mode. HTTP fused-result recall and fill are null
because this API workload has no equivalent exact filtered vector ground
truth or eligible-count API. Use the existing quality suites for evidence
retrieval quality.

## Artifacts, resources, and gates

Each timed run creates samples.jsonl, summary.json and run-manifest.json.
A run refuses to overwrite an existing output directory. Manifests include
corpus/workload and artifact hashes, generator/seed/configuration, dependency
versions, benchmark source hashes, OS/architecture without hostname, query
plans and cache policy. SQL metrics include relation and index sizes, index
build/load times, PostgreSQL settings and before/after database counters.
Client CPU includes setup and warmup; peak RSS is the process lifetime peak,
not a server measurement or an isolated per-mode peak. PostgreSQL
statistics can lag; use a dedicated database and external telemetry when
resource conclusions matter.

Server CPU/RSS, ingestion embedding lag and replication lag are null with
availability reasons. A read-only synthetic workload cannot establish ingestion
lag or replica health. Unknown metrics never become zero.

No thresholds are implied. Without --gates the status is not_evaluated even
when the command completes successfully. Supply a JSON profile with explicit
error_rate_max and degraded_rate_max plus the targets for your use case:

    {
      "minimum_samples": 10000,
      "error_rate_max": 0,
      "degraded_rate_max": 0,
      "recall_min": 0.99,
      "fill_min": 1.0,
      "per_filter": {"minimum_samples": 10000}
    }

This example is a chosen acceptance policy, not a measured BrainCore result.
Optional keys include p95_ms_max, p99_ms_max, healthy_qps_min,
server_cpu_percent_max and embedding_lag_seconds_max. Unavailable required
metrics or insufficient healthy samples yield insufficient_evidence, never
pass. Known violations yield fail. Both return exit status 1. Percentile gates
default to at least 10000 healthy samples; overriding that minimum is an explicit
operator choice for functional smoke tests, not acceptance evidence.

Opt in to per-bucket acceptance with per_filter. Each declared tenant-class and
scope-threshold bucket inherits the global recall, fill, error, degradation and
tail-latency targets, with optional overrides. Every bucket independently needs
10000 healthy observations by default, even if the aggregate has enough.
A failing bucket fails the run; an undersampled or unmeasured bucket makes it
insufficient_evidence. Dropped arrivals and failed requests remain attributed
to their intended bucket. Empty expected sets skip recall/fill ratio targets
but retain correctness, error, latency and minimum-sample checks. Buckets
declared in the query matrix but never reached by a short run cannot disappear
from gate evaluation. HTTP mode has no declared SQL filter matrix and reports
insufficient_evidence if per_filter is requested.

For the default ten buckets, --requests 100000 supplies 10000 observations per
bucket before failures/degradation. Global throughput/resource targets are not
automatically inherited by buckets; an explicit bucket throughput override is
supported, using the whole run duration. Without per_filter, thresholds only
apply globally and cannot establish that every filter bucket meets the target.

Repeat the acceptance experiment independently and preserve each manifest.
Confidence intervals are not computed by this harness; calculate and document
them separately from repeated trials. Repeated requests from one run must not
be presented as independent trial confidence intervals.

Review artifacts before sharing. No results are committed by this tooling.
Stop the disposable database with docker compose down when finished; removing
the chosen data directory is a separate operator action.


## Application search policy

Semantic search defaults to `iterative_hnsw` and requires pgvector 0.8 or newer.
The older `planner` setting is a compatibility alias for this policy. Nearest
candidates are selected with distance-only ordering and all eligibility filters
inside the materialized selection; timestamp/ID tie ordering follows on that
bounded set. HNSW remains approximate, and ordering within the selected set does
not promise exact global membership at a tied candidate boundary. Sparse filters
or scan limits can underfill the candidate set; healthy streams do not prove recall.

`exact` remains an explicit choice for workloads where filtering and ranking the
entire eligible set is acceptable. Capability, setting, or ANN query failures
skip/disclose the vector stream and preserve full-text results. They never retry
an unbounded exact scan. Readiness checks iterative capability under the normal
policy, so an unsupported installation cannot silently pass that prerequisite.

Search diagnostics include query-vector cache hit status and overlapping stage
costs. Aborted searches log only a correlation ID, active stage, elapsed stage and
request-work durations, and vector mode; no query, vector, result or exception text.
Use a fixed representative request set and label embedding cache versus storage
cache independently. Repeated identical cache hits are not a release speed claim.
