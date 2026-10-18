# Operations

## Local and shared deployment

For this source candidate, run CLI commands as `bun dist/cli.js <command>`
from the built checkout; the examples below use `braincore` as shorthand.
The checkout is required to select local image builds. Registry 2.0.0 images
are not supplied by this source publication. `compose.yaml` defaults to the
local image `braincore:2.0.0-full`; `compose.dev.yaml` supplies its build context.
A packaged CLI lacks that context, so its `up` command needs a prebuilt local
image or a separately verified `BRAINCORE_IMAGE` override.

`braincore up` preserves existing `.braincore/.env` content and adds only
missing CLI defaults. Shell overrides for keys already saved apply to that
invocation; they do not replace the saved values. The stock Compose file inserts
`BRAINCORE_DATABASE_PASSWORD` into the PostgreSQL URL without URL escaping, so
manually supplied database passwords must use URL-safe characters. CLI-generated
passwords already do.

`braincore up` binds to `127.0.0.1` by default, so no domain is needed. For a
LAN deployment, set `BRAINCORE_BIND_HOST` to the intended interface and set
`BRAINCORE_ALLOWED_ORIGINS` to the exact dashboard origin. For Internet access,
keep the application private behind a reverse proxy that terminates TLS,
applies request-rate limits, and forwards the original host/protocol headers.
Do not enable wildcard CORS.

The Full stack includes PostgreSQL/pgvector, a transaction-safe migration job,
a separate resumable lexical backfill, the deterministic local embedder, a Bun
worker, and the API/dashboard. The
optional semantic extraction provider is configured with
`BRAINCORE_LLM_BASE_URL`, `BRAINCORE_LLM_API_KEY`, and
`BRAINCORE_LLM_MODEL`. Direct `remember` and retrieval work without it. When
TLS is terminated in front of the service, set `BRAINCORE_COOKIE_SECURE=true`.

## Health and metrics

- `/v1/health` reports process liveness and requires authentication.
- `/v1/readiness` verifies dependencies and requires authentication.
- `/v1/admin/status` reports queue and service state to administrators.
- `/v1/metrics` returns local Prometheus text to administrators.

Structured request logs contain request ID, method, path, status, and duration.
Opt-in search diagnostics add stage timings, cache-hit/degradation flags and result count. Request headers and bodies are not logged. Keep container and proxy
logs access-controlled.

## Back up and restore

```bash
braincore backup ./backups/braincore.dump
braincore restore-backup ./backups/braincore.dump --confirm
braincore doctor
```

Run these commands against the intended stack and its protected configuration;
use `--state-dir <directory>` if it differs from the checkout's `.braincore`.
Stop or drain any other database writers before restoring. The CLI stops only
the Compose `app` and `worker` services.

A backup streams into an exclusively created file with mode `0600` beside the
requested destination. Only a successful `pg_dump`, completed transfer, and file
synchronization allow it to replace that destination by atomic rename. A failed
or canceled backup preserves an existing regular-file destination and removes
the temporary file during normal cleanup. Symlink and nonregular destinations
are rejected. A process killed without cleanup can leave a private temporary
file; filesystem crash durability remains the operator's responsibility.
Backups are not encrypted by BrainCore. Encrypt them at rest, restrict access,
and periodically test restoration in a disposable stack.

Restore uses `pg_restore --single-transaction --exit-on-error` to clean and
restore the objects represented in the archive. A restore error rolls back its
database transaction. Only a successful restore restarts `worker` and `app` and
waits for readiness before reporting success. Atomic restore can need additional
database locks and transaction resources; rehearse representative backups before
operational use.

If restoring or reading the archive fails or is canceled, the command exits
nonzero and leaves the API and worker stopped. Inspect the failure and verify
the database before restarting writers; do not restart against a database whose
recovery state is uncertain. After resolving the failure and verifying the
database and compatible source revision, restart the source stack explicitly:

```bash
docker compose --env-file .braincore/.env -f compose.yaml -f compose.dev.yaml up -d --wait worker app
bun dist/cli.js doctor
```

Use the same configuration directory and Compose files as the original stack.
A restart/readiness failure also returns nonzero; it does not establish that all
services are stopped. Database recovery and accepted-write reconciliation remain
separate from application restart; see [recovery limits](recovery.md).

## Key administration

Administrator endpoints are described by the [HTTP API](api.md) and
[`openapi.json`](../openapi.json). Non-admin credentials need a nonempty scope.
Protect administrator credentials separately from ordinary agent credentials.

The rotation endpoint refuses to rotate the key authenticating its own request
with `409 cannot_rotate_current_key`, including a dashboard session bound to
that key. Create or use another administrator credential to rotate the target.
Store and verify the one-time replacement before separately retiring the
authorizing credential. The old target secret and its dashboard sessions stop
working; a replay cannot recover the new secret.

The bootstrap key has a separate configuration path: update
`BRAINCORE_BOOTSTRAP_ADMIN_KEY` in protected stack configuration and run the
`migrate` service with that configuration. Its bootstrap step updates the default
tenant's bootstrap credential and revokes its dashboard sessions when the digest
changes. Keep the configured value synchronized; a later bootstrap run with an
older value can reset it again. This is privileged configuration maintenance,
not self-rotation through the HTTP endpoint.

The dashboard clears session data, form drafts, and in-memory one-time issued
keys when signing out, starting a new sign-in, or losing authentication. It
cancels pending session requests, ignores stale responses, and serializes
sign-in/sign-out requests that change the session cookie. Local data is cleared
even if an offline connection or failed sign-out request prevents confirmation
of server revocation.

Local clearing and request cancellation do not prove the server session was
revoked or roll back writes already accepted by the API. A failed sign-out can
leave that session active until it expires or is revoked. When revocation is
needed, have an authorized administrator revoke or rotate its backing API key.
Inspect accepted writes through the existing operation endpoint
using an authorized credential before retrying them; see
[operation outcomes and idempotency](api.md#idempotency-and-partial-failure).

## Upgrade within the 2.x line

These instructions apply only to BrainCore 2.0 and later releases using the
`braincore` schema. They do not describe a 1.x-to-2.0 upgrade.

1. Create and verify a backup.
2. Read `CHANGELOG.md` and inspect the new image digest.
3. Build the desired reviewed source revision locally, or use a separately
   verified immutable image if one is available. Do not deploy `latest`.
4. Run the migration service. The supplied migrations are transactional and
   checksum-verified on restart. Inspect each migration for data and permission
   changes; compatibility of future 2.x migrations is not guaranteed. Migration
   `033_embedding_coverage.sql` adds a partial tenant index used to detect missing
   applicable embeddings. Its ordinary transactional index build can block
   writers; stop/drain writers for a planned maintenance window and provide
   disk/WAL headroom. It changes no stored memory content or embedding identity.
5. Run the native lexical backfill and then the legacy lexical backfill
   services (`braincore-lexical-backfill`, `braincore-legacy-lexical-backfill`) before
   admitting search traffic. Compose waits for it before starting the API;
   projection readiness blocks incomplete data. Follow [performance rollout](performance.md)
   for its costs, restricted-reader permissions, embedding identity and model changes.
6. Start the worker and API, run `braincore doctor`, then exercise one isolated
   scope before expanding traffic.

## Application rollback

If the new application fails but migrations succeeded, stop the API and worker,
restore the previously pinned application image, and run readiness plus the
isolated-scope smoke test. Use only a revision whose compatibility with the
current schema and accepted-write records has been demonstrated in an isolated
rehearsal. Additive schema changes alone do not establish rollback safety.

Database recovery is separate from application rollback. Stop and drain writers,
retain the current database and journal, and account for every accepted write
before restoring a matching image/database. An older backup alone omits later
accepted writes; replay and reconcile them before transferring writer authority.

There is no automated live-production canary in the project. Operators must use
an isolated database and scope, and separately approve changes to any existing
production service.

## Moving from BrainCore 1.x

BrainCore 2.0 is a breaking platform replacement, not an in-place 1.x upgrade.
It uses a new `braincore` database schema, API, CLI, MCP contract, package name,
and environment-variable set. It does not automatically read or migrate the 1.x `preserve`
schema. Optional legacy accounting and mapping tools require explicit configuration
and do not certify full migration. Pointing 2.0 at a 1.x database leaves old data intact but makes it
invisible to 2.0, which can look like an empty successful installation.

Run 2.0 as a new installation on an isolated database. Keep the 1.x service and
its verified backup until required records have been explicitly exported,
reviewed, and re-ingested through the 2.0 evidence-native API. BrainCore 2.0
does not include an automated 1.x data converter. Application-image rollback
is not compatible across the 1.x/2.0 boundary; restore the matching database
backup and application version together.

### Read-only legacy ownership preflight

Before designing a 1.x import, run `braincore-legacy-preflight --output receipt.json`
with `BRAINCORE_LEGACY_DATABASE_URL` set through protected configuration. It reads
one consistent, read-only `preserve` snapshot and reports aggregate table counts,
missing owners, populated tables requiring an explicit ownership mapping, and
cross-tenant foreign-key links (including composite relationships). It refuses
to overwrite a receipt and never exports source payloads or credentials.

A failed ownership gate blocks promotion. Do not relabel owners from neighboring
records or discard evidence to make it pass. An ownership pass alone does not
establish migration, scoped visibility, full object reconciliation, client
compatibility, or write recovery. This command is a preflight, not an importer;
the explicit 1.x-to-2.x migration requirement above still applies.

## Shared evidence ownership and access

An artifact keeps its owner when another tenant uses it as evidence. Only an
administrator of that owner can create a scoped grant with
`POST /v1/artifacts/{artifact_id}/grants`; supply `grantee_tenant_id`, a nonempty
native `scope`, a reason, and an `Idempotency-Key`. Optional expiration must be
in the future. Revoke with `POST /v1/evidence-grants/{grant_id}/revoke`, a reason
and an idempotency key. Both actions are audited. Grants cannot be broadened or
re-enabled by editing them; create a new reviewed grant if access changes.

A cross-owner evidence link requires a matching current grant. Explain checks
it again on each read, including the stored quote, and hides revoked, expired
or out-of-scope evidence. Sharing never makes all records of an owner public.
Revoking evidence access does not erase a recipient's independently owned
memory or the protected audit history. Database-level import remains privileged;
there is no arbitrary public endpoint for attaching another tenant's evidence.

## Legacy source accounting and change capture

`braincore-legacy-capture --acknowledge-source-schema-change` installs durable
insert/update/delete capture in the source configured by
`BRAINCORE_LEGACY_DATABASE_URL`. It locks source writers during installation,
requires a primary key on every source table, and rejects truncate. Install it
before the consistent copy. Its journal contains private original payloads;
restrict database access and include it in protected backup/capacity budgets.
Schema or key changes require renewed reviewed coverage.

`braincore-legacy-ledger copy --source-id UUID --output receipt.json` copies the
source into the native database configured by `BRAINCORE_DATABASE_URL` using
bounded batches. Original keys, complete JSON representations and server-verified
SHA-256 hashes are retained in immutable versions. Do not point a rehearsal at
an ordinary production target. Retry re-scans a consistent source snapshot and
reuses existing versions without duplicating evidence or resurrecting replayed
deletions. Batch receipts identify committed work.

Use `replay` to apply captured changes and `reconcile` to compare all declared
source tables and current per-object payloads. A source journal sequence is an
allocation ID, **not a commit watermark**: a lower ID can commit later. Replay
acknowledges each change only after an idempotent target commit and continually
revisits undelivered IDs. Stop/drain all writers before final reconciliation.
Keep tombstones and original versions for deleted or corrected records.

This ledger is source accounting, not a complete 1.x converter. Native typed
mapping, scope/tenant policy, relationship handling, active-client compatibility,
embedding validation, post-cutover write recovery, and deployment acceptance
remain separate gates. A passing ledger receipt cannot establish those gates or
make an empty native memory store ready for traffic.

### Typed legacy mapping and direct provenance

Apply migration `032_control_reference_integrity.sql` before running the corrected
compatibility writer. It adds constrained helper references and supports UUID
arrays in compatibility reads. Existing migration checksums and source payloads
remain unchanged. New helper endpoints must resolve to the declared type in the
writer's tenant and authorized scope; invalid endpoints and SQL numeric values
are rejected before a successful mutation receipt is stored.

The ledger records the SQL-null column set separately from JSON values, retaining
the difference between a SQL NULL and a JSON null. This metadata is reserved;
a source column with the reserved name fails validation. Change capture compiles
quoted per-table triggers and checks exact source tables. An older empty capture
journal can be upgraded; a populated journal lacking this information cannot
be silently declared lossless.

`braincore-legacy-objects` maps a completed source-accounting copy to explicit
native entity/fact/memory/segment/episode types. Source tenant labels are mapped
explicitly, original IDs remain in a durable typed mapping, and native IDs are
deterministic per source, type and key. A source segment never becomes an
ordinary synthesized memory. Unknown validity remains NULL. Source timestamps
and their origins remain available; new migration timestamps do not masquerade
as original dates. Unknown or non-published memory governance stays inactive.
Vectors remain pending until their model/text identity is proven or refreshed.

Native search accepts an object type, a literal legacy scope prefix and an
optional `as_of` validity time. These predicates apply before candidate limits
and again during hydration, together with authenticated tenant/key scope.
Literal scope prefixes do not interpret underscores or percent signs as
wildcards. Entities require an explicit type request; the legacy unfiltered
result family consists of facts, memories, segments and episodes.

The privileged evidence mapper retains artifact database manifests with explicit
`source_record_manifest` metadata, not an assertion that those manifests contain
original raw file bytes. Original URIs, archive references and hashes remain
preserved. Direct segment/episode links require original artifact mappings.
Cross-owner links need a protected reviewed policy naming each artifact,
recipient and exact native scope hash. Grant creation is audited as a migration
event; re-running the mapper cannot re-enable a revoked grant. Full fact/support
graphs, auxiliary ownership/behavior, client parity, live write recovery and
embedding coverage still require their own reconciliation and acceptance gates.


The native provenance mapper retains each fact-evidence, memory-support and
memory-revision source record with its typed endpoints and ledger version hash.
Repeated support edges keep distinct original IDs. Missing mapped endpoints or
conflicting endpoint ownership fail the batch transaction. Nullable original
endpoints remain nullable; unassigned records do not gain an invented owner.
Original revision payloads stay separate from new native audit events.

Explain returns bounded `legacy_provenance` pages and discloses truncation. It
checks the current root and related-object scope before including a source
quote or support reference. A cross-owner artifact quote also requires a
current grant for the root object's exact scope; a segment grant does not
broaden its parent's access. Revoked grants hide previously attached quotes.
Complete auxiliary storage, helper/direct-writer parity, recovery and migration
reconciliation remain separate rollout gates.


Legacy records with empty canonical text remain stored and explainable, with
`embedding_state=not_applicable`. They have no vector/model label, are counted
explicitly in diagnostics, and are excluded from embedding refresh jobs.
Restoring meaningful source text returns them to pending. This distinction does
not exempt nonempty records with an unknown model or a failed provider call.


Native mapping can resume with `braincore-legacy-objects --changed-only`. This
uses each immutable source head hash and its existing mapping receipt; it skips
unchanged mapped heads, processes new/changed heads, and checks complete mapping
accounting at the end. Full semantic reconciliation is still required before
cutover. A full pass remains available to repair or validate native fields.
Unchanged full-pass retries avoid rewriting content and its text indexes.


### Compatibility candidate reads and finite capture delivery

The compatibility lexical projection retains the effective legacy vector,
including current subtype overrides and content-hash invalidation. Run
`braincore-legacy-lexical-backfill` after migrations and native lexical backfill;
its independent readiness gate must complete before compatibility candidate
reads. Both projections remain derived from canonical memories. Extension-only
changes synchronize in the same transaction. The backfill locks canonical IDs
in bounded batches and resumes its durable checkpoint. Keep index, WAL, storage
and write-maintenance costs within deployment budgets; never restore an old
snapshot over accepted writes to roll back a reader. Attribute and canonical
writers must use a consistent lock order or retry an aborted transaction.

`legacy_search` selects ranked lexical or indexed vector IDs before bounded
compatibility hydration. Callers retain original per-type limits, stream fusion,
path LIKE rules and historical visibility. Do not substitute native search
filters or native vectors for compatibility semantics. Callers must prove plans,
filtering, recall and useful-result parity on their actual deployment.

Optional reader aggregation requires explicit, currently valid credentials for
every participating tenant. No tenant name confers public access. All reader
scopes and scope privileges must match; differing constraints are refused.
Configure transaction-local readers before each operation. Native APIs retain
single authenticated tenant behavior. Server database credentials and reader
keys must stay private; do not expose compatibility session settings to clients.

`legacy_capture.stage_committed_transactions` streams complete selected source
transactions in bounded pages, committing ledger pages with resume receipts.
The source transaction must already be read-only repeatable-read. A durable
complete staging receipt permits mapping, not acknowledgement. Verify destination
identity and every affected mapping before per-event source acknowledgement.
Each destination needs its own subscriber identity; a copied database is not
certified by acknowledgements delivered only to its predecessor. Preserve pending
events on interruption, including late commits; never use an allocation watermark
or truncate a large committed transaction to satisfy a delivery cap.
