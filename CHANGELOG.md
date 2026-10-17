# Changelog

All notable changes follow Keep a Changelog. BrainCore uses Semantic
Versioning. Version 2.0.0 identifies this source candidate. Registry artifacts and a
GitHub release are separate publication steps and are not implied by the
version in the source.

## [Unreleased]

## [2.0.0] - October source candidate

### Breaking

- BrainCore 2.0 is a clean platform replacement. It is not API-, CLI-, MCP-,
  environment-, package-, or database-schema-compatible with BrainCore 1.x.
- The new service uses the `braincore` schema and does not automatically read or
  migrate the 1.x `preserve` schema. Explicit legacy tools do not certify complete
  migration; unconverted records remain outside native retrieval.
- There is no automated 1.x data converter or cross-major image rollback.
  Operators must use a separate database and explicitly export, review, and
  re-ingest records. See `docs/operations.md`.
- The npm package is now `@synapsegrid-labs/braincore`; the Python distribution
  is `synapsegrid-braincore` with import namespace `braincore_memory`.

### Added

- Evidence-native direct memory, asynchronous extraction, hybrid retrieval,
  explanations, revisions, feedback, and reversible lifecycle operations.
- Tenant-bound role-based API keys, durable idempotency, native batch outcomes,
  worker retry/backoff/dead-letter handling, and explicit degraded stream state.
- Scope-specific references and evidence artifacts, database-enforced composite
  tenant relationships, bounded streaming request bodies, and strict CORS origins.
- One-command loopback-first Compose stack with PostgreSQL/pgvector, local
  384-dimensional embedder, API/dashboard, worker, migrations, backup, restore,
  readiness, metrics, and metadata-only structured request logs.
- TypeScript and synchronous/asynchronous Python SDKs generated around one
  OpenAPI contract.
- MCP, Codex, Claude Code, and LangGraph integration surfaces.
- LoCoMo, LongMemEval, and BEAM-compatible benchmark adapters with provenance
  manifests and complete-run enforcement.

### October successor changes

- Corrected root LangGraph searches and retained reference identity during revisions.
- Validate helper relationships, confidence, SQL numeric ranges and UUID arrays
  before acknowledging live compatibility writes. Migration `032` constrains
  helper references and repairs UUID array hydration without rewriting source records.
- Use Node process and stream APIs for CLI startup, diagnostics, backup and restore;
  packaged executable checks now exercise these commands with a fake container runtime.
- Added typed legacy source accounting, immutable provenance records and
  explicit ownership mapping. These tools do not certify a complete 1.x migration.
- Added lexical projection/backfill, explicit embedding identity and bounded
  embedding refresh; unknown vectors are not assigned an invented model.
- Preserved relationship identity during metadata-only updates and kept SQL NULL
  distinct from JSON null in captured source identity.
- Preserved fact/segment content metadata when updates omit those fields.
- Bounded legacy capture delivery across complete committed transactions with
  resumable staging; allocation order is never treated as a commit watermark.
- Retained dependency repairs and required separate source, package and image
  acceptance. No performance score or production-readiness claim accompanies
  this source candidate.

### Removed

- The 1.x application, CLI, MCP, configuration, and `preserve`-schema runtime
  surfaces. Their release history remains documented below.

## [1.2.0] - 2026-05-23

### Added

- Added additive memory governance migration `022_memory_governance.sql` with prompt recall, feedback, quality audit, context audit, lifecycle outbox, cue, compaction, and source attribution support.
- Added additive assistant memory source migration `023_assistant_memory_sources.sql`
  with `vestige_memory` and `pai_auto_memory` source types for deterministic
  assistant-memory migration sources.
- Added governed memory CLI commands for event ingestion, recall/read auditing, status updates, feedback, compaction, conflict detection, and source attribution.
- Added memory governance policy checks to CI and local sanitization coverage.
- Added assistant memory import review commands so imported assistant memories
  stay non-prompt-eligible until operator review and memory governance approve
  promotion.
- Added audited BrainCore snapshot builds with compact, risk, and deep preload
  profiles.
- Added BrainCore shadow-eval support for snapshot behavior checks.

### Fixed

- Governed prompt search now excludes archived, quarantined, suppressed, retired, and retired-superseded memories by default while preserving an explicit operator inspection override.
- Governed MCP search now applies the same default exclusion to facts, segments, and episodes connected to excluded memories.
- Governance conflict detection now writes to BrainCore's existing typed memory graph schema on the public repository line.

### Changed

- Added repository CODEOWNERS for `@trentdoney` and `@SynapseOpsAgent`.
- Aligned the example MCP server with the repository-wide Python 3.11+
  support floor and accepted the NumPy 2.4.x dependency line.
- Added enterprise memory lifecycle migration `021_enterprise_lifecycle.sql`,
  including lifecycle outbox, target intelligence, cues, context recall audit,
  feedback, score audit, and audit log tables.
- Added CLI and MCP-first lifecycle administration surfaces. The browser/admin
  web app remains a future upgrade path.
- The open-source preserve schema is documented as 50 tables after migrations
  `001` through `023` plus the runtime migration ledger bootstrap.
- Lifecycle `suppressed` and `retired` overlays are enforced in retrieval and
  procedure search paths without mutating BrainCore native truth rows.
- Memory governance metadata, lifecycle sensitivity/redaction values, and cue
  scoring semantics are documented and validated at lifecycle ingestion.
- Lifecycle rollback is documented as a development/test rollback only unless
  lifecycle audit/outbox data has first been exported.

## [1.1.6] - 2026-04-30

Public readiness patch for the SynapseGrid Labs repository promotion.

### Changed

- Updated public repository links and release metadata for
  `SynapseGrid-Labs/BrainCore`.
- Updated README and GitHub presentation assets for the `v1.1.6` release
  surface.
- Updated the pinned CodeQL action SHA and the MCP example `pgvector`
  lower bound through the curated dependency gate.

### Fixed

- Hardened Codex shared ingestion against out-of-root document paths.
- Fixed YAML/frontmatter string escaping for backslashes and quoted values.
- Verified README asset manifests and public numeric claims after the visual
  refresh.

## [1.1.5] - 2026-04-28

Initial public release for BrainCore.

### Added

- Reference stdio MCP server under `examples/mcp_server/`.
- Public MCP tool index and verifier covering 17 registered tools.
- Evidence-backed benchmark claim verifier for README numeric claims.
- Migration set through `020_embedding_index_roles.sql`, including graph,
  event-frame, procedure, reflection, working-memory, multimodal, and
  embedding-index tables.
- Down migrations for all post-launch upgrade migrations that introduce new
  schema.
- Working-memory operations, procedure operational tools, event timeline tools,
  causal-chain tools, and graph retrieval helpers.
- Multimodal metadata ingestion helpers and 384-dimensional embedding-index
  role support.

### Changed

- Public docs describe four core retrieval streams plus optional graph-path
  retrieval.
- External Claude CLI fallback is explicit opt-in with
  `BRAINCORE_ALLOW_EXTERNAL_LLM_FALLBACK=1` or `--use-claude`.
- Benchmark artifacts and runners are aligned to package version `1.1.5`.
- The open-source preserve schema documentation was aligned to the migration
  set through `020_embedding_index_roles.sql`.
- Nightly extraction skips optional sources when their configuration is absent.
- Public setup examples use placeholder credentials and local-only defaults.

### Fixed

- Secret redaction coverage across TypeScript extraction/publish paths and
  Python embedding backfill.
- FastMCP tool-index verification when the repo-local `mcp/` package shadows
  the PyPI package name.
- Public test path so plain `bun test` passes on a fresh clone after
  dependencies are installed.
- Bulk archive, deterministic extract, and semantic extract scripts so summary
  counters and SQL quoting behave correctly.
- Release workflow checks now verify the benchmark artifacts it publishes.
- Source path environment variables now match the documented `.env.example`
  names.

### Removed

- Private host, path, incident, and deployment-specific references from the
  public release tree.
- Stale release-history compare links that do not apply to the public release
  tree.
