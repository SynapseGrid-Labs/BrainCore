# AGENTS.md — BrainCore

BrainCore is a free, self-hosted, public-facing memory application.

## Boundaries

- Never commit credentials, sessions, local databases, machine-specific paths,
  private host details, logs, or unpublished benchmark data.
- Implement contributions independently or preserve every license and notice
  required by an approved dependency or source contribution.
- Keep public claims tied to reproducible tests or benchmark artifacts.

## Product Contract

- PostgreSQL/pgvector is canonical.
- API tenant identity comes only from authenticated keys.
- Writes are idempotent and have per-item terminal outcomes.
- Memory correction and removal are non-destructive and audited.
- Search discloses degraded streams; accepted write failures are never silent.
- The browser never stores API keys.

## Verification

Run formatting, linting, unit/integration tests, package builds, dependency
audits, secret/private-term scans, and the public-readiness gate before a
candidate is described as release-ready.
