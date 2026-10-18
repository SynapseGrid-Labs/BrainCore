# Contributing

BrainCore uses pull requests, focused changes, automated tests, and an
evidence-backed public-claim policy. Do not include credentials, private data,
local infrastructure identifiers, generated databases, or benchmark datasets
whose licenses prohibit redistribution.

Before submitting a change, run the documented format, lint, type, unit,
integration, security, and public-sanitization checks.

Use the frozen installation and development commands in the README. Check the
OpenAPI document and generated TypeScript contract for drift after API changes.
Run `scripts/test-packages.sh` for clean artifact installation and run normal
`npm pack` to exercise preparation hooks. Changes to the database, authentication,
write outcomes, or compatibility mapping require their relevant integration and
Compose tests in addition to unit tests.

Integration fixtures can truncate tenant data. Use only a disposable PostgreSQL
instance with a unique Compose project, isolated volumes and a verified test
connection. Never set `BRAINCORE_E2E=1` against a shared or operational database.

Public-facing changes must keep version and installation instructions accurate.
Run `bun run check:public` and complementary secret/history/artifact checks; one
regex scanner does not establish that a candidate is safe to publish. Keep raw
logs, local infrastructure detection patterns and private review evidence out of
source and package contents. Follow the repository's protected checks and require
independent review of the exact commit and built artifacts before publication.

The package installation gate supports the minimum Bun runtime by invoking
Bun 1.3.8 explicitly for dependency auditing (`bun x bun@1.3.8 audit`).
This audit tool version is independent of the runtime used for package checks.
