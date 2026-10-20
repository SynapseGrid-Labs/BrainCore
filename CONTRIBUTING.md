# Contributing

BrainCore uses pull requests, focused changes, automated tests, and an
evidence-backed public-claim policy. Do not include credentials, private data,
local infrastructure identifiers, generated databases, or benchmark datasets
whose licenses prohibit redistribution.

Before submitting a change, run the documented format, lint, type, unit,
integration, security, and public-sanitization checks.

Use the frozen installation and development commands in the README. Check the
OpenAPI document and generated TypeScript contract for drift after API changes.

The type-check and declaration-build scripts invoke the native TypeScript 7
compiler explicitly. The separate `typescript` 5.x dependency supplies the
compiler API required by `openapi-typescript`; keep it within that generator's
peer dependency range. Use `bun run typecheck` rather than calling `tsc` directly,
because both compiler packages expose that binary name.

Dependabot leaves compiler-API and Commander major upgrades for explicit
compatibility review; their patch and minor updates remain enabled. Review
TypeScript upgrades against the OpenAPI generator's peer range, and Commander
upgrades against the supported Node.js minimum. The `typescript-native` npm alias
is maintained separately: Dependabot's Bun parser skips aliased manifest entries,
so do not rely on it to propose updates to that native compiler. Keep the alias
pinned and verify compiler, generation, build, and audit checks when updating it.

Python application and development dependencies are locked in `uv.lock`, and CI
installs them with `uv sync --locked`. Dependabot uses its `uv` ecosystem to keep
dependency declarations and the lockfile consistent; an update may change either
file or both. Build requirements such as Hatchling are declared separately in
`[build-system]`. After editing Python dependencies by hand, run `uv lock` with
the uv version pinned in CI and commit the changed files.

Run `scripts/test-packages.sh` for clean artifact installation and run normal
`npm pack` to exercise preparation hooks. Changes to the database, authentication,
write outcomes, or compatibility mapping require their relevant integration and
Compose tests in addition to unit tests.

After dashboard dependency or UI changes, run `bun run build:dashboard` and
`bun run test:dashboard`. The latter mounts the built dashboard in headless
Chrome and checks the login screen for browser errors, including incompatible
React and React DOM versions. Install Google Chrome or set `CHROME_BIN` to a
Chromium-compatible executable. CI runs this check after the complete build.
Dependabot groups React, React DOM and their types into one update so compatible
versions can be reviewed together; the browser check still verifies the actual
runtime rather than relying on package version declarations alone.

Integration fixtures can truncate tenant data. Use only a disposable PostgreSQL
instance with a unique Compose project, isolated volumes and a verified test
connection. Never set `BRAINCORE_E2E=1` against a shared or operational database.

Public-facing changes must keep version and installation instructions accurate.
Run `bun run check:public` and complementary secret/history/artifact checks; one
regex scanner does not establish that a candidate is safe to publish. Keep raw
logs, local infrastructure detection patterns and private review evidence out of
source and package contents. Follow the repository's protected checks and require
independent review of the exact commit and built artifacts before publication.

The package installation gate invokes Bun 1.4.3 for dependency auditing
(`bun x bun@1.4.3 audit`), matching the container and CI runtime.
