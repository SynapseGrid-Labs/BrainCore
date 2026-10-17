#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
package_test_dir="$(mktemp -d /tmp/braincore-packages.XXXXXX)"

cleanup() {
  if [[ "$package_test_dir" == /tmp/braincore-packages.* ]]; then
    rm -rf -- "$package_test_dir"
  fi
}
trap cleanup EXIT

cd "$repo_dir"
bun run generate:openapi
bun run build
npm pack --pack-destination "$package_test_dir"
uv build --out-dir "$package_test_dir/python-dist"

npm_tarball="$(find "$package_test_dir" -maxdepth 1 -name '*.tgz' -print -quit)"
python_wheel="$(find "$package_test_dir/python-dist" -maxdepth 1 -name '*.whl' -print -quit)"
[[ -n "$npm_tarball" && -n "$python_wheel" ]]

tar -tzf "$npm_tarball" >"$package_test_dir/npm-files.txt"
grep -qx 'package/dist/cli.js' "$package_test_dir/npm-files.txt"
grep -qx 'package/dist/mcp.js' "$package_test_dir/npm-files.txt"
grep -qx 'package/compose.yaml' "$package_test_dir/npm-files.txt"
if grep -E '(^|/)(\.env|\.git|node_modules|tests?|private-ops)(/|$)' "$package_test_dir/npm-files.txt"; then
  echo "npm tarball contains a forbidden path" >&2
  exit 1
fi

mkdir "$package_test_dir/node"
cd "$package_test_dir/node"
npm init --yes >/dev/null
npm install "$npm_tarball" >/dev/null
npm audit --audit-level=low
node --input-type=module -e \
  'import { BrainCoreClient } from "@synapsegrid-labs/braincore"; const c = new BrainCoreClient({apiKey:"test"}); if (typeof c.remember !== "function" || typeof c.waitForOperation !== "function") process.exit(1)'
./node_modules/.bin/braincore --version | grep -qx '2.0.0'
node "$repo_dir/scripts/test-cli-runtime.mjs" "$package_test_dir/node/node_modules/@synapsegrid-labs/braincore/dist/cli.js"
if ./node_modules/.bin/braincore-mcp >"$package_test_dir/mcp.out" 2>&1; then
  echo "MCP server unexpectedly started without an API key" >&2
  exit 1
fi
grep -q 'BRAINCORE_API_KEY is required' "$package_test_dir/mcp.out"

mkdir "$package_test_dir/bun"
cd "$package_test_dir/bun"
bun init --yes >/dev/null
bun add "$npm_tarball" >/dev/null
bun x bun@1.3.8 audit
node "$repo_dir/scripts/test-cli-runtime.mjs" "$package_test_dir/bun/node_modules/@synapsegrid-labs/braincore/dist/cli.js" "$(command -v bun)"
bun -e \
  'import { BrainCoreClient } from "@synapsegrid-labs/braincore"; const c = new BrainCoreClient({apiKey:"test"}); if (typeof c.explain !== "function") process.exit(1)'

python3 -m venv "$package_test_dir/python-venv"
"$package_test_dir/python-venv/bin/pip" install --disable-pip-version-check "${python_wheel}[langgraph]" >/dev/null
"$package_test_dir/python-venv/bin/pip" check
"$package_test_dir/python-venv/bin/python" - <<'PY'
from inspect import signature

from braincore_memory import AsyncBrainCore, BrainCore
from braincore_memory.langgraph import BrainCoreStore
from braincore_memory.migrate import migration_root

assert callable(BrainCore.remember)
assert callable(AsyncBrainCore.wait_for_operation)
assert BrainCoreStore.__name__ == "BrainCoreStore"
assert "client" in signature(BrainCoreStore).parameters
assert "scope_field" in signature(BrainCoreStore).parameters
assert (migration_root() / "001_initial.sql").is_file()
assert (migration_root() / "032_control_reference_integrity.sql").is_file()
PY

echo "Clean npm/Node, npm/Bun, and Python wheel installation checks passed."
