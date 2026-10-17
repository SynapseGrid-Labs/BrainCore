#!/usr/bin/env bash
set -euo pipefail

# Audit the pinned application dependency closure, not uvx's tool environment.
# The local project is excluded because it has no published advisory identity;
# all runtime, dev, and LangGraph dependencies remain in this export.
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
audit_dir="$(mktemp -d)"
trap 'rm -rf -- "$audit_dir"' EXIT
cd "$repo_dir"
uv export --locked --extra dev --extra langgraph --no-emit-project \
  --no-header --format requirements-txt --output-file "$audit_dir/requirements.txt" >/dev/null
uvx --from pip-audit==2.9.0 pip-audit \
  --requirement "$audit_dir/requirements.txt" --require-hashes --strict \
  --progress-spinner off "$@"
