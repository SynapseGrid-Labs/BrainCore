#!/usr/bin/env bash
set -euo pipefail
umask 077

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
test_state="$(mktemp -d /tmp/braincore-db-e2e.XXXXXX)"
export COMPOSE_PROJECT_NAME="braincore-db-e2e-$(basename "$test_state" | tr '[:upper:].' '[:lower:]-')"
# Override inherited credentials before starting any test. Never reuse a runtime URL.
unset BRAINCORE_E2E BRAINCORE_DATABASE_URL
export BRAINCORE_BOOTSTRAP_ADMIN_KEY='bck_test_0123456789abcdefghijklmnopqrstuvwxyz'
export BRAINCORE_TEST_DATABASE_PORT="${BRAINCORE_TEST_DATABASE_PORT:-$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    print(sock.getsockname()[1])
PY
)}"
embedder_port="${BRAINCORE_TEST_EMBEDDER_PORT:-$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    print(sock.getsockname()[1])
PY
)}"
export BRAINCORE_DATABASE_URL="postgresql://braincore_test:test-only-password@127.0.0.1:${BRAINCORE_TEST_DATABASE_PORT}/braincore_test"
export BRAINCORE_EMBEDDER_URL="http://127.0.0.1:${embedder_port}"
compose=(docker compose --project-name "$COMPOSE_PROJECT_NAME" --env-file /dev/null -f compose.test.yaml)
embedder_pid=""
cleanup() {
  if [[ -n "$embedder_pid" ]]; then
    kill "$embedder_pid" 2>/dev/null || true
    wait "$embedder_pid" 2>/dev/null || true
  fi
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf -- "$test_state"
}
trap cleanup EXIT

"${compose[@]}" up -d --wait
container_id="$("${compose[@]}" ps -q postgres)"
# Independently verify ownership, tmpfs storage, bound port, and DB identity
# before the fixture can enable its destructive TRUNCATE.
[[ -n "$container_id" ]]
[[ "$(docker inspect "$container_id" --format '{{index .Config.Labels "com.docker.compose.project"}}')" == "$COMPOSE_PROJECT_NAME" ]]
docker inspect "$container_id" --format '{{json .HostConfig.Tmpfs}}' | \
  python3 -c 'import json, sys; assert "/var/lib/postgresql/data" in json.load(sys.stdin)'
[[ "$("${compose[@]}" port postgres 5432)" == "127.0.0.1:${BRAINCORE_TEST_DATABASE_PORT}" ]]
[[ "$("${compose[@]}" exec -T postgres psql -U braincore_test -d braincore_test -Atc 'SELECT current_database() || chr(58) || current_user')" == braincore_test:braincore_test ]]
export BRAINCORE_E2E=1
uv run --locked braincore-migrate
uv run --locked python -m braincore_memory.lexical_projection
uv run --locked python -m braincore_memory.legacy_lexical_projection
uv run --locked uvicorn braincore_memory.embedder:app --host 127.0.0.1 --port "$embedder_port" >"$test_state/embedder.log" 2>&1 &
embedder_pid="$!"
for _ in {1..60}; do
  if curl -fsS "$BRAINCORE_EMBEDDER_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -fsS "$BRAINCORE_EMBEDDER_URL/health" >/dev/null
uv run --locked pytest -q "$@"
