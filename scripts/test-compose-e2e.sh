#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_state="$(mktemp -d /tmp/braincore-e2e.XXXXXX)"
test_suffix="${test_state##*.}"
export COMPOSE_PROJECT_NAME="braincore-e2e-$(printf '%s' "$test_suffix" | tr '[:upper:]' '[:lower:]')"
export BRAINCORE_IMAGE="${BRAINCORE_IMAGE:-braincore-e2e:${test_suffix,,}}"
compose_port="${BRAINCORE_TEST_APP_PORT:-$(python3 - <<'PORT'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    print(sock.getsockname()[1])
PORT
)}"
# The source CLI preserves these settings and generates disposable credentials.
printf 'BRAINCORE_PORT=%s\nBRAINCORE_URL=http://127.0.0.1:%s\nBRAINCORE_ALLOWED_ORIGINS=http://127.0.0.1:%s\n' \
  "$compose_port" "$compose_port" "$compose_port" >"$test_state/.env"
chmod 600 "$test_state/.env"


cleanup() {
  exit_code=$?
  if [[ "$exit_code" -ne 0 && -f "$test_state/.env" ]]; then
    # Keep failure diagnostics before teardown, with all configured values redacted.
    docker compose --env-file "$test_state/.env" \
      -f "$repo_dir/compose.yaml" -f "$repo_dir/compose.dev.yaml" \
      logs --no-color --tail 60 >"$test_state/failure.log" 2>&1 || true
    python3 - "$test_state/.env" "$test_state/failure.log" <<'PY' || echo "Failure diagnostics unavailable; proceeding with isolated cleanup" >&2
import sys
from pathlib import Path
values = [line.split("=", 1)[1].strip() for line in Path(sys.argv[1]).read_text().splitlines()
          if "=" in line and not line.lstrip().startswith("#")]
message = Path(sys.argv[2]).read_text(errors="replace")
for value in sorted(set(values), key=len, reverse=True):
    if value:
        message = message.replace(value, "[redacted]")
print(message[-16000:], file=sys.stderr)
PY
  fi
  if [[ -f "$test_state/.env" ]]; then
    docker compose --env-file "$test_state/.env" \
      -f "$repo_dir/compose.yaml" -f "$repo_dir/compose.dev.yaml" \
      down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  if [[ "$test_state" == /tmp/braincore-e2e.* ]]; then
    rm -rf -- "$test_state"
  fi
}
trap cleanup EXIT

ready_started="$(date +%s)"
bun "$repo_dir/dist/cli.js" up --state-dir "$test_state" >"$test_state/up.out"
ready_seconds="$(( $(date +%s) - ready_started ))"
[[ "$ready_seconds" -le "${BRAINCORE_TEST_READY_TIMEOUT:-900}" ]]
[[ "$(stat -c '%a' "$test_state/.env")" == "600" ]]
round_trip_started="$(date +%s)"
bun "$repo_dir/scripts/compose-smoke.ts" "$test_state/.env"
round_trip_seconds="$(( $(date +%s) - round_trip_started ))"
[[ "$round_trip_seconds" -le "${BRAINCORE_TEST_ROUND_TRIP_TIMEOUT:-60}" ]]


compose=(docker compose --env-file "$test_state/.env" \
  -f "$repo_dir/compose.yaml" -f "$repo_dir/compose.dev.yaml")
"${compose[@]}" restart postgres
"${compose[@]}" up -d --wait
bun "$repo_dir/dist/cli.js" doctor --state-dir "$test_state"
bun "$repo_dir/scripts/compose-smoke.ts" "$test_state/.env"
"${compose[@]}" run --rm migrate
bun "$repo_dir/dist/cli.js" doctor --state-dir "$test_state"
bun "$repo_dir/dist/cli.js" backup "$test_state/backup.dump" --state-dir "$test_state"
[[ "$(stat -c '%a' "$test_state/backup.dump")" == "600" ]]
[[ -s "$test_state/backup.dump" ]]
bun "$repo_dir/dist/cli.js" restore-backup "$test_state/backup.dump" --state-dir "$test_state" --confirm
bun "$repo_dir/dist/cli.js" doctor --state-dir "$test_state"
bun "$repo_dir/scripts/compose-smoke.ts" "$test_state/.env"

for benchmark_suite in locomo longmemeval beam; do
  BRAINCORE_API_KEY="$(sed -n 's/^BRAINCORE_BOOTSTRAP_ADMIN_KEY=//p' "$test_state/.env")" \
    uv run --locked python "$repo_dir/benchmarks/run.py" \
    --url "$(sed -n 's/^BRAINCORE_URL=//p' "$test_state/.env")" \
    --suite "$benchmark_suite" --input "$repo_dir/benchmarks/fixtures/smoke.json" \
    --output-dir "$test_state/benchmark-$benchmark_suite-smoke"
  [[ "$(jq -r '.declared_cases == 1 and .completed_cases == 1 and .evidence_recall_at_k == 1' \
    "$test_state/benchmark-$benchmark_suite-smoke/summary.json")" == "true" ]]
done

bootstrap_key="$(sed -n 's/^BRAINCORE_BOOTSTRAP_ADMIN_KEY=//p' "$test_state/.env")"
if "${compose[@]}" logs --no-color | \
  grep -Fq "$bootstrap_key"; then
  echo "Bootstrap key leaked into container logs" >&2
  exit 1
fi

echo "Protected bootstrap configuration and log-redaction checks passed."
echo "Readiness ${ready_seconds}s; first round trip ${round_trip_seconds}s."
