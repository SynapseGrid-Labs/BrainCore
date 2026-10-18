#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

mapfile -d '' all_files < <(git ls-files --cached --others --exclude-standard -z)

mapfile -d '' candidate_files < <(
  printf '%s\0' "${all_files[@]}" |
    grep -zvE '^(scripts/check-public-tree\.sh|bun\.lock|uv\.lock)$'
)

if [[ ${#candidate_files[@]} -eq 0 ]]; then
  echo "No candidate files were found." >&2
  exit 1
fi

generic_private_pattern='ssh://git@|https?://[^/[:space:]]*\.(internal|lan|local)([:/]|$)|/(home|Users)/[^/[:space:]]+|/srv/[^/[:space:]]+|[A-Z]:\\Users\\[^\\[:space:]]+|10\.([0-9]{1,3}\.){2}[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}|192\.168\.[0-9]{1,3}\.[0-9]{1,3}'
if rg -n -I -i -- "$generic_private_pattern" "${candidate_files[@]}"; then
  echo "Public candidate contains a private product, host, path, or address marker." >&2
  exit 1
fi

# These superseded internal prefixes must never re-enter runtime, API, log, or
# generated-contract files. This script is excluded from its own candidate scan.
superseded_identity_pattern='tt_session|tts_|ttc_|hbcp_|bcp_'
if rg -n -I -- "$superseded_identity_pattern" "${candidate_files[@]}"; then
  echo "Public candidate contains a superseded internal identity marker." >&2
  exit 1
fi

private_markers_file="${BRAINCORE_PRIVATE_MARKERS_FILE:-}"
if [[ -n "$private_markers_file" ]]; then
  if [[ ! -f "$private_markers_file" ]]; then
    echo "BRAINCORE_PRIVATE_MARKERS_FILE does not name a readable file." >&2
    exit 1
  fi
  if rg -n -I -i -f "$private_markers_file" "${all_files[@]}"; then
    echo "Public candidate contains an operator-supplied private marker." >&2
    exit 1
  fi
fi

if git ls-files -z | grep -zEq '(^|/)\.env$'; then
  echo "A runtime .env file is tracked." >&2
  exit 1
fi

for file in "${candidate_files[@]}"; do
  if [[ -L "$file" && "$(readlink "$file")" == /* ]]; then
    echo "$file -> $(readlink "$file")" >&2
    echo "Public candidate contains an absolute symlink." >&2
    exit 1
  fi
done

echo "Public-tree private-boundary checks passed."
