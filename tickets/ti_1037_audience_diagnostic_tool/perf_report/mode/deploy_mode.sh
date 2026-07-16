#!/usr/bin/env bash
# deploy_mode.sh — push the staging SQL (+ attempt HTML) into the Mode report via the
# Mode REST API, replacing the 16-paste UI relay.
#
#   ./deploy_mode.sh check        auth test + match staging files to live Mode queries
#   ./deploy_mode.sh diff         which staging files differ from what Mode has now
#   ./deploy_mode.sh apply        PATCH every changed query (+ attempt the HTML layout)
#   ./deploy_mode.sh apply --run  apply, then trigger a report Run and poll to completion
#   ./deploy_mode.sh run          just trigger a Run (refreshes window.datasets) and poll
#
# Credentials (never hardcode): export in ~/.zshrc —
#   export MODE_API_TOKEN=...   # Mode: Workspace Settings > Personal > My API Keys
#   export MODE_API_SECRET=...  # (Member keys must be enabled by a Mode admin:
#                               #  Workspace Settings > Features > API Keys > Member keys)
#
# Matching rule: Mode query NAME == staging filename minus ".sql" and minus a trailing
# ".<hex token>" (the mode-assets export suffix). Unmatched files/queries are listed,
# never guessed. Remote SQL is backed up to ~/.cache/mode_deploy/<ts>/ before any PATCH.
#
# KNOWN LIMIT: the report's HTML layout is NOT a documented PATCH field (docs allow only
# name/description/space_token). apply ATTEMPTS {"report":{"layout":...}} and then
# re-GETs to verify; if Mode silently ignores it, the script says so and index.html
# still needs one UI paste. Queries always deploy via the API either way.
# After any apply: window.datasets = the LAST RUN — a Run (in UI or --run) is required
# before the report renders the new SQL.

set -euo pipefail

WORKSPACE="mntn"
REPORT="6c4fc72afcfb"   # Client Performance Diagnostic (🗂️ Audience Intelligence)
API="https://app.mode.com/api"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_ROOT="${HOME}/.cache/mode_deploy"

[[ -n "${MODE_API_TOKEN:-}" && -n "${MODE_API_SECRET:-}" ]] || {
  echo "ERROR: MODE_API_TOKEN / MODE_API_SECRET not set (see header comment)." >&2; exit 1; }
command -v jq >/dev/null || { echo "ERROR: jq required." >&2; exit 1; }

# mcurl <method> <path> [json-body-file] -> body on stdout; fails loudly on non-2xx
mcurl() {
  local method="$1" path="$2" body="${3:-}" out http
  out="$(mktemp)"
  if [[ -n "$body" ]]; then
    http="$(curl -sS -u "${MODE_API_TOKEN}:${MODE_API_SECRET}" -X "$method" \
      -H "Content-Type: application/json" -H "Accept: application/hal+json" \
      --data-binary "@${body}" -o "$out" -w "%{http_code}" "${API}${path}")"
  else
    http="$(curl -sS -u "${MODE_API_TOKEN}:${MODE_API_SECRET}" -X "$method" \
      -H "Accept: application/hal+json" -o "$out" -w "%{http_code}" "${API}${path}")"
  fi
  if [[ "$http" != 2* ]]; then
    echo "ERROR: ${method} ${path} -> HTTP ${http}" >&2
    head -c 600 "$out" >&2; echo >&2; rm -f "$out"; return 1
  fi
  cat "$out"; rm -f "$out"
}

# staging .sql files -> "name<TAB>path" (name = basename minus .sql minus trailing .hextoken)
staging_files() {
  find "$DIR" "$DIR/batch1_queries" -maxdepth 1 -name '*.sql' -type f 2>/dev/null | sort |
  while IFS= read -r f; do
    local_name="$(basename "$f" .sql)"
    local_name="$(echo "$local_name" | sed -E 's/\.[0-9a-f]{8,16}$//')"
    printf '%s\t%s\n' "$local_name" "$f"
  done
}

fetch_remote_queries() {  # -> "token<TAB>name" lines
  mcurl GET "/${WORKSPACE}/reports/${REPORT}/queries" |
    jq -r '._embedded.queries[] | [.token, .name] | @tsv'
}

# find_tok <name> <remote-tsv>: match ignoring case and underscore-vs-space
# ("04_yoy_metrics" == "04 YoY Metrics"); first match wins, check surfaces misses.
find_tok() {
  local name="$1" remote="$2"
  awk -F'\t' -v n="$name" 'BEGIN{gsub(/_/," ",n); n=tolower(n)}
    {m=$2; gsub(/_/," ",m); if(tolower(m)==n){print $1; exit}}' <<<"$remote"
}

cmd_check() {
  echo "== auth =="
  mcurl GET "/account" | jq -r '"authenticated as: \(.username // .name)"'
  echo "== report =="
  mcurl GET "/${WORKSPACE}/reports/${REPORT}" | jq -r '"\(.name)  (last run: \(.last_run_at // "never"))"'
  echo "== match (staging file -> live Mode query) =="
  local remote; remote="$(fetch_remote_queries)"
  local matched=0 missing=0
  while IFS=$'\t' read -r name path; do
    tok="$(find_tok "$name" "$remote")"
    if [[ -n "$tok" ]]; then echo "  OK   ${name}  ->  ${tok}"; ((matched+=1))
    else echo "  MISS ${name}  (no Mode query with this exact name)"; ((missing+=1)); fi
  done < <(staging_files)
  echo "== live queries with NO staging file (left untouched) =="
  while IFS=$'\t' read -r tok name; do
    staging_files | cut -f1 | grep -qxF "$name" || echo "  ${name} (${tok})"
  done <<<"$remote"
  echo "matched=${matched} missing=${missing}"
}

cmd_diff() {
  local remote; remote="$(fetch_remote_queries)"
  while IFS=$'\t' read -r name path; do
    tok="$(find_tok "$name" "$remote")"
    [[ -z "$tok" ]] && { echo "MISS   ${name}"; continue; }
    mcurl GET "/${WORKSPACE}/reports/${REPORT}/queries/${tok}" | jq -r '.raw_query' > /tmp/mode_remote_q.sql
    if diff -q /tmp/mode_remote_q.sql "$path" >/dev/null 2>&1; then echo "same   ${name}"
    else echo "DIFFER ${name}  ($(diff /tmp/mode_remote_q.sql "$path" | grep -c '^[<>]') changed lines)"; fi
  done < <(staging_files)
}

cmd_apply() {
  local ts backup remote patched=0 skipped=0
  ts="$(date +%Y%m%d_%H%M%S)"; backup="${BACKUP_ROOT}/${ts}"; mkdir -p "$backup"
  remote="$(fetch_remote_queries)"
  while IFS=$'\t' read -r name path; do
    tok="$(find_tok "$name" "$remote")"
    [[ -z "$tok" ]] && { echo "SKIP (no match): ${name}"; ((skipped+=1)); continue; }
    mcurl GET "/${WORKSPACE}/reports/${REPORT}/queries/${tok}" | jq -r '.raw_query' > "${backup}/${name}.sql"
    if diff -q "${backup}/${name}.sql" "$path" >/dev/null 2>&1; then
      echo "same: ${name}"; continue
    fi
    body="$(mktemp)"; jq -n --rawfile sql "$path" '{query: {raw_query: $sql}}' > "$body"
    mcurl PATCH "/${WORKSPACE}/reports/${REPORT}/queries/${tok}" "$body" > /dev/null
    rm -f "$body"
    # verify: re-GET and compare
    mcurl GET "/${WORKSPACE}/reports/${REPORT}/queries/${tok}" | jq -r '.raw_query' > /tmp/mode_verify_q.sql
    if diff -q /tmp/mode_verify_q.sql "$path" >/dev/null 2>&1; then
      echo "PATCHED+verified: ${name}"; ((patched+=1))
    else
      echo "ERROR: PATCH accepted but re-GET differs: ${name}" >&2; exit 1
    fi
  done < <(staging_files)
  echo "queries: patched=${patched} skipped=${skipped}  (backups: ${backup})"

  # --- HTML layout: undocumented field; attempt + verify, warn honestly on failure ---
  if [[ -f "${DIR}/index.html" ]]; then
    mcurl GET "/${WORKSPACE}/reports/${REPORT}" > /tmp/mode_report.json
    if jq -e 'has("layout")' /tmp/mode_report.json >/dev/null; then
      jq -r '.layout // ""' /tmp/mode_report.json > "${backup}/layout.html"
      if diff -q "${backup}/layout.html" "${DIR}/index.html" >/dev/null 2>&1; then
        echo "html: same"
      else
        body="$(mktemp)"; jq -n --rawfile h "${DIR}/index.html" '{report: {layout: $h}}' > "$body"
        mcurl PATCH "/${WORKSPACE}/reports/${REPORT}" "$body" > /dev/null || true
        rm -f "$body"
        mcurl GET "/${WORKSPACE}/reports/${REPORT}" | jq -r '.layout // ""' > /tmp/mode_verify_l.html
        if diff -q /tmp/mode_verify_l.html "${DIR}/index.html" >/dev/null 2>&1; then
          echo "html: PATCHED+verified (undocumented layout field works)"
        else
          echo "html: NOT updated — Mode ignored the layout PATCH; paste index.html in the UI (1 paste)."
        fi
      fi
    else
      echo "html: report payload has no layout field; paste index.html in the UI (1 paste)."
    fi
  fi

  [[ "${1:-}" == "--run" ]] && cmd_run
}

cmd_run() {
  echo "triggering report run (default params)..."
  body="$(mktemp)"; echo '{}' > "$body"
  run_tok="$(mcurl POST "/${WORKSPACE}/reports/${REPORT}/runs" "$body" | jq -r '.token')"
  rm -f "$body"
  echo "run ${run_tok} started; polling..."
  for _ in $(seq 1 120); do  # up to 20 min
    state="$(mcurl GET "/${WORKSPACE}/reports/${REPORT}/runs/${run_tok}" | jq -r '.state')"
    case "$state" in
      succeeded) echo "run succeeded — window.datasets refreshed."; return 0 ;;
      failed|cancelled) echo "run ${state} — check the report in the UI." >&2; return 1 ;;
      *) printf '  %s\n' "$state"; sleep 10 ;;
    esac
  done
  echo "timeout after 20 min — check the run in the UI." >&2; return 1
}

case "${1:-}" in
  check) cmd_check ;;
  diff)  cmd_diff ;;
  apply) cmd_apply "${2:-}" ;;
  run)   cmd_run ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
