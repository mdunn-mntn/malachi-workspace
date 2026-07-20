#!/usr/bin/env bash
# PostToolUse:Bash — after a scripts/bq_run.sh call, flag any referenced table that has no catalog
# doc into knowledge/bq/_UNDOCUMENTED.queue (the documentation-debt ledger). Detection only — a
# human/cataloger populates the doc later (a shell hook can't invoke an agent).
# Defensive: any failure exits 0.
set -uo pipefail
ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
CMD="$(jq -r '.tool_input.command // ""' 2>/dev/null)" || exit 0
# Only act when bq_run.sh was actually INVOKED (command position, followed by args) — not when the
# command merely names the file (e.g. `wc -l scripts/bq_run.sh`, `cat .../bq_run.sh | grep x`).
printf '%s' "$CMD" | grep -qE '(^|[|&;[:space:]])(\./|[^[:space:]]*/)?bq_run\.sh[[:space:]]' || exit 0

LOG="$ROOT/knowledge/bq_perf_log.jsonl"
QUEUE="$ROOT/knowledge/bq/_UNDOCUMENTED.queue"
[[ -s "$LOG" ]] || exit 0

TABLES="$(tail -n1 "$LOG" | jq -r '.sql_tables[]?' 2>/dev/null)" || exit 0
[[ -z "$TABLES" ]] && exit 0

new=()
while IFS= read -r t; do
  [[ -z "$t" ]] && continue
  # Skip metadata pseudo-tables + view-backing physicals (documented via their parent silver view):
  # INFORMATION_SCHEMA.* / region-* metadata; sqlmesh__* versioned physicals; raw.*/history.* UNION legs.
  case "$t" in
    *INFORMATION_SCHEMA*|region-*|sqlmesh__*.*|raw.*|history.*) continue ;;
  esac
  ds="${t%%.*}"; tbl="${t#*.}"
  [[ -f "$ROOT/knowledge/bq/$ds/$tbl.md" ]] || new+=("$t")
done <<< "$TABLES"

if [[ ${#new[@]} -gt 0 ]]; then
  { [[ -f "$QUEUE" ]] && cat "$QUEUE"; printf '%s\n' "${new[@]}"; } \
    | grep -v '^$' | sort -u > "$QUEUE.tmp" && mv "$QUEUE.tmp" "$QUEUE"
  echo "[doc-debt] net-new table(s) with no catalog doc → knowledge/bq/_UNDOCUMENTED.queue: ${new[*]}" >&2
  echo "  next: .claude/scripts/bq_introspect.sh <dataset>  then enrich (cataloger / skeleton→enriched)." >&2
fi
exit 0
