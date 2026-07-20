#!/usr/bin/env bash
# SessionStart — print a compact orientation so a fresh chat can route to the right doc without
# ingesting the whole knowledge base. stdout is added to the session context. Always exit 0.
set -uo pipefail
ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

echo "── AI Workflow Kit ─────────────────────────────────────────────"
echo "Retrieval: CLAUDE.md → knowledge/START_HERE.md → _ROUTING.md (keyword→doc) /"
echo "           bq/_TOPICS.md (by domain) / bq/_CATALOG_INDEX.md → the one doc. Load indexes, not the tree."
echo "BigQuery : ALWAYS via .claude/scripts/bq_run.sh (dry-run gate + cost log). Sample first. Raw 'bq query' is blocked."

COV="$ROOT/knowledge/bq/_COVERAGE.md"
[[ -f "$COV" ]] && grep -m1 '^Rollup:' "$COV" | sed 's/\*\*//g; s/^Rollup:/Coverage :/'

Q="$ROOT/knowledge/bq/_UNDOCUMENTED.queue"
if [[ -s "$Q" ]]; then
  echo "Doc-debt : $(grep -c . "$Q") undocumented table(s) queued — see knowledge/bq/_UNDOCUMENTED.queue"
fi

L="$ROOT/knowledge/bq_perf_log.jsonl"
if [[ -f "$L" ]]; then
  echo "Perf log : $(grep -c . "$L" 2>/dev/null) queries logged — mine with scripts/perf_digest.py --mode all"
fi

HS="$ROOT/.claude/scripts/health_scorecard.py"
[[ -f "$HS" ]] && python3 "$HS" 2>/dev/null
echo "────────────────────────────────────────────────────────────────"
exit 0
