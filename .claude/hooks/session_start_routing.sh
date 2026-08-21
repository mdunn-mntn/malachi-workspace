#!/usr/bin/env bash
# SessionStart — print a compact orientation so a fresh chat can route to the right doc without
# ingesting the whole knowledge base. stdout is added to the session context. Always exit 0.
set -uo pipefail
ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

# Safe pull: only when the tree is clean, so overnight Pi commits land without clobbering local work.
# ORIENT_NO_PULL=1 = read-only orientation (dsh sessions set it: a pull from a session-start hook
# races concurrent sessions in this shared worktree; Claude Code's own SessionStart keeps the pull).
if [[ "${ORIENT_NO_PULL:-}" != "1" ]] && git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1 && [[ -z "$(git -C "$ROOT" status --porcelain 2>/dev/null)" ]]; then
  git -C "$ROOT" pull --quiet origin main >/dev/null 2>&1 && echo "Git      : pulled origin/main (tree was clean)"
fi

echo "── AI Workflow Kit ─────────────────────────────────────────────"
echo "Retrieval: CLAUDE.md → knowledge/START_HERE.md → _ROUTING.md (keyword→doc) /"
echo "           bq/_TOPICS.md (by domain) / bq/_CATALOG_INDEX.md → the one doc. Load indexes, not the tree."
echo "BigQuery : ALWAYS via .claude/scripts/bq_run.sh (perf+provenance log, us-central1 reservation). Sample first; dry-run unfamiliar SQL. Raw 'bq query' is blocked."

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
