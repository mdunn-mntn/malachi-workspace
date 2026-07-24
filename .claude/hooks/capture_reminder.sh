#!/usr/bin/env bash
# Stop — advisory capture-due reminder when knowledge is out of sync: the doc-debt queue is
# non-empty, or a knowledge doc changed since the last index build. Prints to stderr; exit 0 does
# NOT block. To make capture a hard gate, change the final `exit 0` to `exit 2` (see .claude/README.md).
set -uo pipefail
ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
Q="$ROOT/knowledge/bq/_UNDOCUMENTED.queue"
IDX="$ROOT/knowledge/INDEX.md"

due=""
[[ -s "$Q" ]] && due="doc-debt queue non-empty ($(grep -c . "$Q") table(s))"

if [[ -f "$IDX" ]]; then
  newer="$(find "$ROOT/knowledge" -name '*.md' -newer "$IDX" \
            ! -name 'INDEX.md' ! -name '_*' 2>/dev/null | head -1)"
  [[ -n "$newer" ]] && due="${due:+$due; }knowledge docs edited since last index build"
fi

if [[ -n "$due" ]]; then
  echo "[capture-due] $due" >&2
  echo "  → run /capture (or the curator agent) to route new facts to their home docs, then .claude/scripts/build_index.sh." >&2
fi

# framing-due nudge: an OPTED-IN ticket (has framing_state) left in an illegal framing state.
# Fires on VIOLATIONs only — legacy cards (no framing_state) are a migration backlog surfaced by
# lint on demand, not a per-Stop nag. lint_tickets is the single source of truth for what's illegal.
LT="$ROOT/.claude/scripts/lint_tickets.py"
if [[ -f "$LT" ]]; then
  fr="$(python3 "$LT" --check 2>&1 | grep '^VIOLATION' | grep -i 'framing' | head -3)"
  if [[ -n "$fr" ]]; then
    echo "[framing-due] ticket(s) not in a valid §0 Framing state:" >&2
    echo "$fr" | sed -E 's/^VIOLATION /  · /' >&2
    echo "  → run /frame <TI-XXX> to lock the Question/Goal/Objective/Approach (or set framing_state: 'skip: <reason>')." >&2
  fi
fi
exit 0
