#!/usr/bin/env bash
# Stop — soft nudge to keep outward-facing writing terse. Fires only when this session touched a
# Jira write or an .xlsx read-me/notes surface, so it stays quiet on pure analysis sessions.
# Advisory (exit 0). The hard check is comms_lint_precheck.sh (PreToolUse), which lints the
# actual payload before it posts.
set -uo pipefail
echo "[comms] Before anything ships to Jira / an .xlsx read-me: lead with the answer in line 1, facts only." >&2
echo "  Caps — comment 500ch/75w · completion 800ch/120w · description 400ch/60w · xlsx read-me ≤12 lines." >&2
echo "  Cut hedges/adjectives/throat-clearing. Nothing decision-relevant to add? Post nothing. (CLAUDE.md §9)" >&2
echo "  Check a draft:  python3 .claude/scripts/lint_comms.py --kind comment --file draft.txt" >&2
exit 0
