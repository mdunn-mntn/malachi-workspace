#!/usr/bin/env bash
# Stop — advisory on-call-triage reminder. If a raw alert log sits in on-call/ that is NEWER than the
# incident log (i.e. an alert landed but was never triaged/logged), nudge to run /oncall. This is the
# safety net for the "append every incident" rule — the same leak that left the fangorn alert un-logged
# until INC-002. Prints to stderr; exit 0 does NOT block (advisory, like capture_reminder.sh).
set -uo pipefail
ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
OCDIR="$ROOT/on-call"
JLOG="$OCDIR/incident_log.jsonl"

[[ -d "$OCDIR" ]] || exit 0

# Raw alert logs = everything in on-call/ that ISN'T a doc/index/log-of-logs. Downloaded task logs are
# typically logs_*.txt / *.log. We compare against incident_log.jsonl's mtime: once every incident is
# logged, the JSONL is the newest file and this stays silent. A fresh un-triaged log flips it on.
newest_raw="$(find "$OCDIR" -maxdepth 1 -type f \
                ! -name '*.md' ! -name 'incident_log.jsonl' ! -name 'INDEX.md' \
                -newer "$JLOG" 2>/dev/null | head -1)"

# If the JSONL doesn't exist yet, ANY raw file counts as un-triaged debt.
if [[ ! -f "$JLOG" ]]; then
  newest_raw="$(find "$OCDIR" -maxdepth 1 -type f ! -name '*.md' ! -name 'INDEX.md' 2>/dev/null | head -1)"
fi

if [[ -n "$newest_raw" ]]; then
  echo "[oncall-triage-due] un-triaged alert log in on-call/: $(basename "$newest_raw")" >&2
  echo "  → run /oncall to triage it and log the incident (§3 + §2 + incident_log.jsonl), or delete the log if it's not an alert." >&2
fi
exit 0
