#!/usr/bin/env bash
# daily_gap_check.sh [YYYY-MM-DD] — pull yesterday's debugger + optimizer prod artifacts and
# write one gap report skeleton to on-call/gap_checks/gaps_<date>.md for a session to review.
# Mechanical signals only; the reasoning half is a Claude session reading the output.
# Idles quietly when an artifact is missing (the day may not have run yet).
set -euo pipefail
cd "$(dirname "$0")/../.."

DS="${1:-$(date -v-1d +%Y-%m-%d)}"
OUT_DIR="on-call/gap_checks"
OUT="$OUT_DIR/gaps_${DS}.md"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$OUT_DIR"

GS="gsutil -o GSUtil:check_hashes=never"
$GS cp "gs://mntn-data-archive-prod/debugger/rca_${DS}.json" "$TMP/" 2>/dev/null || true
$GS cp "gs://mntn-data-archive-prod/optimizer/optimizer_coverage_${DS}.md" "$TMP/" 2>/dev/null || true
$GS cp "gs://mntn-data-archive-prod/optimizer/optimizer_digest_${DS}.md" "$TMP/" 2>/dev/null || true

{
  echo "# Daily gap check — ${DS}"
  echo
  if [[ -f "$TMP/rca_${DS}.json" ]]; then
    python3 - "$TMP/rca_${DS}.json" << 'PY'
import json, sys
d = json.load(open(sys.argv[1]))
res = d.get("results", [])
low = [r for r in res if r.get("confidence") == "low"]
sl_err = [r for r in res if (r.get("slack") or {}).get("error")]
unthreaded = [r for r in res if (r.get("slack") or {}).get("sent") and not r["slack"].get("threaded")]
print(f"## Debugger — {d.get('diagnosed', 0)} diagnosed, {d.get('resolved', 0)} root-caused, "
      f"{d.get('slack_posted', 'n/a')} posted, {d.get('slack_threaded', 'n/a')} threaded\n")
for r in low:
    print(f"- LOW CONFIDENCE (taxonomy gap candidate): {r.get('dag_id')}/{r.get('task_id')} "
          f"sig={r.get('signature')}")
for r in sl_err:
    print(f"- SLACK ERROR: {r.get('dag_id')}/{r.get('task_id')}: {r['slack']['error']}")
for r in unthreaded:
    print(f"- POSTED LOOSE (no alert matched): {r.get('dag_id')}/{r.get('task_id')}")
if not (low or sl_err or unthreaded):
    print("- no debugger gaps flagged mechanically")
PY
  else
    echo "## Debugger — rca_${DS}.json not in GCS (run missing or not yet fired)"
  fi
  echo
  if [[ -f "$TMP/optimizer_coverage_${DS}.md" ]]; then
    echo "## Optimizer coverage — unresolved names"
    sed -n '/could not be tied/,/^## /p' "$TMP/optimizer_coverage_${DS}.md" | grep '^- `' || \
      echo "- every scanned job resolved"
  else
    echo "## Optimizer — coverage_${DS}.md not in GCS"
  fi
  echo
  if [[ -f "$TMP/rca_${DS}.json" ]]; then
    echo "## Triage tickets (auto-filed, Bug under AUDI-1054, playbook row appended)"
    export JIRA_API_TOKEN="${JIRA_API_TOKEN:-$(zsh -c 'source ~/.zshrc >/dev/null 2>&1; echo $JIRA_API_TOKEN')}"
    python3 -m airflow_debugger.triage "$TMP/rca_${DS}.json" 2>&1 || echo "- triage filing failed; run by hand"
    echo
  fi
  echo "## Review checklist (a session works this, not a script)"
  echo "- Any LOW CONFIDENCE row: read its log, decide signature vs resolver vs mask."
  echo "- Any POSTED LOOSE row: was the alert past the 100-message page (IMP-087)?"
  echo "- Any new unresolved coverage name: naming rule gap or genuinely unownable?"
  echo "- Fold real gaps into the open follow-ups branch; one gauntlet at the end."
} > "$OUT"

echo "wrote $OUT"
