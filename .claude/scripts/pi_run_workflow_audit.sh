#!/usr/bin/env bash
# pi_run_workflow_audit.sh — weekly DETERMINISTIC workflow-audit signal capture on the Pi.
#
# SOURCE OF TRUTH: this file (repo). DEPLOYED TO: ~/run_workflow_audit.sh on the Pi (pi5@192.168.10.177),
# run by crontab `0 8 * * 1` (Mon 08:00 America/Los_Angeles). It is deployed OUTSIDE the repo checkout
# on purpose: it git-pulls the repo mid-run, and a script must not modify its own file while executing.
# To update the Pi: edit here, then `scp .claude/scripts/pi_run_workflow_audit.sh pi5@<host>:~/run_workflow_audit.sh`.
#
# Runs the read-only aggregator (.claude/scripts/workflow_audit.sh) — pure Python + git, NO API key,
# NO model — and commits a dated signals file. The reasoning/report half runs on the Mac via
# /workflow-audit. DO NOT add an ANTHROPIC_API_KEY or any Claude credential to the Pi — that is the
# pattern MNTN security decommissioned (Slack bot, 2026-06-10). This script must stay key-free.
set -uo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin
REPO="$HOME/workspace"
LOG="$HOME/workflow_audit.log"

# rotate log if it gets big (>1MB)
[ -f "$LOG" ] && [ "$(stat -c%s "$LOG" 2>/dev/null || echo 0)" -gt 1048576 ] && mv "$LOG" "$LOG.1"
exec >> "$LOG" 2>&1
echo "===== $(date '+%Y-%m-%d %H:%M:%S %Z') run start ====="

cd "$REPO" || { echo "ABORT: repo $REPO missing"; exit 1; }

# single-run lock (skip silently if a prior run is still going)
exec 9>"$HOME/.workflow_audit.lock"
if ! flock -n 9; then echo "another run in progress; skip"; exit 0; fi

# get latest; abort if the pull would not fast-forward (avoid diverging histories)
if ! git pull --ff-only origin main; then
  echo "ABORT: pull not fast-forward — resolve on the Mac, then re-run"; exit 1
fi

DATE="$(date +%Y-%m-%d)"
OUTDIR="claude-prompts/workflow_audits"
OUT="$OUTDIR/signals_${DATE}.md"
mkdir -p "$OUTDIR"

# capture the deterministic signal rollup into the committed dated file
bash .claude/scripts/workflow_audit.sh > "$OUT" 2>/dev/null || { echo "ABORT: aggregator failed"; exit 1; }

git add "$OUT"
if git diff --cached --quiet; then echo "no change to commit"; exit 0; fi
git commit -m "workflow-audit: Pi signals ${DATE} (deterministic; reasoning pending on Mac)"

# in case the Mac pushed while we ran, rebase our single new-file commit then push
git pull --rebase origin main || echo "WARN: rebase pull failed; attempting push anyway"
if git push origin main; then echo "OK: pushed $OUT"; else echo "ERROR: push failed"; exit 1; fi
echo "===== run end ====="
