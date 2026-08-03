#!/usr/bin/env bash
# Daily retrospective RCA over the on-call/paging DAGs (AUDI-1191 trust-building).
#
# Scans a day for FAILED / upstream_failed tasks on the paging tags and auto-runs the
# deterministic (key-free, no-LLM) RCA on each, writing <log>.rca.md beside the log. Built
# for a once-a-day cron: failures are rare, so this idles most days and produces an RCA to
# eyeball against the real resolution when one lands. Reboot-safe, rolls the date each run,
# no long-lived process to babysit.
#
# Usage: oncall_daily_rca.sh [YYYY-MM-DD]   (default: yesterday UTC)
# Auth : needs a live `astro login` session (SSO ~daily). If the session is stale, this
#        exits 0 with a note (best-effort) rather than failing the cron.
set -uo pipefail

WORKSPACE="/Users/malachi/Developer/work/mntn/workspace"
PULL="${WORKSPACE}/.claude/scripts/airflow_pull.sh"
DATE="${1:-$(date -u -v-1d +%F 2>/dev/null || date -u -d 'yesterday' +%F)}"
# Two tags cover all known paging DAGs: tpa (targeting/tpa pipelines) + Machine Learning (mntn_match/openai).
TAGS=("tpa" "Machine Learning")
LOGDIR="${WORKSPACE}/on-call/airflow_logs/${DATE}"

cd "$WORKSPACE"
echo "[oncall_daily_rca] ${DATE} — scanning tags: ${TAGS[*]}"

for tag in "${TAGS[@]}"; do
    bash "$PULL" --date "$DATE" --tag "$tag" --state failed --state upstream_failed --diagnose
    rc=$?
    if [[ $rc -eq 3 ]]; then
        echo "[oncall_daily_rca] astro session stale — run 'astro login' and re-run. Skipping."
        exit 0
    fi
done

shopt -s nullglob
rcas=("${LOGDIR}"/*.rca.md)
if [[ ${#rcas[@]} -eq 0 ]]; then
    echo "[oncall_daily_rca] no failures diagnosed for ${DATE} (clean day)."
else
    echo "[oncall_daily_rca] ${#rcas[@]} RCA(s) written — review vs the real resolution:"
    for f in "${rcas[@]}"; do
        echo "  ${f#"$WORKSPACE"/}"
        head -n 1 "$f" | sed 's/^/    /'
    done
fi
