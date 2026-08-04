#!/usr/bin/env bash
# Weekly Spark fleet optimizer (AUDI-1191). Pulls the recent event logs from the live
# spark-events prefix, runs the deterministic (key-free, no-LLM) optimizer crawl on each,
# and writes a ranked cross-job backlog. Idles gracefully until event-log enablement lands
# (empty/absent prefix -> a one-line "pending" note, exit 0). Built for a weekly cron.
#
# Usage:
#   oncall_weekly_optimizer.sh                 # crawl the live GCS prefix (default)
#   oncall_weekly_optimizer.sh /path/to/logs   # crawl a local dir of event logs (testing)
#
# Auth : gsutil uses the user's gcloud creds (key-free, SSO). Stale creds -> exit 0 with a note.
set -uo pipefail

WORKSPACE="/Users/malachi/Developer/work/mntn/workspace"
PREFIX="${SPARK_EVENTS_PREFIX:-gs://mntn-data-archive-prod/spark-events}"
CAP="${OPTIMIZER_LOG_CAP:-40}"        # newest N logs per run (bounded: -m bulk pulls have crashed)
DATE="$(date +%F)"
OUTDIR="${WORKSPACE}/tickets/audi_1191_airflow_spark_debugger/outputs"
REPORT="${OUTDIR}/optimizer_backlog_${DATE}.md"

cd "$WORKSPACE"
mkdir -p "$OUTDIR"

# ---- Local-dir mode (testing): crawl a directory that already holds event logs. ----------
if [[ $# -ge 1 && -d "$1" ]]; then
    echo "[weekly_optimizer] local mode: crawling $1"
    body="$(python3 -m airflow_debugger.crawl "$1")"
    { echo "# Spark fleet optimizer backlog — ${DATE} (local: $1)"; echo; echo "$body"; } > "$REPORT"
    echo "[weekly_optimizer] wrote ${REPORT#"$WORKSPACE"/}"
    echo "$body" | head -n 1
    exit 0
fi

# ---- Live mode: pull the newest logs from the GCS prefix, then crawl. --------------------
TMP="$(mktemp -d "${TMPDIR:-/tmp}/spark_events.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

# List the prefix by recency. A stale/absent/denied prefix is not an error here (enablement
# may not be live yet) — we degrade to a "pending" note rather than failing the cron.
listing="$(gsutil ls -l "${PREFIX}/**" 2>/dev/null \
    | grep -E '\.zstd$|\.inprogress$' \
    | sort -k2 \
    | awk '{print $NF}' \
    | tail -n "$CAP")"

if [[ -z "$listing" ]]; then
    msg="[weekly_optimizer] no event logs at ${PREFIX} — enablement pending (AUDI-1191 step #1). Idling."
    echo "$msg"
    # No git noise until real logs flow: only touch the report when there is something to report.
    exit 0
fi

n=0
while IFS= read -r obj; do
    [[ -z "$obj" ]] && continue
    # gsutil cp corrupts .zstd via the crc32c gatekeeper; check_hashes=never is required.
    if gsutil -o "GSUtil:check_hashes=never" cp "$obj" "$TMP/" >/dev/null 2>&1; then
        n=$((n + 1))
    fi
done <<< "$listing"

if [[ $n -eq 0 ]]; then
    echo "[weekly_optimizer] prefix listed but 0 logs downloaded (creds stale?). Idling."
    exit 0
fi

echo "[weekly_optimizer] pulled ${n} log(s) from ${PREFIX}; crawling."
body="$(python3 -m airflow_debugger.crawl "$TMP")"
{
    echo "# Spark fleet optimizer backlog — ${DATE}"
    echo
    echo "Source: ${PREFIX} (newest ${n} logs, cap ${CAP})."
    echo
    echo "$body"
} > "$REPORT"

echo "[weekly_optimizer] wrote ${REPORT#"$WORKSPACE"/}"
echo "$body" | head -n 1
