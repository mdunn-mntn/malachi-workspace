#!/usr/bin/env bash
# Daily Spark fleet optimizer (AUDI-1194). Pulls a full day of event logs from the live
# spark-events prefix plus the PHS-attached ipdsc/tpa batches, runs the deterministic
# (key-free, no-LLM) optimizer crawl on each, and writes a ranked cross-job backlog.
# Idles gracefully when a source is unreachable (empty prefix or a PHS 403 -> a one-line
# note, exit 0). Cadence is daily because the fleet emits ~160 logs/day: a weekly cap-40
# sweep saw ~4% of it.
#
# Usage:
#   oncall_daily_optimizer.sh                 # crawl the live GCS prefix (default)
#   oncall_daily_optimizer.sh /path/to/logs   # crawl a local dir of event logs (testing)
#   oncall_daily_optimizer.sh --selftest      # hermetic regression check (no GCS needed)
#
# Auth : gsutil uses the user's gcloud creds (key-free, SSO). Stale creds -> exit 0 with a note.
set -uo pipefail

# A v2 rolling part must keep its eventlog_v2_* batch dir: flattened into one download dir,
# the crawler reads every events_* part as ONE merged log (cross-batch spill sums, colliding
# stage IDs) and standalone app-*.zstd logs beside them are never analyzed.
dest_for() {
    local root="$1" parent
    parent="$(basename "$(dirname "$2")")"
    if [[ "$parent" == eventlog_v2_* ]]; then echo "${root}/${parent}"; else echo "$root"; fi
}

WORKSPACE="/Users/malachi/Developer/work/mntn/workspace"
PREFIX="${SPARK_EVENTS_PREFIX:-gs://mntn-data-archive-prod/spark-events}"
CAP="${OPTIMIZER_LOG_CAP:-200}"       # newest N logs per run; ~160/day, so 200 covers a full day
PHS="${OPTIMIZER_PHS:-1}"             # 0 skips the ipdsc/tpa PHS half
DATE="$(date +%F)"
OUTDIR="${WORKSPACE}/tickets/audi_1194_optimizer_efficiency_crawler/outputs"
REPORT="${OUTDIR}/optimizer_backlog_${DATE}.md"

cd "$WORKSPACE"
mkdir -p "$OUTDIR"

# ---- Self-check: replay the live download loop on a synthetic fleet (2 standalone apps +
# 2 v2 rolling batches, cp standing in for gsutil cp) and assert the crawler sees 4 separate
# jobs. The pre-fix flat copy merged every events_* part into 1 fake job (cron-flatten-1). --
if [[ "${1:-}" == "--selftest" ]]; then
    SRC="$(mktemp -d "${TMPDIR:-/tmp}/spark_selftest_src.XXXXXX")"
    DL="$(mktemp -d "${TMPDIR:-/tmp}/spark_selftest_dl.XXXXXX")"
    trap 'rm -rf "$SRC" "$DL"' EXIT
    for app in app-1 app-2; do
        printf '%s\n' "{\"Event\":\"SparkListenerApplicationStart\",\"App Name\":\"$app\",\"Timestamp\":1000}" \
                      '{"Event":"SparkListenerApplicationEnd","Timestamp":2000}' > "$SRC/$app.zstd"
    done
    for b in aaa bbb; do
        mkdir -p "$SRC/eventlog_v2_batch-$b"
        printf '%s\n' "{\"Event\":\"SparkListenerApplicationStart\",\"App Name\":\"batch-$b\",\"Timestamp\":1000}" \
            > "$SRC/eventlog_v2_batch-$b/events_1_batch-$b.zstd"
        printf '%s\n' '{"Event":"SparkListenerApplicationEnd","Timestamp":2000}' \
            > "$SRC/eventlog_v2_batch-$b/events_2_batch-$b.zstd"
    done
    [[ "$(dest_for /t "gs://b/p/app-1.zstd")" == "/t" ]] \
        || { echo "[selftest] FAIL: standalone log not routed flat"; exit 1; }
    [[ "$(dest_for /t "gs://b/p/eventlog_v2_batch-x/events_1_batch-x.zstd")" == "/t/eventlog_v2_batch-x" ]] \
        || { echo "[selftest] FAIL: v2 part lost its batch dir"; exit 1; }
    while IFS= read -r obj; do
        dest="$(dest_for "$DL" "$obj")"
        mkdir -p "$dest"
        cp "$obj" "$dest/"
    done < <(find "$SRC" -name '*.zstd' | sort)
    OUT="$(mktemp -d "${TMPDIR:-/tmp}/spark_selftest_out.XXXXXX")"
    LED="${OUT}/ledger.jsonl"
    trap 'rm -rf "$SRC" "$DL" "$OUT"' EXIT
    head1="$(python3 -c "
import sys
from airflow_optimizer.sweep import run
out = run(['$DL'], '$DATE', outdir='$OUT', ledger_path='$LED')
print(f\"{out['scanned']} jobs scanned, {out['ledger_entries']} ledger entries\")
")"
    [[ "$head1" == *"4 jobs scanned"* ]] \
        || { echo "[selftest] FAIL: expected 4 jobs scanned, got: $head1"; exit 1; }
    [[ -s "${OUT}/optimizer_digest_${DATE}.md" ]] \
        || { echo "[selftest] FAIL: no digest written"; exit 1; }
    echo "[selftest] PASS: $head1"
    exit 0
fi

# ---- Local-dir mode (testing): crawl a directory that already holds event logs. ----------
if [[ $# -ge 1 && -d "$1" ]]; then
    echo "[daily_optimizer] local mode: crawling $1"
    python3 -m airflow_optimizer.sweep "$1" --date "$DATE" --source "local: $1"
    exit 0
fi

# ---- Live mode: pull the newest logs from the GCS prefix, then crawl. --------------------
TMP="$(mktemp -d "${TMPDIR:-/tmp}/spark_events.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

# List the prefix by recency. A stale/absent/denied prefix is not an error here (enablement
# may not be live yet) — we degrade to a "pending" note rather than failing the cron.
# Only finalized .zstd logs — the crawler discards .inprogress, so including them wastes
# the newest-N download budget and can pass the "0 downloaded" guard with a misleading report.
listing="$(gsutil ls -l "${PREFIX}/**" 2>/dev/null \
    | grep -E '\.zstd$' \
    | sort -k2 \
    | awk '{print $NF}' \
    | tail -n "$CAP")"

if [[ -z "$listing" ]]; then
    msg="[daily_optimizer] no event logs at ${PREFIX} (enablement pending or creds stale). Idling."
    echo "$msg"
    # No git noise until real logs flow: only touch the report when there is something to report.
    exit 0
fi

n=0
while IFS= read -r obj; do
    [[ -z "$obj" ]] && continue
    dest="$(dest_for "$TMP" "$obj")"
    mkdir -p "$dest"
    # gsutil cp corrupts .zstd via the crc32c gatekeeper; check_hashes=never is required.
    if gsutil -o "GSUtil:check_hashes=never" cp "$obj" "$dest/" >/dev/null 2>&1; then
        n=$((n + 1))
    fi
done <<< "$listing"

# ---- PHS half: ipdsc/tpa batches attach a history server and write per-uuid under the
# Dataproc temp bucket, not the archive prefix. Enumerate the SUCCEEDED PHS-attached batches
# and pull each log into the SAME download root so both sources rank in one backlog. The
# bucket 403s until mntn-devops#4724 merges and fetch_logs skips those quietly, so until then
# this stage costs one batches-list call and adds nothing.
phs_n=0
if [[ "$PHS" == "1" ]]; then
    phs_n="$(python3 - "$TMP" 2>/dev/null <<'PY' || echo 0
import sys

from airflow_optimizer import phs

print(len(phs.fetch_logs(phs.phs_succeeded(phs.list_batches()), sys.argv[1])))
PY
)"
    [[ "$phs_n" =~ ^[0-9]+$ ]] || phs_n=0
    echo "[daily_optimizer] PHS: ${phs_n} ipdsc/tpa batch log(s)."
fi

if [[ $((n + phs_n)) -eq 0 ]]; then
    echo "[daily_optimizer] prefix listed but 0 logs downloaded (creds stale?). Idling."
    exit 0
fi

echo "[daily_optimizer] pulled ${n} log(s) from ${PREFIX} + ${phs_n} PHS; crawling."

# Coverage needs the Airflow API to enumerate active DAGs and name what has no Spark task.
# A stale astro token is not fatal: the sweep then reports coverage as unknown.
base="${AIRFLOW_TI_API_URL:-}"
if [[ -z "$base" && "${OPTIMIZER_COVERAGE:-1}" == "1" ]]; then
    base="$(astro deployment inspect "${AIRFLOW_TI_DEPLOYMENT_ID:-cmd6bd10c0gl901rfuokgryiq}" \
            --key metadata.airflow_api_url 2>/dev/null | tr -d '"[:space:]')"
    [[ -z "$base" || "$base" == "null" ]] && base=""
fi
if [[ -n "$base" ]]; then
    [[ "$base" == http*://* ]] || base="https://${base}"
    [[ "$base" == */api/v2 ]] || base="${base%/}/api/v2"
fi

python3 -m airflow_optimizer.sweep "$TMP" \
    --date "$DATE" \
    --source "${PREFIX} (newest ${n} logs, cap ${CAP}) + ${phs_n} PHS batch log(s)." \
    --airflow-base "$base"
