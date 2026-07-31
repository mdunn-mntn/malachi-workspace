#!/usr/bin/env bash
# airflow_pull.sh — pull Astronomer (Airflow 3) task logs for a day + a completion sensor for on-call.
# Usage:
#   Day-dump:  bash .claude/scripts/airflow_pull.sh [--date YYYY-MM-DD] [--dag NAME] [--tag TAG] [--state failed] [--all-tries] [--deployment ID]
#   Sensor:    bash .claude/scripts/airflow_pull.sh --watch --tag <tag> [--dag NAME] [--interval 30] [--persistent]
#   Auth gate: bash .claude/scripts/airflow_pull.sh --check
#
# Downloads every task-instance log that ran on --date (UTC), renames each to
# <HHMMSS>__<dag>__<task>__try<N>__<state>.log under on-call/airflow_logs/<date>/, and writes a
# _manifest.jsonl (the pass/fail grid that replaces the screenshot). --watch polls task state and, on
# each terminal transition, downloads that task's log — dropping failures into on-call/ so the existing
# triage hook + /oncall self-diagnose. Auth is the interactive `astro` login context (no stored secret).
set -euo pipefail

WORKSPACE="/Users/malachi/Developer/work/mntn/workspace"
SCRIPT_DIR="${WORKSPACE}/.claude/scripts"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/config.env"
cd "$WORKSPACE"   # so manifest log_paths are repo-relative

DATE="$(date -u +%F)"
DEPLOYMENT_ID="${AIRFLOW_TI_DEPLOYMENT_ID}"
OUTDIR="${AIRFLOW_PULL_DIR}"
ONCALL_DIR="on-call"
MODE="list"
INTERVAL="${AIRFLOW_POLL_INTERVAL}"
PY_ARGS=()
PERSISTENT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --date)        DATE="$2"; shift 2 ;;
        --date=*)      DATE="${1#--date=}"; shift ;;
        --dag)         PY_ARGS+=(--dag "$2"); shift 2 ;;
        --dag=*)       PY_ARGS+=(--dag "${1#--dag=}"); shift ;;
        --tag)         PY_ARGS+=(--tag "$2"); shift 2 ;;
        --tag=*)       PY_ARGS+=(--tag "${1#--tag=}"); shift ;;
        --state)       PY_ARGS+=(--state "$2"); shift 2 ;;
        --state=*)     PY_ARGS+=(--state "${1#--state=}"); shift ;;
        --deployment)  DEPLOYMENT_ID="$2"; shift 2 ;;
        --deployment=*) DEPLOYMENT_ID="${1#--deployment=}"; shift ;;
        --interval)    INTERVAL="$2"; shift 2 ;;
        --interval=*)  INTERVAL="${1#--interval=}"; shift ;;
        --all-tries)   PY_ARGS+=(--all-tries); shift ;;
        --watch)       MODE="watch"; shift ;;
        --persistent)  PERSISTENT="--persistent"; shift ;;
        --check)       MODE="check"; shift ;;
        -h|--help)     sed -n '2,13p' "$0"; exit 0 ;;
        *)             echo "airflow_pull: unknown arg '$1'" >&2; exit 2 ;;
    esac
done

# --- auth: require an interactive astro login context (no stored secret) ---
if ! astro context list >/dev/null 2>&1; then
    echo "airflow_pull: not logged in to Astro. Run 'astro login' first (SSO, ~daily)." >&2
    exit 3
fi

# --- resolve the Airflow API base URL (cache via AIRFLOW_TI_API_URL in config.env to skip this call) ---
BASE="${AIRFLOW_TI_API_URL}"
if [[ -z "$BASE" ]]; then
    BASE="$(astro deployment inspect "$DEPLOYMENT_ID" --key metadata.airflow_api_url 2>/dev/null | tr -d '"[:space:]')"
    if [[ -z "$BASE" || "$BASE" == "null" ]]; then
        echo "airflow_pull: could not resolve airflow_api_url for deployment $DEPLOYMENT_ID." >&2
        echo "  Try: astro deployment inspect $DEPLOYMENT_ID  (then set AIRFLOW_TI_API_URL in config.env)." >&2
        exit 4
    fi
fi
[[ "$BASE" == *"/api/v2" ]] || BASE="${BASE%/}/api/v2"
[[ "$BASE" == http*://* ]] || BASE="https://${BASE}"   # inspect returns host+path with no scheme

PY="python3 ${SCRIPT_DIR}/airflow_api.py --base ${BASE}"

case "$MODE" in
    check)
        exec $PY version ;;
    list)
        exec $PY list --date "$DATE" --outdir "$OUTDIR" "${PY_ARGS[@]}" ;;
    watch)
        exec $PY watch --date "$DATE" --outdir "$OUTDIR" --oncall-dir "$ONCALL_DIR" \
            --interval "$INTERVAL" $PERSISTENT "${PY_ARGS[@]}" ;;
esac
