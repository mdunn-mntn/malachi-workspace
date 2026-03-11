#!/usr/bin/env bash
# bq_run.sh — BQ query wrapper that captures performance metrics
# Usage: bash .claude/scripts/bq_run.sh [--ticket TI-XXX] [--label "description"] [bq query flags] 'SQL'
#
# Assigns a unique job ID, runs the query, fetches job stats, and appends
# a one-line JSON record to knowledge/bq_perf_log.jsonl.

set -euo pipefail

WORKSPACE="/Users/malachi/Developer/work/mntn/workspace"
LOG_FILE="${WORKSPACE}/knowledge/bq_perf_log.jsonl"

# Parse our custom flags and extract project_id from bq args
TICKET=""
LABEL=""
PROJECT_ID="dw-main-silver"
BQ_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ticket)
            TICKET="$2"; shift 2 ;;
        --label)
            LABEL="$2"; shift 2 ;;
        --project_id=*)
            PROJECT_ID="${1#--project_id=}"
            BQ_ARGS+=("$1"); shift ;;
        --project_id)
            PROJECT_ID="$2"
            BQ_ARGS+=("$1" "$2"); shift 2 ;;
        *)
            BQ_ARGS+=("$1"); shift ;;
    esac
done

# Generate a unique job ID
JOB_ID="perf_$(date +%Y%m%d_%H%M%S)_$$"

# Run the query with our job ID
set +e
bq query --job_id="$JOB_ID" "${BQ_ARGS[@]}"
EXIT_CODE=$?
set -e

# Fetch job stats — try us-central1 first (where MNTN data lives), fall back to US
JOB_JSON=""
for LOCATION in us-central1 US; do
    JOB_JSON=$(bq show --format=json --project_id="$PROJECT_ID" --location="$LOCATION" -j "$JOB_ID" 2>/dev/null || echo "")
    if [[ -n "$JOB_JSON" && "$JOB_JSON" != *"error"* ]]; then
        break
    fi
    JOB_JSON=""
done

if [[ -n "$JOB_JSON" ]]; then
    # Extract key metrics (handle missing fields gracefully)
    BYTES_PROCESSED=$(echo "$JOB_JSON" | jq -r '.statistics.totalBytesProcessed // "0"')
    BYTES_BILLED=$(echo "$JOB_JSON" | jq -r '.statistics.query.totalBytesBilled // "0"')
    SLOT_MS=$(echo "$JOB_JSON" | jq -r '.statistics.totalSlotMs // "0"')
    START_TIME=$(echo "$JOB_JSON" | jq -r '.statistics.startTime // "0"')
    END_TIME=$(echo "$JOB_JSON" | jq -r '.statistics.endTime // "0"')
    ELAPSED_MS=$(( END_TIME - START_TIME ))
    CACHE_HIT=$(echo "$JOB_JSON" | jq -r '.statistics.query.cacheHit // "false"')
    STATEMENT_TYPE=$(echo "$JOB_JSON" | jq -r '.statistics.query.statementType // "unknown"')

    # Referenced tables (may be empty for cached queries)
    REFERENCED_TABLES=$(echo "$JOB_JSON" | jq -c '[.statistics.query.referencedTables[]? | "\(.datasetId).\(.tableId)"]' 2>/dev/null || echo "[]")

    # Query plan stages (may not exist for cached queries)
    QUERY_PLAN_STAGES=$(echo "$JOB_JSON" | jq -r '(.statistics.query.queryPlan | length) // 0' 2>/dev/null || echo "0")

    # Convert bytes to human-readable
    GB_PROCESSED=$(echo "scale=3; ${BYTES_PROCESSED:-0} / 1073741824" | bc 2>/dev/null || echo "0")
    GB_BILLED=$(echo "scale=3; ${BYTES_BILLED:-0} / 1073741824" | bc 2>/dev/null || echo "0")
    SLOT_SEC=$(echo "scale=1; ${SLOT_MS:-0} / 1000" | bc 2>/dev/null || echo "0")
    ELAPSED_SEC=$(echo "scale=1; ${ELAPSED_MS:-0} / 1000" | bc 2>/dev/null || echo "0")

    # Build and append log entry
    LOG_ENTRY=$(jq -nc \
        --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --arg ticket "$TICKET" \
        --arg label "$LABEL" \
        --arg job_id "${PROJECT_ID}:${JOB_ID}" \
        --arg bytes_processed "$BYTES_PROCESSED" \
        --arg bytes_billed "$BYTES_BILLED" \
        --arg gb_processed "$GB_PROCESSED" \
        --arg gb_billed "$GB_BILLED" \
        --arg slot_ms "$SLOT_MS" \
        --arg slot_sec "$SLOT_SEC" \
        --arg elapsed_ms "$ELAPSED_MS" \
        --arg elapsed_sec "$ELAPSED_SEC" \
        --arg cache_hit "$CACHE_HIT" \
        --arg stages "$QUERY_PLAN_STAGES" \
        --argjson tables "$REFERENCED_TABLES" \
        --arg exit_code "$EXIT_CODE" \
        --arg statement_type "$STATEMENT_TYPE" \
        '{
            timestamp: $ts,
            ticket: $ticket,
            label: $label,
            job_id: $job_id,
            bytes_processed: ($bytes_processed | tonumber),
            bytes_billed: ($bytes_billed | tonumber),
            gb_processed: ($gb_processed | tonumber),
            gb_billed: ($gb_billed | tonumber),
            slot_ms: ($slot_ms | tonumber),
            slot_sec: ($slot_sec | tonumber),
            elapsed_sec: ($elapsed_sec | tonumber),
            cache_hit: ($cache_hit == "true"),
            query_plan_stages: ($stages | tonumber),
            referenced_tables: $tables,
            statement_type: $statement_type,
            exit_code: ($exit_code | tonumber)
        }'
    )

    echo "$LOG_ENTRY" >> "$LOG_FILE"

    # Print summary to stderr so it's visible but doesn't pollute results
    >&2 echo ""
    >&2 echo "--- BQ Performance ---"
    >&2 echo "Bytes processed: ${GB_PROCESSED} GB (billed: ${GB_BILLED} GB)"
    >&2 echo "Slot time: ${SLOT_SEC}s | Wall time: ${ELAPSED_SEC}s"
    >&2 echo "Cache hit: ${CACHE_HIT} | Stages: ${QUERY_PLAN_STAGES}"
    >&2 echo "Logged to: knowledge/bq_perf_log.jsonl"
    >&2 echo "----------------------"
else
    >&2 echo "[bq_run] Warning: could not fetch job stats for ${PROJECT_ID}:${JOB_ID}"
fi

exit $EXIT_CODE
