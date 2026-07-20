#!/usr/bin/env bash
# bq_run.sh — BQ query wrapper that captures performance metrics
# Usage: bash .claude/scripts/bq_run.sh [--ticket TI-XXX] [--label "description"] [bq query flags] 'SQL'
#
# Assigns a unique job ID, runs the query, fetches job stats, and appends
# a one-line JSON record to knowledge/bq_perf_log.jsonl.
# Includes full execution plan, timeline, optimizations, and index usage.

set -euo pipefail

WORKSPACE="/Users/malachi/Developer/work/mntn/workspace"
LOG_FILE="${WORKSPACE}/knowledge/bq_perf_log.jsonl"

# Parse our custom flags and extract project_id from bq args
TICKET=""
LABEL=""
PROJECT_ID="dw-main-silver"
LOCATION_SET=false
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
        --location=*)
            LOCATION_SET=true
            BQ_ARGS+=("$1"); shift ;;
        --location)
            LOCATION_SET=true
            BQ_ARGS+=("$1" "$2"); shift 2 ;;
        *)
            BQ_ARGS+=("$1"); shift ;;
    esac
done

# Default to us-central1: MNTN's slot reservation (dw-main-bronze:us-central1.background-jobs)
# only covers jobs in us-central1. Queries with no dataset reference (e.g. inline
# --external_table_definition over GCS) otherwise default to the US multi-region and
# bill on-demand ($6.25/TiB). All MNTN datasets + gs://mntn-data-archive-prod are us-central1.
if [[ "$LOCATION_SET" == false ]]; then
    BQ_ARGS=(--location=us-central1 "${BQ_ARGS[@]}")
fi

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
    # Use a single jq invocation to extract everything from the job JSON
    LOG_ENTRY=$(echo "$JOB_JSON" | jq -c \
        --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --arg ticket "$TICKET" \
        --arg label "$LABEL" \
        --arg full_job_id "${PROJECT_ID}:${JOB_ID}" \
        --argjson exit_code "$EXIT_CODE" \
    '{
        # --- identity ---
        timestamp: $ts,
        ticket: $ticket,
        label: $label,
        job_id: $full_job_id,
        exit_code: $exit_code,

        # --- cost ---
        bytes_processed:  (.statistics.totalBytesProcessed  // "0" | tonumber),
        bytes_billed:     (.statistics.query.totalBytesBilled // "0" | tonumber),
        gb_processed:     ((.statistics.totalBytesProcessed   // "0" | tonumber) / 1073741824 | . * 1000 | round / 1000),
        gb_billed:        ((.statistics.query.totalBytesBilled // "0" | tonumber) / 1073741824 | . * 1000 | round / 1000),
        billing_tier:     (.statistics.query.billingTier // null),

        # --- time ---
        slot_ms:          (.statistics.totalSlotMs // "0" | tonumber),
        slot_sec:         ((.statistics.totalSlotMs // "0" | tonumber) / 1000 | . * 10 | round / 10),
        elapsed_ms:       (((.statistics.endTime // "0" | tonumber) - (.statistics.startTime // "0" | tonumber))),
        elapsed_sec:      (((.statistics.endTime // "0" | tonumber) - (.statistics.startTime // "0" | tonumber)) / 1000 | . * 10 | round / 10),
        final_exec_ms:    (.statistics.finalExecutionDurationMs // null | if . then tonumber else null end),

        # --- cache & partitions ---
        cache_hit:        (.statistics.query.cacheHit // false),
        statement_type:   (.statistics.query.statementType // "unknown"),
        partitions_processed: (.statistics.query.totalPartitionsProcessed // null | if . then tonumber else null end),

        # --- reservation ---
        reservation:      (.statistics.reservation_id // null),
        edition:          (.statistics.edition // null),

        # --- referenced tables (resolved through views to underlying SQLMesh tables) ---
        referenced_tables: [.statistics.query.referencedTables[]? | "\(.datasetId).\(.tableId)"],

        # --- sql_tables: clean dataset.table names (SQLMesh physical sqlmesh__ds.ds__tbl__hash -> ds.tbl) ---
        # feeds the net-new-table doc-debt hook + perf_digest by-table keying
        sql_tables: ([.statistics.query.referencedTables[]? |
            if (.datasetId | startswith("sqlmesh__"))
            then ((.tableId | split("__")) as $p |
                  # physical name = <schema>__<table>__<fingerprint>; the TABLE can itself contain "__"
                  # (agg__daily_sum_by_campaign), so it's everything between the first and last segment.
                  if   ($p | length) >= 3 then "\($p[0]).\($p[1:-1] | join("__"))"
                  elif ($p | length) == 2 then "\($p[0]).\($p[1])"
                  else "\(.datasetId).\(.tableId)" end)
            else "\(.datasetId).\(.tableId)" end] | unique),

        # --- query plan stages (compact: per-stage performance) ---
        query_plan: [.statistics.query.queryPlan[]? | {
            stage:              .name,
            status:             .status,
            records_read:       (.recordsRead // "0" | tonumber),
            records_written:    (.recordsWritten // "0" | tonumber),
            parallel_inputs:    (.parallelInputs // "0" | tonumber),
            completed_inputs:   (.completedParallelInputs // "0" | tonumber),
            slot_ms:            (.slotMs // "0" | tonumber),
            compute_mode:       (.computeMode // null),
            read_ms_max:        (.readMsMax // "0" | tonumber),
            compute_ms_max:     (.computeMsMax // "0" | tonumber),
            write_ms_max:       (.writeMsMax // "0" | tonumber),
            wait_ms_max:        (.waitMsMax // "0" | tonumber),
            shuffle_bytes:      (.shuffleOutputBytes // "0" | tonumber),
            shuffle_spill:      (.shuffleOutputBytesSpilled // "0" | tonumber)
        }],

        # --- timeline summary (full per-second timeline is huge; keep scalars only) ---
        timeline_samples: ([.statistics.query.timeline[]?] | length),
        timeline_peak_active_units: ([.statistics.query.timeline[]?.activeUnits // 0] | max // 0),

        # --- optimizations applied ---
        optimizations: [.statistics.query.queryInfo.optimizationDetails.optimizations[]? | to_entries[] | "\(.key)=\(.value)"],

        # --- search index usage ---
        index_usage: (.statistics.query.searchStatistics.indexUsageMode // null),
        index_unused_reasons: [.statistics.query.searchStatistics.indexUnusedReasons[]? | {
            table: "\(.baseTable.datasetId // "").\(.baseTable.tableId // "")",
            code: .code,
            message: .message
        }],

        # --- metadata cache ---
        metadata_cache: [.statistics.query.metadataCacheStatistics.tableMetadataCacheUsage[]? | {
            table: "\(.tableReference.datasetId // "").\(.tableReference.tableId // "")",
            staleness: .staleness
        }]
    }')

    # rotate at 40MB so the log never trips GitHub's size limits
    if [ -f "$LOG_FILE" ] && [ "$(stat -f%z "$LOG_FILE" 2>/dev/null || stat -c%s "$LOG_FILE")" -gt 41943040 ]; then
        mkdir -p "${WORKSPACE}/knowledge/archive"
        gzip -c "$LOG_FILE" > "${WORKSPACE}/knowledge/archive/bq_perf_log_$(date +%Y%m%d).jsonl.gz"
        : > "$LOG_FILE"
    fi
    echo "$LOG_ENTRY" >> "$LOG_FILE"

    # Print human-readable summary to stderr
    SUMMARY=$(echo "$LOG_ENTRY" | jq -r '
        "--- BQ Performance ---",
        "Bytes: \(.gb_processed) GB processed / \(.gb_billed) GB billed" + (if .billing_tier then " (tier \(.billing_tier))" else "" end),
        "Time: \(.slot_sec)s slot / \(.elapsed_sec)s wall" + (if .final_exec_ms then " / \(.final_exec_ms)ms exec" else "" end),
        "Cache: \(.cache_hit) | Partitions: \(.partitions_processed // "n/a") | Stages: \(.query_plan | length)",
        (if .reservation then "Reservation: \(.reservation)" else empty end),
        (if (.optimizations | length) > 0 then "Optimizations: \(.optimizations | join(", "))" else empty end),
        (if (.query_plan | length) > 0 then
            (.query_plan[] | "  \(.stage): \(.records_read) rows read → \(.records_written) written | \(.parallel_inputs) workers | slot \(.slot_ms)ms | shuffle \(.shuffle_bytes)B" + (if .shuffle_spill > 0 then " (SPILL: \(.shuffle_spill)B)" else "" end))
        else empty end),
        (if .index_usage == "UNUSED" then "Index: UNUSED — \(.index_unused_reasons | map(.table) | join(", "))" else empty end),
        "Logged to: knowledge/bq_perf_log.jsonl",
        "----------------------"
    ')
    >&2 echo ""
    >&2 echo "$SUMMARY"
else
    >&2 echo "[bq_run] Warning: could not fetch job stats for ${PROJECT_ID}:${JOB_ID}"
fi

exit $EXIT_CODE
