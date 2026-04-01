#!/bin/bash
# TI-809: Run training dataset query for each date pair
# Each query scans ~75-100GB — run sequentially, expect ~5-7 min each
# Total: ~7 queries × 7 min ≈ 50 min
#
# Usage: bash ti_809_run_all_queries.sh [PAIR_NUMBER]
#   No args = run all 7 pairs
#   PAIR_NUMBER = run just that pair (1-7)

set -euo pipefail

TICKET_DIR="/Users/malachi/Developer/work/mntn/workspace/tickets/ti_809_multiday_validation"
QUERY_TEMPLATE="$TICKET_DIR/queries/ti_809_training_dataset_parameterized.sql"
OUTPUT_DIR="$TICKET_DIR/outputs"

mkdir -p "$OUTPUT_DIR"

# Date pairs: feature_date label_date
PAIRS=(
  "2026-03-22 2026-03-23"
  "2026-03-24 2026-03-25"
  "2026-03-25 2026-03-26"
  "2026-03-27 2026-03-28"
  "2026-03-28 2026-03-29"
  "2026-03-29 2026-03-30"
  "2026-03-30 2026-03-31"
)

run_pair() {
  local idx=$1
  local feat_date=$(echo "${PAIRS[$idx]}" | cut -d' ' -f1)
  local label_date=$(echo "${PAIRS[$idx]}" | cut -d' ' -f2)
  local output_csv="$OUTPUT_DIR/ti_809_training_${feat_date}.csv"

  if [ -f "$output_csv" ]; then
    echo "SKIP: $output_csv already exists"
    return 0
  fi

  echo "========================================"
  echo "Running pair $((idx+1))/7: features=$feat_date labels=$label_date"
  echo "========================================"

  # Substitute dates into query
  local query
  query=$(cat "$QUERY_TEMPLATE" | sed "s/FEATURE_DATE/$feat_date/g" | sed "s/LABEL_DATE/$label_date/g")

  # Run via bq_run.sh for perf logging
  bash /Users/malachi/Developer/work/mntn/workspace/.claude/scripts/bq_run.sh \
    --ticket "TI-809" --label "training_${feat_date}" \
    --use_legacy_sql=false --format=csv --max_rows=999999 --project_id=dw-main-silver \
    "$query" > "$output_csv"

  local rows=$(wc -l < "$output_csv")
  echo "  Output: $output_csv ($rows rows)"
}

if [ $# -eq 1 ]; then
  # Run single pair
  pair_idx=$(($1 - 1))
  run_pair $pair_idx
else
  # Run all pairs
  for i in "${!PAIRS[@]}"; do
    run_pair "$i"
  done
fi

echo ""
echo "All queries complete. Run the validation script:"
echo "  python $TICKET_DIR/artifacts/ti_809_multiday_validation.py"
