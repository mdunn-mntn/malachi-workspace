#!/bin/bash
# TI-810: Sequential backfill — one model at a time to avoid Dataproc batch ID collisions
# Usage: bash ti_810_backfill.sh [MODEL_NAME]
#   No args = run all models
#   MODEL_NAME = run just that model (e.g., win_logs_ip)

set -euo pipefail

REPO_DIR="$HOME/Developer/work/mntn/airflow-ti"
DEV_BUCKET="gs://mntn-data-archive-dev/feature_store/feature_group_1_source"

# Daily models and their date ranges
DAILY_MODELS="win_logs_ip bae_ip cil_ip guid_log_ip conv_log_ip"
DAILY_START="2026-03-03"
DAILY_END="2026-04-02"

run_daily_model() {
  local model=$1
  local suffix="_feature_ti_810_bidstream_ip_features"
  local output_base="${DEV_BUCKET}/${model}${suffix}"

  echo ""
  echo "========================================"
  echo "Backfilling $model ($DAILY_START to $DAILY_END)"
  echo "========================================"

  current="$DAILY_START"
  while [[ "$current" < "$DAILY_END" ]] || [[ "$current" == "$DAILY_END" ]]; do
    # Check if already done
    if gsutil -q stat "${output_base}/dt=${current}/_SUCCESS" 2>/dev/null; then
      echo "  SKIP $current (already exists)"
    else
      echo "  RUN  $current ..."
      cd "$REPO_DIR"
      uv run python model_run.py "$model" -a "{\"run_date\": \"$current\"}" 2>&1 | grep -E "(Batch run|DONE|Error|Aborted)" || true
    fi
    current=$(date -j -v+1d -f "%Y-%m-%d" "$current" "+%Y-%m-%d")
  done
  echo "=== $model backfill complete ==="
}

run_hourly_model() {
  local model="aug_log_ip_hourly"
  local suffix="_feature_ti_810_bidstream_ip_features"
  local output_base="${DEV_BUCKET}/${model}${suffix}"

  echo ""
  echo "========================================"
  echo "Backfilling aug_log_ip_hourly ($DAILY_START to $DAILY_END)"
  echo "This will take a while (~720 runs)"
  echo "========================================"

  current="$DAILY_START"
  while [[ "$current" < "$DAILY_END" ]] || [[ "$current" == "$DAILY_END" ]]; do
    # Run for each pair of hours (the model processes 2 hours per run)
    # Run at hours 02, 04, 06, ..., 24 to cover all 24 hours
    for hour in 02 04 06 08 10 12 14 16 18 20 22 24; do
      # hour 24 = next day 00:00, which processes hours 22 and 23
      if [ "$hour" = "24" ]; then
        next_day=$(date -j -v+1d -f "%Y-%m-%d" "$current" "+%Y-%m-%d")
        run_date="${next_day} 00:00:00"
      else
        run_date="${current} ${hour}:00:00"
      fi
      echo "  RUN  $current hh=${hour} ..."
      cd "$REPO_DIR"
      uv run python model_run.py "$model" -a "{\"run_date\": \"$run_date\"}" 2>&1 | grep -E "(Batch run|DONE|Error|Aborted)" || true
    done
    current=$(date -j -v+1d -f "%Y-%m-%d" "$current" "+%Y-%m-%d")
  done
  echo "=== aug_log_ip_hourly backfill complete ==="
}

run_daily_rollup() {
  local model="aug_log_ip"
  local suffix="_feature_ti_810_bidstream_ip_features"
  local output_base="${DEV_BUCKET}/${model}${suffix}"

  echo ""
  echo "========================================"
  echo "Backfilling aug_log_ip daily rollup ($DAILY_START to $DAILY_END)"
  echo "========================================"

  current="$DAILY_START"
  while [[ "$current" < "$DAILY_END" ]] || [[ "$current" == "$DAILY_END" ]]; do
    if gsutil -q stat "${output_base}/dt=${current}/_SUCCESS" 2>/dev/null; then
      echo "  SKIP $current (already exists)"
    else
      echo "  RUN  $current ..."
      cd "$REPO_DIR"
      uv run python model_run.py "$model" -a "{\"run_date\": \"$current\"}" 2>&1 | grep -E "(Batch run|DONE|Error|Aborted)" || true
    fi
    current=$(date -j -v+1d -f "%Y-%m-%d" "$current" "+%Y-%m-%d")
  done
  echo "=== aug_log_ip backfill complete ==="
}

if [ $# -eq 1 ]; then
  if [ "$1" = "aug_log_ip_hourly" ]; then
    run_hourly_model
  elif [ "$1" = "aug_log_ip" ]; then
    run_daily_rollup
  else
    run_daily_model "$1"
  fi
else
  # Run all daily models first
  for model in $DAILY_MODELS; do
    run_daily_model "$model"
  done
  # Then hourly + daily rollup
  run_hourly_model
  run_daily_rollup
fi
