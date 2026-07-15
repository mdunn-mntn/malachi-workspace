#!/usr/bin/env bash
# ============================================================================
# DDP quality-score runbook, STEP 14: measured ingestion footprint per vendor (GCS)
# NOT SQL — svs (site_visit_signal) is GCS parquet partitioned dt=/hh=/data_source_id=N/,
# so each vendor's actual byte footprint is directly measurable with gsutil.
# Feeds the workbook's WASTE sheet (GB/day, accumulated GB, storage-floor $) via
# outputs/run_<date>/q14_gcs_ingest_bytes.csv (columns: ds, gb_day, gb_accum).
#
# Method (2026-07-15 run):
#   gb_day   = avg of two full-day samples (2026-06-15, 2026-07-01)
#   gb_accum = accumulated footprint integrated from 1st-of-month day samples
#              (Sep 2025 .. Jun 2026, x days-in-month) + July-to-date at the 07-01
#              rate + augmentor (ds30) mid-May start correction (~19 days May).
#              svs has NO TTL — first partition is dt=2025-08-31, everything since
#              is still on disk.
# Storage floor $ = gb_accum x $0.02/GB-month (GCS standard list) x 12, computed in
# fill_template.py. This is a FLOOR on ingestion cost: Kafka cluster share (RT
# vendors 24/33/39/40), batch ingest DAG compute, and DS13/DS19 classifier compute
# are NOT included (need Data Eng numbers — Sean Yang's team).
#
# Usage: bash q14_gcs_ingest_bytes.sh DAY1 [DAY2 ...]
#   each DAY (YYYY-MM-DD) prints per-ds GB for that day; integrate + write the CSV
#   per the method above (integration arithmetic is in the header of the CSV commit).
# ============================================================================
set -euo pipefail
BUCKET="gs://mntn-data-archive-prod/signals/site_visit_signal"
for d in "$@"; do
  echo "== ${d} =="
  gsutil du "${BUCKET}/dt=${d}/hh=*/data_source_id=*/*" 2>/dev/null \
    | awk -F'data_source_id=' '{split($2,a,"/"); sum[a[1]]+=$1}
        END {for (k in sum) printf "ds%s %.2f GB\n", k, sum[k]/1e9}' \
    | sort -t s -k2 -n
done
