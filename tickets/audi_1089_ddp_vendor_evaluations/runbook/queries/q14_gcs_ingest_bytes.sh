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
#
# Claim: per-vendor ingest bytes and the accumulated no-TTL footprint are directly
# measurable from the GCS partition layout — no BQ scan needed.
#
# RECORDED SAMPLES (2026-07-15 run; GB/day per ds on the 1st of each month) + the
# integration that produced gb_accum — kept HERE because the CSV is gitignored:
#   month    ds23   ds24  ds25  ds26  ds28   ds30  ds33  ds36 ds39  ds40
#   2025-09  23.33  1.89  9.35  -     31.70  -     12.05 0.05 0.27  25.63
#   2025-10   3.67  1.90 12.66  5.82  55.65  -      6.54 0.21 0.67  19.48
#   2025-11  31.42  1.77  8.82  5.03  29.94  -      0.35 0.14 0.23  20.04
#   2025-12  55.76  3.06 11.15  6.35  32.10  -      8.51 0.21 0.57  19.78
#   2026-01  36.66  2.91  8.42  3.99  32.12  -      7.93 0.06 0.51  30.59
#   2026-02  40.69  2.98  7.12  4.60  35.11  -     11.45 0.09 0.17  31.93
#   2026-03  43.63  2.84  7.17  4.59 118.59  -     12.13 0.09 0.40  30.18
#   2026-04  49.37  3.20  7.09  4.81 107.51  -     11.68 0.12 0.39  30.28
#   2026-05  45.04  3.11  6.65  6.43 120.42  -      5.44 0.09 0.33  39.42
#   2026-06  50.94  2.90  3.37  8.40 126.34 81.45   7.08 0.11 0.33  28.63
#   2026-07  47.31  2.54  5.17  6.15 119.98 80.79   6.18 0.08 0.22  28.37  (07-01)
# gb_accum(ds) = Sigma(month_sample x days_in_month, Sep'25..Jun'26)
#                + ds30 mid-May start correction (81.45 x 19 days of May)
#                + July-to-date (07-01 sample x 15 days).
# gb_day(ds)   = avg of the 2026-06-15 and 2026-07-01 full-day samples.
# Results (GB): 28: 22,773 | 23: 12,228 | 40: 8,780 | 30: 5,203 | 33: 2,605 |
#               25: 2,563 | 26: 1,611 | 24: 843 | 39: 122 | 36: 37
#               (paid total 39.3 TB -> $9,440/yr floor at $0.02/GB-mo).
# Storage floor $ = gb_accum x $0.02/GB-month (GCS standard list) x 12, computed in
# fill_template.py. This is a FLOOR on ingestion cost: Kafka cluster share (RT
# vendors 24/33/39/40), batch ingest DAG compute, and DS13/DS19 classifier compute
# are NOT included (need Data Eng numbers — Sean Yang's team).
#
# Usage: bash q14_gcs_ingest_bytes.sh DAY1 [DAY2 ...]
#   each DAY (YYYY-MM-DD) prints per-ds GB for that day; integrate + write the CSV
#   per the method and recorded samples ABOVE (the CSV itself is gitignored plumbing).
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
