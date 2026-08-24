---
name: reference_local_vendor_data_analysis
description: How to analyze an s3-delivered vendor data sample locally (duckdb load-once pattern, gitignore trap for extensionless parquet, BQ-side join via hash-sampled pulls)
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [vendor sample, s3 vendor data, duckdb, parquet, extensionless, gitignore, Trino export, load once, FARM_FINGERPRINT sample, local join, proxima, AUDI-1074, BigQuery Omni]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-08-24
---

s3-delivered vendor samples cannot be read by BQ external tables (no Omni; GCS copy forbidden under the read-only rule), so they are analyzed locally. Established on AUDI-1074 (Proxima, 13GB / 175 parquet files):

1. **Load once into a `.duckdb` file** (`analysis/00_load.py` pattern), never `CREATE VIEW` over the parquet tree — views re-scan the full tree per query; the AUDI-1074 QC battery sat 40+ min unfinished on views and completed in minutes against native tables.
2. **Extensionless Trino/Presto export files dodge the root `.gitignore` `*.parquet` blanket.** Add an explicit `tickets/<ticket>/outputs/` ignore line BEFORE the first `aws s3 sync`, or `git add` will stage gigabytes of vendor PII (caught at commit on AUDI-1074).
3. AWS creds from vendor email: export env-vars inline per command, never write to disk or commit.
4. MNTN-side joins under the BQ read-only rule: pull `MOD(ABS(FARM_FINGERPRINT(ip)),100)<k` samples of the denominator (DS14 gate via `ipdsc__v1`, served IPs via CIL) with `bq_run.sh`, join locally; overlap share = matches x (100/k) / N, Wilson CI. Watch `--max_rows` truncation (a capped pull is a biased sample — rerun with headroom).
5. In-flight `aws s3 sync` temp files carry an 8-char hex suffix and fail parquet reads — filter them or wait for sync completion.

Heavy fallback stays the TI-837 Databricks cluster ([[reference_bq_location_reservation]] for query routing).
