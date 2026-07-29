---
name: dataproc-cost-awareness
description: Always consider Dataproc Serverless costs before running batch jobs. Check with team before large backfills. Budget is monitored.
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 90ae114b-824a-4a09-8ae2-53026431ded6
doc_type: memory
keywords: [dataproc_cost_awareness, dataproc, cost, awareness, consider, serverless, costs, before]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-29
---
Be aware of Dataproc Serverless compute costs before running batch jobs. The TI team's Dataproc spend is monitored by Zach Schoenberger and flagged when it spikes.

**Why:** During TI-810 backfill (2026-04-03), we ran ~140 Dataproc Serverless jobs for 5 models × 30 days. Coincided with Brian McAdams' Vertex scoring pipeline ($200/hr × 7hrs = $1,400+/day spike). Zach flagged the spend increase in Slack. Our jobs were smaller but still add up.

**How to apply:**
- Before running large backfills (>10 jobs), estimate cost and check with Ryan/team
- Use minimum executor counts possible — don't use 200 executors when 20 will do
- aug_log_ip_hourly uses 50-200 executors × 360 jobs for 30-day backfill — get cost approval first
- Smaller models (conv_log_ip, guid_log_ip) use 5-20 executors — much cheaper
- Always mention Dataproc plans to Ryan so he can flag to Zach if needed
- model_run.py submits to Dataproc Serverless (charges per-second of compute)

**GCP compute cost is IAM-walled for the analyst (confirmed 2026-07-29):** `gcloud dataproc batches list` = PERMISSION_DENIED on dw-main-*, and there is NO reachable `gcp_billing_export` BQ dataset (the `dw-main-silver/gold.billing` datasets are ADVERTISER payments, not cloud cost). So you cannot self-serve Dataproc DCU $ — route the pull to data-platform / the DAG owner. Ready pattern (in `tickets/audi_1175_ds14_scoring_cost/queries/audi_1175_dataproc_billing_probe.sql`): sum Cloud Dataproc SKUs **net of credits** filtered to the batch labels (`goog-dataproc-batch-id LIKE '<prefix>-%'`); the `credits` field reveals a committed-use discount — a CUD / minimum-spend floor means cut DCU-seconds save $0, so "we cut compute → the bill drops" is UNPROVEN until that check passes. Config-math cost estimates (executor size × assumed runtime × assumed concurrency) are order-of-magnitude only; don't lead a cost RFD with them unqualified.
