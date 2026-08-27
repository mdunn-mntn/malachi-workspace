---
name: reference_gcp_billing_export
description: "GCP billing export lives in BigQuery at mntn-billing-00; real blended Dataproc DCU rates and the executor-hour dollar conversion for the optimizer"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [billing export, mntn-billing-00, gcp_billing_export_v1, dataproc cost, DCU rate, blended rate, OPTIMIZER_USD_PER_EXEC_H, cost per executor hour, PAM, bq_read, SOP 054]
domain: [infra, bigquery, pricing]
lifecycle: active
last_verified: 2026-08-27
---
GCP billing exports to BigQuery in project `mntn-billing-00` (via Compass billing-analyst, 2026-08-27; Dustin was unaware it exists):
- Standard export (use this): `mntn-billing-00.gcp_cloud_billing_standard.gcp_billing_export_v1_01E62F_CDF2FC_8AC7A4`, day-partitioned, ~600 MB/day scan.
- Resource-level: `...gcp_cloud_billing_detailed.gcp_billing_export_resource_v1_...` (~6 GB/day; per-resource only).
- Use `cost` (post-discount), not `cost_at_list`; exclude the last 2 days (finalization lag).

Measured blended Dataproc Serverless rates, 30d window ending 2026-08-27: standard DCU $0.0498/DCU-h ($125.6k / 2.52M DCU-h), premium DCU $0.0739, shuffle storage $10.6k/30d separate. Dataproc-on-VM cost lands under Compute Engine plus a Dataproc licensing surcharge, no clean SKU.

Executor-hour conversion for `OPTIMIZER_USD_PER_EXEC_H`: measured DCU-h per exec-h is shape-dependent (INC-005 batch 5.44; site_network family 7.3-9.9). 5.44 x $0.0498 = **$0.27/exec-h conservative floor**; heavy-memory shapes reach ~$0.49. Set 0.27 to undersell rather than oversell; revisit with a per-DAG weighted ratio if leadership wants precision.

Human access to `mntn-billing-00`: NOT PAM-onboarded (SOP 054); no self-serve path today. Query via Compass billing-analyst, or open the PAM onboarding PR (`terragrunt/gcp/iam/pam/mntn-billing-00`, `bq_read` catalog role, approver devops-squad).
