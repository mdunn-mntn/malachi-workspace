---
name: reference_databricks_stockout_flexible_nodes
description: "Databricks-on-GCP job clusters fail with GCP_INSUFFICIENT_CAPACITY / ZONE_RESOURCE_POOL_EXHAUSTED when the requested Intel node type stocks out; the fix is flexible node types (worker_node_type_flexibility / driver_node_type_flexibility) in the dbt job_cluster_config, plus how to tell a stockout from a quota error."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [GCP_INSUFFICIENT_CAPACITY, ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS, VM_MIN_COUNT_NOT_REACHED, databricks stockout, flexible node types, worker_node_type_flexibility, driver_node_type_flexibility, alternate_node_type_ids, job_cluster_config, mntn_matched_taxonomy_vector, taxonomy_vector, shopper_graph dbt, mntn-databricks project, us-central1-f, c2-standard-8, LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY, quota vs stockout, PREEMPTIBLE_WITH_FALLBACK_GCP, first_on_demand, INC-021, shopper_graph PR 300]
domain: [infra, repos]
lifecycle: active
last_verified: 2026-08-19
---
From on-call 2026-08-19, when `batch_post.taxonomy_vector` failed ~11 straight tries over 8 hours and blocked `keyword_ddp_reporting`.

**The error, and what it is NOT.** Full text from the Airflow task log (`mntn_match_incrementals_fetch` → `batch_post.taxonomy_vector`):
```
Cluster '<id>' was terminated. Reason: GCP_INSUFFICIENT_CAPACITY (CLIENT_ERROR).
databricks_error_message: The VM launch operation failed due to resource exhaustion. To reduce future
stockout errors, enable flexible node types ... [details] VM_MIN_COUNT_NOT_REACHED|
ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS: Requested minimum count of 1 VMs could not be created.|
The zone 'projects/mntn-databricks/zones/us-central1-f' does not have enough resources available
```
**This is a STOCKOUT, not a quota problem.** A quota problem reads `Quota '<NAME>' exceeded` (e.g. `LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY`). Both were happening org-wide the same morning on different projects, and the team conflated them; they need different fixes (stockout → node-type fallback, quota → devops raise). Brian McAdams: "Sometimes you get QUOTA_EXCEEDED errors when it is actually a stock-out issue," so read the detail string, not just the headline.

**Do NOT reach for zone or retry spacing first.** `zone_id` was already `auto` on this cluster, so the zone was not the constraint. Retry backoff (airflow-ti#1208) does not help either: the shortage lasted 8+ hours, longer than any retry window.

**The fix: flexible node types.** Databricks' own error text names it. Two API fields, settable per-model in the dbt `job_cluster_config`:
```yaml
worker_node_type_flexibility:
  alternate_node_type_ids: [n2d-standard-8, c2d-standard-8, n2-standard-8]
driver_node_type_flexibility:
  alternate_node_type_ids: [n2d-standard-4, c2d-standard-4, n2-standard-4]
```
Shipped for this model in [shopper_graph#300](https://github.com/SteelHouse/shopper_graph/pull/300) (merged 2026-08-19). There is ALSO a workspace-wide admin toggle, **Compute → "Enable auto flexible node types"**, which applies to every new classic compute resource without per-model edits.

**Cost answer (the objection this will draw).** Auto-generated fallbacks are constrained: *"Same vCPU count and memory as the preferred instance type (fallback instances must have between 100% and 110% of the preferred instance type's memory)"* — so it CANNOT silently fall back to a beefier node. Billing is *"based on the standard DBU rates for the instance types actually acquired"*, and instance-level discounts carry to matching types. The residual exposure is only the per-hour delta between same-size families, on runs where the primary type is unavailable. The docs do not claim fallbacks are never pricier, only never bigger. Docs: https://docs.databricks.com/gcp/en/compute/flexible-node-types

**Where the config lives.** `SteelHouse/shopper_graph` → `dbt/models/mntn_matched/taxonomy/mntn_matched_taxonomy_vector.yml`, under `config.job_cluster_config`. The airflow-ti `DbxDbtOperator` only launches a K8s pod running dbt; it holds NO cluster spec, so do not look for it in airflow-ti. Cluster runs in GCP project `mntn-databricks`. This model already uses `PREEMPTIBLE_WITH_FALLBACK_GCP` with `first_on_demand: 2`, so only the driver plus one worker are on-demand — the ones that stocked out.

Related: [[reference_databricks]], [[reference_shopper_graph_deploy]], [[reference_github_pr_no_clone]], [[reference_oncall_runbook]].
