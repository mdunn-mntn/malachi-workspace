---
name: reference_databricks_stockout_flexible_nodes
description: "Databricks-on-GCP job clusters fail with GCP_INSUFFICIENT_CAPACITY / ZONE_RESOURCE_POOL_EXHAUSTED when the requested Intel node type stocks out; the fix is flexible node types (worker_node_type_flexibility / driver_node_type_flexibility) in the dbt job_cluster_config, plus how to tell a stockout from a quota error."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [list-node-types, node type not supported, INVALID_PARAMETER_VALUE node type, local_disks parity, HYPERDISK_BALANCED, all ARM or all x86, alternate node type rules, shopper_graph PR 301, shopper_graph PR 302, enable auto flexible node types toggle, GCP_INSUFFICIENT_CAPACITY, ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS, VM_MIN_COUNT_NOT_REACHED, databricks stockout, flexible node types, worker_node_type_flexibility, driver_node_type_flexibility, alternate_node_type_ids, job_cluster_config, mntn_matched_taxonomy_vector, taxonomy_vector, shopper_graph dbt, mntn-databricks project, us-central1-f, c2-standard-8, LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY, quota vs stockout, PREEMPTIBLE_WITH_FALLBACK_GCP, first_on_demand, INC-021, shopper_graph PR 300]
domain: [infra, repos]
lifecycle: active
last_verified: 2026-08-20
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
Shipped for this model across THREE PRs, because the first two lists were rejected: [#300](https://github.com/SteelHouse/shopper_graph/pull/300) (the idea), [#301](https://github.com/SteelHouse/shopper_graph/pull/301) (`c2d-*` is not in this workspace's catalog at all), [#302](https://github.com/SteelHouse/shopper_graph/pull/302) (the local-SSD rule below). Final: worker `n2d-standard-8`, `n2-standard-8`; driver `c3d-standard-4`, `n4d-standard-4`.

**⚠ Merging is not shipping.** The dbt project is baked into `steelhousedev/mntn_matched_data_pipeline:gcp-prod`, and *Deploy dbt to Dockerhub* is `workflow_dispatch`-only. #300 sat inert for hours because the image had last been built 2026-06-17. Every dbt model change needs a manual deploy run on `main`. See [[reference_shopper_graph_deploy]].

**⚠ The validation rules Databricks enforces on the alternate list** (verbatim from the rejection): all standard and alternate types must be **all x86 or all ARM**, non-GPU, **same core count**, alternate memory **within 90-100%** of the preferred, same **HYPERDISK_BALANCED** support, and **compatible local SSD counts**. The last one is the trap: `c2-standard-8` carries `local_disks: 2` / 750 GB, so `c3d-standard-8` and `n4d-standard-8` (zero local disks) are rejected even though vCPU and memory match exactly.

**The authoritative node list is an API call, not the docs:**
```bash
databricks clusters list-node-types -p malachi@mountain.com -o json   # 169 entries
# compare num_cores, memory_mb, and node_instance_type.local_disks
```
The Job Compute policy (`001D160AE4052091`) leaves `node_type_id` `unlimited`, so the GCP catalog is the only gate. A workspace-wide OAuth login is `databricks auth login --profile malachi@mountain.com`; the refresh token expires and the CLI says so plainly.

**The fallback list has never actually fired.** The run that finally succeeded (try 22, 15.9 min) got the *preferred* `c2-standard-8` because capacity returned on its own. Confirm with `databricks jobs get-run <run_id>` and read `node_type_id`. Do not credit the fallback for a recovery you have not verified. There is ALSO a workspace-wide admin toggle, **Compute → "Enable auto flexible node types"**, which applies to every new classic compute resource without per-model edits. **Turned ON 2026-08-19** after Alyson Lefkowitz accepted marginal cost for reliability and Brian McAdams agreed conditional on cost monitoring. It affects NEW compute only; existing all-purpose compute is unchanged.

**Cost answer (the objection this will draw).** Auto-generated fallbacks are constrained: *"Same vCPU count and memory as the preferred instance type (fallback instances must have between 100% and 110% of the preferred instance type's memory)"* — so it CANNOT silently fall back to a beefier node. Billing is *"based on the standard DBU rates for the instance types actually acquired"*, and instance-level discounts carry to matching types. The residual exposure is only the per-hour delta between same-size families, on runs where the primary type is unavailable. The docs do not claim fallbacks are never pricier, only never bigger. **Three real cost risks the docs do NOT cover** (Brian McAdams, 2026-08-19): (a) same-vCPU does not mean same family — an auto fallback from compute-optimized to memory-optimized can multiply the VM hourly rate; (b) **DBU multipliers differ by instance type**, so platform spend can rise even at equal vCPU, and it surfaces only on the invoice; (c) a nominally cheaper node with slower local disk lengthens the job, burning more DBU-hours than it saves. **An EXPLICIT `alternate_node_type_ids` list defuses (a) and (c)** — pin same-class compute nodes, as shopper_graph#300 does — but (b) applies either way, so check DBU multipliers for the fallback types before enabling anything. Docs: https://docs.databricks.com/gcp/en/compute/flexible-node-types

**Where the config lives.** `SteelHouse/shopper_graph` → `dbt/models/mntn_matched/taxonomy/mntn_matched_taxonomy_vector.yml`, under `config.job_cluster_config`. The airflow-ti `DbxDbtOperator` only launches a K8s pod running dbt; it holds NO cluster spec, so do not look for it in airflow-ti. Cluster runs in GCP project `mntn-databricks`. This model already uses `PREEMPTIBLE_WITH_FALLBACK_GCP` with `first_on_demand: 2`, so only the driver plus one worker are on-demand — the ones that stocked out.

Related: [[reference_databricks]], [[reference_shopper_graph_deploy]], [[reference_github_pr_no_clone]], [[reference_oncall_runbook]].
