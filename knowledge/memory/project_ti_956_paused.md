---
name: ti-956-paused-2026-06-10
description: "TI-956 (interest-segment quality scoring schedule, airflow-ti) paused 2026-06-10 — AUD bandwidth conflict on downstream command center work. PR"
metadata: 
  node_type: memory
  type: project
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [ti_956_paused, interest-segment scoring, segment_quality_scoring_weekly, airflow-ti, PR #1073, paused, command center, DS35, astro ui]
domain: [project, audience-scoring, repos]
lifecycle: active
last_verified: 2026-06-10
---
TI-956 (interest-segment quality scoring schedule, airflow-ti) paused 2026-06-10.

**Why:** Stakeholder direction — AUD lacks bandwidth for the downstream command center work this feeds, so the recurring score doesn't have a consumer right now.

**How to apply:** Don't restart dev validation, don't merge PR #1073, don't unpause the DAG without re-checking AUD bandwidth and getting fresh stakeholder sign-off. Status comment is on the ticket.

**State at pause:**
- Model code: PR #1073 **merged 2026-06-10** (Victor approved without successful dev run, given the pause + cost). All review comments addressed. Cluster sizing matches Alex's Databricks (64g × 16-core × 10–50 executors); broadcast fix in place (`broadcast_weights=False` + `MEMORY_AND_DISK`).
- DAG `segment_quality_scoring_weekly` (weekly Sunday 06:00 UTC) is in main; **paused in Astro UI** by Alyson. Pause state lives in Airflow metadata DB and persists across deploys.
- No successful end-to-end dev run yet — every dev iteration failed before completion. Operational profile is estimated: ~2h wall, ~$107/run (1639.66 DCU-h × $0.060 + 212.6 GB-mo × $0.040, us-central1 standard), ~291k rows/run output (one per DS35 segment).

**To resume cleanly:**
1. Confirm AUD has bandwidth for the downstream consumer.
2. Re-request PAM `vm-ssh` entitlement on `mntn-prj-dev-00` (4h grant, auto-revokes).
3. Do one clean dev validation at then-current main head (~$100).
4. Unpause the DAG in Astro UI.

**Safety nets:**
- DAG file does NOT set `is_paused_upon_creation=True`. If the DAG file is ever deleted+re-added, or `dag_id` is renamed, it would import as unpaused by default. Consider a 1-line follow-up PR to add the flag if extra insurance is wanted during the pause window.

**Related:** [[reference_airflow_ti]], [[feedback_airflow_prod_safety]], [[reference_airflow_ti_cross_repo_deps]]
