---
name: reference_holdout_lift_lineage
description: "A separate, newer holdout-based (observational) lift lineage exists in gold.reporting + silver.enriched (lift__holdout_*, v_lift__results_by_month), redeployed 2026-08-02 — distinct from ghost-bid ITT, NOT a v2 of lift__ghost_bid_rollup"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [holdout lift, lift__holdout, lift__ghost_bid_rollup, v_lift__results_by_month, lift__conversions, v_lift__conversions, incrementality dashboard, holdout-based lift, observational lift, Matt Brorby, Nick Martin, reporting dataset, enriched dataset, is there a newer incrementality table, 43-day lookback, multiplier, audi-1215]
domain: [incrementality, data-catalog, experimentation]
lifecycle: active
last_verified: 2026-08-21
---
When asked "is there a NEWER incrementality report table that supersedes `dw-main-gold.reporting.lift__ghost_bid_rollup`?" — the answer (verified live 2026-08-03): **nothing renames or supersedes the ghost-bid rollup, but a SEPARATE, newer holdout-based lift lineage exists** and an incrementality-dashboard builder should choose between them deliberately.

**The newer lineage (redeployed 2026-08-02, ~1 wk after the ghost-bid views' 2026-07-26 deploy):**
- `dw-main-gold.reporting`: `lift__conversions`, `lift__holdout_advertisers`, `lift__holdout_conversions`, `lift__holdout_conversions_export`, `lift__holdout_results_step1`, `lift__holdout_results_step2`, `lift__results_by_month_raw`, `lift__stg_holdout_results_by_day`, `v_lift__conversions`, `v_lift__results_by_month`, `v_lift__results_by_month_review`.
- `dw-main-silver.enriched`: `lift__holdout_advertisers`, `lift__holdout_audiences`, `lift__holdout_campaign_groups`, `lift__holdout_households`, `lift__holdout_visits`.

This is **holdout-based (observational) lift**, a different methodology from the ghost-bid ITT that `lift__ghost_bid_rollup`/`_results`/`enriched.lift__ghost_bid_visits` implement. It is NOT a rename or v2 of the ghost-bid rollup. **Open question — which is canonical for a generalized incrementality dashboard: ask Matt Brorby** (he owns the ghost-bid lift pipeline; ownership of the holdout lineage unconfirmed).

**Naming fact:** all incrementality reporting in these two datasets is under the `lift__` prefix; **zero objects contain `incr`**. Every `lift__`/`ghost` object in `reporting`+`enriched` is a **VIEW**, not a materialized table — so a view's `lastModifiedTime` is DDL-author time, not data freshness (read freshness off the underlying `sqlmesh__` physical table).

**Verified live (AUDI-1215, 2026-08-21) — grain + arm semantics:**
- `v_lift__results_by_month` = **one row per MONTHLY run per advertiser×objective**. Latest run `begin_date` = 2026-07-01 as of 2026-08-21 (**NO August run**; physical table last modified 2026-08-11), so any post-window read caps at 07-31 until the next run lands.
- **Control arm is advertiser-level only**: `campaign_group_id` is NULL on every control row, never CG-attributed; treated rows ARE campaign_group-attributed. `multiplier` = `users_reached / control_users`. Window splits run in **America/New_York**.
- `v_lift__conversions` carries both arms with conversion timestamps and attributes treated conversions via a **43-day (3,715,200s) impression lookback**: a post-change window is contaminated by pre-change impressions (AUDI-1215: 27.8% of post conversions attached to pre-period impressions, 14.5% to the blackout). The carryover flatters POST, so a measured post decline is a LOWER bound; split windows on the impression date.
- **AUDI-1215 used this lineage as the POWERED conversion instrument** for the ElevenLabs pre/post read: fixed 10% MD5 membership means no entry-cohort depletion, structurally right for pre/post. Its "lift" is an attributed-style multiplier far above clean ITT levels; only the RATIO over time is meaningful, never the level.

See [[reference_ghost_bid_lift_register]] for the ghost-bid method + biases, and `knowledge/data_catalog.md` §"lift__ghost_bid_*" for the full per-table detail. Related: [[project_incrementality_experiment]].
