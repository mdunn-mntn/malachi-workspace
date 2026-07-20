---
doc_type: ticket
title: "TI-933: MNTN Select Campaign Lift Analysis"
status: done
date: 2026-06-04
summary: "Does MNTN Select drive incremental lift? Pooled holdout lift across Select advertisers"
result: "MNTN Select drives +2.06 pp visit-rate lift (pooled, significant); it is incremental"
---

# TI-933: Lift analysis on MNTN Select campaigns

**Jira:** https://mntn.atlassian.net/browse/TI-933
**Status:** In Review
**Date Started:** 2026-05-06
**Date Completed:** 2026-05-07
**Assignee:** Malachi
**Parent Epic:** TI-916 (Incrementality findings — communication & followup) → BER-2250 (Incrementality Overhaul)
**Sprint:** TI Sprint 05/04/26 - 05/18/26 (id 6160)
**Story Points:** 3

---

## 1. Introduction

Follow-up to TI-917 (combined-Loom lift findings deck). Kale McNaney (Director — Identity & Attribution) reviewed the deck and asked whether MNTN Select campaigns were analyzed separately. They were not — TI-917's 4 segments (`all` / `prosp` / `stage1` / `rtg`) cut by `objective_id`, not by product. Hannah Burke (Sr. Director, Product MNTN Select) is sourcing top-10 historical Select CGIDs for context.

Hypothesis (Kale): Select shows higher incrementality than scoring-driven targeting because Select targeting is qualitatively different — geo + show only, no audience scoring, top-of-funnel awareness.

## 2. The Problem

TI-917 measured incrementality across all-product campaigns and segmented by funnel stage / retargeting. Result: retargeting +21pp, all +3pp, prospecting +0.78pp, stage1 ~0pp. Stakeholders cannot answer "is Select more incremental?" without a Select-only cut. `augmentor_log` retains only ~10 days, so historical campaigns from Hannah's top-10 list are mostly unqueryable. Decision: run on **all currently active Select advertisers**, gated by volume/MDE thresholds, not the historical top-10 list.

## 3. Plan of Action

1. **Phase 1 — Volume reconnaissance (gate).** Per-Select-advertiser volume over (a) TI-917 parity window 2026-04-20 → 2026-04-26 and (b) last 30d. Filter: `bronze.integrationprod.campaign_groups WHERE product_id = 2 AND deleted = FALSE AND is_test = FALSE`. Outputs: impressions, spend, unique IPs, conversions, revenue, prospecting_intent universe size, per advertiser. Cohort decision rule: include advertisers with monthly-equivalent spend ≥ $200k (visit-rate MDE floor from TI-917). If <5 clear the bar → pooled-only readout.
2. **Phase 2 — Method validation.** Verify Select bids appear in `augmentor_log` (no advertiser_id/campaign_id, but logs every augmentor-seen bid). Sanity check: intersect Select-served IPs with augmentor IPs on a single recent day. >50% → biddability filter applies; <10% → pivot to `bidder_bid_events` filtered on Select campaign IDs.
3. **Phase 3 — Lift query.** Clone `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_30adv_7day_v5_segments.sql`. Modifications:
   - Replace `campaign_dim` with JOIN to `campaign_groups WHERE product_id = 2`.
   - Replace hardcoded 30-advertiser list with `select_cohort` CTE seeded from Phase 1 survivors (lines 73, 89, 155, 190, 206).
   - Collapse 4 segments to 1 ("all Select") — Select is awareness-only; objective_id breakdown unlikely to apply.
   - Recompute per-advertiser win rates via `ti_837_compute_winrates_per_advertiser_segment.sql` template on the Select cohort.
   - Keep `prospecting_intent__v1` as the IP universe — scopes to Select advertisers also running PTV (only apples-to-apples population). Phase 1 must count how many Select advertisers have rows there.
4. **Phase 4 — Deck.** Standalone 6–8 slide RevealJS deck. Single bar chart, same y-axis as TI-917 Slide 12: `all (3pp) | prosp (0.78pp) | rtg (21pp) | select (X pp)` with 95% CIs. Reuse `ti_917_combined_loom/artifacts/build_combined_deck.py` chart template. Auto-share via `share_deck.sh` → githack URL to Kale + Hannah.

## 4. Investigation & Findings

### 4.1 Volume reconnaissance (Phase 1 — done 2026-05-06)

Query: `queries/ti_933_select_volume_by_advertiser.sql` (92 GB processed, 9.6s wall, ~5min slot — clustered on campaign_id, partition-pruned).
Output: `outputs/ti_933_select_volume_by_advertiser.{json,csv}` — 38 active Select advertisers in last 30 days.

| | |
|---|---|
| Active Select advertisers (last 30d, with cost_imp activity) | **38** |
| Active Select campaign_groups | 87 |
| Active Select campaigns | 192 |
| Total impressions, last 30d | ~27.0M |
| Total spend, last 30d | ~$549k |
| Highest single-advertiser monthly equiv. spend | Masterbuilt — **$106k/mo** |
| Advertisers clearing TI-917's $200k/mo MDE floor | **0** |
| Retargeting campaigns across all 38 | **0** (entirely prospecting) |

Top 10 by impression volume:

| Rank | AID | Advertiser | Imps 30d | Spend 30d | Monthly equiv. |
|----:|-----|------------|---------:|----------:|---------------:|
| 1 | 40598 | Masterbuilt | 6.29M | $104,851 | $106k |
| 2 | 41034 | Hugo Insurance | 4.68M | $79,721 | $81k |
| 3 | 31460 | Extra Space Storage | 2.38M | $37,546 | $38k |
| 4 | 45921 | Goldfish Swim School | 1.75M | $27,585 | $28k |
| 5 | 40807 | NinjaOne | 1.37M | $21,662 | $22k |
| 6 | 36743 | Lulus | 1.35M | $42,540 | $43k |
| 7 | 47228 | Pioneer Mini Split | 1.34M | $21,115 | $21k |
| 8 | 40601 | Kamado Joe | 1.24M | $19,580 | $20k |
| 9 | 35086 | TurboTenant | 1.01M | $15,868 | $16k |
| 10 | 59241 | Bauer | 0.86M | $41,390 | $42k |

**Key finding:** No single Select advertiser has the volume to be individually statistically powered for visit-rate lift detection. **Pooling is the only path to a defensible number.** Also: **zero retargeting campaigns** across all 38 — confirms Kale's framing that Select is purely awareness/prospecting. The TI-917 prosp/stage1/rtg segment split does not apply; we collapse to a single segment ("all Select").

### 4.2 Augmentor biddability sanity check (Phase 2 — done 2026-05-06)

Query: `queries/ti_933_augmentor_select_intersection.sql`.
Result: on 2026-05-04, **99.99%** of Select-served IPs (409,580 of 409,604) appear in `augmentor_log`. The 10% biddable-holdout filter applies to Select cleanly — no methodology pivot needed.

### 4.3 Cross-check vs Kale's "Select Live Campaigns" xlsx (done 2026-05-06)

Kale shared a 58-AID list (`artifacts/Select Live Campaigns.xlsx`, 151 CGID rows) of advertisers he was thinking about for the analysis. Cross-tabulation:

| | Count |
|---|---:|
| AIDs in Kale's xlsx | 58 |
| AIDs in our 30-day BQ cohort | 38 |
| Overlap (in both) | **38** |
| In our cohort but NOT in xlsx | **0** |
| In xlsx but NOT in our cohort | 20 |

The 20 xlsx-only advertisers either have zero recent impressions (campaigns not yet started or ended) or fall outside our 30-day window. **Notable: LifeMD shows 5M imps in the xlsx but isn't in our 30d data** — its flights ended before our window or the imps are aggregated over a longer historical period than the augmentor TTL allows us to query.

**Bottom line:** our 38-advertiser cohort is a complete superset of Kale's currently-actionable list. No advertisers are missing.

Output: `outputs/ti_933_xlsx_vs_our_cohort.csv` (58 rows).

### 4.4 Pooled Select lift (Phase 3 — DONE 2026-05-07)

**Compute path:** BigQuery hit the 6-hour interactive-query wall on three attempts (v1 partition-pruning bug, v2 timed out parallel, v3 timed out alone — all ~73% complete at S15/S0D Output stage with 9-34B records being shuffled). Ported to Spark on Databricks Jobs Compute. Victor (DE) revised my notebook with two key optimizations: (1) substitute the airflow-ti `aug_log_ip` feature-store output for raw `augmentor_log` (much smaller; same biddability filter), (2) materialize `ip_assigned` to GCS parquet sorted by bucket so the holdout/targeted/wr_bucket splits don't re-scan the prospecting universe 3×. Ran on a 400-core c3d-highmem cluster in ~3 hours.

**Result:** pooled across 23 active Select advertisers (the other 15 of 38 had no biddable-holdout / served-treatment overlap in the 7d window), 7-day window 2026-04-29 → 2026-05-05.

| Metric | Treated rate | Holdout rate | Lift | 95% CI | Sig |
|--------|------------:|-------------:|-----:|--------|-----|
| **Visit rate (guid)** | 2.6334% | 0.5782% | **+2.055 pp** | [+2.011, +2.100] | YES |
| Visit rate (clickpass) | 1.6650% | 0.0096% | +1.655 pp | [+1.634, +1.676] | (biased — needs MNTN imp) |
| **Conversion rate** | 0.1447% | 0.0048% | **+0.140 pp** | [+0.133, +0.147] | YES |

Treated arm: 1,513,123 IPs. Holdout arm: 167,257 IPs.

**Comparison to TI-917 baselines:**

| Segment | Visit-rate lift |
|---------|----------------:|
| All campaigns (TI-917) | +3.12 pp |
| Prospecting (TI-917) | +0.78 pp |
| Stage 1 only (TI-917) | -0.06 pp |
| Retargeting (TI-917) | +21.07 pp |
| **MNTN Select (this ticket)** | **+2.055 pp** |

Select sits between "all campaigns" and prospecting, well above prospecting-only. **Result Branch 1 from the deck framing: Select IS incremental.**

**Per-advertiser power:** 0 / 23 advertisers have CIs that exclude zero individually. Largest single Select advertiser by treated IPs is AID 41034 (Hugo Insurance) at 437,262 IPs — still under the volume needed for individual stat power. Pooled is the only viable headline number until ghost-bidder lands.

### 4.5 Deck

Standalone RevealJS deck at [`artifacts/ti_933_select_lift_deck.html`](artifacts/ti_933_select_lift_deck.html), 11 slides. Power Line: "MNTN Select drives +2.06 percentage point visit-rate lift." Three-act: question → numbers → action.

Shared via githack: https://gist.githack.com/mdunn-mntn/23125c48396b25f3459183af929de765/raw/ti_933_select_lift_deck.html

## 5. Solution

**Pooled MNTN Select visit-rate lift = +2.055 pp (95% CI [+2.011, +2.100])**, conversion-rate lift = +0.140 pp (95% CI [+0.133, +0.147]). Both significant. Select is incremental.

Action recommendations delivered in the deck's slide 9:
- **DO:** treat Select as confirmed-incremental; sell pooled (not per-advertiser) results until ghost-bidder lands
- **CONSIDER:** layer intent scoring on Select inventory as a possible easy-mode lift bump
- **UNLOCK:** ghost-bidder (TI-886) is the path to per-advertiser readouts

Deck URL: https://gist.githack.com/mdunn-mntn/23125c48396b25f3459183af929de765/raw/ti_933_select_lift_deck.html

## 6. Questions Answered

- **Q:** Is the per-(AID, IP) hash holdout assignment product-agnostic?
  **A:** Yes — hash is `MD5(advertiser_id || ip) mod 1000`, independent of product type. The mechanism applies to Select unchanged.
- **Q:** Does `augmentor_log` carry advertiser_id or campaign_id?
  **A:** No — fields are `ip, pmp[], mntn_segments[], domain, publisher_id, ...`. It logs every augmentor-seen bid regardless of product. Biddability filter works **if** Select bids flow through the augmentor at all (verified in Phase 2).

## 7. Data Documentation Updates

_To be populated as work progresses. Anticipated: Select augmentor_log coverage findings, Select prospecting_intent universe coverage._

## 8. Open Items / Follow-ups

- **v3 notebook ready** at [`artifacts/ti_933_select_lift_notebook_v3.py`](artifacts/ti_933_select_lift_notebook_v3.py) — fixes the per-impression attribution-window asymmetry (every IP gets a fixed `ATTRIBUTION_DAYS` lookahead from its first eligibility moment, vs the v2 fixed-calendar window). Ready to run for 14d if needed.
- **14d window deferred** — could run overnight on the same Jobs Compute config. Not on critical path; 7d already has tight CIs.
- **Per-advertiser readouts** — gated on ghost-bidder ([TI-886](https://mntn.atlassian.net/browse/TI-886)). Today we can run this analysis ad-hoc but it's a multi-hour Spark job; ghost-bidder makes per-customer numbers cheap and queryable from a logged ghost-bid table.
- **Methodology lesson captured** in [`knowledge/experimentation.md`](../../../knowledge/experimentation.md) under "TI-933 — Per-impression attribution window" so the next ATT lift iteration gets it right.

## Critical Risks (from approved plan)

1. Select bypasses augmentor → Phase 2 catches; fallback to `bidder_bid_events` biddability proxy.
2. Volume thin → Phase 1 catches; pooled-only readout if <5 advertisers clear $200k.
3. Geo confound → per-(AID,IP) holdout doesn't restrict denominator to in-DMA; bias toward zero (conservative). Document; don't fix in v1.
4. `prospecting_intent__v1` dry-run under-estimates ~30× (`knowledge/data_knowledge.md` line 1530). Test on 1-day window first.
5. Awareness conversion sparsity → if pooled Select revenue <$2M/mo, restrict deliverable to visit-rate lift only.
