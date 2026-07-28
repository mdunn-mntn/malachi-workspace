---
doc_type: ticket
title: "AUDI-1175: Quantify $ cost of scoring non-addressable MM/vertical IPs (DS14 gate)"
status: backlog
date: 2026-07-28
summary: "Cost the MM/vertical IPs DS14's 8-day gate makes non-biddable; go/no-go on gating scoring"
result: ""
question: "How much compute $ is spent scoring MM/vertical IPs DS14 makes non-biddable, and can we gate it with no coverage loss?"
framing_state: locked
---

# AUDI-1175: Quantify $ cost of scoring non-addressable MM/vertical IPs (DS14 gate)

**Jira:** https://mntn.atlassian.net/browse/AUDI-1175
**Status:** Backlog
**Date Started:** 2026-07-28
**Assignee:** Malachi
**Blocks:** [AUDI-1176](https://mntn.atlassian.net/browse/AUDI-1176) (gate scoring input to DS14-addressable set)

---

## 0. Framing (locked 2026-07-28)

- **Question:** How much daily/annual Dataproc compute (and IPDSC storage) is spent scoring the MM/vertical IP universe that DS14's ~8-day availability gate makes non-biddable, and can scoring be gated to the addressable set with zero biddable-coverage loss?
- **Goal (why):** Cost reduction (Kale focus area). The answer decides whether to fund AUDI-1176 (gate the scoring input). North-star tie: Tier 3 infra/cost that also de-risks MembershipDB resilience.
- **Objective (done-when):** A dollar figure (daily + annual) for the wasted scoring compute/storage, a list of any non-bidding consumers of the full scored universe, and a binary go/no-go on AUDI-1176.
- **Approach:** (1) sizing — DONE (HLL distinct-IP on `ipdsc__v1`). (2) Dataproc cost — estimate from the `audience_intent` cluster config x runtime. (3) consumer audit — grep every reader of the scored outputs (intent_score tables, GCS prospecting-scores, IPDSC DS13/19/46) beyond the bidder. (4) validate the no-loss argument with the bidder owners.
- **What would change the answer:** if a non-bidding consumer (AUD-5221 deciles / LiftLab / lookalike seeds / Fangorn training) legitimately needs the full universe, gate only the serving-bound path (savings shrink toward storage-only). If the Dataproc cost of the scoring stage is trivial (<~$1k/mo), AUDI-1176 is not worth it.

## 1. Introduction

Origin: user hypothesis (2026-07-28) that DS14 ("MNTN Global Data") restricts bidding to recently-seen IPs, so MNTN may be scoring/storing far more IPs on MM than it can ever bid on. Investigated DS14's mechanics, effective window, the scoring pipeline's input universe, and sized the gap.

## 2. The Problem

MM/vertical intent scoring materializes scores for the full ~31-day IP universe, but DS14 (ANDed onto ~every audience expression at serving time) makes only the recently-seen subset biddable. The scored-but-non-biddable remainder is recomputed daily = wasted Dataproc compute + IPDSC storage.

## 3. Plan of Action

1. Confirm what DS14 is and its effective serving window. **DONE.**
2. Confirm whether MM/vertical scoring is gated to the addressable set. **DONE (it is not).**
3. Size the scored-vs-addressable gap. **DONE.**
4. Estimate the Dataproc $ of the scoring stage. **IN PROGRESS.**
5. Audit non-bidding consumers of the full scored universe. **TODO.**
6. Validate the no-loss gating argument with Ryan Kleck / Sean Yang / Zach Schoenberger. **TODO.**
7. Go/no-go recommendation on AUDI-1176.

## 4. Investigation & Findings

Three parallel code investigations (airflow-ti, sqlmesh, membership-db) + one BQ sizing. Full write-back in `knowledge/data_knowledge.md` "DS14-opt spike" block and `knowledge/ds_catalog.md`.

**Fact 1 — DS14 origin (not a pre-MM recency filter).** Born 2025-08-13 (Sean Yang prototype), productionized 2025-12-11 under AUDI-369. POST-MM. Positive addressability set from `augmentor_log` (1d) ∪ `bidder_auction_events` (1d) ∪ `guid_log` (4d); `category_id`=1 (first-party) / `exchange_id` (auction-only) / 1000 (`.0` quarantine, unread downstream).

**Fact 2 — effective window = ~8 days, not 1-4.** membership-db applies per-source TTL `data_source_ttls['14']=8` via per-IP epoch decay (not archive overwrite). Matchable 8 days past last build inclusion. (30d if DS14 is Portal audience-type registered; 8 is the code-intended path.)

**Fact 3 — DS14 IS materialized in IPDSC** (`ipdsc__v1`, `data_source_id=14`, ~149M IPs/day). Corrects the prior `data_knowledge.md` "zero ipdsc rows / computed at bid time" note.

**Fact 4 — scoring is UNGATED by DS14.** `spark/audience_intent/vertical_high.py` / `vertical_mid.py` score every (ip, vertical_id) over a 31-day DS13 IPDSC window; `vertical_mid` BUCKET 2 deliberately scores IPs with no recent activity. DS14 is a separate DAG, ANDed only at bid time.

**Sizing (HLL distinct-IP on `ipdsc__v1`, ~1.5% err; query in `queries/`):**

| Source | Scored (31d) | Addressable ∩ (DS14 8d) | Not addressable | % waste |
|---|---:|---:|---:|---:|
| DS19 (MM Core) | 499.4M | 156.6M | 342.8M | **69%** |
| DS13 (verticals / PP-v1) | 269.8M | 164.7M | 105.1M | **39%** |

DS14 8d set = 259M IPs; 1d = 149M. At the strict 1-day window (original thesis) DS13 waste = 56%. Addressable count uses ALL DS14 categories; the standard `cats:[1]` gate is a subset, so true waste is >= these figures (conservative).

### Consumer audit — is the gate safe? (2026-07-28)

Swept airflow-ti, sqlmesh, olympus, membership-db, mode-assets, airflow-camperbid, DDM/Redshift, targeting-infra-ml. **Verdict: safe for every serving/bidding path, but NOT safe to apply globally without scoping** — one live control-plane consumer reads the full scored universe.

**SAFE-TO-GATE (serving-bound, already addressable):** camperbid `intent_score` DAG (scores→Aerospike), `intent_score_household_map`, membership-db, `tpa_mntn_id_export`, the LIVE holdout-membership model (`tpa_membership_updates_log_insegments.sql`), Mode reporting (reads delivered CIL scores, not the raw set), Olympus (catalog-only, no query consumer).

**SEPARATE PIPELINE (unaffected):** Fangorn training/inference read a monthly **1%-sampled feature store** (`run_target_engineering.py` / `run_inference.py`), NOT `prospecting_intent`. Caveat: do NOT gate feature-store generation itself (needs full-population negatives).

**MUST-KEEP-FULL / VERIFY — the blocker:** the DDM/Redshift **automated HHST threshold recommender** (`ETL-DCO-Automated-Threshold-Adjustment.py` → `ddm.hhst_bucket_collections` → `hhst_generate_recommendation` → writes `dso.update_campaign_household_score_presets`). A LIVE closed loop that SETS the production HHST gate. It reads the FULL scored `prospecting_intent`/`advertiser_intent` as `ip_population` per score bucket; sibling sizing procs (`cache_hhst_population_filters`/`win_conditions`/`augmentor_volume`) use it as the `pct_available/pct_visible` denominator. Two sensitivities under an 8-day gate: (a) guardrails (`scored_population<100→6666`, `>1M & remaining=0→6666`) would trip the high-intent-default path for small-addressable campaigns (under-delivery risk); (b) winnable uses a 30-day win lookback vs DS14's ~8-day, dropping IPs winnable in the 8–30-day window and biasing recommended thresholds low. Scoped to `ddm.test_hhst_campaigns` (pilot) so blast radius is bounded. **Owner: Devon Rogers.**

**UNKNOWN (confirm with owners, not resolvable from code):** AUD-5221 population deciles (no implementation in any repo; if it splits the full US IP population it needs the full set — Alex/Zach) and any LiftLab/DS52 full-scored-universe incrementality export (none found; DS52 is an IPDSC *input* only, Liftlab is outbound Orca sync — #dev-incremental-lift owner).

**Design implication:** the prize survives, but AUDI-1176 must EITHER (1) gate only the serving-bound output while keeping a cheap full-universe population COUNT for the HHST recommender + sizing (aggregate over full, expensive per-IP scoring only on addressable), OR (2) validate with Devon that the guardrail + 8-vs-30-day effects are immaterial. Clear AUD-5221 + LiftLab with their owners before any global input-gate.

## 5. Solution / Recommendation (draft)

**Lever (→ AUDI-1176):** intersect the 31-day DS13/DS19 scoring input with the current DS14 (8d) set before `vertical_high/mid` + `populate_data_source`. Est. daily scoring-compute cut ~39% (verticals) / ~69% (MM Core). Zero biddable-coverage loss: any IP that becomes addressable is scored that day from still-retained DS13 history; intra-day-new IPs handled by RTC.

**Not the lever:** cutting raw-visit / DS13 retention. 30d = the model's scoring lookback, not slack.

### Cost estimate (Dataproc Serverless; order-of-magnitude, 2026-07-28)

Pipeline runs on **Dataproc Serverless** (DCU-billed, autoscaled per job), not provisioned clusters. Whole `audience_intent` scoring DAG + `populate_data_source` ≈ **$1,300/day ≈ $39k/mo** (point estimate; band ~$13k-$59k/mo). Cost concentrates in `prospecting_keywords` ($396/day, 34% — the DS19 keyword job over ~33.8B rows), `prospecting_join` + `advertiser_join` (~22% each). The vertical high/mid jobs are only ~$100/day combined.

**The prize is where the DS19 volume lands.** DS19 MM Core is consumed by `prospecting_keywords`, NOT the vertical jobs. Mapping input-cut levers to the jobs they drive:

| Lever | Job(s) | Savings |
|---|---|---|
| DS13 −39% | vertical high/mid + ½ populate | ~$1.3k/mo |
| **DS19 −69%** | **`prospecting_keywords` + ½ populate** | **~$9.6k/mo** (the real prize) |
| Both | — | **~$11k/mo (~$130k/yr)** |

Confidence: LOW / order-of-magnitude. Executor sizes/tiers/ceilings are exact from source; runtime + avg concurrent executors are assumed (jobs rarely sit at ceiling). Assumes the 69% distinct-IP cut shrinks `prospecting_keywords`' exploded keyword×IP rows ~linearly. **Firm up to a point estimate** via the GCP Billing BQ export (Dataproc Serverless SKUs, grouped by batch labels `team=ti`, `application=tpa-export`) or `gcloud dataproc batches describe` (`runtimeInfo.approximateUsage.milliDcuSeconds` per `aud-int-*` batch).

## 6. Questions Answered

- **Q:** Is DS14 a pre-MM recency filter? **A:** No. Born Aug 2025, prod Dec 2025 (AUDI-369), post-MM.
- **Q:** Is the effective bid-eligibility window 24h? **A:** No, ~8 days (serving TTL, per-IP decay).
- **Q:** Is scoring already gated to addressable IPs? **A:** No. Full 31-day universe scored.
- **Q:** How big is the waste? **A:** 39% (DS13) / 69% (DS19) of scored IPs are non-biddable at 8d.

## 7. Data Documentation Updates

- `knowledge/data_knowledge.md` — added "DS14-opt spike" block; corrected the "not materialized in IPDSC" line.
- `knowledge/ds_catalog.md` — DS14 resolved bullet updated (8d TTL, materialized, ungated scoring, origin).

## 8. Open Items / Follow-ups

- **$ figure — DONE (order-of-magnitude):** gate optimization saves ~$1.3k/mo (DS13) to ~$11k/mo (~$130k/yr, if DS19 cut is applied to `prospecting_keywords` where the volume lands). Whole scoring DAG ≈ $39k/mo. Firm up to a point estimate via GCP Billing BQ export / `gcloud dataproc batches describe` (DCU-seconds per batch).
- **Consumer audit — DONE (see §4).** Safe for all serving paths + Fangorn (separate feature store). Blocker = DDM HHST recommender (Devon Rogers) reads full scored set to SET the HHST gate. 2 unknowns to clear with owners: AUD-5221 deciles (Alex/Zach), LiftLab full-scored export (#dev-incremental-lift).
- **Owner validation** — confirm no-loss with Ryan Kleck / Sean Yang / Zach Schoenberger before AUDI-1176.
- **Exact intersection** — optional exact DISTINCT+JOIN to confirm the HLL estimate.
- **`cats:[1]` refinement** — recompute addressable set restricted to category 1 (unnest `data_source_category_ids`) for the true gate.
