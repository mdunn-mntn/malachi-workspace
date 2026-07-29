---
doc_type: ticket
title: "AUDI-1172: MNTN Select vs non-Select prospecting incrementality (ghost-bid)"
status: done
date: 2026-07-28
framing_state: "skip: ad-hoc incrementality pull for Kirsa's Select Performance Analysis; scope fixed by her 93-AID list, delivered in-session"
question: "For advertisers running both, is MNTN Select prospecting more incremental than non-Select (PTV) prospecting?"
summary: "Select vs non-Select ghost-bid visit lift for Kirsa's 93 AIDs, advertisers running both, from lift__ghost_bid_rollup"
result: "Select prospecting drives ~5x the relative visit lift of non-Select: +22.0% vs +4.3% pooled (both sig); Select wins in 27/35 advertisers"
keywords: [mntn select, non-select, ptv, product_id, incrementality, ghost-bid, holdout, visit lift, lift__ghost_bid_rollup, prospecting, ivw, kirsa, select performance analysis, audi-1172, ber-2250, ti-933]
---

## TL;DR

**Q:** For advertisers running both, is MNTN Select prospecting more incremental than non-Select (PTV) prospecting?

**A:** Yes, markedly. Pooled across the **35 advertisers running both** (ghost-bid holdout, 2026-06-22 to 07-27, prospecting only), **Select shows +22.0% relative visit lift vs non-Select's +4.3%** — roughly **5x**. Both significant, with near-identical holdout baselines (Select 1.19% vs non-Select 1.21%), so it's a clean comparison: Select ~doubles the visit rate over its holdout; non-Select barely moves it. The gap is not a pooling artifact — Select beats non-Select in **27 of 35** advertisers (median edge +46pp of relative lift), and the largest advertisers all confirm it (Zazzle +95% vs +5%, Hugo Insurance +84% vs +2%, Planned Parenthood +142% vs +11%, Upneeq +79% vs +25%).

**How:** Gold `reporting.lift__ghost_bid_rollup` (campaign_group grain, time-boxed / entry-cohort-anchored upstream, AUDI-1148) filtered to Kirsa's 93 AIDs, joined to `campaign_groups.product_id` (2=Select, 1=PTV), clean-gated (`se>0 AND NOT low_coverage`), pooled cross-campaign-group to advertiser x product by **inverse-variance weights** (weight = 1/SE^2 — the captured cross-campaign rule; a naive count pool produces Simpson artifacts). Rel lift = IVW abs lift / holdout visit rate.

**Coverage:** of the 93 AIDs, clean-gated lift data exists for **74** — 35 run both, 8 Select-only, 31 non-Select-only. 19 had only low-coverage campaign groups. All rows are objective_id=1 (prospecting) — the ghost-bid holdout only exists on the prospecting pool.

**Caveats:** numbers are ghost-bid **ITT at bid grain** (win-rate-diluted, ~10% win) so compare **relative** lift, not the absolute pp; window is 2026-06-22 onward only (no backfill); source is the **Beeswax bidder leg** (MNTN Rust bidder not folded in yet), which is why Select coverage is a subset of all live Select advertisers.

**Deliverable:** [`artifacts/AUDI-1172 Select vs Non-Select Incrementality.xlsx`](artifacts/AUDI-1172%20Select%20vs%20Non-Select%20Incrementality.xlsx) (also in Drive: `My Drive/Tickets/AUDI-1172/`). 7 sheets: Overview, Headline (pooled), By advertiser (both, side-by-side), All by product, Read me, Query, Method & caveats.

**Tables:** `dw-main-gold.reporting.lift__ghost_bid_rollup`, `dw-main-silver.public.campaign_groups`, `dw-main-silver.public.advertisers`

**Learned:**
- Select prospecting is substantially more incremental than non-Select prospecting for advertisers running both: +22.0% vs +4.3% relative visit lift pooled, 27/35 advertisers favor Select.
- This is the per-advertiser Select-vs-non-Select cut that TI-933 said was "gated on ghost-bidder" — now unblocked because the ghost-bid lift views are queryable.
- Ghost-bid lift data floor is 2026-06-22 (Beeswax leg); MNTN Rust-bidder leg not yet folded into `lift__ghost_bid_*`.
- `lift__ghost_bid_rollup` at level=campaign_group is the sanctioned per-CG estimate; `low_coverage` is the coverage gate; aggregate CGs to advertiser x product with IVW.

**Reuse when:** anyone asks whether Select out-performs non-Select on incrementality; designing a product_id-split lift cut; needing the ghost-bid data window/coverage for a cohort.

---

# AUDI-1172: MNTN Select vs non-Select prospecting incrementality

**Jira:** https://mntn.atlassian.net/browse/AUDI-1172
**Status:** Done
**Date:** 2026-07-28
**Assignee:** Malachi
**Origin:** Slack request from Kirsa (MNTN Select Performance Analysis) — incrementality for Select vs non-Select campaigns for advertisers running both. She supplied a 93-AID list; Malachi pulls all their prospecting Select + non-Select ghost-bid lift.

## 1. Question / Objective

For advertisers running both products, is MNTN Select (product_id=2) prospecting more incremental than non-Select / PTV (product_id=1) prospecting? Deliver a pooled and per-advertiser Select-vs-non-Select visit-lift comparison from the ghost-bid holdout.

## 2. Method

1. Cohort = Kirsa's 93 AIDs. Source = `dw-main-gold.reporting.lift__ghost_bid_rollup`, `level='campaign_group'`.
2. Join `entity_id` (campaign_group_id) to `dw-main-silver.public.campaign_groups.product_id` → Select (2) vs non-Select (1). All matched rows are `objective_id=1` (prospecting); the ghost-bid holdout exists only on the prospecting pool, so no separate prospecting filter is needed.
3. Clean gate: `se>0 AND NOT low_coverage`. (Upstream the rollup is time-boxed and applies the entry-cohort anchor + drops the left-censored first window day — AUDI-1148.)
4. Pool campaign groups → advertiser x product by **inverse-variance weights**: `abs = SUM(abs_itt/se^2)/SUM(1/se^2)`, `se = SQRT(1/SUM(1/se^2))`. Rel lift = `abs / (SUM(vis_holdout)/SUM(n_holdout))`.
5. Cohort splits: advertisers running both vs Select-only vs non-Select-only. Pooled comparison restricted to "both" for an apples-to-apples advertiser mix; per-advertiser paired sign test as the robustness check.

Query: [`queries/audi_1172_select_lift.sql`](queries/audi_1172_select_lift.sql). Script: [`artifacts/audi_1172_select_lift.py`](artifacts/audi_1172_select_lift.py) (queries BQ via ADC, computes, builds the xlsx — fully reproducible).

## 3. Findings

### 3.1 Pooled (advertisers running both, n=35)

| Product | Treated VR | Holdout VR | Visit lift (pp) | Rel lift | 95% CI (pp) | z | Sig |
|---|---:|---:|---:|---:|---|---:|---|
| **Select** | 2.39% | 1.19% | +0.262 | **+22.0%** | [+0.254, +0.270] | 61.9 | Yes |
| **non-Select (PTV)** | 1.38% | 1.21% | +0.052 | **+4.3%** | [+0.047, +0.058] | 19.2 | Yes |

Select treated bids 23.1M / holdout 2.0M; non-Select treated 63.7M / holdout 6.6M.

### 3.2 Per-advertiser robustness

- Select rel lift > non-Select in **27 of 35** advertisers running both (1 undefined — zero holdout visits; 7 favor non-Select).
- Median paired edge (Select rel − non-Select rel) = **+45.6pp**.
- Largest advertisers by Select volume all favor Select: Zazzle +95% vs +5%, LesserEvil +43% vs +13%, Upneeq +79% vs +25%, Blueland +149% vs +64%, Hugo Insurance +84% vs +2%, Maurices +43% vs +20%, Planned Parenthood +142% vs +11%.

### 3.3 Coverage

| | Advertisers |
|---|---:|
| Requested (Kirsa's list) | 93 |
| With clean-gated lift data | 74 |
| Running both products | 35 |
| Select-only | 8 |
| non-Select-only | 31 |
| No clean-gated data (all low-coverage) | 19 |

### 3.4 Negative lift readings (e.g. 59460 Choose New Jersey, non-Select −13.8%)

A per-advertiser negative is noise, not harm, unless a bias flag trips. Pulled `reporting.lift__ghost_bid_results` (`stratum_type='overall'`) for 59460: non-Select side had **every bias flag clean** — `ghost_frac_inflated=false`, `arm_imbalance_suspect=false`, `has_valid_holdout=true`, `holdout_won_rate=0`, `meets_min_n/compliance=true`, `ip_compliance=0.42`. So the −13.8% (nominal z −2.8) is noise around ~0: bid-grain z is N-inflated (bids within an IP correlated) and ~42% compliance dilutes the ITT. Teaching point: the Select side of the same advertiser is the one flagged `arm_imbalance_suspect=true`, yet reads +262% — **the flags mark imbalance RISK, not direction.** Captured as a methodology lesson in `experimentation.md` §"Ghost-bid lift".

## 4. Caveats

- **Bid-grain ITT.** The unit is a bid, treatment bids win ~10% of auctions, so absolute pp is win-rate-diluted roughly equally across products. Relative lift is the fair comparison; do not read the pp as a served-user rate.
- **Window 2026-06-22 → 2026-07-27**, no earlier data (no backfill). 7-day per-user visit window → trailing ~7 days still maturing.
- **Beeswax bidder leg only.** The MNTN Rust-bidder leg (`bidder_bid_events`, partner ~79) is not folded into `lift__ghost_bid_*` yet — Select coverage is a subset of all live Select advertisers.
- Low-volume single campaigns have wide intervals; the pooled and paired advertiser-level reads are the defensible outputs, not any single small campaign.

## 5. Data documentation updates

Added: `data_catalog.md` §lift — rollup `level`/`entity_id` grain, the `entity_id→campaign_groups.campaign_group_id` join (PK column is `campaign_group_id`, not `id`), product_id Select/PTV split, Select coverage sparsity (Beeswax-only), 6/22 floor. `experimentation.md` §Ghost-bid lift — the negative-lift diagnostic (read the results-table flags; flags mark risk not direction; 59460 example).

## 6. Deliverable format iterations

The xlsx went through several format-clarity passes driven by review feedback, all folded back into the shared builder `lib/mntn_xlsx.py` (so every future workbook inherits them): abs lift shown as `pp` vs relative as `%`; "clean-gated" jargon replaced with plain "usable holdout"; a Read-me note for blank-rel-but-significant rows; subtitle wrap to table width (v7); one-line subtitle + accent rule + alignment standard (v8); restrained reference-tab structure (v9); top breathing room (v10); sequential green heat + semantic red/amber/green signal coloring for lift columns (v11); Read-me terseness build-guard (v12). See `documentation/docs/xlsx_deliverable_standard.md` changelog v7–v12.

## 7. Delivery

Delivered to Kirsa 2026-07-28; she's sharing to the Select-analysis tiger team. RT exclusion confirmed to her (all rows objective_id=1 prospecting; ghost-bid holdout is prospecting-only). Overwriting the live Drive copy on rebuild is fine (Malachi confirmed) — regenerate over `My Drive/Tickets/AUDI-1172/` as needed; make a separate copy only if asked. Clean rebuildable source = the committed builder + local artifact.

## 8. Open items

**CPIV/CPIA (Kirsa follow-up, 2026-07-29) — computed but NOT shipped.** She asked for cost per incremental visit/conversion. First-pass pooled (`total_spend / incremental_visits`, `all_facts` obj=1 spend, 2026-06-22..07-27): Select ~$6 vs non-Select ~$30 CPIV; CPIA Select ~$117 vs non-Select ~$1,030. Directional only. Adversarial verification (workflow) found it not tiger-team-ready. **Matt call 2026-07-29 (`meetings/audi_1172_01_matt_brorby_spend_scaling_2026_07_29.txt`) resolved the two open items — CPIV is now computable, no longer blocked on Matt:**
- **Leg — RESOLVED, no restriction needed.** Every campaign_group sits entirely on ONE bidder (Select→Mountain/Rust p79, PTV→Beeswax p8, never split). all_facts spend maps cleanly to a CG's single leg; the leg IS the product split Kirsa asked about. (Retires the "confounds the +22% vs +4%" worry — the legs don't mix within a group.)
- **Spend↔Verified-Visit — SOLVED (see computed block below).** Matt tracks no spend (no CIL); his "visit" = pixel-fire within 7d of first bid, not Reporting VV. Resolution: use metered `all_facts` spend + express incremental on the Reporting VV basis via `incremental_VV = Reporting_VV × rel_lift/(1+rel_lift)`. (An intermediate attempt to bridge via `first_day..seventh_day_visits` was wrong — those are last-touch day-buckets, not the VV.)
- **Headline internal inconsistency (fix regardless).** The Headline shows count-pooled treated/holdout rates (imply ~+100% Select lift) next to the IVW abs-lift (+22%). Different estimators side by side; a reader can catch that 2.39% − 1.19% ≠ 0.262pp. Pick one basis per table before the copy propagates.

**CPIV/CPIA COMPUTED + SELF-VERIFIED (2026-07-29). The "5x cheaper per incremental visit" is a PIPELINE-MEASUREMENT ARTIFACT; on the client basis it's ~1.6x.** Two bases:

| Basis | CPIV Select | CPIV non-Select | ratio | CPIA Select | CPIA non-Select | ratio |
|---|---|---|---|---|---|---|
| Pipeline (spend ÷ Matt's incremental_visits) | $6.01 | $30.47 | 5.1x | $130 | $1,273 | 9.8x |
| **CLIENT / Reporting Verified-Visit (ship this)** | **$5.23** | **$8.23** | **1.6x** | **$84** | **$256** | **3.0x** |

**Why they diverge:** Matt's ghost-bid pipeline UNDERCOUNTS Reporting visits for display — non-Select 223K pipeline incremental vs **6.5M** Reporting VV; Select (CTV) roughly agrees. So the pipeline basis overstates non-Select cost 3-4x. Verified via our own testing (`queries/audi_1172_cpiv_vv_correct.sql`, `artifacts/audi_1172_cpiv_vv_compute.py`, `outputs/audi_1172_cpiv_vv_pooled.csv`): the authoritative UI Verified Visit = `clicks+views+competing_views` (AUDI-1070, reproduced to the dollar), conv = `click+view+competing_view_conversions`. **Method (Matt's call): incremental_VV = Reporting_VV × rel_lift/(1+rel_lift)**, rel_lift = volume-weighted raw-count pooled pipeline lift (Select +102.7%, non-Select +14.5%).

**CORRECTION — the earlier k-factor pass (this same session) was WRONG.** It bridged via `first_day..seventh_day_visits` (last-touch-only day-buckets, omit clicks/first-touch/CTV view paths, NOT the VV) giving k=0.28/0.83 and CPIV_VV $21/$37 — discarded. **Spend method stands:** metered `all_facts` spend joined on `campaign_group_id`, `objective_id=1` (non-Select CGs also run obj 5/6 ≈$0.6M the lift never measured; Select 100% obj=1). **Estimator:** CPIV uses raw-count (volume) rel_lift, NOT the IVW +22%/+4% in the shipped lift sheet (that answers "average campaign lift") — label bases separately so they don't read as contradictory. **Residual (Matt's nod, not a blocker):** borrowing the 7d pipeline rel_lift onto a full-window Reporting VV assumes a basis-invariant treated/holdout ratio (unavoidable; holdout has no Reporting VV).

**Still open before shipping to Kirsa:** (1) build a CPIV/CPIA tab (client VV basis, one caveat line) and push to Drive — NOT yet done; the Drive xlsx is still lift-only. (2) The Headline internal inconsistency (count-pooled rates next to IVW abs-lift) — fix in the same pass.
