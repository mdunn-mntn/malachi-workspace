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

## 4. Caveats

- **Bid-grain ITT.** The unit is a bid, treatment bids win ~10% of auctions, so absolute pp is win-rate-diluted roughly equally across products. Relative lift is the fair comparison; do not read the pp as a served-user rate.
- **Window 2026-06-22 → 2026-07-27**, no earlier data (no backfill). 7-day per-user visit window → trailing ~7 days still maturing.
- **Beeswax bidder leg only.** The MNTN Rust-bidder leg (`bidder_bid_events`, partner ~79) is not folded into `lift__ghost_bid_*` yet — Select coverage is a subset of all live Select advertisers.
- Low-volume single campaigns have wide intervals; the pooled and paired advertiser-level reads are the defensible outputs, not any single small campaign.

## 5. Data documentation updates

None net-new; relies on the existing `data_catalog.md` §"lift__ghost_bid_*" and `experimentation.md` §"Ghost-bid lift". Coverage/window facts (6/22 floor, Beeswax-only, product-split cardinality) are consistent with those docs.
