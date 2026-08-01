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

**Two tabs added for Kirsa/Hannah (2026-07-30; meeting `meetings/kirsa_incrementality_ask_update.txt`).** Both shipped to the Drive workbook.
1. **Cost by advertiser** — per-advertiser × product CPIV/CPIA (both cohort), same VV-basis method as the pooled tab, with Sig columns Kirsa can filter; `n/a` where an advertiser's lift is ≤0 or too small to measure (tiny holdout). Query `queries/audi_1172_cpiv_vv_by_adv.sql` (advertiser-grain extension of the pooled CPIV query). Hand-verified (Tello Select $10.23).
2. **AID-level lift by group** — overall advertiser-level incrementality (all of an advertiser's prospecting, both products), across ALL MNTN advertisers, split PTV-only / Select-only / Both. **Finding: Select-running advertisers are more incremental overall than PTV-only, but how much depends on weighting** — IVW (inverse-variance weighted; larger advertisers dominate) **Both +6.3% vs Select-only +88.4% vs PTV-only +0.2%**; median advertiser **+16.0% / +64.5% / +12.4%**. PTV-only lands near 0% on the IVW basis because its largest spenders are barely incremental; the median PTV advertiser is +12%. (Labeled IVW, not "volume-weighted" — the method is inverse-variance, though for proportions the weight ≈ sample size.) **Observational, not causal** (advertisers self-select into Select). Groups (clean-gated, test accounts + WGU excluded): PTV-only 1,118 · Both 37 · Select-only 7. Conversion lift: Both +6.6% (n=33), PTV +2.1% (n=599), Select-only n/a (only 2 advertisers with holdout conversions). Query `queries/audi_1172_aid_group_lift.sql` (rollup-only, cheap; uses `level='advertiser'` rows, partner-split re-aggregated); compute `artifacts/audi_1172_aid_group_lift.py`. **Method notes:** `level='advertiser'` pools across both products (verified); classify by `campaign_groups.product_id`, NOT partner_id (partner≠product). Also made the builder's headline numbers dynamic (cover/findings f-string from the pooled df) so they track the tabs as the rollup accumulates (Select rel lift drifted +22%→+23% between runs).

**CPIV/CPIA (Kirsa follow-up, 2026-07-29) — computed but NOT shipped.** She asked for cost per incremental visit/conversion. First-pass pooled (`total_spend / incremental_visits`, `all_facts` obj=1 spend, 2026-06-22..07-27): Select ~$6 vs non-Select ~$30 CPIV; CPIA Select ~$117 vs non-Select ~$1,030. Directional only. Adversarial verification (workflow) found it not tiger-team-ready. **Matt call 2026-07-29 (`meetings/audi_1172_01_matt_brorby_spend_scaling_2026_07_29.txt`) resolved the two open items — CPIV is now computable, no longer blocked on Matt:**
- **Leg — RESOLVED, no restriction needed.** Every campaign_group sits entirely on ONE bidder (Select→Mountain/Rust p79, PTV→Beeswax p8, never split). all_facts spend maps cleanly to a CG's single leg; the leg IS the product split Kirsa asked about. (Retires the "confounds the +22% vs +4%" worry — the legs don't mix within a group.)
- **Spend↔Verified-Visit — SOLVED (see computed block below).** Matt tracks no spend (no CIL); his "visit" = pixel-fire within 7d of first bid, not Reporting VV. Resolution: use metered `all_facts` spend + express incremental on the Reporting VV basis via `incremental_VV = Reporting_VV × rel_lift/(1+rel_lift)`. (An intermediate attempt to bridge via `first_day..seventh_day_visits` was wrong — those are last-touch day-buckets, not the VV.)
- **Headline internal inconsistency (fix regardless).** The Headline shows count-pooled treated/holdout rates (imply ~+100% Select lift) next to the IVW abs-lift (+22%). Different estimators side by side; a reader can catch that 2.39% − 1.19% ≠ 0.262pp. Pick one basis per table before the copy propagates.

**CPIV/CPIA COMPUTED + SELF-VERIFIED (2026-07-29). The "5x cheaper per incremental visit" is a PIPELINE-MEASUREMENT ARTIFACT; on the client basis it's ~1.6x.** Two bases:

| Basis | CPIV Select | CPIV non-Select | ratio | CPIA Select | CPIA non-Select | ratio |
|---|---|---|---|---|---|---|
| Pipeline (spend ÷ Matt's incremental_visits) | $6.01 | $30.47 | 5.1x | $130 | $1,273 | 9.8x |
| **CLIENT / Reporting Verified-Visit (ship this)** | **$5.23** | **$8.23** | **1.6x** | **$84** | **$256** | **3.0x** |

**Why they diverge:** Matt's ghost-bid pipeline UNDERCOUNTS Reporting visits, far more for non-Select (PTV) than Select — non-Select 223K pipeline incremental vs **6.5M** Reporting VV (~2.9x); Select roughly agrees (~1.1x). (Not a display-vs-CTV thing — both products are CTV; Select is the media-marketplace layer, PTV the automated product. The undercount difference is empirical, mechanism not firmly pinned.) So the pipeline basis overstates non-Select cost 3-4x. Verified via our own testing (`queries/audi_1172_cpiv_vv_correct.sql`, `artifacts/audi_1172_cpiv_vv_compute.py`, `outputs/audi_1172_cpiv_vv_pooled.csv`): the authoritative UI Verified Visit = `clicks+views+competing_views` (AUDI-1070, reproduced to the dollar), conv = `click+view+competing_view_conversions`. **Method (Matt's call): incremental_VV = Reporting_VV × rel_lift/(1+rel_lift)**, rel_lift = volume-weighted raw-count pooled pipeline lift (Select +102.7%, non-Select +14.5%).

**CORRECTION — the earlier k-factor pass (this same session) was WRONG.** It bridged via `first_day..seventh_day_visits` (last-touch-only day-buckets, omit clicks/first-touch/CTV view paths, NOT the VV) giving k=0.28/0.83 and CPIV_VV $21/$37 — discarded. **Spend method stands:** metered `all_facts` spend joined on `campaign_group_id`, `objective_id=1` (non-Select CGs also run obj 5/6 ≈$0.6M the lift never measured; Select 100% obj=1). **Estimator:** CPIV uses raw-count (volume) rel_lift, NOT the IVW +22%/+4% in the shipped lift sheet (that answers "average campaign lift") — label bases separately so they don't read as contradictory. **Method CONFIRMED by Matt Brorby (Slack, 2026-07-29):** "This makes sense and aligns with how i was calculating this for a customer-facing dashboard." The `incremental = Reporting_VV × rel_lift/(1+rel_lift)` approach is the same one he uses for the client-facing dashboard, so the method is validated (not just "the approach he described"). The only remaining modeling assumption — borrowing the 7d pipeline rel_lift onto a full-window Reporting VV (basis-invariant treated/holdout ratio) — is inherent and accepted (holdout is never served, so it has no Reporting VV to measure directly).

**CPIV/CPIA SHIPPED (2026-07-30).** Cost per incremental (pooled) + Cost by advertiser tabs are live on the Drive workbook, client VV basis, caveat lines on Read me / Method. Headline internal inconsistency resolved (Headline now shows the IVW basis with treated/holdout VR and abs/rel lift consistent).

**CPIV/CPIA added to AID-level lift by group (2026-07-31, Kirsa Slack ask).** Added two cost columns to the 3-group tab: **CPIV — Both $6.12 · Select-only $10.51 (n=7, thin) · PTV-only $12.83; CPIA — Both $95 · Select-only n/a · PTV-only $218.** Advertisers running Both get the cheapest incremental outcomes (~2x cheaper per incremental visit than PTV-only, ~2.3x per conversion). Group-pooled cost `CPIV = spend / (Reporting_VV × L/(1+L))`, L = **volume-weighted** raw-count lift (total-cost basis, per experimentation.md), NOT the IVW/median in the lift columns — caveated on Read me + a new Method block. Query `queries/audi_1172_aid_group_cost.sql` (all clean-gated CGs of in-group advertisers, same exclusions as the lift query; all_facts join, obj=1, window). **Scan is date-partition-bound: ~251 GB for all ~1,162 advertisers, same as the 93-AID cohort** (the AID filter is applied post-scan, so widening the population barely changes bytes). Consistent with the per-product Cost tab (Both $6.12 sits between Select $5.23 and non-Select $8.23). **Builder now has a loud CSV fallback** on the lift query (`try client.query except → read cached audi_1172_lift_by_adv_product.csv`) so an expired Python-client ADC token can't hard-fail a rebuild; the `bq` CLI (gcloud user auth) is unaffected. This build's lift columns used the same-day cached CSV (ADC expired mid-session); group-cost numbers are a fresh `bq` CLI pull.

**Per-sheet query deep-link + screenshot review (2026-07-30).** Added `table(query="<file>.sql")` to the shared builder (`lib/mntn_xlsx.py` v17): every data sheet's Source footer now names its `.sql` and deep-links to that exact query block on the Query tab (build hard-fails if a referenced query is missing). Ran a full per-sheet screenshot review against 6 criteria; applied 6 fixes and rebuilt: (1) footer hyperlink `display` was hiding the Source/Period line — Google Sheets renders a hyperlink's `display` over the cell value, so `display` must be the FULL footer text; (2) removed a "Matt Brorby confirmed" name-drop from the Method tab (no person-names in shared deliverables); (3) Overview contents "By advertiser (both)" said "ranked by Select's edge" but it's ranked by Select bid volume; (4) widened the "Advertisers" column (mid-word header break); (5) Read me relabeled IVW from "average-campaign" to "inverse-variance-weighted"; (6) trimmed the Cost tab subtitle to one line. All verified in the built file + pushed.
