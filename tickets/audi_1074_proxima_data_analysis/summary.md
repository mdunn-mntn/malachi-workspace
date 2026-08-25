---
doc_type: ticket
title: "AUDI-1074: proxima data analysis"
status: in_progress
date: 2026-08-24
summary: "Evaluate Proxima's DTC Shopify transaction sample against the 7 AUDI-929 data-property questions; GO/NOGO + reusable ecomm-vendor rubric"
result: "in progress"
question: "Does Proxima's transaction feed contain enough unique, fresh, MNTN-addressable purchase signal (per the 7 AUDI-929 questions) to justify continuing toward a paid integration?"
framing_state: locked
---

# AUDI-1074: proxima data analysis

**Jira:** https://mntn.atlassian.net/browse/AUDI-1074
**Status:** in_progress (Spike, sprint 08/24/26–09/07/26, parent AUDI-1054 Q3 Tech Debt)
**Date Started:** 2026-08-24
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Does Proxima's DTC Shopify transaction feed contain enough unique, fresh, MNTN-addressable purchase signal — measured on the 7 data-property questions from AUDI-929 comment 574269 — to justify continuing toward a paid integration?
- **Goal (why / the decision):** A defensible GO/NOGO on the Proxima partnership (cross-team scope: Targeting/Identity/Select, Brian McAdams leads requirements; Kale reignited the eval late August per his 7/21 email) plus a reusable ecomm-transaction-vendor rubric per `documentation/docs/data_vendor_valuation_framework.md` (value = unique usable signal at net-new margin). North-star: Tier 2 — TI-189→TI-718 "Modularize and extend model inputs" and the Theme-3 "Feature inventory and data quality evaluations" bullet. (North-star doc is Q2-vintage, last updated 2026-04-17; no Q3 OKR table exists — framed against the stale doc knowingly.)
- **Objective (done-when):** By 09/04: GO/NOGO comment posted on AUDI-1074 (cross-linked on AUDI-929) with all 7 questions answered quantitatively, a branded xlsx scorecard in Drive `My Drive/Tickets/AUDI-1074/`, and a rubric doc scoring three axes (predictive-power proxy / uniqueness-overlap / integration cost). Done = every question has a number and a rubric row; not done = any question answered only qualitatively without a documented blocker.
- **Approach (how):** Download the 13.0 GB / 175-object parquet sample (s3://proxima-ops-data-customer-mntn/20260717/, confirmed accessible 2026-08-24) to gitignored `outputs/`; analyze locally with duckdb (BQ external tables impossible over s3 — no Omni, and BQ is read-only: no table/GCS writes; first local-parquet eval in this repo). MNTN-side denominators via read-only BQ (`bq_run.sh`, us-central1): DS14 addressable gate (~149M IPs) + CIL 30d served IPs, `MOD(ABS(FARM_FINGERPRINT(ip)),100)<k` sampled, joined locally. Q5 cohorts = Fangorn score bands (HI 8000–10000 vs Max Reach vs unscored-but-served), user-confirmed 2026-08-24. Key assumptions to resolve empirically first: customer_id cross-brand vs per-brand scope; date span vs the 1-year claim; IPv4/IPv6 split; category flag consistency.
- **What would change the answer:** (1) QC shows the data is not as sold — order_id dup rate >5%, span <9mo vs the 1-yr claim, or customer_id per-brand with no cross-brand join path → pause, flag to vendor via user, re-scope. (2) DS14 overlap of last-30d-order Proxima IPs <~5% → fast NOGO on addressability alone. (3) Q5 matched N <~1k/cohort → Q5 downgraded to descriptive with explicit power caveat, not silently skipped.

## 1. Introduction
Proxima is a DTC conversion-data vendor: ~2,200 Shopify-integrated brands, claimed 100M+ US shoppers, timestamped SKU-level order data with deterministic identity signals (browser_ip at purchase, customer_id→IP mapping). Prior tickets: AUDI-935 (initial call 5/13, follow-up scoping) and AUDI-929 (GO/NOGO spike definition, Brian McAdams' 7 evaluation questions + tentative analysis plan in comment 574269 — the contract for this ticket). Proxima delivered sample files 2026-07-17 (Byu Jareonvongrayab email; AWS creds by separate email — env-var use only, never committed). Kale paused the eval 7/21 for July/August roadmap priorities and committed to reigniting "back half of August"; Bryce pulled AUDI-1074 into sprint 08/24–09/07 on 2026-08-24. Nobody confirmed receipt to Proxima before the pause; Jason Cook's midpoint-meeting ask is also outstanding (user-owned, drafts on request — decision 2026-08-24: no reply now, Kale owns the thread).

Sample datasets (dictionary: `~/Downloads/MNTN Data Dictionary.xlsx`, 3 sheets):
- **basket/** — order grain (order_id unique): brand_id, brand_category, order_created_at (TIMESTAMP, no timezone), order economics (subtotal/discount/tax/shipping/total, currency), financial_status, email/sms marketing-consent booleans, browser_ip (IPv4 or IPv6), customer_id, billing/shipping/default city/state/zip/country, and 30 vendor-derived `category_<x>_buyer_{6,12,36}mo` booleans (10 categories × 3 windows). ~1 year of transactions claimed. NOTE: no customer_email / customer_phone fields, despite both appearing in Brian's AUDI-929 tentative plan and the AUDI-935 scoping ("deterministic signals include email, IP, full shipping/billing address") — logged as a delivery-reality gap for the integration-cost axis.
- **items/** — line_item grain (line_item_id unique), joins basket on order_id: line_item_product_id, price/discount/tax fields, quantity, product_taxonomy_level1–5 (modeled Shopify Product Taxonomy).
- **ip_mapping/** — customer_id → ip_address (beyond the at-purchase browser_ip).

## 2. The Problem
MNTN needs to decide whether to license Proxima's feed. The scoping tickets established what the data claims to be; nobody has verified the sample against those claims or quantified whether the signal is (a) real and internally consistent, (b) fresh enough to act on, (c) addressable by MNTN's bidding infrastructure, and (d) unique vs what MNTN's free logs already provide (AUDI-1089 bar: free logs retain 99.94% of HI audiences — vendor value must live in what free logs can't see). The 7 questions (comment 574269): 1 repurchase cadence, 2 cross-category purchase patterns, 3 brand roster/volume concentration, 4 identity fill rates + IP/addressability overlap, 5 IP-cohort behavioral separability, 6 new-to-brand identification, 7 data freshness.

Known constraints going in:
- **No Audience Acuity baseline exists.** AUDI-929 Q3 asks for match rate "compare to Audience Acuity benchmark" — DS58 has "no current use", no eval, no match-rate number anywhere in the workspace. The comparison is unanswerable as written; Q4 will report absolute overlap vs the DS14 (~149M addressable) and CIL-30d (served) denominators instead, and say why. Nearest anchor (unverified slack, WIP): identity-graph precision vs Shopify ground truth 14%/20%.
- **Fangorn context (framing only; lift test out of scope, user-confirmed):** Fangorn consumes only guid_log-derived L1 features, IPv4-only feature store, IP grain (not IP×advertiser, by design — TI-832). Transaction data is feedback-class (leaky) per TI-789/790 (pre-visit AUC 0.896 vs leaky 0.999): the freshness lag (Q7) decides whether a purchase can ever be a pre-exposure feature or only a measurement/seed input. GO/NOGO must be GO-for-what.

## 3. Plan of Action
Full plan: approved 2026-08-24 (plan file archived in session; timeline targets 09/04 finish, buffer to 09/07).
1. ~~Scaffold ticket, lock §0~~ (done 08/24).
2. Ingest: `aws s3 sync` → `outputs/proxima_20260717/` (running, background + stall monitor); manifest into §4. `pip install duckdb` (done, 1.5.5).
3. QC battery (`analysis/01_qc.py`): grain uniqueness (order_id, line_item_id, orphans), **customer_id scope test first** (distinct brands per customer — decides whether cross-brand Q2/Q6 are possible), date span vs 1-yr claim, null profile vs dictionary, IP parse/IPv4-v6/bogon rates, category-flag monotonicity (6⇒12⇒36mo) + agreement vs observed transactions, brand-collapse dedup, currency/geo scope.
4. BQ pulls (dry-run first): `queries/audi_1074_ds14_ip_sample_pull.sql` (adapt audi_1117_ds14_overlap_sizing.sql, k=1% FARM_FINGERPRINT), `queries/audi_1074_cil_served_ips_30d.sql`.
5. Q1–Q7 per approved methods (`analysis/02_cadence.py` … `08_freshness.py`); local duckdb throughout; Q4/Q5 join BQ samples locally; Q4 overlap recency-bucketed (30d/90d/365d churn curve) + served-but-unscored slice.
6. `analysis/09_scorecard.py` → three-axis rubric; branded xlsx (lib/mntn_xlsx.py, AUDI-1089 question-per-row pattern) → Drive; `ecomm_vendor_eval_rubric.md`.
7. Jira cadence: Day-1 kickoff (received/scope/Q5 cohort choice), Day-2/3 QC+data-gap comment, Day-5 midpoint numbers, Day-9/10 GO/NOGO cross-linked to AUDI-929. /capture at close.

## 4. Investigation & Findings
- 2026-08-24: s3 access verified with the 7/17 emailed creds: 175 objects, 13,037,949,694 bytes (13.0 GB) under `20260717/{basket,items,ip_mapping}/`. Basket and items files are extensionless (Trino/Presto-style names); ip_mapping is `.snappy.parquet`.
- 2026-08-24: AUDI-1074 had zero Jira comments; kickoff posted (comment 610622).
- 2026-08-24 **DELIVERY GAP (major): basket has 44 columns, not the dictionary's 57 — `browser_ip` and all 12 billing/shipping/default address columns are absent.** Verified consistent across sampled complete files. The 7/17 delivery email explicitly says "NOTE: This dataset contains browser_ip address at time of purchase" — it does not. Consequences: (a) every IP analysis must route through `ip_mapping` (customer grain, no timestamps), so Q4 recency bucketing degrades to customer-last-order-date grain; (b) Q5 geographic mix and all address fill-rate checks are unanswerable; (c) joins email/phone absence on the integration-cost axis and the vendor-questions list. Also missing vs the AUDI-935 scoping: customer_email, customer_phone (never in the dictionary either).
- 2026-08-24 **ip_mapping ships positional headers `_COL_0`/`_COL_1`** (no column names in the parquet) — the exact "schema risk" flag from data_vendor_valuation_framework.md Step 1. `_COL_0` = 32-hex customer_id, `_COL_1` = ip_address (dotted-quad observed).
- 2026-08-24 BQ denominator pulls (1% FARM_FINGERPRINT samples, outputs/): CIL 30d served IPv4 sample = 497,479 rows → **~49.7M distinct served IPv4 in 30d** (2026-07-25..08-23, RTC excluded), with per-IP ever_hi/ever_scored flags. DS14 gate (dt=2026-08-23): 2,010,355-row 1% sample (uncapped rerun) → **gate ≈ 201.0M IPv4**, up from the ~149M 2026-07-27 anchor (ds_catalog also cites ~259M elsewhere — the gate size moves; use the same-day 201M as this eval's denominator).

### QC battery results (2026-08-24, analysis/01_qc.py → outputs/qc_results.json)
**All three kill-criteria PASS — the eval proceeds on the full plan:**
- Grain exact: order_id dup rate 0.0000%, line_item_id dup 0.0000%, items→basket orphan rate 0.0000%, orders-without-items 0.045%.
- Date span exactly as sold: 2025-07-16 00:00:00 → 2026-07-15 10:54:04 (1 year). Monthly volume 3.7M (partial Jul-25) → 9.1M peak Nov-25 → 5.5-6.5M/mo steady; Jul-26 2.68M in 15 days = no cliff.
- **customer_id is CROSS-BRAND**: 32,635,713 distinct customers, 19.20% bought from 2+ brands (4.0M at 2, 1.2M at 3, max observed 20+). Proxima resolves identity across stores; cross-brand Q2/Q6 are possible.

Scale: 79,965,455 orders · 162,031,287 line items · 1,163 brands · 32.6M customers · 100% have customer_id (0% guest rows) · 99.65% USD · financial_status: 2.44% refunded/partially_refunded/voided.

**Freshness headline (feeds Q7): max order_created_at = 2026-07-15 10:54, delivered 2026-07-17 → nominal lag ≈ 2 days.**

**Identity/IP reality (feeds Q4 + integration cost):**
- ip_mapping: 33,458,138 rows, 14,829,909 customers, 24,668,002 distinct IPs (96.09% IPv4 → 23,506,901 distinct IPv4; 3.91% IPv6; 0.00% bogon/private).
- **Only 45.44% of basket customers have any ip_mapping row** — the identity-signal fill rate, well below the "60-80% IP fill" scoped in AUDI-935 (that claim was per-transaction browser_ip, which was not delivered at all).
- Cardinality healthy: IPs/customer p50=2 p90=4 max=131; customers/IP p50=1 p99=4; IPs with >10 customers = 0.0004%.
- 100% of ipmap customers exist in basket.

**Vendor-data-quality flags (feed the scorecard + vendor questions):**
- **category_*_buyer flag monotonicity VIOLATIONS** (6mo=true but 12mo=false is logically impossible): fashion 10.36%/8.90% (6>12 / 12>36), health 8.71%/8.80%, home 7.35%/8.43%, beauty 5.28%/6.65%, fooddrink 4.02%/5.04%; travel/other <1%. Per-customer flag values are perfectly consistent across rows (0% conflicts), so the flags look like snapshots computed at DIFFERENT times per window. Vendor question.
- Items nulls: line_item_product_id 22.34% NULL, taxonomy L1 23.61% NULL (≈1/4 of line items unclassifiable), L2 25.32%, L3 77.67%, L4 88.09%, L5 96.90% — usable taxonomy is effectively L1/L2.
- items.order_id/line_item_id delivered VARCHAR vs dictionary BIGINT (basket.order_id IS BIGINT — join needs a cast).

### Q-results (2026-08-24, first full pass; scripts in analysis/, raw JSON/CSV in outputs/)

**Q1 repurchase cadence** (02_cadence.py; day-grain dedup per (customer, key) — first run had a same-day-duplicate bug that read median 0, fixed): category level (Shopify taxonomy L1): pooled median gap **30 days** (p25 19, p75 55), median-of-customer-medians 32d, n=32.2M gaps. Item level (products with ≥50 repurchasers, n=13,343 products): pooled median **30 days** (p25 27, p75 51). 28.48% of (customer, category) pairs repurchase within the file. Right-censoring: 1-yr window biases medians short.

**Q2 cross-category affinity** (03_affinity.py; censoring-safe anchors, windows 30/60/90d): real structure exists. 30d base rate (any follow-up purchase) 24.21%; top lifts: Software→Health & Beauty 42.99% (1.78x), Vehicles & Parts→Health & Beauty 36.55% (1.51x), Software→Home & Garden 35.11% (1.45x). 60d base 33.60%, same pairs strengthen (Software→H&B 52.43%). Full matrices outputs/q2_affinity_{30,60,90}d.csv, first-purchase Markov transitions outputs/q2_first_purchase_transitions.csv.

**Q3 brand roster/concentration** (04_concentration.py; USD, non-refunded: excludes 0.35% non-USD + 1.28% refunded): 1,112 brands, **$10.06B GMV/yr**, 78.66M orders. Concentration LOW: top-10 = 25.06% of GMV, top-50 = 53.93%, top-100 = 70.46%, HHI 0.0101; long tail (<100 orders/yr) only 9.7% of brands. Category GMV: Fashion $3.64B, Home $2.39B, Health $1.59B, Beauty $0.96B. **Monthly active brands DECLINE 1,082 (Jul-25) → 878 (Jul-26), -19% over the year** — panel attrition or consolidation; vendor question.

**Q4 identity/overlap** (05_identity_overlap.py; estimator = matches x100/k / N, k=1% FARM_FINGERPRINT samples; Wilson 95% CI): of 23,506,901 distinct Proxima IPv4:
- **DS14 addressable gate: 91.99% [91.60, 92.37] are in-gate** — vs the <5% fast-NOGO line, addressability is a non-issue. Stable across recency: last-30d-order IPs 93.27%, 31-90d 92.51%, 91-365d 91.39% (residential IPs churn slowly).
- CIL 30d served: 40.15% [39.90, 40.41] of Proxima IPv4 were served an MNTN impression in the last 30d.
- Score-band mix: 44.9% of the Proxima∩CIL slice is ever-HI (8000-10000) vs 40.45% CIL base rate — mild HI enrichment (1.11x), Proxima households skew slightly high-intent.
- ~~The "served-but-unscored" AUDI-1089 lens is EMPTY: 100% of CIL sample IPs have household_score populated~~ **RETRACTED same day: my ever_scored flag tested `IS NOT NULL`, but CIL stamps unscored rows `household_score = -1` (data_catalog.md CIL §, AUDI-1070) — the flag was vacuously true.** Lesson: honor the documented -1 sentinel on every CIL score predicate.
- **Corrected unscored-slice numbers (rerun with `!= -1`, same window):** CIL 30d served sample composition = 46.73% never-scored / 12.82% scored-never-HI / 40.45% ever-HI. Proxima∩CIL slice: 42.9% unscored (vs 46.7% base, 0.92x — Proxima UNDER-indexes on dark IPs) and 44.9% ever-HI (vs 40.5% base, 1.11x). **17.23% [17.07, 17.40] of all Proxima IPv4 are served-but-unscored** — the properly-computed AUDI-1089 lens: a real but not dominant slice where purchase signal could reach households the scorer can't see. Q5 cohort note: never-HI = unscored + scored-low mixed (232,458 + 63,774 sample IPs).
- Every ip_mapping IP belongs to a customer with ≥1 basket order (ips_with_no_basket_order = 0).

**Q6 new-to-brand** (07_ntb.py): NTB is identifiable and the curve behaves: 89.59% (Jul-25, left-censored) → plateaus by month ~5-6 → steady state **~44-46% of orders are first-time-at-brand** (Jul-26 43.71%). A 6-mo lookback suffices for NTB separation; the 1-yr file is adequate. 54.73% of beauty-36mo-flagged customers have zero observed beauty transactions → the vendor flags DO carry history beyond the file window (consistent with their 36mo claim; combine with the monotonicity violations when weighing flag trust). Guest checkout 0%.

**Q7 freshness** (08_freshness.py): max order 2026-07-15 10:54, delivery 2026-07-17 → **nominal lag 2 days**; daily volume holds ≥95% of trailing median through 2026-07-14 (ramp-down = 1 day) → **effective lag ~2-3 days**. Trailing median 187,980 orders/day. Cadence beyond one drop unmeasurable (single delivery); vendor states weekly standard, sub-weekly negotiable (AUDI-935). Feedback-class framing: at ~2-3d lag + weekly drops, purchase signals arrive 2-9 days post-purchase — usable for measurement/seeding and post-purchase suppression/cross-sell windows (30d median cadence gives runway), but NOT as pre-exposure bid-time features without the leakage split (TI-789/790).

**Q5 cohort separability** (06_cohorts.py; cohorts = CIL 30d served IPv4 sample split ever-HI (n=201,246) vs never-HI (n=296,232); Proxima side = last-90d orders via ip_mapping; matched 18,205 vs 21,654 IPs — above the 1k/cohort power floor): **NO separability.**
- 5-fold CV logistic AUC on 14 Proxima-only features (orders, AOV, GMV, brand count, 6 category shares, 4 vendor 12mo flags): **0.506 ± 0.007 — chance.**
- Category-mix JS divergence 1.49e-5 (distributions essentially identical: fashion 24.5% vs 24.4%, health 22.9% both, home 20.2% both). Medians identical (AOV $81.85 vs $81.67; orders 1 vs 1). brands_90d Mann-Whitney p=8.7e-6 but zero effect size (median 1 vs 1; n=40K makes noise significant).
- Reading: on this test, Proxima purchase behavior is ORTHOGONAL to Fangorn's intent signal. Cuts both ways: no demographic/behavioral separability of the score bands (the AUDI-929 Q5 ask, answered NO), but orthogonality does not preclude incremental lift on OUTCOMES (visits/conversions) — that requires the out-of-scope offline lift test (follow-up ticket, leakage-controlled per TI-789/790).
- Caveats: matched subsample = IPs with any 90d Proxima order (39,859 of 497,478 sample IPs ≈ 8.0%); IP-grain join mixes household members; ever-HI = HI for ANY advertiser (dilution); 90d window.

## 5. Solution
- **Official report shipped 2026-08-24:** branded workbook at Drive `My Drive/Tickets/AUDI-1074/AUDI-1074 Proxima Sample Evaluation.xlsx` (builder: `artifacts/audi_1074_build_xlsx.py`, regenerable from outputs/). 13 tabs: Overview cover, question-per-row answers scorecard, delivery gaps, Q1-Q7 detail tabs, Read me, Method & caveats (incl. the -1 sentinel retraction), Queries (both BQ sample pulls via sql_dir).
- Remaining: GO/NOGO recommendation comment + reusable ecomm-vendor rubric doc.

## 6. Questions Answered
- **Q:** Is the Proxima sample still accessible after the July pause?
  **A:** Yes — bucket listed successfully 2026-08-24 with the emailed creds; contents match the 7/17 delivery email (basket/items/ip_mapping under 20260717/).

## 7. Data Documentation Updates
(pending — vendor-data facts stay in this ticket; any MNTN-table facts confirmed along the way go to knowledge/ via /capture)

## 8. Open Items / Follow-ups
- **Linked-ticket coverage check (2026-08-24, user ask):** AUDI-1074 carries no formal issuelinks; AUDI-929↔AUDI-935 link only to each other and both were reviewed in full (descriptions + all comments) at session start. Beyond the 7 questions, AUDI-929's five higher-level asks map to: (1) predictive power → Q5 proxy answered (AUC 0.51), full offline lift test = follow-up ticket; (2) uniqueness vs Audience Acuity / Shopify-partnership data → NOT computable (AA S3 behavioral data never integrated, no AA eval exists; no Shopify-partnership dataset found in ds_catalog) — GO/NOGO states this; (3) match rate vs AA benchmark → answered in absolute terms (no AA baseline exists); (4) integration shape → scorecard integration-cost axis; (5) reusable framework → the rubric deliverable.
- **Vendor questions FINAL (2026-08-24, with Alyson for review before sending).** Email carries asks 1-3; 4-9 are midpoint-call agenda:
  1. Can you redeliver basket with browser_ip and the address columns? Files carry 44 of the 57 dictionary columns.
  2. Can you walk us through how the category buyer flags are computed? 5-10% of rows read 6mo=true but 12mo=false.
  3. Can you resend ip_mapping with column headers? Files arrive as unnamed _COL_0/_COL_1.
  4. What share of customers should have an IP mapping? Observed 45%; scoping discussed 60-80%.
  5. Are email or phone signals available in the full product? In scoping, not in the dictionary.
  6. What's the refresh cadence, and are sub-weekly deliveries an option? Unmeasurable from one drop.
  7. Are orders append-only, or restated after refunds? Refunded statuses present; update semantics unknown.
  8. Is a brand_id to brand-name mapping available? Opaque IDs block advertiser-roster overlap checks.
  9. Should we expect the active-brand count to move much? Observed 1,082 → 878 over the file year.
- Jira comments to date: 610622 kickoff · 610694 QC gaps · 610700 findings · 610742 per-question answers.
- Follow-up ticket candidate (post-GO only): Fangorn offline lift test via champion/challenger slot, with feedback-class leakage handling (TI-789/790 split).
- Receipt confirmation + midpoint meeting scheduling: user-owned; decision 2026-08-24 was no reply yet (Kale owns the thread).
