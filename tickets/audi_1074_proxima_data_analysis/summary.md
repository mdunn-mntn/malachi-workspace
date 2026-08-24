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
- 2026-08-24: s3 access verified with the 7/17 emailed creds: 175 objects, 13,037,949,694 bytes (13.0 GB) under `20260717/{basket,items,ip_mapping}/`. Items files are extensionless (Trino/Presto-style names `20260717_062534_00115_x8848_<uuid>`, ~233MB each); basket and ip_mapping are `.snappy.parquet`.
- 2026-08-24: AUDI-1074 has zero Jira comments to date; description points at AUDI-935, AUDI-929, and comment 574269.

## 5. Solution
(pending)

## 6. Questions Answered
- **Q:** Is the Proxima sample still accessible after the July pause?
  **A:** Yes — bucket listed successfully 2026-08-24 with the emailed creds; contents match the 7/17 delivery email (basket/items/ip_mapping under 20260717/).

## 7. Data Documentation Updates
(pending — vendor-data facts stay in this ticket; any MNTN-table facts confirmed along the way go to knowledge/ via /capture)

## 8. Open Items / Follow-ups
- Vendor questions accumulating for the user's thread (send at user's discretion): refresh cadence + sub-weekly options, append-only vs updated-order semantics (refunds), email/phone fields absent vs scoping, brand_id → brand-name mapping, customer_id cross-brand scope confirmation.
- Follow-up ticket candidate (post-GO only): Fangorn offline lift test via champion/challenger slot, with feedback-class leakage handling (TI-789/790 split).
- Receipt confirmation + midpoint meeting scheduling: user-owned; decision 2026-08-24 was no reply yet (Kale owns the thread).
