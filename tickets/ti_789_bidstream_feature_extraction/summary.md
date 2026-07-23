---
doc_type: ticket
title: "TI-789: Bidstream Feature Extraction & Audience Augmentation"
status: in_progress
date: 2026-04-07
summary: "Extract predictive bidstream features + augment DS13/DS19 audience pools for targeting."
result: "in progress — pre-visit XGBoost AUC ~0.896; DS13/19 augmentation experiment pending"
keywords: [bidstream, feature extraction, augmentor_log, xgboost, shap, feature ablation, ds13, ds19, fangorn feature store, auc, leakage, content genre, incrementality, ti-789]
---

## TL;DR

**Q:** What is TI-789 (bidstream feature extraction & audience augmentation) doing and finding?

**A:** TI-789 (In Progress) has two workstreams: (1) extract predictive bidstream features from augmentor_log / bidder_auction_events, rank via XGBoost + SHAP, and integrate into the Fangorn feature store; (2) augment DS13/DS19 IP pools with bidstream signals and validate with holdout experiments (RTC applicability for DS13 to explore). Related TI-790 feature-importance work shows pre-visit features alone give AUC ~0.896 while feedback features produce AUC ~0.999 (leakage), so pre-visit and feedback features must be separated. Top pre-visit features are al_avg_segments (existing MNTN segment count), ci_pct_new, ci_pct_rtc; feature importance barely changes between 11 and 58 features (AUC stable at 0.896 +/- 0.005). Content genre ranked ~25th for raw visit prediction but is valuable for vertical classification. Kale's direction (2026-03-31 1x1): evaluate features by incremental lift via feature ablation rather than championing any single feature (keywords, income). DS13/DS19 augmentation holdout experiment still pending.

**How:** From the summary's Investigation & Findings, citing related TI-790 feature-importance analysis: XGBoost with IVR as target; SHAP/gain/weight/cover importance; pre-visit vs feedback feature separation to avoid leakage. Kale 2026-03-31 1x1 notes supply the feature-ablation / incrementality framing. Work is in progress; Solution and Questions Answered sections are unfilled.

**Tables:** augmentor_log, bidder_auction_events

**Learned:**
- Pre-visit features alone give AUC ~0.896; feedback features produce AUC ~0.999 (leakage), so the two must be separated.
- Top pre-visit features: al_avg_segments, ci_pct_new, ci_pct_rtc; AUC stable at 0.896 +/- 0.005 between 11 and 58 features.
- Content genre ranked ~25th for raw visit prediction but valuable for vertical classification.
- Kale wants features evaluated by incremental lift via feature ablation, not by championing any single feature (keywords/income).

**Reuse when:**
- Building or ranking bidstream/Fangorn features for targeting
- Deciding pre-visit vs feedback feature separation to avoid leakage
- Designing DS13/DS19 audience augmentation or holdout experiments
- Questions about which bidstream signals predict visits/spend

# TI-789: Bidstream Feature Extraction & Audience Augmentation

**Jira:** https://mntn.atlassian.net/browse/TI-789
**Status:** In Progress
**Date Started:** 2026-03-31
**Date Completed:**
**Assignee:** Malachi
**Team:** Malachi Dunn, Alex Knorr, Ryan Kleck

---

## 1. Introduction

Extract high-value features from bidstream data (augmentor_log, bidder_auction_events) to improve targeting performance. Two workstreams:

1. **Intent Scoring / Feature Store** — Identify and extract features from bidstream data that are predictive of Spend or Visits. Build models to evaluate feature importance, iterate to keep only the most valuable signals, and integrate into Fangorn feature store.
2. **Audience Size Augmentation (DS13/DS19)** — Use bidstream signals to expand DS13 and DS19 IP pools. Validate incrementality, test predictiveness, run holdout experiments, and union into existing staging jobs. Explore RTC applicability for DS13.

**Key context from Kale (2026-03-31 1x1):**
- Kale is narrowing TI team focus toward **incrementality** — feature evaluation should consider incremental lift, not just IVR/ROAS
- Keywords are "a feature in a predictive model" not a standalone product — should be evaluated alongside other features via feature ablation
- The question is: "which features are the most predictive?" — avoid tunnel-visioning on any single feature
- Kale emphasized need for techniques to evaluate feature predictive power at scale

## 2. The Problem

Current targeting relies heavily on existing signals (site visits, keywords, Fangorn IP scoring). The bidstream contains rich signals (content genre, device info, geo, bid provider metadata) that aren't being leveraged. Need to:
- Inventory all available bidstream features
- Evaluate which are predictive of visits/spend
- Build a shared feature store so the team uses consistent features
- Augment DS13/DS19 audience pools with new bidstream-derived signals

## 3. Plan of Action

1. Inventory all unique features from bidstream log tables (augmentor_log, bidder_auction_events)
2. Build XGBoost model with IVR as target, evaluate feature importance (SHAP values, gain, weight, cover)
3. Separate pre-visit features from feedback features (avoid leakage — TI-790 lesson)
4. Identify top features, validate with feature ablation
5. Work with Ryan on feature store integration
6. Design holdout experiments for DS13/DS19 augmentation
7. Meeting with team 2026-04-01 to review initial findings

## 4. Investigation & Findings

*Related work in TI-790 (feature importance analysis):*
- Pre-visit features alone give AUC ~0.896; feedback features produce AUC ~0.999 (leakage)
- Top pre-visit features: `al_avg_segments` (existing MNTN segment count), `ci_pct_new`, `ci_pct_rtc`
- Content genre ranked ~25th for raw visit prediction but valuable for vertical classification
- Feature importance barely changes between 11 and 58 features (AUC stable at 0.896 +/- 0.005)

*Kale meeting notes (2026-03-31):*
- Malachi proposed income as a high-value feature — Kale agreed it's a good hypothesis but emphasized not championing any single feature; let feature ablation decide
- Apple vs Android performance differential may already capture income signal via user agent
- Feature ablation technique: train model, remove features one by one, check if confusion matrix changes dramatically
- Malachi already running XGBoost with SHAP values against features scraped from log tables

## 5. Solution

*In progress*

## 6. Questions Answered

*In progress*

## 7. Data Documentation Updates

- Updated `experimentation.md` with exploitation vs exploration insight from Kale
- Updated `mntn_business.md` with strategic direction shift, org changes, BUK status

## 8. Open Items / Follow-ups

- [ ] Meeting with Ryan/Alex/Matt on 2026-04-01 to review bidstream feature inventory
- [ ] Complete feature importance ranking from XGBoost model
- [ ] Evaluate income as a feature vs user agent proxy (Kale's hypothesis)
- [ ] Design DS13/DS19 augmentation holdout experiment
- [ ] Kale coming with sharpened TI focus plan — align feature store work with incrementality direction

## Key Data Constraints

- augmentor_log: 30-day TTL, 241 GB/day — always dry-run first, use 1-hour samples for dev
- bidder_auction_events: ~400 GB/day, 90-day TTL
- Bidstream data: 2.5B rows per 2-day 10% sample
- Filter blank IPs and non-US geo values
- Multiple bid providers (Magnite, others) — reference OpenRTB spec
- Data also accessible via parquet at gs://mntn-data-archive-prod/
