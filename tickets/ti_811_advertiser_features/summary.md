---
doc_type: ticket
title: "TI-811: Add Advertiser-Side Features to Model"
status: backlog
date: 2026-04-01
summary: "Add advertiser features + genre-by-vertical interactions to the XGBoost match model"
result: "not started — tests whether content genre helps match IPs to specific advertisers"
keywords: [ti-811, advertiser features, xgboost match model, genre x vertical interaction, fpa_advertiser_verticals, ti-790 limitation, bidstream ti-789]
---

## TL;DR

**Q:** TL;DR card for TI-811 (Add Advertiser-Side Features to Model).

**A:** Not started. TI-811 proposes adding advertiser-level features (vertical, campaign type, funnel level) to the XGBoost match model and interacting them with IP content features (genre x vertical) to test whether content genre helps match IPs to specific advertisers — addressing Known Limitation #1 from TI-790, whose features are all IP-level aggregates that don't know which advertiser is asking. Only Intro/Problem/Plan are written; Investigation, Solution, and Questions Answered are empty. No results yet.

**How:** No analysis performed. Plan (not executed): join advertiser vertical from fpa_advertiser_verticals via advertiser_id; add campaign type/funnel level from campaigns; build genre% x advertiser-vertical interaction features; retrain XGBoost; compare AUC across IP-only vs IP+advertiser vs IP+advertiser+interactions.

**Tables:** fpa_advertiser_verticals, campaigns, advertisers

**Learned:**
- Ticket is a not-started backlog item under epic TI-789 (Bidstream Feature Extraction & Audience Augmentation); no findings exist
- Motivation: TI-790's features are all IP-level aggregates while the label is per-(IP, advertiser), so the model cannot distinguish which advertiser is asking (Known Limitation #1)

**Reuse when:**
- Planning advertiser-side or interaction features for the XGBoost match model
- Revisiting TI-790 known limitations
- Designing genre-by-vertical interaction features

# TI-811: Add Advertiser-Side Features to Model

**Jira:** https://mntn.atlassian.net/browse/TI-811
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** Not Started
**Date Started:**
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Add advertiser-level features (vertical, campaign type, funnel level) to the XGBoost model and interact them with IP content features (genre x vertical). Tests whether content genre helps match IPs to *specific* advertisers — addresses Known Limitation #1 from TI-790.

## 2. The Problem

TI-790 features are all IP-level aggregates. The label is per-(IP, advertiser) but nothing in the model knows which advertiser is asking. An IP that watches sports might convert for a sports brand but not a cosmetics brand — the current model can't distinguish this.

## 3. Plan of Action

1. Add advertiser vertical from `fpa_advertiser_verticals` (join via advertiser_id)
2. Add campaign type/funnel level from `campaigns` table
3. Create interaction features: genre % x advertiser vertical (e.g., bae_pct_sports x is_sports_vertical)
4. Retrain XGBoost with interaction features
5. Compare AUC: IP-only vs IP+advertiser vs IP+advertiser+interactions
6. Key test: do interaction features improve per-advertiser prediction?

## 4. Investigation & Findings

_(To be filled)_

## 5. Solution

_(To be filled)_

## 6. Questions Answered

_(To be filled)_

## 7. Data Documentation Updates

_(To be filled)_

## 8. Open Items / Follow-ups

- [ ] Map advertiser verticals — is `fpa_advertiser_verticals.vertical_id` usable or too coarse?
- [ ] Consider using advertiser's own content categories from campaign setup
- [ ] Interaction feature design: multiplicative, concatenated categorical, or both?
- [ ] Reference: `fpa_advertiser_verticals.advertiser_name` is unreliable — join to `advertisers.company_name`
