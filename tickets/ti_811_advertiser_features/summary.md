---
doc_type: ticket
title: "TI-811: Add Advertiser-Side Features to Model"
status: backlog
date: 2026-04-01
summary: "Add advertiser features + genre-by-vertical interactions to the XGBoost match model"
result: "not started — tests whether content genre helps match IPs to specific advertisers"
---

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
