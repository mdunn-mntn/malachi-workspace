# TI-790: Bidstream Feature Inventory & Quality Assessment

**Jira:** https://mntn.atlassian.net/browse/TI-790
**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Status:** In Progress
**Date Started:** 2026-03-30
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction
Part of the Bidstream Feature Store initiative (TI-789) with Alex Knorr and Ryan Kleck. The goal is to catalog all available features in bidstream data, assess their quality, and determine which are most promising for improving targeting performance (predicting Spend or Visits).

Primary data source: `augmentor_log` — contains enriched bidstream fields tied to IPs. Available via parquet at `gs://mntn-data-archive-prod/augmentor_log/` and in BQ at `silver.logdata.augmentor_log` (10-day TTL in BQ, ~30 days in archive).

The `inventory_source` field in augmentor_log identifies the bid provider (Magnite, etc.).

## 2. The Problem
Fangorn's feature store needs more signals to improve targeting predictions. The bidstream contains rich contextual data (device, geo, content categories, app/site info) that we're not currently leveraging. Need to inventory what's available and assess quality before modeling.

Key constraints:
- Data is massive (~2.5B rows per 2-day 10% sample)
- augmentor_log has 10-day TTL in BQ, ~30 days in parquet archive
- Must filter blank IPs and non-US geo (geo contains "USA", "US", "us")
- Multiple bid providers — `inventory_source` field identifies them

## 3. Plan of Action
1. Explore augmentor_log schema — all columns, types, nested fields
2. Profile each field: fill rate, cardinality, top values, distribution
3. Identify which fields are potentially predictive for Spend/Visits
4. Cross-reference with OpenRTB spec fields (Ryan's TI-792 work)
5. Produce feature inventory document with quality scores

## 4. Investigation & Findings
*In progress*

## 5. Solution
*Pending*

## 6. Questions Answered
*Pending*

## 7. Data Documentation Updates
*Pending*

## 8. Open Items / Follow-ups
- Wednesday deliverable: feature list with quality metrics
- Alex (TI-791): vertical classification signals
- Ryan (TI-792): OpenRTB spec mapping & test vertical
