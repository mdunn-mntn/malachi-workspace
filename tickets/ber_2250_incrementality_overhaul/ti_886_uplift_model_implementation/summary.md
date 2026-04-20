# TI-886: Uplift model implementation — T-learner with Platt Scaling (based on Matt Brorby prototype)

**Jira:** https://mntn.atlassian.net/browse/TI-886
**Status:** In Progress
**Date Started:** 2026-04-20
**Date Completed:**
**Assignee:** Malachi (co-driver with Alex Knorr)
**Story Points:** 5
**Priority:** P3 (will likely carry to next sprint)
**Due:** May 15 (carries across sprint boundary)
**Parent Epic:** BER-2250 Incrementality Overhaul

---

## 1. Introduction

Finalize and productionize the impression-uplift model Matt Brorby prototyped on the
`mbrorby/workspace/impression-uplift` branch in `SteelHouse/databricks_targeting`.
Matt explicitly does not want to own implementation: *"I do not necessarily want to
build the models."* Malachi + Alex Knorr co-drive; Matt advises on methodology.

## 2. The Problem

TI-837's ghost bidding answers "did the exposed group get lift?" at the (advertiser,
intent-tier) level. A T-learner uplift model goes further — it ranks IPs by predicted
incrementality so we can preferentially target those with the highest lift. Matt's
prototype showed this is tractable (Qini curves, two-XGBoost-model design, Platt
Scaling calibration), but:
- Qini evaluation outputs aren't captured in any shared notebook
- Zero airflow-ti integration — all outputs go to a dev bucket
- Monotonicity in uplift deciles is still being fixed
- Postgres-based impression reads need a BQ port

Without a productionized version, we can't feed uplift predictions into the Fellowship
combination engine Alex is designing.

## 3. Plan of Action

1. Check out branch `mbrorby/workspace/impression-uplift` in `SteelHouse/databricks_targeting`.
2. Reproduce Matt's Qini evaluation end-to-end on a known slice.
3. Document findings, gaps, and questions in `artifacts/t_learner_review_notes.md`.
4. Scope airflow-ti integration with Alex Knorr:
   - Port Postgres impression reads to `silver.logdata.cost_impression_log` (BQ).
   - Decide feature-store integration vs standalone pipeline.
   - Wire into TI-789 bidstream feature-extraction epic where applicable.
5. Fix monotonicity in uplift deciles (co-design with Matt).
6. Extend target from 7-day forward visit to value-weighted outcomes.
7. Ship v1 to airflow-ti (goal: next sprint).

## 4. Investigation & Findings

_(Populated as work progresses.)_

## 5. Solution

_(Populated at completion.)_

## 6. Questions Answered

_(Populated as questions are resolved.)_

## 7. Data Documentation Updates

_(Populated as new knowledge emerges.)_

## 8. Open Items / Follow-ups

- Need access to Matt's Databricks workspace + Vault GCP workload auth for the
  reproduction step.
- Multi-model-per-advertiser question — Matt's prototype is single-model; production
  likely needs per-advertiser fits. Scope the engineering implication before committing.
- Integration with Fellowship combination engine — the toolbox architecture Alex is
  building needs a clear interface contract.
