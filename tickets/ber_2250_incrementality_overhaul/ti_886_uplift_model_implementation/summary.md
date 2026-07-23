---
doc_type: ticket
title: "TI-886: Uplift Model Implementation — T-learner with Platt Scaling"
status: in_progress
date: 2026-04-20
summary: "Productionize Matt Brorby's T-learner impression-uplift model (Qini + Platt scaling)."
result: "in progress — reproducing Matt's Qini eval; needs Databricks access + airflow-ti port"
keywords: [ti-886, t-learner, platt scaling, qini, uplift model, matt brorby, impression-uplift, fellowship, cost_impression_log, ber-2250, alex knorr]
---

## TL;DR

**Q:** What is the state and content of TI-886 (uplift model implementation — T-learner with Platt scaling)?

**A:** TI-886 productionizes Matt Brorby's impression-uplift T-learner prototype (from branch `mbrorby/workspace/impression-uplift` in `SteelHouse/databricks_targeting`). Malachi + Alex Knorr co-drive; Matt advises but explicitly does not want to build the models. Status: in progress — the summary is a stub. Sections 4 (Investigation & Findings), 5 (Solution), and 6 (Questions Answered) are all empty/unpopulated; only the plan of action is written. The stated goal: reproduce Matt's Qini evaluation, port Postgres impression reads to `silver.logdata.cost_impression_log` (BQ), fix uplift-decile monotonicity, extend the target from 7-day forward visit to value-weighted outcomes, and ship v1 to airflow-ti to feed Alex's Fellowship combination engine. The model design (per the plan/problem sections): two-XGBoost T-learner, Platt Scaling calibration, ranks IPs by predicted incrementality. Open blockers: needs Databricks workspace access + Vault GCP workload auth for reproduction; multi-model-per-advertiser scoping (prototype is single-model); Fellowship interface contract. No results are concluded in this summary. (Separately noted in knowledge/experimentation.md but not in this summary: TI-886 was later reassigned away from the TI side per an 2026-05-08 ownership update.)

**How:** Read the summary.md in full. Sections 4-6 are unpopulated placeholders, so all content comes from the Introduction, Problem, Plan of Action, and Open Items sections. Grepped the four knowledge docs to confirm delta facts are already documented.

**Tables:** silver.logdata.cost_impression_log

**Learned:**
- The summary is a plan-only stub: Investigation & Findings, Solution, and Questions Answered sections are all empty — no results concluded.
- TI-886 productionizes Matt Brorby's T-learner (two XGBoost models + Platt Scaling, Qini eval) from branch mbrorby/workspace/impression-uplift in SteelHouse/databricks_targeting.
- Plan: port Postgres impression reads to silver.logdata.cost_impression_log (BQ), fix uplift-decile monotonicity, extend target from 7-day forward visit to value-weighted outcomes, ship v1 to airflow-ti to feed Alex Knorr's Fellowship combination engine.
- All durable technical facts in this summary are already captured in knowledge/experimentation.md (line 907, T-learner prototype; line 909, ownership reassignment) and knowledge/data_catalog.md (ghost-bid lift tables).

**Reuse when:**
- asked about TI-886 or uplift model implementation status
- asked about the T-learner / Qini / Platt scaling uplift model
- asked who owns the impression-uplift productionization
- asked how BER-2250 incrementality connects to IP-level uplift ranking

---

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
