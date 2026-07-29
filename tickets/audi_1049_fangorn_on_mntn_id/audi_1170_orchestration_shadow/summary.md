---
doc_type: ticket
title: "AUDI-1170: Orchestration, backfill, and shadow validation for the household FS"
status: backlog
date: 2026-07-28
summary: "Wire L1→L2→L3 as an additive task group in feature_store_setup_model.py; backfill runner + shadow parity dashboards"
result: ""
question: "Can we stand up the household FS as an additive hh_ task group that runs end-to-end daily and emits a first shadow-parity readout (household-vs-IP sizes, coverage, day-over-day stability) proving it's safe to consume?"
framing_state: locked
---

# AUDI-1170: Orchestration, backfill, and shadow validation

**Jira:** https://mntn.atlassian.net/browse/AUDI-1170
**Parent epic:** AUDI-1049 · **Build umbrella frame:** `../audi_1134_feature_store_build/summary.md`
**Status:** backlog · **Assignee:** Malachi — **ASSIGNED 2026-07-29** (run `/frame` to lock §0 before moving to `in_progress`; the framing gate blocks it while `framing_state: draft`)

---
## 0. Framing  ← LOCKED 2026-07-29 (framed via /frame; locally, no Jira write)
- **Question:** Can we stand up the household FS as an **additive `hh_` task group** in
  `feature_store_setup_model.py` (one schedule, no forked DAG) that runs **end-to-end daily** and emits a
  **first shadow-parity readout** (household-vs-IP audience sizes per vertical, resolution/coverage split,
  day-over-day household-count stability) — proving the household pipeline runs and is plausibly correct before
  anything consumes it?
- **Goal (why / the decision):** 1170 is the **"make it run + prove it's right"** layer — the **go/no-go on
  whether the household FS is safe for AUDI-1103 (train) and the HHDSC export to consume.** A failed parity
  readout (implausible sizes / poor coverage / high churn) blocks consumption and sends the resolution/graph-join
  back for rework; a clean one green-lights training for the **Sept-4 MVP.** Waiting: Brian (1103 train), Sean
  (HHDSC export). North-star: MM-AI identity-graph integration (ID-327).
- **Objective (done-when — binary, for Sept-4):** (1) the `hh_`-prefixed models run as an **additive task
  group** in `feature_store_setup_model.py` **end-to-end daily** (compiled `model_task_config.json`, dependency
  edges); (2) `docs/feature_store_naming_standards.md` updated with the `mntn_id` dimension; (3) a **first
  shadow-parity dashboard** exists (household-vs-IP audience sizes per vertical, resolution/coverage split,
  day-over-day household stability) **plus a proposed reconciliation band** (audience-size-delta + stability
  thresholds) for sign-off. **Full backfill (per AUDI-1101 depth) + the multi-day N-day shadow-run are
  fast-follows past the gate.**
- **Approach (how):** **Thin end-to-end `hh_` skeleton first** — minimal/stub L2/L3 household models — to stand
  up the orchestration + shadow harness **in parallel** with the real AUDI-1168/1169 (co-owned with Brian), then
  swap the real models in. Extend `feature_store_setup_model.py` + `model_task_config.json` with the `hh_` task
  group. **Note vs the ticket description:** for IPv4-only v1 the **L1 mirror is OPTIONAL (join the graph
  directly)** and resolution lands **inline at L2** (before L158 of `guid_log_derived_ip_vertical_id.py`), so the
  dependency edges are **graph-join@L2 → L3**, not the `L1mirror → L2 → L3` the description assumed. Backfill
  runner over `as_of_date` (60d BQ + `household_graph_parquet` fallback). Shadow-parity queries compare the
  `hh_` output vs the IP store. **Resolve first:** AUDI-1101 retention (backfill depth) + the **lookback-churn
  choice** (features follow the day's vs the snapshot's household — sets what "correct" parity means).
- **What would change the answer:** the shadow-parity readout comes back **implausible** — household audience
  sizes far off, poor resolution/coverage, or day-over-day household-count churn beyond the ~15% baseline (§6.4)
  — meaning the graph-join/resolution is wrong and must be reworked before 1103 trains. Or the skeleton shows the
  additive task group **can't run on the one schedule without a forked DAG**, forcing an orchestration redesign.

## 1. Introduction
Component 5 of 5. Orchestration + the trust layer. The **shadow-run parity check is the gate** — nothing
downstream (train, HHDSC export) consumes the household output until parity holds.

## 2. The Problem
The four models must run in order on one schedule (no forked DAG), backfill deep enough to train, and prove —
via shadow parity — that household audience sizes/coverage/stability are plausible before consumption.

## 3. Plan of Action
1. **Thin `hh_` skeleton** — minimal/stub L2/L3 household models to prove orchestration + the shadow harness now,
   in parallel with the real AUDI-1168/1169 (swap in later).
2. **Additive task group** + dependency edges (**graph-join@L2 → L3**; L1 mirror optional) in
   `feature_store_setup_model.py`; compile `model_task_config.json`.
3. **Shadow-parity dashboards** (the Sept-4 readout): household-vs-IP audience sizes per vertical,
   resolution/coverage split, day-over-day household stability (the HHID-churn check, §6.4) — **+ propose the
   reconciliation band** (size-delta + stability thresholds) for sign-off.
4. **Update `docs/feature_store_naming_standards.md`** with the `mntn_id` dimension.
5. **Fast-follows (past the Sept-4 gate):** backfill runner over `as_of_date` (60d BQ + `household_graph_parquet`
   fallback, depth per AUDI-1101) + the multi-day N-day shadow-run.

**Resolve first (empirical unknowns from the Approach):** AUDI-1101 retention answer (backfill depth); the
lookback-churn choice (features follow the day's vs the snapshot's household); whether the additive task group
runs on the one schedule without a forked DAG.

## 4. Investigation & Findings
_(queries in `queries/`, results in `outputs/`)_

## 5. Solution
_(PRs, config, code)_

## 6. Questions Answered
- **Q:** — **A:** —

## 7. Data Documentation Updates
_(document the shadow-parity methodology + household-stability metric)_

## 8. Open Items / Follow-ups
### Clarify with Sean Yang (FS lead + ticket author, now on DS13/19) — before/early in the build
1. **PR #1156 base + the `(ip,guid)` L1 table:** should the household `hh_` L2/L3 models build on your draft
   PR (airflow-ti #1156) or a fresh branch — and does its `(ip,guid)` L1 table stub anything I'd duplicate?
   Confirming your own question back: for IPv4-only Sept-4 we use the **existing IP-keyed guid_log L1 as-is**,
   and `(ip,guid)` is only for Ryan's GUID fast-follow — right?
2. **Resolver interface:** where will your `household_resolution.py` / `identity_graph_ip_mntn_id` stubs live,
   and their signature? Should my L2 resolution **call the stub (swap-ready)** or **inline the max-conf join**
   for v1? (Avoid building a parallel resolver.)
3. **Direct-graph-join vs mirror:** did your cost test on joining the full `household_graph_parquet` directly
   land — is direct-join OK for v1, or do we need the daily mirror (1166)? (Sets whether 1166 is real or stays optional.)
4. **DS13/19 seam:** now that you own DS13/19 — where's the boundary vs my Fangorn `guid_log` L2/L3 re-key? Do my
   `hh_` L2/L3 outputs feed your DS13/19 (`site_visit_signal`/`tpa_ipdsc_export`) work, or are they independent?
5. **Split with Brian:** preferred division of the real L2/L3 models (1168/1169) vs the 1170 orchestration/shadow
   skeleton across me + Brian?

### Decide with the group (Sean/Ryan/Matt)
6. **Lookback-churn:** do features follow the **day's household** or the **snapshot's household**? Defines what
   "correct" shadow parity even means (§7i / §6 open item).

### I'll self-serve from the airflow-ti clone (not Sean's time)
Exact `hh_` model naming pattern (naming-standards doc), the IP-side parity comparand tables/paths, L158
context in `guid_log_derived_ip_vertical_id.py`, and the additive-task-group mechanics in `feature_store_setup_model.py`.

- Backfill depth gated by 60-vs-90d retention (§6.1, AUDI-1101). Reconciliation band (§6.6) undefined — this
  ticket produces the parity numbers that band will be set against.
- **Convention confirmed (Slack 2026-07-29, epic §7h): REUSE the existing DAGs; add HHID work as new MODELS with
  an `hh_` prefix** — NOT a separate `feature_store_hhid_*` DAG set. This is the concrete form of the "additive
  task group, no forked DAG" scope. Resolution happens in L2 (IPv4-only), so the household models are L2/L3
  `hh_`-prefixed additions to `feature_store_setup_model.py`.
