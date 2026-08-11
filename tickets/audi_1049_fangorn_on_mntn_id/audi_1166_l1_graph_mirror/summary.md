---
doc_type: ticket
title: "AUDI-1166: Build Layer-1 identity-graph mirror model (daily IP→household snapshot)"
status: backlog
date: 2026-07-28
summary: "Daily L1 model mirroring identity_graph_history (id_type=30) into the feature store as the IP→household_id map"
result: ""
question: "Can we land a daily L1 model that mirrors identity_graph_history (id_type=30) into the FS as a partitioned IP→household_id map for downstream resolution?"
framing_state: draft
---

# AUDI-1166: Build Layer-1 identity-graph mirror model

**Jira:** https://mntn.atlassian.net/browse/AUDI-1166
**Parent epic:** AUDI-1049 · **Build umbrella frame:** `../audi_1134_feature_store_build/summary.md`
**Status:** backlog · **Assignee:** Malachi (unassigned in Jira — claim at planning)
> **⚠ Update 2026-07-29 (epic §7i): this ticket is now OPTIONAL.** Default is to **join the graph directly**;
> the mirror is a reserve/fallback for full-graph-join performance issues. **Sean is landing it as a stub.**
> Not on Malachi's IPv4-only critical path.

---
## 0. Framing  ← run `/frame` when you start; inherits the AUDI-1134 build-frame
- **Question:** Can we land a daily L1 model that mirrors `identity_graph_history` (`id_type=30`) into the
  feature store as a partitioned IP→household_id map, correct as-of each graph day, for AUDI-1167 to consume?
- **Goal:** First link in the household FS chain (blocks 1167→1168→1169). Sept-4 MVP. North star: MM-AI / ID-327.
- **Objective (done-when):** an additive L1 model in `feature_store_setup_model.py` writing a `dt=`-partitioned
  Parquet snapshot of the day's `id_type=30` map (ip, household_id, is_shared, confidence_score, as_of_date),
  registered in `model_task_config.json`, with `--run_date` backfill working.
- **Approach:** read `dw-main-bronze.raw.identity_graph_history` WHERE `id_type=30`, pinned to the d-1
  `as_of_date`; write to `feature_group_1_source/` layout. Confirm one household per (ip, as_of_date) or carry
  is_shared for 1167 to resolve.
- **What would change the answer:** graph day not landed by DAG time (01:03 UTC) → need lag/fallback; IPv4
  coverage gap vs delivery IPs → resolution rate too low to proceed.

## 1. Introduction
Component 1 of 5 of the household FS build (was AUDI-1134). Produces the raw daily IP→household mapping the
rest of the pipeline resolves against. Source-of-truth mapping table documented in `knowledge/data_catalog.md`
→ "Identity Graph" (to be added). Repo: `SteelHouse/airflow-ti`, `models/feature_store/feature_group_1_source/`.

## 2. The Problem
The FS has no household key. Everything downstream (resolution, L2, L3, train) needs a correct, point-in-time
IP→household_id map landed daily in the feature store, partitioned so backfill can replay historical graph days.

## 3. Plan of Action
1. New L1 model reading `identity_graph_history` (`id_type=30`), pinned to d-1 `as_of_date`.
2. Output Parquet `dt=` partition in `feature_group_1_source/`; columns ip, household_id, is_shared, confidence_score, as_of_date, graph_version.
3. Register in `dags/model_task_config.json` + add to `feature_store_setup_model.py` task group.
4. `--run_date` backfill path (feeds AUDI-1170's backfill runner).
5. Profile coverage: resolution rate, clean/shared split, confidence distribution (see first-BQ-steps in epic §7).

## 4. Investigation & Findings
_(queries in `queries/`, results in `outputs/`)_

## 5. Solution
_(PRs, config, code)_

## 6. Questions Answered
- **Q:** — **A:** —

## 7. Data Documentation Updates
_(add Identity Graph table schema + as-of semantics to `data_catalog.md`)_

## 8. Open Items / Follow-ups
- Depends on graph landing ~20:00 UTC (before 01:03 DAG). Retention 60-vs-90d (AUDI-1101) affects backfill depth.
- **Meeting 2026-07-28 (see epic §7b):** Ryan's recommended design keeps L1 **IP-keyed as a STRUCT keyset**
  (IPv4/IPv6/GUID/HEM, null if absent), NOT converted to household in L1 — the graph join happens at L2. The
  graph snapshot source is **`household_graph_parquet`** (partitioned `as_of_date` + `as_of_date_revision_number`).
- **Scope narrowing:** Fangorn's L1 = **`guid_log` (IP, advertiser_id)** only; **`augmentor_log` is NOT used by
  the ML model → leave it out.**
- **Identifier scope (Slack 2026-07-29, corrects the IPv6 note):** `guid_log` has **no IPv6** → IPv6 is moot for
  the guid_log-only v1 (only matters if augmentor_log is added). But `guid_log` carries **`guid` = graph
  `id_type=41` (`MNTN_GUID`; corrected 2026-08-11 from "42" = `GA_CLIENT_ID`, see epic §7j)**, not scoped into
  the current IPv4-only design. **Open for Sept-4: bake GUID into the L1 keyset
  as a 2nd identifier?** Initial version covers only households with an IPv4 (non-IPv4 households punted).
- **IPv4-only v1 leaves the guid_log L1 UNTOUCHED (Ryan, epic §7d):** for v1 you edit only L2/L3 to add the
  graph join; the **keyset-struct rebuild of L1 is the FAST-FOLLOW**, not this ticket's v1 deliverable. This
  ticket's graph-snapshot **mirror** is still built (it's what L2/L3 join against) — but don't re-key the
  existing guid_log L1 for v1. Deferring multi-identifier avoids the **multiple-membership intent shift**
  (adding IPv6/GUID later moves a household's score as more signal rolls in — Brian McAdams).
- **The GUID fast-follow is RYAN's, in parallel (epic §7h):** once IPv4 works, Ryan Kleck takes a stab at adding
  GUID to L1 + the ip/guid-combo lookup in L2. So this ticket's L1 keyset/GUID work is off Malachi's critical
  path — coordinate with Ryan rather than build it here.
