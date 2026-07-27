---
doc_type: ticket
title: "AUDI-1134: Build MNTN-ID household feature store (Option 1 pipeline)"
status: backlog
date: 2026-07-27
summary: "Build the household-grain feature store pipeline (L1 graph mirror -> resolution -> L2/L3 -> orchestration) at v1 IP-parity aggregation"
result: "not started"
question: "Can we stand up the MNTN-ID household feature store (L1 graph mirror -> resolution -> L2/L3 -> orchestration) at v1 IP-parity aggregation, passing shadow parity vs the IP store and emitting a trainable ~900-col household pivot for AUDI-1103?"
framing_state: locked
---

# AUDI-1134: Build MNTN-ID household feature store (Option 1 pipeline)

**Jira:** https://mntn.atlassian.net/browse/AUDI-1134
**Status:** backlog
**Date Started:** 2026-07-27
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** Can we stand up the MNTN-ID-keyed daily household feature store — L1 identity-graph mirror → shared `household_resolution` → L2/L3 household models → orchestration — at **v1 IP-parity aggregation**, passing shadow parity vs the IP store and emitting a trainable ~900-col household pivot (+ column-parity report) for AUDI-1103?
- **Goal (why / the decision):** The household feature store is the **root dependency** of epic AUDI-1049 — it **blocks AUDI-1103 (train)** and feeds AUDI-1105 (validate). Ship for the **Sept-4 MVP**. Scoped by Sean Yang (owns adjacent pipeline AUDI-1055/1056). Waiting: Brian McAdams (train), Matt Brorby (epic), Alyson Lefkowitz (RFD decider). North-star tie: Theme 3 MM AI / identity-graph integration (ID-327).
- **Objective (done-when):** Four additive models in `feature_store_setup_model.py` — (1) L1 `identity_graph_ip_mntn_id` mirror, (2) `household_resolution` util + unit tests, (3) L2 `guid_log_derived_mntn_id_vertical_id` + `conv_log_derived_mntn_id`, (4) L3 ~900-col `mntn_id`-grain pivot + **column-parity report** vs the IP pivot — producing a **trainable v1 household pivot** consumable by AUDI-1103, with shadow-run parity dashboards (household vs IP audience sizes, resolution/coverage split, day-over-day household stability). v1 aggregation = IP parity (sum/max as-is); **tuning + enrichment deferred to AUDI-1100**. Full backfill + multi-day shadow-run may continue past the gate.
- **Approach (how):** Sean's Option-1 breakdown (ticket description). L1 reads `bronze.raw.identity_graph_history` (`id_type=30`) daily, d-1 `as_of` lag; `household_resolution` = same-day equi-join (daily) + interval join (backfill), shared-IP `confidence_score` cutoff, unresolved tagging (never silent-drop → coverage metrics), fan-out guard (one household per (ip,date)). L2/L3 mirror the IP models at household grain. Orchestration additive (edges L1→L2→L3), backfill over `as_of_date`. **Backfill depth: proceed on 60-day BQ + `household_graph_parquet` fallback** (don't block on AUDI-1101). Forward-label columns stay IP-rollup placeholders until AUDI-1102 (`guid_hh_log`) lands.
- **What would change the answer:** Shadow parity fails (implausible household sizes, poor coverage/resolution, high day-over-day churn) → resolution/graph-join rework before 1103 can train. Fan-out guard trips (IP → >1 household/day) → as-of/interval join is wrong. Graph history depth < backtest window (AUDI-1101) → trainable window shrinks / parquet fallback required.

## 1. Introduction
The active build ticket under epic **AUDI-1049** (Fangorn on MNTN ID). Today's Fangorn feature store is
IP-keyed end to end. Sean Yang scoped this ticket (2026-07-27) as the **Option-1 hybrid-translation build**:
re-key the store to household grain by joining the identity graph inside the existing pipeline, emitting a
second household-keyed output next to the IP one. **v1 = mechanical IP-parity aggregation**; the aggregation
*tuning* (sum/mean/recency) and cross-IP *enrichment* are the follow-up (**AUDI-1100**). Source of truth for
the IP→MNTN ID mapping: `knowledge/data_catalog.md` → "Identity Graph".

**Decomposition (sprint planning today):** per Bryce, the five components below become **new Tasks under
epic AUDI-1049** (not subtasks of 1134). Sean creates them at planning after team review. This card holds the
build-level frame; each new Task inherits it. 1134 itself resolves to the umbrella / one of the five at planning.

## 2. The Problem
The household model (AUDI-1103) cannot train until the ~896-feature signal exists at MNTN-ID grain. Re-graining
requires a correct IP→household resolution (shared IPs, ~20% out-of-graph, fan-out), a parallel additive pipeline
(no forked DAG), and shadow parity before anything consumes the output. Blocks AUDI-1103 → the epic's Sept-4 MVP.

## 3. Plan of Action (the five components → new Tasks under AUDI-1049)
1. **L1 identity-graph mirror model** — daily `identity_graph_history` (id_type=30) → Parquet `dt=` partition; register in `model_task_config.json` + `feature_store_setup_model.py`; `--run_date` backfill.
2. **`household_resolution` module** (utils_model) — `resolve_households(df, ip_col, date_col)` → `+mntn_id, resolution_status`; same-day equi-join + interval variant; shared-IP rule; unresolved tagging; fan-out guard; unit tests.
3. **L2 household derived models** — `guid_log_derived_mntn_id_vertical_id`, `conv_log_derived_mntn_id`; v1 aggregation = IP parity; forward-outcome cols = IP-rollup placeholder until AUDI-1102.
4. **L3 household pivot** — wide ~900-col `mntn_id`-grain model-ready table + column-parity report vs IP pivot.
5. **Orchestration / backfill / shadow validation** — additive task group, dependency edges, backfill runner (60-day BQ + parquet fallback), parity dashboards, update `docs/feature_store_naming_standards.md`.

## 4. Investigation & Findings
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
