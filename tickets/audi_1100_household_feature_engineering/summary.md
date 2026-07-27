---
doc_type: ticket
title: "AUDI-1100: Household Feature Engineering (Fangorn on MNTN ID)"
status: backlog
date: 2026-07-27
summary: "Re-grain the ~896-col IP Fangorn feature store to MNTN ID / household grain"
result: "not started"
question: "Which per-IP->household aggregation for each of the ~896 Fangorn features yields a MNTN-ID-keyed daily feature store that holds up on distribution/audience-size parity vs the IP store and is trainable by AUDI-1103?"
framing_state: locked
---

# AUDI-1100: Household Feature Engineering (Fangorn on MNTN ID)

**Jira:** https://mntn.atlassian.net/browse/AUDI-1100
**Status:** backlog
**Date Started:** 2026-07-27
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** For each of the ~896 IP-grain Fangorn features, which per-IP→household aggregation (sum / mean / recency / max / confidence-weighted) yields a MNTN-ID-keyed daily feature store that re-grains the signal without material loss — holding up on distribution + audience-size parity vs the IP store — and is trainable by AUDI-1103?
- **Goal (why / the decision):** Produce the household-grain feature store that unblocks **AUDI-1103** (train MNTN ID Fangorn) — the root of the Fangorn-on-MNTN-ID critical path (epic **AUDI-1049**), on the **Sept-4 MVP**. Waiting: Brian McAdams (training), Matt Brorby (epic owner), Alyson Lefkowitz (RFD decider). North-star tie: Theme 3 Mountain Matched AI / identity-graph integration (ID-327).
- **Objective (done-when):** A MNTN-ID-keyed daily feature store in BQ (**spec + validated prototype table**) over the ~896 **re-grained** features — out-of-graph households excluded, 148 verticals kept, **re-grain only** (no new enrichment for MVP) — that (a) passes audience-size-parity + null/degenerate-distribution sanity checks vs the IP store and (b) is consumable by AUDI-1103. Aggregation spec handed to Sean for DAG productionization (**AUDI-1056**). The beat-the-baseline AUC/PR-AUC proof is **out of scope** (lives in **AUDI-1105**).
- **Approach (how):** As-of join per-IP daily aggregates to `bronze.raw.identity_graph_history` (`id_type=30`, `as_of_date` within `start_time`/`end_time`); re-key (advertiser_id, ip) → (advertiser_id, household_id). Aggregate by feature type: counts→sum, rates/scores→mean (or confidence-weighted), timestamps→recency/max, flags→max. Shared IPs weighted on `confidence_score`; out-of-graph → excluded. Validate: distribution deltas + audience-size parity per vertical/DSCID. Prototype in BQ; hand the aggregation spec to Sean (AUDI-1056). **Resolve first:** graph-history depth vs label/backtest window (AUDI-1101 — the 60d-vs-90d contradiction) and the 1100/1056 seam with Sean.
- **What would change the answer:** Audience-size parity implausible, or a large feature block goes degenerate/null after re-grain → aggregation rules or shared-IP handling need rework before training. Graph history depth < backtest window (AUDI-1101 unresolved) → trainable window shrinks, MVP feature set narrows. If Identity can't confirm `is_shared`/`confidence_score` semantics → the ~20% out-of-graph exclusion boundary shifts.

## 1. Introduction
Part of epic **AUDI-1049** (Fangorn on MNTN ID: parallel feature store + model). As Identity rolls out
MNTN ID (household ID / ID-graph, initiative ID-327), AUDI is building a Fangorn model keyed on MNTN ID
at the household grain rather than transforming IP scores at the end. Today's Fangorn is an IP-keyed
XGBoost per vertical (~896 features, `advertiser_vertical` dominant, label = visit in a ~14-day forward
window) served as a hardcoded Dec-2025 artifact. This ticket owns the **feature-engineering** half of the
household port: re-graining the ~896-column IP feature store to MNTN ID.

Scope agreed in framing (2026-07-27): **spec + validated prototype** (Sean's AUDI-1056 productionizes the
DAG); **re-grain only** (enrichment deferred); **validation lives in AUDI-1105**. Source of truth for the
IP→household mapping is the identity graph — see `knowledge/data_catalog.md` → "Identity Graph".

## 2. The Problem
The Fangorn feature store is IP-keyed end to end (Spark aggregates per `(vertical_id, ip)`). The household
model (AUDI-1103) cannot train until the same feature signal exists at the household grain. Re-graining is
non-trivial: each feature needs a defensible per-IP→household aggregation, shared IPs (~20% don't compress
cleanly) need `confidence_score` handling, and out-of-graph households are excluded per the 15-Jul decision.
Blocks: **AUDI-1103** (train) → the whole epic's Sept-4 MVP. Depends on: **AUDI-1055** (data path, done),
Sean's **AUDI-1056** (pipeline), and the open **AUDI-1101** (mapping-history depth).

## 3. Plan of Action
Apply the Approach in §0. Empirical unknowns to resolve first (Empirical Analysis Protocol):
1. **Confirm the mapping join** — `bronze.raw.identity_graph_history`, `id_type=30`, as-of semantics
   (`as_of_date` within `start_time`/`end_time`); sample IP→household cardinality + `is_shared` rate.
2. **Resolve history depth** (AUDI-1101) — is graph history deep enough vs the ~14-day label / backtest
   window? Reconcile the RFD's 60-day retention vs the decision log's "90+ days."
3. **Inventory the ~896 features** and assign an aggregation rule per feature type (counts→sum,
   rates/scores→mean/conf-weighted, timestamps→recency/max, flags→max).
4. **Prototype the re-grained store** in BQ for a sample of verticals/advertisers.
5. **Validate** — distribution deltas + audience-size parity vs the IP store per vertical/DSCID.
6. **Hand the aggregation spec to Sean** (AUDI-1056) for DAG productionization; confirm the 1100/1056 seam.

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
