---
doc_type: ticket
title: "AUDI-1167: Build shared household_resolution module (resolve_households) in utils_model"
status: backlog
date: 2026-07-28
summary: "Reusable resolve_households() util: IP→mntn_id + resolution_status via as-of graph join, shared-IP rule, fan-out guard"
result: ""
question: "Can we build a tested resolve_households(df, ip_col, date_col) util that adds mntn_id + resolution_status via an as-of graph join without silent-dropping or fan-out?"
framing_state: draft
---

# AUDI-1167: Build shared household_resolution module

**Jira:** https://mntn.atlassian.net/browse/AUDI-1167
**Parent epic:** AUDI-1049 · **Build umbrella frame:** `../audi_1134_feature_store_build/summary.md`
**Status:** backlog · **Assignee:** Malachi (unassigned in Jira — claim at planning)
> **⚠ Update 2026-07-29 (epic §7i): this is now a PLACEHOLDER.** A shared single-source-of-truth resolver is
> hard given edge cases across features/grains, so for IPv4-only v1 the resolution logic lives **inline in the
> L2 model (before L158 of `guid_log_derived_ip_vertical_id.py`)**, not in this shared util. **Sean is stubbing
> `household_resolution.py`;** promote it to the real util once the resolution logic is finalized.

---
## 0. Framing  ← run `/frame` when you start; inherits the AUDI-1134 build-frame
- **Question:** Can we build a tested `resolve_households(df, ip_col, date_col)` util that adds `mntn_id` +
  `resolution_status` (clean/shared/unresolved) via an as-of graph join — no silent-drop, no fan-out?
- **Goal:** The reusable seam every L2/L3 model calls to re-key IP→household. Blocks 1168/1169. Sept-4 MVP.
- **Objective (done-when):** `resolve_households()` in `utils_model` with same-day equi-join (daily) + interval
  join (backfill) variants, a shared-IP `confidence_score` cutoff, explicit `unresolved` tagging (never drop),
  a fan-out guard (≤1 household per (ip, date)), and unit tests.
- **Approach:** join input on the AUDI-1166 L1 mirror (or `identity_graph_history` directly for backfill via
  start/end_time × as_of_date interval). Decide the **keyset-vs-household_id** keying (Ryan Kleck) here.
- **What would change the answer:** fan-out (ip→>1 household/day) → join is wrong; too many unresolved → graph
  coverage insufficient; shared-IP cutoff materially shifts audience size → needs tuning (feeds 1100/1105).

## 1. Introduction
Component 2 of 5. The single re-keying primitive shared across the household FS. Correctness here is
load-bearing: shared IPs (`is_shared`), ~20% out-of-graph, and fan-out are the failure modes.

## 2. The Problem
Re-graining requires one correct IP→household resolution that: (a) handles shared IPs by confidence, (b) tags
unresolved rows into coverage metrics rather than dropping them, (c) never fans one IP out to multiple
households on a given date, and (d) works both same-day (daily run) and interval (historical backfill).

## 3. Plan of Action
1. `resolve_households(df, ip_col, date_col)` → `+mntn_id, resolution_status`.
2. Same-day equi-join variant (daily) + interval-join variant (backfill, start/end_time × as_of_date).
3. Shared-IP rule: `confidence_score` cutoff for `is_shared` rows (cutoff = a §6.4 decision — measure sensitivity).
4. Unresolved tagging → coverage metrics; fan-out guard assertion.
5. Unit tests (clean, shared, unresolved, fan-out cases).

## 4. Investigation & Findings
_(queries in `queries/`, results in `outputs/`)_

## 5. Solution
_(PRs, config, code)_

## 6. Questions Answered
- **Q:** — **A:** —

## 7. Data Documentation Updates
_(document the resolution semantics + shared-IP cutoff + fan-out guard)_

## 8. Open Items / Follow-ups
- Keyset-vs-household_id L2 keying decision (Ryan Kleck) lands here. Shared-IP cutoff undefined (epic §6.4).
- Collapse-function for multi-IP feature aggregation is AUDI-1100/1168, not here (this resolves identity only).
- **Meeting 2026-07-28 (see epic §7b):** resolution rule = **MAX CONFIDENCE** — a row can carry several ids
  (IPv4/IPv6/GUID/HEM) each mapping to a different household; join per-id, take highest `confidence_score`,
  one household per row (no fan-out / no visit double-count). **Must MATCH the bidder's auction-time
  resolution** (id-service `resolveHouseholdId` = max confidence; takes one id, not IPv6) or scores won't
  correspond to the household the bidder resolved — **biggest open risk, bidder has no strategy yet** (→ Jack).
- As-of join pattern against the snapshot graph: `max(as_of_date) < day` → `max(as_of_date_revision_number)`
  for partition elimination. HHID ~85% stable over 30d (~15% churn; key not semantically reassigned).
- **NEW REQUIREMENT — graph-translation-signal logging (Slack 2026-07-29, Jack Barbey/Luis):** log **every
  ID→household translation event** here and pipe to **`dw-main-silver.identity.graph_translation_signal`**
  (Weiang Li dev table; modeled on `hashed_email_signal`) for **graph-vendor crediting** — required even though
  the FS sources only internal logs (the graph contains licensed-vendor data). ID team ships a **pyspark graph
  interface** (current-graph selection + translation logging) ~end of next week; **Sean drops it into the FS
  code**; Weiang → Sean to spec the event schema. Resolution is where translation happens → this logging lands
  in the `resolve_households()` path.
- **GUID (id_type=41 `MNTN_GUID`; corrected 2026-08-11 from "42" = `GA_CLIENT_ID`)** may be a 2nd resolution
  identifier (guid_log carries guid); IPv6 moot for v1 (no IPv6 in guid_log). See epic §7c and §7j.
- **NOT superseded by the ID team's library (2026-08-11).** `mntn_graph` does translation only (returns every
  matching edge, no winner selection, no dedup, no drop). This ticket owns the consumer half. Plan of record
  (Sean): wrap the library inside `household_resolution.py` so downstream FS jobs don't change. **Known fix to
  make: the equal-confidence tiebreak takes the highest `household_id`, the bidder takes the lowest.** Full
  audit in epic §7j.
