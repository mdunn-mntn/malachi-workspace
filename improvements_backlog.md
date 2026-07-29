---
doc_type: backlog
title: Improvements Backlog
summary: "Single lightweight tracker for improvement ideas / durable fixes / tech-debt we want to remember but not put on the Jira board yet. On-call durable-fixes feed it; so can anything. Promote a row to Jira only when it's actually prioritized."
last_verified: 2026-07-28
keywords: [improvements backlog, tech debt, durable fix, follow-up, backlog, improvement ideas, deferred work, IMP-001, IMP-002, soft_fail sensor, dataproc quota, champion challenger staggering, would-be ticket, not on jira]
tags: [backlog, improvements, tech-debt]
---

# Improvements Backlog

One place for **improvements we've identified but don't want to open a Jira ticket for yet.** Keeps the
board uncluttered while making sure a good idea (a durable fix, a tooling gap, a cleanup) isn't lost.

## How to use
- **Add a row** whenever we spot an improvement worth remembering — an on-call durable-fix, a workflow gap,
  a code cleanup, a "we should really automate X." One line, then move on. Don't let it block real work.
- **Trigger for on-call:** the runbook's INC decision trees say "durable fix → spawn a ticket." Instead of
  opening Jira immediately, **log it here first**; promote only when prioritized.
- **Promote to Jira** only when a row is actually going to be worked. Set `Status: promoted:TI-XXX` and keep
  the row (don't delete — the history is useful). Then it lives on the board, not here.
- **Never delete** a `done` / `wontfix` row — flip its status. The log is the point.

**Status values:** `idea` (captured, unranked) · `proposed` (recommended, awaiting a yes) ·
`promoted:TI-XXX` (now a Jira ticket) · `done` · `wontfix: <reason>`.
**Effort:** S (≤½ day / one-liner) · M (1–3 days) · L (>3 days, likely needs its own ticket).

## Backlog

| ID | Added | Area | Improvement | Trigger / why | Effort | Owner | Status | Ref |
|---|---|---|---|---|---|---|---|---|
| IMP-001 | 2026-07-28 | airflow-ti / `ipdsc_monitor` | Set `soft_fail=_partner.optional` on the registry-driven optional-partner preconditions (and `mode="reschedule"`), so optional partners that don't drop (Bombora/DS51) SKIP instead of paging an 18h hard-fail. Mirrors `wait_{name}_src` in `tpa_ipdsc_export`. | INC-001: benign optional-partner skips page every time Bombora misses a drop; Jordan (Staff SWE) raised remove-vs-keep the sensor. Keeps QA coverage on drop days while killing the false page. | S | TPA_EXPORT (Jordan / Sean) | idea | on-call INC-001 |
| IMP-002 | 2026-07-28 | Fangorn / Dataproc | Stagger champion vs challenger inference runs (or raise the Dataproc quota / add a concurrency guard) so Fangorn-like inference pipelines don't collide at the ~94% Dataproc cap. | INC-002: a concurrent Dataproc job (even QA / a challenger) starves `create-dataproc-cluster` → code 9. Currently hand-re-triggered each time; a standing collision risk by design. | M | Fangorn/ML + infra (Brian) | idea | on-call INC-002 |
| IMP-004 | 2026-07-29 | airflow-ti / audience_intent | Add a `GCSObjectExistenceSensor` on `gs://mntn-data-archive-prod/ipdsc_geo/dt={{ds}}/_SUCCESS` before `fangorn_score_monitor` (mirror INC-001's `precondition_*` pattern), or widen its retry window, so it stops racing the `ipdsc_geo` producer. | INC-004: monitor reads `ipdsc_geo/dt=<run_date>`; producer lands on D+1 with ~3.5h-variable timing (04:56–08:17Z over 4 days); monitor's `retries=2×10min` gives only ~30-40min slack → pages on late-producer days with a `PATH_NOT_FOUND`. | S | targeting (Ryan) | idea | on-call INC-004 |
| IMP-003 | 2026-07-28 | airflow-ti / targeting-infra-ml — Fangorn Vertex | `TiVertexPipelineOperator` always injects `reference_date` into every submitted pipeline's `parameter_values`; a template whose date-param name drifts (drift pipeline's KFP source uses `run_date`) fails only at task-exec with a ValueError, not at DAG-parse. Standardize Fangorn pipeline param names on `reference_date`, or validate the operator↔template param contract at parse/compile time (fail fast / CI check). | INC-003: drift template declared `run_date` not `reference_date` → hard ValueError, PagerDuty, retries exhausted. The param mismatch is INVISIBLE at DAG-parse, so PR #1158 (airflow-ti) "fixed" it without touching the failing param and a re-run re-failed identically — the real fix is the targeting-infra-ml recompile. A contract check would have caught it in CI. | S | Fangorn/ML + infra (Brian) | idea | on-call INC-003 |
