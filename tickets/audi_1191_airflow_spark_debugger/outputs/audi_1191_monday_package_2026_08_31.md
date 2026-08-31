# Monday package: incident tickets, Confluence writeup, talking points (2026-08-31)

## 1. Spike ticket draft (Bryce convention: ONE spike, per-item outcome checklist)

Title: [SPIKE] Debugger gaps and OpenAI batch outage: fixes and follow-ups from 08-28..30
Description (lint target: 400ch/60w/4 bullets):

    Objective: close every gap the 08-28..30 incident window exposed.
    Task: land the debugger fixes (PR 1248, round-2 PR), document the OpenAI outage, and
    hand the pipeline hardening items to their owners.
    Done when: both PRs merged, Confluence page published, owner items handed off.

Outcome checklist (per item, in ticket body):
- [ ] PR 1248 merged (tag coverage + vertical_classification_api 68m timeout)
- [ ] Round-2 PR merged (openai signatures, fast-fail sensor RCA, watermark, clarity)
- [ ] OpenAI outage: account-side answer from reps (Alyson), affected days resubmitted
- [ ] Confluence incident page published, linked from runbook
- [ ] Handoffs: batch-runner status logging + dead-cohort alarm (shopper_graph, Matt/Sean)

## 2. Confluence writeup skeleton (space TAR, link "Relates To" AUDI-1191)

Title: OpenAI batch outage and debugger coverage gaps, Aug 28-30

- What happened: every OpenAI batch cohort submitted after Aug 28 06:00 PT was accepted then
  never ran (0/~1100 progressed per day; last healthy day Aug 26 cohort). All three DAGs in
  the MNTN Matched keyword chain went red; keyword_ddp reporting stalled from Aug 28.
- How it was found: fetch failed on missing openai_batch_results/dt=; the tracking flags in
  openai_batch_submissions/dt= showed 0/N batches reaching in_progress/completed across
  status passes 30h apart, which isolates the fault to OpenAI's side without dashboard access.
- What was tried: submissions ledger delete + resubmit of the 08-27 cohort (Matt-approved);
  the resubmitted batches died identically, proving account-level cause. Escalated to the
  OpenAI reps (Alyson, 08-30). No further resubmits until they answer.
- Deterministic recovery once the account works: delete openai_batch_submissions/dt=<D>,
  clear submit-<D> from batch_cleanup_1, wait for completion, clear fetch-<D+1> from
  batch_transition, clear keyword_ddp sensor. (Now in the runbook + batch-pipeline memory.)
- Debugger gaps found and fixed: tag filter missed 2 alerting DAGs (PR 1248); chronic 45m
  timeout on vertical_classification_api (PR 1248); fast-fail sensors got a one-layer-short
  answer (round-2 PR); scheduler pauses could skip alerts (watermark, round-2 PR); reply
  wording fixes (round-2 PR).
- Asks: batch runner should print each batch's status+error on transition (shopper_graph);
  a dead-cohort alarm (0 transitioned N hours after submit) beats discovering it a day later.

## 3. Monday talking points (60 seconds)

1. The keyword pipeline outage is OpenAI-account-side; evidence is deterministic, reps are
   engaged via Alyson, recovery is one documented sequence once they answer.
2. The debugger missed two alerts on tag filtering and answered one sensor a layer short;
   both fixed in two PRs (one merged-pending-review, one in gauntlet now).
3. Ask: merge reviews for the two airflow-ti PRs; shopper_graph owners take the two
   hardening items (status logging, dead-cohort alarm).
