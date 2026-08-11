---
doc_type: runbook
title: "Full audit of the ecommerce whitelist + blocklist (3.31M domains)"
date: 2026-08-11
summary: "Plan to verify every domain on the ecommerce whitelist and blocklist by live-site evidence, at 3.3M scale, using tiered sampling then targeted exhaustive sweeps."
keywords: [ecommerce whitelist audit, blocklist audit, 3.3M domains, full corpus verification, false whitelist rate, agent fan-out, sampling plan, AUDI-431 followup]
domain: [audience-scoring, workflow]
---

# Full audit of the ecommerce whitelist + blocklist

**Read this cold and you should be able to start.** This is the scaled-up version of what AUDI-431
did to 2,484 domains. See the
[quarterly refresh runbook](https://github.com/mdunn-mntn/malachi-workspace/blob/main/documentation/runbooks/ecommerce_list_refresh_runbook.md)
first — it holds the mechanics, the source-code map and the hard-won lessons. This document is
only about **scale and sequencing**.

## 1. Why

The two lists are load-bearing and largely unexamined:

- **whitelist: 3,310,225 domains** — every one is asserted to be an online store, and that assertion
  short-circuits the classifier entirely. Provenance is unknown; it predates TI-200 (2025-09).
- **blocklist: 4,395 domains** — every one is asserted to never be a store, permanently discarding
  its signal.

AUDI-431 measured the error rate on the *newly proposed* blocklist rows by fetching them:
**3.06% were real stores** (76 of 2,484). Nobody has ever measured the error rate on the
**3.3M existing whitelist**. If it is even 1%, that is ~33,000 domains wrongly asserted to be
shops, feeding false ecommerce signal into DS13 verticals and therefore into MM 2.0 scoring
states (PP 8000 / HI 10000).

**The deliverable is a measured error rate per stratum, and a corrected pair of lists.**

## 2. The hard constraint

At ~25 domains per agent and ~40 fetches per agent, AUDI-431 swept 1,883 domains with 128 agents in
about two hours. **Linearly, 3.3M domains is ~225,000 agents and ~9,000 hours.** That is not a plan.

So: **never sweep the whole corpus. Stratify, measure, and only sweep strata that fail.**

## 3. Phased plan

### Phase 0 — Cheap signals over the whole corpus (no agents)
Run these first; they will resolve a large fraction with zero LLM cost.

1. **DNS liveness for all 3.31M.** Bulk-resolve (public resolver, not the local one — Pi-hole
   returns `0.0.0.0`). AUDI-431 saw **6-7% unreachable** in its samples. Dead domains on the
   *whitelist* are pure waste and can be dropped or blocklisted without judgment.
2. **Traffic join.** Left-join the lists against 28d `missing_domains` + `ddp_url_verticals` domain
   volumes. **Most of the 3.31M has zero recent traffic** — an error on a zero-traffic domain costs
   nothing today. Rank everything by observed volume; this is the single most important axis.
3. **Prod classifier score aggregates** per domain that has traffic (one BQ external scan, ~7 days,
   the AUDI-431 Query A pattern). Gives `med_score` / `pct_ge_04` / `n_urls` for free.
4. **Cheap structural flags:** platform apexes (see `PLATFORMS` in `audi_431_validate_deploy.py`),
   parse artifacts, IP literals, punycode, and the 365 known cross-list conflicts.

### Phase 1 — Stratified measurement (agents, bounded)
Define strata on **(list, traffic decile, score band)**. Sample **300 per stratum** — enough for a
Wilson 95% upper bound near 1-2% — and fetch-verify each with the AUDI-431 prompt (check `/shop`,
`/store`, shop subdomain, nav) plus an independent confirm pass on any claimed store.

Reuse `audi_431_audit_blocklist.py`, which already computes the Wilson upper bound and emits a
per-stratum sweep/no-sweep verdict at a 1% threshold.

Rough size: ~20 strata x 300 = **6,000 domains ≈ 300 agents ≈ one long run**. This is the whole
measurement, and it is affordable.

### Phase 2 — Targeted exhaustive sweeps
Sweep **only** strata whose upper bound breaches the threshold, and **only** down to the traffic
level where an error still costs something. Expected: the high-traffic whitelist strata and any
score-contradicted stratum (whitelisted but `med_score` near 0). Budget by domain count returned
from Phase 1 — likely tens of thousands, not millions.

### Phase 3 — Apply + deploy
Same gate and deploy discipline as AUDI-431: `audi_431_validate_deploy.py`, back up first, strictly
additive where possible, verify from the live object. **Whitelist removals are deletions** — unlike
AUDI-431 this audit will produce them, so it needs its own rollback plan and a staged rollout
(remove the zero-traffic dead ones first, then the evidenced errors).

## 4. Orchestration notes

- Use the **Workflow** tool with `pipeline()` (not `parallel()`), so each batch flows fetch -> confirm
  without a barrier. AUDI-431's 128-agent sweep ran this way with zero errors.
- Batch 20-25 domains per agent. Larger batches time out on fetch-heavy work.
- Arm `.claude/scripts/stall_monitor.sh` on every run; re-arm it, monitors cap at 60 min.
- The workflow journal (`journal.jsonl`) holds every agent's result — `resumeFromRunId` replays
  completed agents instantly, so a killed run is cheap to continue.
- **Model limits are a real failure mode**: one AUDI-431 run lost 10 of 28 batches to a usage cap
  and had to be resumed on a different model. Check the failure list, always resume.

## 5. Success criteria

1. A measured **false-whitelist rate** and **false-blocklist rate** with 95% bounds, per stratum.
2. An estimate, in domains and in daily URL volume, of how much bad ecommerce signal the lists inject.
3. Corrected list files deployed through the gate, with a rollback path.
4. A documented, re-runnable sampling harness so this becomes a periodic measurement, not a project.

## 6. Known traps (all hit in AUDI-431)

- Content sites with a shop attached are the dominant miss — always check shop paths.
- `pct_ge_04` near 100% is not evidence of a store at any median.
- Pi-hole makes live domains look dead; diff local vs public DNS before believing "unreachable".
- Constrain any LLM-proposed vertical to the real 152-name roster with a schema `enum`.
- One resolver function for shared state, or the deliverables silently drift.
- Do not assert the lists are disjoint — 365 domains are in both by design/accident; blocklist wins.
