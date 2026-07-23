---
doc_type: reference
title: "Retrieval Eval Probes — cold-start regression suite"
status: active
date: 2026-07-23
summary: "Cold-start retrieval probes: a fresh chat using only START_HERE + _ROUTING + tickets/INDEX must reach each probe's targets. Every real cold-start miss becomes a new probe here."
keywords: [retrieval eval, regression suite, cold start, routing test, eval probes]
---

# Retrieval Eval Probes

The fitness function for the self-optimizing context system. A **cold chat** — no prior context —
may open ONLY `knowledge/START_HERE.md`, `knowledge/_ROUTING.md`, and `tickets/INDEX.md` (plus docs
those name). For each probe it must reach every target with only a handful of opens.

**How to run:** `Workflow scriptPath: claude-prompts/retrieval_eval.js` (runs every probe as an
independent cold-start agent, reports per-probe pass/gaps). **When a probe fails**, fix the named gap
in routing (add a keyword / a START_HERE task row / a cross-link), then re-run. **When a real
cold-start miss happens in normal work**, add it here as a new probe — misses become permanent
regression tests, never silently dropped.

Machine-readable block below (the `## PROBES` fenced JSON) is what the workflow parses. Keep it valid JSON.

## PROBES

```json
[
  {
    "id": "mm_pre_post",
    "question": "MM-campaign performance pre/post after a given date — where's the context, the right tables, the method, and what did we learn before?",
    "must_reach": [
      "MM-definition ticket/doc (AUDI-1083 or decisions/0001 MM component taxonomy)",
      "pre/post method (experimentation.md Standard Analysis Protocol; never naive pre/post)",
      "correct perf tables (summarydata.sum_by_campaign_by_day for long pre-periods)",
      "the agg__daily_sum_by_campaign 'only from Sep 2025 -> use sum_by_campaign_by_day' gotcha"
    ]
  },
  {
    "id": "ddp_vendor_valuation",
    "question": "How do we value a 3P data (DDP) vendor and decide keep/drop + willingness-to-pay?",
    "must_reach": [
      "the DDP valuation / WTP framework (data_vendor_valuation_framework.md or ddp_quality_score_runbook.md)",
      "a worked vendor eval ticket (AUDI-1089 children, e.g. Cybba/Klickly)",
      "the metered-bill source coredw.usage_reporting_data (never price off signal-row volume)"
    ]
  },
  {
    "id": "incrementality_experiment",
    "question": "Did a feature/rollout move visit rate — how do we design and measure it causally?",
    "must_reach": [
      "experimentation.md Standard Analysis Protocol (DiD + CausalImpact, cluster bootstrap)",
      "a canonical experiment ticket (BER-2250 children / TI-961 / TI-933 Select lift)",
      "power analysis / MDE up front (TI-884)"
    ]
  },
  {
    "id": "availability_gate",
    "question": "Why is an advertiser's audience smaller than the UI size, and what gates bidding availability?",
    "must_reach": [
      "the DS14 augmentor 7-day availability gate (size != availability)",
      "an audience-eval ticket that decomposed it (TI-1026 or AUDI-1117)",
      "HHST intent-gate mechanics (data_knowledge.md)"
    ]
  },
  {
    "id": "bidstream_features",
    "question": "Which bidstream/log features predict visits and feed the Fangorn feature store?",
    "must_reach": [
      "the feature-inventory ticket (TI-790) + bidstream epic (TI-789)",
      "SHAP ranking method with pre-visit vs feedback leakage split",
      "the source log tables (augmentor_log, win_logs, ci)"
    ]
  }
]
```
