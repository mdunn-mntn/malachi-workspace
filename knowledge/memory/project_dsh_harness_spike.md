---
name: project_dsh_harness_spike
description: Local-only spike (no Jira, personal) building a dsh sidecar harness + machine-gated self-improvement engine; v0 = Phases 1-4 green.
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [dsh spike, dsh-lab, self-improvement engine, machine-gated, autonomy ladder, engine floors, sidecar harness, ti_xxx_dsh_harness_spike, replay eval, dsh-bq]
domain: [project, workflow, infra]
lifecycle: active
last_verified: 2026-08-21
---

Started 2026-08-21. Local-only spike at `tickets/ti_xxx_dsh_harness_spike/` — **no Jira, personal; user asked artifacts to say "plugin harness", not the vendor name.**

**Why:** close the self-improvement loop (propose-only `/workflow-audit` → machine-gated auto-adoption) using dsh replay-eval primitives; Claude Code stays the daily driver, dsh is sidecar/lab only.

**How to apply:** decisions locked 2026-08-21 — Anthropic-only model routing (DeepSeek toggleable later, disabled row in lab profile only, API-billed so spend caps stay enforced); machine-gated autonomy ladder with permanent floors (no knowledge deletion, no prod, no spend over $5/day engine cap, no external posts, verifier never self-modifies); all dsh TS lives in sibling repo `/Users/malachi/Developer/work/mntn/dsh-lab/` pinned exact `0.1.1-rc.1` (never in the workspace repo); knowledge stays git markdown — no parallel memory stores (OpenViking etc. rejected). Spike closes at v0 = Phases 1-4 gates green; Phases 5-6 (engine on dsh, integration/soak) are follow-on. Master plan + designs: `tickets/ti_xxx_dsh_harness_spike/artifacts/ti_xxx_master_plan.md`. [[reference_dsh_harness]]
