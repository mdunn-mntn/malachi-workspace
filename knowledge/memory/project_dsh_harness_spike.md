---
name: project_dsh_harness_spike
description: Local-only spike (no Jira, personal) building a dsh sidecar harness + machine-gated self-improvement engine; Phases 1-6 built and gated green, verdict GO to L0/L1.
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

**How to apply:** decisions locked 2026-08-21 — Anthropic-only model routing (DeepSeek toggleable later, disabled row in lab profile only, API-billed so spend caps stay enforced); machine-gated autonomy ladder with permanent floors (no knowledge deletion, no prod, no spend over $5/day engine cap, no external posts, verifier never self-modifies); all dsh TS lives in sibling repo `/Users/malachi/Developer/work/mntn/dsh-lab/` pinned exact `0.1.1-rc.1` (never in the workspace repo); knowledge stays git markdown — no parallel memory stores (OpenViking etc. rejected). **Status 2026-08-21: Phases 1-6 all built and gated green.** `dsh-lab/` (local-only sibling repo, node@24 keg-only, pnpm) holds @mntn/dsh-bq + @mntn/dsh-kit (55 vitest), 5 profiles, the REJECT-by-default gate, behavioral + replay + integration harnesses; `engine/` holds the full loop (harvest→hypothesize→verify_gate→adopt/rollback→observe→ladder), floors enforced by a commit-msg guard. Adversarial review found + fixed 3 BLOCKERs. Verdict GO (`artifacts/ti_xxx_go_no_go.md`). **Blockers 1+2 CLEARED 2026-08-21:** live BQ assertion PASSED (dsh-bq wrote a real perf-log line; gcloud default project must be `dw-main-silver` not `mntn-coredw-prod`); egress cage INSTALLED + verified (dshagent user + pf + tinyproxy allowlist, egress_selftest 6/6). **Only remaining gate = the 10-day soak** (calendar; just use it supervised, watch for Sev-1, then flip L1). Before/after writeup: `artifacts/ti_xxx_before_after.md`; quickstart: `dsh-lab/QUICKSTART.md`. Run the engine by hand meanwhile: `python3 engine/scripts/run_engine.py --candidate <id> --llm`. Enter the harness: `dsh-lab/bin/dsh-mntn --profile mntn-analyst`. Kill switch: `dsh-lab/scripts/killswitch.sh`. Master plan + designs + go/no-go: `tickets/ti_xxx_dsh_harness_spike/artifacts/`. [[reference_dsh_harness]] [[reference_dsh_lab_runtime]]
